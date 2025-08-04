// Copyright (c) 2025 RISC Zero, Inc.
//
// All rights reserved.

use crate::chain_monitor::ChainHead;
use crate::OrderRequest;
use crate::{
    chain_monitor::ChainMonitorService,
    config::{ConfigLock, OrderCommitmentPriority},
    db::DbObj,
    errors::CodedError,
    impl_coded_debug, now_timestamp,
    task::{RetryRes, RetryTask, SupervisorErr},
    utils, FulfillmentType, Order,
};
use alloy::{
    network::Ethereum,
    primitives::{
        utils::{format_ether, parse_units},
        Address, U256,
    },
    providers::{Provider, WalletProvider},
};
use anyhow::{Context, Result};
use boundless_market::contracts::{
    boundless_market::{BoundlessMarketService, MarketError},
    IBoundlessMarket::IBoundlessMarketErrors,
    RequestStatus, TxnErr,
};
use boundless_market::selector::SupportedSelectors;
use moka::{future::Cache, Expiry};
use std::sync::Arc;
use std::time::{Duration, Instant};
use thiserror::Error;
use tokio::sync::{mpsc, Mutex};
use tokio_util::sync::CancellationToken;
use crate::db::DbError;

/// Hard limit on the number of orders to concurrently kick off proving work for.
const MAX_PROVING_BATCH_SIZE: u32 = 10;

#[derive(Error)]
pub enum OrderMonitorErr {
    #[error("{code} Failed to lock order: {0}", code = self.code())]
    LockTxFailed(String),

    #[error("{code} Failed to confirm lock tx: {0}", code = self.code())]
    LockTxNotConfirmed(String),

    #[error("{code} Insufficient balance for lock", code = self.code())]
    InsufficientBalance,

    #[error("{code} Order already locked", code = self.code())]
    AlreadyLocked,

    #[error("{code} RPC error: {0:?}", code = self.code())]
    RpcErr(anyhow::Error),

    #[error("{code} Unexpected error: {0:?}", code = self.code())]
    UnexpectedError(#[from] anyhow::Error),
}

impl_coded_debug!(OrderMonitorErr);

impl CodedError for OrderMonitorErr {
    fn code(&self) -> &str {
        match self {
            OrderMonitorErr::LockTxNotConfirmed(_) => "[B-OM-006]",
            OrderMonitorErr::LockTxFailed(_) => "[B-OM-007]",
            OrderMonitorErr::AlreadyLocked => "[B-OM-009]",
            OrderMonitorErr::InsufficientBalance => "[B-OM-010]",
            OrderMonitorErr::RpcErr(_) => "[B-OM-011]",
            OrderMonitorErr::UnexpectedError(_) => "[B-OM-500]",
        }
    }
}

/// Represents the capacity for proving orders that we have available given our config.
/// Also manages vending out capacity for proving, preventing too many proofs from being
/// kicked off in each iteration.
#[derive(Debug, PartialEq)]
enum Capacity {
    /// There are orders that have been picked for proving but not fulfilled yet.
    /// Number indicates available slots.
    Available(u32),
    /// There is no concurrent lock limit.
    Unlimited,
}

impl Capacity {
    /// Returns the number of proofs we can kick off in the current iteration. Capped at
    /// [MAX_PROVING_BATCH_SIZE] to limit number of proving tasks spawned at once.
    fn request_capacity(&self, request: u32) -> u32 {
        match self {
            Capacity::Available(capacity) => {
                if request > *capacity {
                    std::cmp::min(*capacity, MAX_PROVING_BATCH_SIZE)
                } else {
                    std::cmp::min(request, MAX_PROVING_BATCH_SIZE)
                }
            }
            Capacity::Unlimited => std::cmp::min(MAX_PROVING_BATCH_SIZE, request),
        }
    }
}

struct OrderExpiry;

impl<K: std::hash::Hash + Eq, V: std::borrow::Borrow<OrderRequest>> Expiry<K, V> for OrderExpiry {
    fn expire_after_create(&self, _key: &K, value: &V, _now: Instant) -> Option<Duration> {
        let order: &OrderRequest = value.borrow();
        order.expire_timestamp.map(|t| {
            let time_until_expiry = t.saturating_sub(now_timestamp());
            Duration::from_secs(time_until_expiry)
        })
    }
}

#[derive(Default)]
struct OrderMonitorConfig {
    min_deadline: u64,
    peak_prove_khz: Option<u64>,
    max_concurrent_proofs: Option<u32>,
    additional_proof_cycles: u64,
    batch_buffer_time_secs: u64,
    order_commitment_priority: OrderCommitmentPriority,
    priority_addresses: Option<Vec<Address>>,
}

#[derive(Clone)]
pub struct RpcRetryConfig {
    pub retry_count: u64,
    pub retry_sleep_ms: u64,
}

#[derive(Clone)]
pub struct OrderMonitor<P> {
    db: DbObj,
    chain_monitor: Arc<ChainMonitorService<P>>,
    block_time: u64,
    config: ConfigLock,
    market: BoundlessMarketService<Arc<P>>,
    provider: Arc<P>,
    prover_addr: Address,
    priced_order_rx: Arc<Mutex<mpsc::Receiver<Box<OrderRequest>>>>,
    lock_and_prove_cache: Arc<Cache<String, Arc<OrderRequest>>>,
    prove_cache: Arc<Cache<String, Arc<OrderRequest>>>,
    supported_selectors: SupportedSelectors,
    rpc_retry_config: RpcRetryConfig,
}

impl<P> OrderMonitor<P>
where
    P: Provider<Ethereum> + WalletProvider,
{
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        db: DbObj,
        provider: Arc<P>,
        chain_monitor: Arc<ChainMonitorService<P>>,
        config: ConfigLock,
        block_time: u64,
        prover_addr: Address,
        market_addr: Address,
        priced_orders_rx: mpsc::Receiver<Box<OrderRequest>>,
        stake_token_decimals: u8,
        rpc_retry_config: RpcRetryConfig,
    ) -> Result<Self> {
        let txn_timeout_opt = {
            let config = config.lock_all().context("Failed to read config")?;
            config.batcher.txn_timeout
        };

        let mut market = BoundlessMarketService::new(
            market_addr,
            provider.clone(),
            provider.default_signer_address(),
        );
        if let Some(txn_timeout) = txn_timeout_opt {
            market = market.with_timeout(Duration::from_secs(txn_timeout));
        }
        {
            let config = config.lock_all()?;

            market = market.with_stake_balance_alert(
                &config
                    .market
                    .stake_balance_warn_threshold
                    .as_ref()
                    .map(|s| parse_units(s, stake_token_decimals).unwrap().into()),
                &config
                    .market
                    .stake_balance_error_threshold
                    .as_ref()
                    .map(|s| parse_units(s, stake_token_decimals).unwrap().into()),
            );
        }
        let monitor = Self {
            db,
            chain_monitor,
            block_time,
            config,
            market,
            provider,
            prover_addr,
            priced_order_rx: Arc::new(Mutex::new(priced_orders_rx)),
            lock_and_prove_cache: Arc::new(Cache::builder().expire_after(OrderExpiry).build()),
            prove_cache: Arc::new(Cache::builder().expire_after(OrderExpiry).build()),
            supported_selectors: SupportedSelectors::default(),
            rpc_retry_config,
        };
        Ok(monitor)
    }

    async fn lock_order(&self, order: &OrderRequest) -> Result<U256, OrderMonitorErr> {
        let request_id = order.request.id;

        tracing::info!("lock_order ORDER STATUS ADIMI");
        let order_status = self
            .market
            .get_status(request_id, Some(order.request.expires_at()))
            .await
            .context("Failed to get request status")?;
        if order_status != RequestStatus::Unknown {
            tracing::info!("ORDER STATUS UNKNOWN");
            tracing::info!("Request {:x} not open: {order_status:?}, skipping", request_id);
            // TODO: fetch some chain data to find out who / and for how much the order
            // was locked in at
            return Err(OrderMonitorErr::AlreadyLocked);
        }

        tracing::info!("lock_order is_locked ADIMI");
        let is_locked = self
            .db
            .is_request_locked(U256::from(order.request.id))
            .await
            .context("Failed to check if request is locked")?;
        if is_locked {
            tracing::info!("lock_order is_locked IF ADIMI");
            tracing::warn!("Request 0x{:x} already locked by me --- : {order_status:?}, skipping", request_id);
            return Err(OrderMonitorErr::AlreadyLocked);
        }
        tracing::info!("conf_priority_gas LOCKLAMADAN ONCE");
        let conf_priority_gas = {
            tracing::info!("lock_order conf_priority_gas IF ADIMI");
            let conf = self.config.lock_all().context("Failed to lock config")?;
            conf.market.lockin_priority_gas
        };

        tracing::info!(
            "Locking request: 0x{:x} for stake: {}",
            request_id,
            order.request.offer.lockStake
        );

        tracing::info!("****************** LOCKLAMA ADIMI - LOCKLAMA ADIMI - LOCKLAMA ADIMI - LOCKLAMA ADIMI - LOCKLAMA ADIMI - LOCKLAMA ADIMI ******************");
        let lock_block = self
            .market
            .lock_request(&order.request, order.client_sig.clone(), conf_priority_gas)
            .await
            .map_err(|e| -> OrderMonitorErr {
                match e {
                    MarketError::TxnError(txn_err) => match txn_err {
                        TxnErr::BoundlessMarketErr(IBoundlessMarketErrors::RequestIsLocked(_)) => {
                            OrderMonitorErr::AlreadyLocked
                        }
                        _ => OrderMonitorErr::LockTxFailed(txn_err.to_string()),
                    },
                    MarketError::RequestAlreadyLocked(_e) => OrderMonitorErr::AlreadyLocked,
                    MarketError::TxnConfirmationError(e) => {
                        OrderMonitorErr::LockTxNotConfirmed(e.to_string())
                    }
                    MarketError::LockRevert(tx_hash) => { // <-- Şimdi iki alanı da yakalıyoruz: tx_hash (B256) ve reason (String)
                        // Note: lock revert could be for any number of reasons;
                        // 1/ someone may have locked in the block before us,
                        // 2/ the lock may have expired,
                        // 3/ the request may have been fulfilled,
                        // 4/ the requestor may have withdrawn their funds
                        // Currently we don't have a way to determine the cause of the revert.
                        OrderMonitorErr::LockTxFailed(format!("Tx hash 0x{:x}", tx_hash)) // Hem hash'i hem nedeni formatla
                    }
                    MarketError::Error(e) => {
                        // Insufficient balance error is thrown both when the requestor has insufficient balance,
                        // Requestor having insufficient balance can happen and is out of our control. The prover
                        // having insufficient balance is unexpected as we should have checked for that before
                        // committing to locking the order.
                        let prover_addr_str =
                            self.prover_addr.to_string().to_lowercase().replace("0x", "");
                        if e.to_string().contains("InsufficientBalance") {
                            if e.to_string().to_lowercase().contains(&prover_addr_str) {
                                OrderMonitorErr::InsufficientBalance
                            } else {
                                OrderMonitorErr::LockTxFailed(format!(
                                    "Requestor has insufficient balance at lock time: {e}"
                                ))
                            }
                        } else if e.to_string().contains("RequestIsLocked") {
                            tracing::info!("OrderMonitorErr::AlreadyLocked ADIMI");
                            OrderMonitorErr::AlreadyLocked
                        } else {
                            OrderMonitorErr::UnexpectedError(e)
                        }
                    }
                    _ => {
                        if e.to_string().contains("RequestIsLocked") {
                            OrderMonitorErr::AlreadyLocked
                        } else {
                            OrderMonitorErr::UnexpectedError(e.into())
                        }
                    }
                }
            })?;

        // Fetch the block to retrieve the lock timestamp. This has been observed to return
        // inconsistent state between the receipt being available but the block not yet.
        let lock_timestamp = crate::futures_retry::retry(
            self.rpc_retry_config.retry_count,
            self.rpc_retry_config.retry_sleep_ms,
            || async {
                Ok(self
                    .provider
                    .get_block_by_number(lock_block.into())
                    .await
                    .with_context(|| format!("failed to get block {lock_block}"))?
                    .with_context(|| format!("failed to get block {lock_block}: block not found"))?
                    .header
                    .timestamp)
            },
            "get_block_by_number",
        )
            .await
            .map_err(OrderMonitorErr::UnexpectedError)?;

        let lock_price = order
            .request
            .offer
            .price_at(lock_timestamp)
            .context("Failed to calculate lock price")?;

        tracing::info!("lock order Ok(lock_price)");
        Ok(lock_price)
    }

    async fn get_committed_orders_and_capacity(
        &self,
        max_concurrent_proofs: Option<u32>,
        prev_orders_by_status: &mut String,
    ) -> Result<(usize, Vec<Order>), OrderMonitorErr> {
        // Eğer max concurrency sınırsızsa, burada Capacity::Unlimited dönerken
        // kapasite limiti olmadığını ifade ediyoruz,
        // bu durumda committed orders da çekip gönderebiliriz
        if max_concurrent_proofs.is_none() {
            // Commit orderları yine alabiliriz ihtiyaç varsa
            let committed_orders = self
                .db
                .get_committed_orders()
                .await
                .map_err(|e| OrderMonitorErr::UnexpectedError(e.into()))?;

            // Capacity sınırsızsa available_slots olarak 'usize::MAX' ya da istediğin bir büyük sayı verilebilir
            return Ok((usize::MAX, committed_orders));
        }

        // max_concurrent_proofs Some durumunda devam et
        let max = max_concurrent_proofs.unwrap();

        let committed_orders = self
            .db
            .get_committed_orders()
            .await
            .map_err(|e| OrderMonitorErr::UnexpectedError(e.into()))?;

        let committed_orders_count: u32 = committed_orders.len().try_into().unwrap_or(0);

        // Kapasite loglama, orijinal fonksiyondan koruyoruz
        Self::log_capacity(prev_orders_by_status, committed_orders.clone(), max).await;

        // available_slots hesapla, saturating_sub ile negatif olmaz
        let available_slots = max.saturating_sub(committed_orders_count);

        // u32 -> usize dönüşümü
        let available_slots_usize = available_slots as usize;

        Ok((available_slots_usize, committed_orders))
    }


    async fn get_proving_order_capacity(
        &self,
        max_concurrent_proofs: Option<u32>,
        prev_orders_by_status: &mut String,
    ) -> Result<Capacity, OrderMonitorErr> {
        if max_concurrent_proofs.is_none() {
            return Ok(Capacity::Unlimited);
        };

        let max = max_concurrent_proofs.unwrap();
        let committed_orders = self
            .db
            .get_committed_orders()
            .await
            .map_err(|e| OrderMonitorErr::UnexpectedError(e.into()))?;
        let committed_orders_count: u32 = committed_orders.len().try_into().unwrap();

        // `log_capacity`'nin `commited_orders` argümanının tipi `Vec<Order>` olarak bırakıldı.
        Self::log_capacity(prev_orders_by_status, committed_orders, max).await;

        let available_slots = max.saturating_sub(committed_orders_count);
        Ok(Capacity::Available(available_slots))
    }

    // `log_capacity` metodu, `commited_orders` tipini `Vec<Order>` olarak ve `order.status` erişimini `order.status()` olarak güncellendi.
    async fn log_capacity(
        prev_orders_by_status: &mut String,
        commited_orders: Vec<Order>,
        max: u32,
    ) {
        let committed_orders_count: u32 = commited_orders.len().try_into().unwrap();
        // `Order` struct'ının `status()` ve `id()` metodlarına sahip olduğu varsayılır.
        let request_id_and_status = commited_orders
            .iter()
            .map(|order| format!("[{:?}]: {order}", order.status))
            .collect::<Vec<_>>();

        let capacity_log = format!("Current num committed orders: {committed_orders_count}. Maximum commitment: {max}. Committed orders: {request_id_and_status:?}");

        // Note: we don't compare previous to capacity_log as it contains timestamps which cause it to always change.
        // We only want to log if status or num orders changes.
        // `Order` struct'ının `status()` ve `id()` metodlarına sahip olduğu varsayılır.
        let cur_orders_by_status = commited_orders
            .iter()
            .map(|order| format!("{:?}-{}", order.status, order.id()))
            .collect::<Vec<_>>()
            .join(",");
        if *prev_orders_by_status != cur_orders_by_status {
            tracing::info!("{}", capacity_log);
            *prev_orders_by_status = cur_orders_by_status;
        }
    }

    /// Helper method to skip an order in the database and invalidate the appropriate cache
    async fn skip_order(&self, order: &OrderRequest, reason: &str) {
        if let Err(e) = self.db.insert_skipped_request(order).await {
            tracing::error!("Failed to skip order ({}): {} - {e:?}", reason, order.id());
        }

        match order.fulfillment_type {
            FulfillmentType::LockAndFulfill => {
                self.lock_and_prove_cache.invalidate(&order.id()).await;
            }
            FulfillmentType::FulfillAfterLockExpire | FulfillmentType::FulfillWithoutLocking => {
                self.prove_cache.invalidate(&order.id()).await;
            }
        }
    }

    async fn get_valid_orders(
        &self,
        current_block_timestamp: u64,
        min_deadline: u64,
    ) -> Result<Vec<Arc<OrderRequest>>> {
        let mut candidate_orders: Vec<Arc<OrderRequest>> = Vec::new();

        // tracing::info!("is_within_deadline ADIMI");
        fn is_within_deadline(
            order: &OrderRequest,
            current_block_timestamp: u64,
            min_deadline: u64,
        ) -> bool {
            if order.request.expires_at() < current_block_timestamp {
                tracing::info!("is_within_deadline if ADIMI");
                tracing::info!("Request {:x} has now expired. Skipping.", order.request.id);
                false
            } else if order.request.expires_at().saturating_sub(now_timestamp()) < min_deadline {
                tracing::info!("is_within_deadline else if ADIMI");
                tracing::info!("Request {:x} deadline at {} is less than the minimum deadline {} seconds required to prove an order. Skipping.", order.request.id, order.request.expires_at(), min_deadline);
                false
            } else {
                // tracing::info!("is_within_deadline else ADIMI");
                true
            }
        }

        tracing::info!("is_target_time_reached ADIMI");


        tracing::info!("prove_cache iter ADIMI");
        for (_, order) in self.prove_cache.iter() {
            // tracing::info!("is_fulfilled iter ADIMI");
            let is_fulfilled = self
                .db
                .is_request_fulfilled(U256::from(order.request.id))
                .await
                .context("Failed to check if request is fulfilled")?;
            if is_fulfilled {
                tracing::info!("is_fulfilled iter if ADIMI");
                tracing::info!(
                    "Request 0x{:x} was locked by another prover and was fulfilled. Skipping.",
                    order.request.id
                );
                self.skip_order(&order, "was fulfilled by other").await;
            } else if !is_within_deadline(&order, current_block_timestamp, min_deadline) {
                tracing::info!("expired iter else if ADIMI");
                self.skip_order(&order, "expired").await;
            }
            // else if true {
            //     tracing::info!("is_target_time_reached iter ADIMI");
            //     tracing::info!("Request 0x{:x} was locked by another prover but expired unfulfilled, setting status to pending proving", order.request.id);
            //     candidate_orders.push(order);
            // }
        }

        tracing::info!("lock_and_prove_cache ADIMI");
        tracing::info!("self.lock_and_prove_cache.iter().count() : {} ", self.lock_and_prove_cache.iter().count());

        for (_, order) in self.lock_and_prove_cache.iter() {
            let is_lock_expired = order.request.lock_expires_at() < current_block_timestamp;
            if is_lock_expired {
                tracing::info!("is_lock_expired if ADIMI");
                tracing::debug!("Request {:x} was scheduled to be locked by us, but its lock has now expired. Skipping.", order.request.id);
                self.skip_order(&order, "lock expired before we locked").await;
            } else if let Some((locker, _)) = self.db.get_request_locked(U256::from(order.request.id)).await?
            {
                tracing::info!("self.db.get_request_locked 0x{} : ", order.request.id);
                tracing::info!("is_lock_expired else if ADIMI");
                let our_address = self.provider.default_signer_address().to_string().to_lowercase();
                let locker_address = locker.to_lowercase();
                // Compare normalized addresses (lowercase without 0x prefix)
                let our_address_normalized = our_address.trim_start_matches("0x");
                let locker_address_normalized = locker_address.trim_start_matches("0x");

                tracing::info!("locker_address_normalized ADIMI");
                if locker_address_normalized != our_address_normalized {
                    tracing::info!("locker_address_normalized if ADIMI");
                    tracing::debug!("Request 0x{:x} was scheduled to be locked by us ({}), but is already locked by another prover ({}). Skipping.", order.request.id, our_address, locker_address);
                    self.skip_order(&order, "locked by another prover").await;
                } else {
                    tracing::info!("locker_address_normalized else ADIMI");
                    // Edge case where we locked the order, but due to some reason was not moved to proving state. Should not happen.
                    tracing::debug!("Request 0x{:x} was scheduled to be locked by us, but is already locked by us. Proceeding to prove.", order.request.id);
                    candidate_orders.push(order);
                }
            } else if !is_within_deadline(&order, current_block_timestamp, min_deadline) {
                tracing::info!("!is_within_deadline else if ADIMI");
                self.skip_order(&order, "insufficient deadline").await;
            } else if true {
                tracing::info!("is_target_time_reached else if ADIMI - candidate_orders.push(order) : 0x{}", order.request.id);
                candidate_orders.push(order);
            }
        }

        tracing::info!("candidate_orders ADIMI");
        if candidate_orders.is_empty() {
            tracing::info!("candidate_orders.is_empty() ADIMI");
            tracing::info!(
                "No orders to lock and/or prove as of block timestamp {}",
                current_block_timestamp
            );
            return Ok(Vec::new());
        }


        tracing::info!(
            "Valid orders that reached target timestamp; ready for locking/proving, num: {}, ids: {}",
            candidate_orders.len(),
            candidate_orders.iter().map(|order| order.id()).collect::<Vec<_>>().join(", ")
        );

        tracing::info!("Ok(candidate_orders) ADIMI");
        Ok(candidate_orders)
    }

    async fn lock_and_prove_orders(&self, orders: &[Arc<OrderRequest>]) -> Result<()> {
        tracing::info!("lock_and_prove_orders ADIMI");
        let lock_jobs = orders.iter().map(|order| {
            async move {
                let order_id = order.id();
                if order.fulfillment_type == FulfillmentType::LockAndFulfill {
                    let request_id = order.request.id;
                    match self.lock_order(order).await {
                        Ok(lock_price) => {
                            tracing::info!("Locked request: 0x{:x}", request_id);
                            if let Err(err) = self.db.insert_accepted_request(order, lock_price).await {
                                tracing::error!(
                                    "FATAL STAKE AT RISK: {} failed to move from locking -> proving status {}",
                                    order_id,
                                    err
                                );
                            }
                        }
                        Err(ref err) => {
                            match err {
                                OrderMonitorErr::UnexpectedError(inner) => {
                                    tracing::error!(
                                        "Failed to lock order: {order_id} - {} - {inner:?}",
                                        err.code()
                                    );
                                }
                                _ => {
                                    tracing::warn!(
                                        "Soft failed to lock request: {order_id} - {} - {err:?}",
                                        err.code()
                                    );
                                }
                            }
                            if let Err(err) = self.db.insert_skipped_request(order).await {
                                tracing::error!(
                                    "Failed to set DB failure state for order: {order_id} - {err:?}"
                                );
                            }
                        }
                    }
                    self.lock_and_prove_cache.invalidate(&order_id).await;
                } else {
                    if let Err(err) = self.db.insert_accepted_request(order, U256::ZERO).await {
                        tracing::error!(
                            "Failed to set order status to pending proving: {} - {err:?}",
                            order_id
                        );
                    }
                    self.prove_cache.invalidate(&order_id).await;
                }
            }
        });

        futures::future::join_all(lock_jobs).await;

        Ok(())
    }

    /// Calculate the gas units needed for an order and the corresponding cost in wei
    async fn calculate_order_gas_cost_wei(
        &self,
        _order: &OrderRequest,
        _gas_price: u128,
    ) -> Result<U256, OrderMonitorErr> {
        // Gaz hesaplamasını tamamen atlıyoruz
        Ok(U256::ZERO)
    }

    async fn apply_capacity_limits(
        &self,
        orders: Vec<Arc<OrderRequest>>,
        config: &OrderMonitorConfig,
        prev_orders_by_status: &mut String,
    ) -> Result<Vec<Arc<OrderRequest>>> {
        let num_orders = orders.len();
        // Get our current capacity for proving orders given our config and the number of orders that are currently committed to be proven + fulfilled.

        let (capacity_granted, committed_orders) = self
            .get_committed_orders_and_capacity(config.max_concurrent_proofs, prev_orders_by_status)
            .await?;

        let mut final_orders: Vec<Arc<OrderRequest>> = Vec::with_capacity(capacity_granted);

        let gas_price: u128 = 10_012_545;  // Manuel gaz fiyatı

        // Eğer available_balance_wei yine node'dan alınacaksa onu asıl kodda bırakabilirsin

        let val: u64 = 100_000_000_000_000_000;
        let available_balance_wei = U256::from(val);


        // Calculate gas units required for committed orders


        let committed_cost_wei = U256::from(100_000_000_000_000u64); // 0.0001 ETH


        tracing::info!("---------- gas_price {} : ", gas_price);
        tracing::info!("---------- available_balance_wei {} : ", available_balance_wei);
        // tracing::info!("---------- committed_gas_units {} : ", committed_gas_units);
        tracing::info!("---------- committed_cost_wei {} : ", committed_cost_wei);

        // Log committed order gas requirements
        if !committed_orders.is_empty() {
            tracing::debug!(
                "Cost for {} committed orders: {} ether",
                committed_orders.len(),
                format_ether(committed_cost_wei),
            );
        }

        // Ensure we have enough for committed orders
        if committed_cost_wei > available_balance_wei {
            tracing::error!(
                "Insufficient balance for committed orders. Required: {} ether, Available: {} ether",
                format_ether(committed_cost_wei),
                format_ether(available_balance_wei)
            );
            return Ok(Vec::new());
        }

        // Calculate remaining balance after accounting for committed orders
        let mut remaining_balance_wei = available_balance_wei - committed_cost_wei;

        // Apply peak khz limit if specified
        let num_commited_orders = committed_orders.len();
        if config.peak_prove_khz.is_some() && !orders.is_empty() {
            let peak_prove_khz = config.peak_prove_khz.unwrap();
            let total_commited_cycles = committed_orders
                .iter()
                .map(|order| order.total_cycles.unwrap() + config.additional_proof_cycles)
                .sum::<u64>();

            let now = now_timestamp();
            // Estimate the time the prover will be available given our current committed orders.
            let started_proving_at = committed_orders
                .iter()
                .map(|order| order.proving_started_at.unwrap())
                .min()
                .unwrap_or(now);

            let proof_time_seconds = total_commited_cycles.div_ceil(1_000).div_ceil(peak_prove_khz);
            let mut prover_available_at = started_proving_at + proof_time_seconds;
            if prover_available_at < now {
                let seconds_behind = now - prover_available_at;
                tracing::info!("Proofs are behind what is estimated from peak_prove_khz config by {} seconds. Consider lowering this value to avoid overlocking orders.", seconds_behind);
                prover_available_at = now;
            }
            tracing::info!("Already committed to {} orders, with a total cycle count of {}, a peak khz limit of {}, started working on them at {}, we estimate the prover will be available in {} seconds",
                num_commited_orders,
                total_commited_cycles,
                peak_prove_khz,
                started_proving_at,
                prover_available_at.saturating_sub(now),
            );

            // For each order in consideration, check if it can be completed before its expiration
            // and that there is enough gas to pay for the lock and fulfillment of all orders
            // including the committed orders.

            for order in orders {
                if final_orders.len() >= capacity_granted {

                    break;
                }
                // Calculate gas and cost for this order using our helper method
                let order_cost_wei = self.calculate_order_gas_cost_wei(&order, gas_price).await?;

                // Skip if not enough balance
                // if order_cost_wei > remaining_balance_wei {
                //     tracing::info!(
                //         "Insufficient balance for order {}. Required: {} ether, Remaining: {} ether",
                //         order.id(),
                //         format_ether(order_cost_wei),
                //         format_ether(remaining_balance_wei)
                //     );
                //     self.skip_order(&order, "insufficient balance").await;
                //     continue;
                // }

                let order_cycles = order.total_cycles.unwrap();

                // Calculate total cycles including application proof, assessor, and set builder estimates
                let total_cycles = order_cycles + config.additional_proof_cycles;

                let proof_time_seconds = total_cycles.div_ceil(1_000).div_ceil(peak_prove_khz);
                let completion_time = prover_available_at + proof_time_seconds;
                let expiration = match order.fulfillment_type {
                    FulfillmentType::LockAndFulfill => order.request.lock_expires_at(),
                    FulfillmentType::FulfillAfterLockExpire => order.request.expires_at(),
                    _ => panic!("Unsupported fulfillment type: {:?}", order.fulfillment_type),
                };

                tracing::info!("order.request.id {} : ", order.request.id);
                tracing::info!("num_commited_orders {} : ", num_commited_orders);
                tracing::info!("total_cycles {} : ", total_cycles);
                tracing::info!("prover_available_at {} : ", prover_available_at);
                tracing::info!("prover_available_at.saturating_sub(now) {} : ", prover_available_at.saturating_sub(now));
                tracing::info!("proof_time_seconds {} : ", proof_time_seconds);
                tracing::info!("proof_time_seconds {} : ", proof_time_seconds);
                tracing::info!("expiration {} : ", expiration);
                tracing::info!("completion_time {} : ", completion_time);
                tracing::info!("config.batch_buffer_time_secs {} : ", config.batch_buffer_time_secs);

                let max = config.max_concurrent_proofs.unwrap();

                let committed_orders_count: u32 = committed_orders.len().try_into().unwrap();
                // `Order` struct'ının `status()` ve `id()` metodlarına sahip olduğu varsayılır.
                let request_id_and_status = committed_orders
                    .iter()
                    .map(|order| format!("[{:?}]: {order}", order.status))
                    .collect::<Vec<_>>();

                let capacity_log = format!("------> Apply capacity - Current num committed orders: {committed_orders_count}. Maximum commitment: {max}. Committed orders: {request_id_and_status:?}");
                tracing::info!("{}", capacity_log);


                // if completion_time + config.batch_buffer_time_secs > expiration {
                if completion_time > expiration {
                    // If the order cannot be completed before its expiration, skip it permanently.
                    // Otherwise, we keep the order for the next iteration as capacity may free up in the future.

                    if now + proof_time_seconds > expiration {
                        tracing::info!("Order 0x{:x} cannot be completed before its expiration at {}, proof estimated to take {} seconds and complete at {}. Skipping",
                            order.request.id,
                            expiration,
                            proof_time_seconds,
                            completion_time
                        );
                        // If the order cannot be completed regardless of other orders, skip it
                        // permanently. Otherwise, will retry including the order.
                        self.skip_order(&order, "cannot be completed before expiration").await;
                    } else {
                        tracing::debug!("Given current commited orders and capacity, order 0x{:x} cannot be completed before its expiration. Not skipping as capacity may free up before it expires.", order.request.id);
                    }
                    continue;
                }

                tracing::debug!("Order {} estimated to take {} seconds (including assessor + set builder), and would be completed at {} ({} seconds from now). It expires at {} ({} seconds from now)", order.id(), proof_time_seconds, completion_time, completion_time.saturating_sub(now_timestamp()), expiration, expiration.saturating_sub(now_timestamp()));

                final_orders.push(order);
                prover_available_at = completion_time;
                remaining_balance_wei -= order_cost_wei;
            }
        } else {
            tracing::info!("peak khz yok ve order da gönderilmemis lollllll ---------");
            // If no peak khz limit, just check gas for each order
            for order in orders {
                if final_orders.len() >= capacity_granted {
                    break;
                }
                let order_cost_wei = self.calculate_order_gas_cost_wei(&order, gas_price).await?;

                // Skip if not enough balance
                if order_cost_wei > remaining_balance_wei {
                    tracing::info!(
                        "Insufficient balance for order {}. Required: {} ether, Remaining: {} ether",
                        order.id(),
                        format_ether(order_cost_wei),
                        format_ether(remaining_balance_wei)
                    );
                    self.skip_order(&order, "insufficient balance").await;
                    continue;
                }

                tracing::info!("apply_capacity_limit - final_orders.push(order) {} : ", order.request.id);
                final_orders.push(order);
                remaining_balance_wei -= order_cost_wei;
            }
        }

        tracing::info!(
            "Started with {} orders ready to be locked and/or proven. Already commited to {} orders. After applying capacity limits of {} max concurrent proofs and {} peak khz, filtered to {} orders: {:?}",
            num_orders,
            num_commited_orders,
            if let Some(max_concurrent_proofs) = config.max_concurrent_proofs {
                max_concurrent_proofs.to_string()
            } else {
                "unlimited".to_string()
            },
            if let Some(peak_prove_khz) = config.peak_prove_khz {
                peak_prove_khz.to_string()
            } else {
                "unlimited".to_string()
            },
            final_orders.len(),
            final_orders.iter().map(|order| order.id()).collect::<Vec<_>>()
        );

        tracing::info!("f if final_orders.len() {} :",final_orders.len());
        Ok(final_orders)
    }

    pub async fn start_monitor(
        self,
        cancel_token: CancellationToken,
    ) -> Result<(), OrderMonitorErr> {
        let mut last_processed_block = 0u64;
        let mut first_block = 0;

        let mut interval = tokio::time::interval_at(
            tokio::time::Instant::now(),
            tokio::time::Duration::from_millis(20),
        );
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        let mut new_orders = self.priced_order_rx.lock().await;
        let mut prev_orders_by_status = String::new();

        let mut cached_config = {
            let config = self.config.lock_all().context("Failed to read config")?;
            OrderMonitorConfig {
                min_deadline: config.market.min_deadline,
                peak_prove_khz: config.market.peak_prove_khz,
                max_concurrent_proofs: config.market.max_concurrent_proofs,
                additional_proof_cycles: config.market.additional_proof_cycles,
                batch_buffer_time_secs: config.batcher.block_deadline_buffer_secs,
                order_commitment_priority: config.market.order_commitment_priority,
                priority_addresses: config.market.priority_requestor_addresses.clone(),
            }
        };

        let mut pending_new_orders = false;
        let mut tick_count = 0u64;
        let mut last_metrics_log = std::time::Instant::now();

        loop {
            tokio::select! {
            biased;

            Some(result) = new_orders.recv() => {
                self.handle_new_order_result(result).await?;
                pending_new_orders = true;
            }

            _ = interval.tick() => {

                tick_count += 1;
                // tracing::info!("tick_count {} :",tick_count);
                // Zincir sorgusunu ya 3 tickte bir ya da yeni sipariş geldiğinde yap
                let should_check_chain = tick_count % 3 == 0 || pending_new_orders;

                if !should_check_chain {
                    // tracing::info!("!should_check_chain");
                    continue;
                }

                let current_chain_head = match self.chain_monitor.current_chain_head().await {
                    Ok(head) => head,
                    Err(_) => continue, // Zincir hatası olursa atla
                };

                let current_block = current_chain_head.block_number;
                let should_process = current_block > last_processed_block || pending_new_orders;

                if !should_process {
                    // tracing::info!("!should_process");
                    continue;
                }

                let ChainHead { block_number, block_timestamp } = current_chain_head;

                if first_block == 0 {
                    first_block = block_number;
                }

                let valid_orders = match self.get_valid_orders(block_timestamp, cached_config.min_deadline).await {
                    Ok(orders) => orders,
                    Err(_) => {
                        pending_new_orders = false; // Hata olsa da sıfırla
                        continue;
                    },
                };

                if valid_orders.is_empty() {
                    pending_new_orders = false; // Sıfırla, çünkü işlenecek yok
                    continue;
                }

                // tracing::info!("f if final_orders.len() {} :",valid_orders.len());

                // let valid_orders = self.prioritize_orders(
                //     valid_orders,
                //     cached_config.order_commitment_priority,
                //     cached_config.priority_addresses.as_deref()
                // );

                let final_orders = match self.apply_capacity_limits(
                    valid_orders,
                    &cached_config,
                    &mut prev_orders_by_status,
                ).await {
                    Ok(orders) => orders,
                    Err(_) => {
                        pending_new_orders = false; // Hata olsa da sıfırla
                        continue;
                    },
                };

                // tracing::info!("f if final_orders.len() {} :",final_orders.len());

                if final_orders.is_empty() {
                    // Kapasite uygulandı ama order yok, sıfırla flag'ı
                    pending_new_orders = false;
                    continue;
                }

                last_processed_block = current_block;

                // Lock and prove işlemi; hata durumunda bile flag sıfırlanmalı.
                let lock_result = self.lock_and_prove_orders(&final_orders).await;

                pending_new_orders = false;  // Her durumda sıfırla

                // Eğer istersen hata logu ekle:
                if let Err(e) = lock_result {
                    // tracing::error!("Lock and prove failed: {:?}", e);
                    // silent handling olarak bırakabilirsin
                }

                // Periyodik log
                if tick_count % 3000 == 0 {
                    let now = std::time::Instant::now();
                    if now.duration_since(last_metrics_log).as_secs() >= 60 {
                        tracing::info!("Processed {} orders at block {}", final_orders.len(), block_number);
                        last_metrics_log = now;
                    }
                }

                // Config güncellemesi
                if tick_count % 1500 == 0 {
                    if let Ok(config) = self.config.lock_all() {
                        cached_config = OrderMonitorConfig {
                            min_deadline: config.market.min_deadline,
                            peak_prove_khz: config.market.peak_prove_khz,
                            max_concurrent_proofs: config.market.max_concurrent_proofs,
                            additional_proof_cycles: config.market.additional_proof_cycles,
                            batch_buffer_time_secs: config.batcher.block_deadline_buffer_secs,
                            order_commitment_priority: config.market.order_commitment_priority,
                            priority_addresses: config.market.priority_requestor_addresses.clone(),
                        };
                    }
                }
            }

            _ = cancel_token.cancelled() => {
                tracing::info!("Order monitor cancelled");
                break;
            }
        }
        }
        Ok(())
    }


    // Called when a new order result is received from the channel
    async fn handle_new_order_result(
        &self,
        order: Box<OrderRequest>,
    ) -> Result<(), OrderMonitorErr> {
        match order.fulfillment_type {
            FulfillmentType::LockAndFulfill => {
                // Note: this could be done without waiting for the batch to minimize latency, but
                //       avoiding more complicated logic for checking capacity for each order.

                // If not, add it to the cache to be locked after target time
                tracing::trace!("handle_new_order_result lock_and_prove_cache INSERET ADIMI");
                tracing::trace!("handle_new_order_result lock_and_prove_cache INSERET ADIMI");
                self.lock_and_prove_cache.insert(order.id(), Arc::from(order)).await;
            }
            FulfillmentType::FulfillAfterLockExpire | FulfillmentType::FulfillWithoutLocking => {

                tracing::trace!("handle_new_order_result prove_cache INSERET ADIMI");
                tracing::trace!("handle_new_order_result prove_cache INSERET ADIMI");

                self.prove_cache.insert(order.id(), Arc::from(order)).await;
            }
        }
        Ok(())
    }
}

impl<P> RetryTask for OrderMonitor<P>
where
    P: Provider<Ethereum> + WalletProvider + 'static + Clone,
{
    type Error = OrderMonitorErr;
    // fn spawn(&self, cancel_token: CancellationToken) -> RetryRes<Self::Error> {
    //     let monitor_clone = self.clone();
    //     Box::pin(async move {
    //         tracing::info!("Starting order monitor");
    //         monitor_clone.start_monitor(cancel_token).await.map_err(SupervisorErr::Recover)?;
    //         Ok(())
    //     })
    // }
    fn spawn(&self, _cancel_token: CancellationToken) -> RetryRes<Self::Error> {
        // Do nothing, hemen başarılı dön
        Box::pin(async { Ok(()) })
    }

}