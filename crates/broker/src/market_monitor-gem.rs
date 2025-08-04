// Copyright 2025 RISC Zero, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Instant;
use crate::order_monitor::OrderMonitorErr;
use chrono::{DateTime, Utc};
use alloy::{
    network::Ethereum,
    primitives::{Address, U256},
    providers::Provider,
    rpc::types::Filter,
    sol,
    sol_types::SolEvent,
};

use alloy::primitives::{B256, Bytes};
// ÖNEMLİ DÜZELTME: Doğru `Transaction` tipini import et
use alloy::rpc::types::Transaction;
use alloy::consensus::Transaction as _;
use alloy::eips::BlockNumberOrTag;
// Mevcut kodunuzun üzerine ekleyeceğiniz fonksiyonlar
use alloy::sol_types::SolCall;
// SELECTOR için gerekli trait

use anyhow::{Context, Result};
use boundless_market::{
    contracts::{
        boundless_market::BoundlessMarketService, IBoundlessMarket, RequestId, RequestStatus,
    },
    order_stream_client::OrderStreamClient,
};
use futures_util::StreamExt;
use tokio::sync::{broadcast, mpsc};
use tokio_util::sync::CancellationToken;
use alloy::rpc::types::{BlockTransactions};
use crate::{chain_monitor::ChainMonitorService, db::{DbError, DbObj}, errors::{impl_coded_debug, CodedError}, task::{RetryRes, RetryTask, SupervisorErr}, FulfillmentType, OrderRequest, OrderStateChange, storage::{upload_image_uri, upload_input_uri}, now_timestamp};
use thiserror::Error;
use crate::config::ConfigLock;
use crate::provers::ProverObj;

const BLOCK_TIME_SAMPLE_SIZE: u64 = 10;

#[derive(Error)]
pub enum MarketMonitorErr {
    #[error("{code} Event polling failed: {0:?}", code = self.code())]
    EventPollingErr(anyhow::Error),

    #[error("{code} Log processing failed: {0:?}", code = self.code())]
    LogProcessingFailed(anyhow::Error),

    #[error("{code} Unexpected error: {0:?}", code = self.code())]
    UnexpectedErr(#[from] anyhow::Error),

    #[error("{code} Receiver dropped", code = self.code())]
    ReceiverDropped,
}

impl CodedError for MarketMonitorErr {
    fn code(&self) -> &str {
        match self {
            MarketMonitorErr::EventPollingErr(_) => "[B-MM-501]",
            MarketMonitorErr::LogProcessingFailed(_) => "[B-MM-502]",
            MarketMonitorErr::UnexpectedErr(_) => "[B-MM-500]",
            MarketMonitorErr::ReceiverDropped => "[B-MM-502]",
        }
    }
}

impl_coded_debug!(MarketMonitorErr);

pub struct MarketMonitor<P> {
    lookback_blocks: u64,
    market_addr: Address,
    provider: Arc<P>,
    db: DbObj,
    chain_monitor: Arc<ChainMonitorService<P>>,
    prover_addr: Address,
    order_stream: Option<OrderStreamClient>,
    new_order_tx: mpsc::Sender<Box<OrderRequest>>,
    order_state_tx: broadcast::Sender<OrderStateChange>,
    prover: ProverObj,
    config: ConfigLock,
}

sol! {
    #[sol(rpc)]
    interface IERC1271 {
        function isValidSignature(bytes32 hash, bytes memory signature) external view returns (bytes4 magicValue);
    }
}

const ERC1271_MAGIC_VALUE: [u8; 4] = [0x16, 0x26, 0xba, 0x7e];

impl<P> MarketMonitor<P>
where
    P: Provider<Ethereum> + 'static + Clone,
{
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        lookback_blocks: u64,
        market_addr: Address,
        provider: Arc<P>,
        db: DbObj,
        chain_monitor: Arc<ChainMonitorService<P>>,
        prover_addr: Address,
        order_stream: Option<OrderStreamClient>,
        new_order_tx: mpsc::Sender<Box<OrderRequest>>,
        order_state_tx: broadcast::Sender<OrderStateChange>,
        prover: ProverObj,
        config: ConfigLock,
    ) -> Self {
        Self {
            lookback_blocks,
            market_addr,
            provider,
            db,
            chain_monitor,
            prover_addr,
            order_stream,
            new_order_tx,
            order_state_tx,
            prover,
            config
        }
    }






    async fn monitor_mempool(
        market_addr: Address,
        provider: Arc<P>,
        new_order_tx: mpsc::Sender<Box<OrderRequest>>,
        cancel_token: CancellationToken,
        prover_addr: Address,
        db_obj: DbObj,
        prover: ProverObj,
        config: ConfigLock
    ) -> Result<(), MarketMonitorErr>
    where
        P: Provider<Ethereum> + 'static + Clone,
    {
        tracing::info!("🔍 Mempool monitoring not supported with current provider - using polling method");

        let mut interval = tokio::time::interval(tokio::time::Duration::from_millis(100));

        // İşlenmiş transaction hash'lerini takip etmek için
        let mut processed_txs: HashSet<B256> = HashSet::new();

        loop {
            tokio::select! {
        _ = interval.tick() => {
            match provider.get_block_by_number(BlockNumberOrTag::Pending).await {
                Ok(Some(block)) => {
                    if let BlockTransactions::Hashes(tx_hashes) = block.transactions {
                        for tx_hash in tx_hashes {
                            if processed_txs.contains(&tx_hash) {
                                continue;
                            }

                            processed_txs.insert(tx_hash);

                            if let Err(e) = Self::process_mempool_tx(
                                tx_hash,
                                provider.clone(),
                                market_addr,
                            ).await {
                                tracing::error!("Mempool işlemi işlenirken hata oluştu: {:?}", e);
                            }
                        }
                    }
                }
                Ok(None) => {
                    tracing::trace!("Pending block henüz mevcut değil.");
                }
                Err(e) => {
                    tracing::error!("Pending block alınırken hata oluştu: {}", e);
                }
            }

            if processed_txs.len() > 10000 {
                let to_keep: Vec<_> = processed_txs.iter().skip(5000).cloned().collect();
                processed_txs = to_keep.into_iter().collect();
            }
        }
        _ = cancel_token.cancelled() => {
            tracing::info!("Mempool monitoring iptal edildi");
            return Ok(());
        }
    }
        }
    }



















    async fn monitor_orders(
        market_addr: Address,
        provider: Arc<P>,
        new_order_tx: mpsc::Sender<Box<OrderRequest>>,
        cancel_token: CancellationToken,
        prover_addr: Address,
        db_obj: DbObj,
        prover: ProverObj,
        config: ConfigLock
    ) -> Result<(), MarketMonitorErr> {
        let chain_id = provider.get_chain_id().await.context("Failed to get chain id")?;

        let market = BoundlessMarketService::new(market_addr, provider.clone(), Address::ZERO);
        // TODO: RPC providers can drop filters over time or flush them
        // we should try and move this to a subscription filter if we have issue with the RPC
        // dropping filters

        let event = market
            .instance()
            .RequestSubmitted_filter()
            .watch()
            .await
            .context("Failed to subscribe to RequestSubmitted event")?;
        tracing::info!("Subscribed to RequestSubmitted event");

        let mut stream = event.into_stream();
        loop {
            tokio::select! {

                log_res = stream.next() => {

                    match log_res {
                        Some(Ok((event, _))) => {
                            let provider = provider.clone();
                            let new_order_tx = new_order_tx.clone();
                            let prover_addr = prover_addr.clone();
                            let db_obj = db_obj.clone();
                            let prover = prover.clone();
                            let config = config.clone();
                            tokio::spawn(async move {
                                if let Err(err) = Self::process_event(
                                    event,
                                    provider,
                                    market_addr,
                                    chain_id,
                                    &new_order_tx,
                                    prover_addr,
                                    db_obj,
                                    prover,
                                    config
                                )
                                .await
                                {
                                    tracing::error!("Failed to process event log: {:?}", err);
                                }
                            });
                        }
                        Some(Err(err)) => {
                            let event_err = MarketMonitorErr::EventPollingErr(anyhow::anyhow!(err));
                            tracing::warn!("Failed to fetch event log: {event_err:?}");
                        }
                        None => {
                            return Err(MarketMonitorErr::EventPollingErr(anyhow::anyhow!(
                                "Event polling exited, polling failed (possible RPC error)"
                            )));
                        }
                    }
                }
                _ = cancel_token.cancelled() => {
                    return Ok(());
                }
            }
        }
    }

    async fn process_event(
        event: IBoundlessMarket::RequestSubmitted,
        provider: Arc<P>,
        market_addr: Address,
        chain_id: u64,
        new_order_tx: &mpsc::Sender<Box<OrderRequest>>,
        prover_addr : Address,
        db_obj: DbObj,
        prover: ProverObj,
        config: ConfigLock
    ) -> Result<()> {

        let task_id = uuid::Uuid::new_v4();
        let start = Instant::now();

        // tracing::info!("🚀 TASK-{}: Started processing request 0x{:x} at {:?}",
        //            task_id, event.requestId, start);

        // tracing::info!("🥳 DETECED NEW ORDER ➡➡➡➡ 0x{:x} at {:?}", event.requestId, start);
        // Check the request id flag to determine if the request is smart contract signed. If so we verify the
        // ERC1271 signature by calling isValidSignature on the smart contract client. Otherwise we verify the
        // the signature as an ECDSA signature.
        let request_id = RequestId::from_lossy(event.requestId);
        if request_id.smart_contract_signed {
            let erc1271 = IERC1271::new(request_id.addr, provider.clone());
            let request_hash = event.request.signing_hash(market_addr, chain_id)?;
            tracing::debug!(
                "Validating ERC1271 signature for request 0x{:x}, calling contract: {} with hash {:x}",
                event.requestId,
                request_id.addr,
                request_hash
            );
            match erc1271.isValidSignature(request_hash, event.clientSignature.clone()).call().await
            {
                Ok(magic_value) => {
                    if magic_value != ERC1271_MAGIC_VALUE {
                        tracing::warn!("Invalid ERC1271 signature for request 0x{:x}, contract: {} returned magic value: 0x{:x}", event.requestId, request_id.addr, magic_value);
                        return Ok(());
                    }
                }
                Err(err) => {
                    tracing::warn!("Failed to call ERC1271 isValidSignature for request 0x{:x}, contract: {} - {err:?}", event.requestId, request_id.addr);
                    return Ok(());
                }
            }
        } else if let Err(err) =
            event.request.verify_signature(&event.clientSignature, market_addr, chain_id)
        {
            tracing::warn!("Failed to validate order signature: 0x{:x} - {err:?}", event.requestId);
            return Ok(()); // Return early without propagating the error if signature verification fails.
        }

        let mut new_order = OrderRequest::new(
            event.request.clone(),
            event.clientSignature.clone(),
            FulfillmentType::LockAndFulfill,
            market_addr,
            chain_id,
        );

        // Konfigürasyon ve limit parametrelerini oku
        let (
            max_mcycle_limit,
            peak_prove_khz,
            min_allowed_lock_timeout_secs,
            min_deadline,
            lockin_priority_gas,
            allowed_addresses_opt,
            denied_addresses_opt,
            allowed_requestors_opt
        ) =
            {
                let locked_conf = config.lock_all().context("Failed to read config").unwrap();

                (locked_conf.market.max_mcycle_limit,
                 locked_conf.market.peak_prove_khz,
                 locked_conf.market.min_lock_out_time * 60,
                 locked_conf.market.min_deadline,
                 locked_conf.market.lockin_priority_gas,
                 locked_conf.market.allow_client_addresses.clone(),
                 locked_conf.market.deny_requestor_addresses.clone(),
                 locked_conf.market.allow_requestor_addresses.clone())
            };



        // order_id al
        let order_id = new_order.id();

        let client_addr = event.request.client_address(); // örnek getter, varsa kullan
        // İzinli adres kontrolü (varsa)
        if let Some(allow_addresses) = allowed_requestors_opt {

            let client_addr = new_order.request.client_address();
            tracing::info!(" ------------------- client_addr {} ", client_addr);
            if !allow_addresses.contains(&client_addr) {
                tracing::info!("Removing order {} from {} because it is not in allowed addrs", order_id, client_addr);
                tracing::info!(" ------------------- ALLOWED ORDER DEĞİL {} ", order_id);
                return Ok(());
            }
        }

        // ⏰ ORDER GELDİĞİ AN
        let order_received_time = chrono::Utc::now();
        println!("________________________________________________________________");
        println!("                ");
        println!("                ");
        println!("🎯 ALLOWED ORDER RECEIVED: {} at {}", order_id, Self::format_time(order_received_time));
        println!("                ");
        println!("                ");
        println!("________________________________________________________________");


        // 1. DB’den commit edilmiş orderları çek
        let committed_orders = db_obj.get_committed_orders().await
            .map_err(|e| MarketMonitorErr::UnexpectedErr(e.into()))?;


        let committed_count = committed_orders.len();


        let max_capacity = Some(1); // Konfigürasyondan da alınabilir
        if let Some(max_capacity) = max_capacity {
            if committed_count as u32 >= max_capacity {
                tracing::info!("committed_count as u32 >= max_capacity");
                tracing::info!("Committed orders count ({}) reached max concurrency limit ({}), skipping lock for order {:?}",
                    committed_count,
                    max_capacity,
                    event.requestId
                );
                tracing::info!("return Ok(())");
                return Ok(()); // Yeni order locklama yapılmaz
            }
        }


        // Burada hızlı lock çağrısı
        // Rust tarafındaki BoundlessMarketService örneğin:
        // 3. Lock işlemini çağır
        let boundless = BoundlessMarketService::new(market_addr, provider.clone(), prover_addr.clone());


        let bidding_start: u64 = new_order.request.offer.biddingStart;
        let lock_timeout: u32 = new_order.request.offer.lockTimeout;
        let timeout: u32 = new_order.request.offer.timeout;

        let target_timestamp = bidding_start + (lock_timeout as u64);
        let expire_timestamp = bidding_start + (timeout as u64);


        // Bu timestamp değerlerini new_order içindeki ilgili alanlara set edin
        new_order.target_timestamp = Some(target_timestamp);
        new_order.expire_timestamp = Some(expire_timestamp);


        new_order.image_id = Some("34a5c9394fb2fd3298ece07c16ec2ed009f6029a360f90f4e93933b55e2184d4".to_string());


        let input_id = upload_input_uri(&prover, &new_order.request, &config)
            .await
            .map_err(|e| MarketMonitorErr::UnexpectedErr(e.into()))?;

        // input_id atanması
        new_order.input_id = Some(input_id);
        // Siparişin kalan geçerlilik süresini hesaplayalım
        let now = now_timestamp();
        let effective_deadline = new_order.request.offer.biddingStart + new_order.request.offer.lockTimeout as u64;

        if let Some(existing_target_ts) = new_order.target_timestamp {
            if existing_target_ts != effective_deadline {
                tracing::warn!("Order {} (Type: LockAndFulfill) calculated lock_timeout_secs ({}) differs from existing target_timestamp ({}). Using calculated.",
                new_order.request.id, effective_deadline, existing_target_ts);
            }
        }



        let current_peak_prove_khz = peak_prove_khz.unwrap_or(1); // <-- Burası hatayı çözen satır
        // Kalan süreden 11 dakika (660 saniye) çıkaralım.
        // Senin isteğin doğrultusunda: direk lock time out - 1 dk
        let buffer_time_secs: u64 = 1 * 60; // 1 dakika * 60 saniye/dakika = 60 saniye
        let available_time_for_order_secs = effective_deadline.saturating_sub(now);
        let simulated_proof_time_secs = available_time_for_order_secs.saturating_sub(buffer_time_secs);
        let mut simulated_total_cycles = simulated_proof_time_secs.saturating_mul(current_peak_prove_khz * 1_000); // <-- Değişen satır

        simulated_total_cycles = simulated_total_cycles.max(2); // En az 2 cycle olmalı

        if let Some(limit_mcycles) = max_mcycle_limit {
            let limit_cycles = limit_mcycles.saturating_mul(1_000_000);
            simulated_total_cycles = simulated_total_cycles.min(limit_cycles);
        }
        new_order.total_cycles = Some(simulated_total_cycles);



        let pre_lock_time = chrono::Utc::now();
        println!("                                   ");
        println!("                                   ");
        println!("🚀 LOCK REQUEST STARTING: {} at {}", order_id, Self::format_time(pre_lock_time));
        println!("                                   ");
        println!("                                   ");
        match boundless.lock_request(&event.request.clone(), event.clientSignature.clone(), lockin_priority_gas).await {
            Ok(lock_block) => {

                let lock_timestamp = crate::futures_retry::retry(
                    3,
                    500,
                    || async {
                        Ok(
                            provider
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

                let lock_price = &new_order
                    .request
                    .offer
                    .price_at(lock_timestamp)
                    .context("Failed to calculate lock price")?;


                let success_lock_time = chrono::Utc::now();
                println!("                                   ");
                println!("                                   ");
                println!("🚀 LOCK SUCCESS: {} at {}", order_id, Self::format_time(success_lock_time));
                println!("                                   ");
                println!("                                   ");

                tracing::info!(
                    "Lock transaction successful for order {}, lock price: {}",
                    order_id,
                    lock_price.clone()
                );

                if let Err(e) = db_obj.insert_accepted_request(&new_order, lock_price.clone()).await {
                    tracing::error!("FATAL: Failed to insert accepted request for order {}: {:?}", order_id, e);
                }
            }
            Err(err) => {

                let failed_lock_time = chrono::Utc::now();
                println!("                                   ");
                println!("                                   ");
                println!("❌ FAILED LOCK SUCCESS: {} at {} BECAUSE : {}", order_id, Self::format_time(failed_lock_time), err);
                println!("                                   ");
                println!("                                   ");

                if let Err(e) = db_obj.insert_skipped_request(&new_order).await {
                    tracing::error!("Failed to insert skipped request for order {}: {:?}", order_id, e);
                }
            }
        }

        // JSON olarak pretty-print (daha okunaklı)
        // match serde_json::to_string_pretty(&new_order) {
        //     Ok(json_str) => {
        //         tracing::info!("New order JSON :\n{}", json_str);
        //     }
        //     Err(e) => {
        //         tracing::error!("Failed to serialize order to JSON: {}", e);
        //     }
        // }

        // if let Err(e) = new_order_tx.send(Box::new(new_order)).await {
        //     tracing::error!("Failed to send new on-chain order {} to OrderPicker: {}", order_id, e);
        // } else {
        //     tracing::trace!("Sent new on-chain order {} to OrderPicker via channel.", order_id);
        // }
        Ok(())
    }

    /// Queries chain history to sample for the median block time
    pub async fn get_block_time(&self) -> Result<u64> {
        let current_block = self.chain_monitor.current_block_number().await?;

        let mut timestamps = vec![];
        let sample_start = current_block - std::cmp::min(current_block, BLOCK_TIME_SAMPLE_SIZE);
        for i in sample_start..current_block {
            let block = self
                .provider
                .get_block_by_number(i.into())
                .await
                .with_context(|| format!("Failed get block {i}"))?
                .with_context(|| format!("Missing block {i}"))?;

            timestamps.push(block.header.timestamp);
        }

        let mut block_times =
            timestamps.windows(2).map(|elm| elm[1] - elm[0]).collect::<Vec<u64>>();
        block_times.sort();

        Ok(block_times[block_times.len() / 2])
    }

    fn format_time(dt: DateTime<Utc>) -> String {
        dt.format("%H:%M:%S%.3f").to_string()
    }



    async fn process_mempool_tx(
        tx_hash: B256,
        provider: Arc<P>,
        market_addr: Address,
    ) -> Result<()> {


        let tx_data = match provider.get_transaction_by_hash(tx_hash).await {
            Ok(Some(tx)) => tx,
            Ok(None) => return Ok(()),
            Err(_) => return Ok(()),
        };

        let (tx_to, tx_input) = match &*tx_data.inner {
            alloy::consensus::TxEnvelope::Eip4844(tx) => (tx.to(), tx.input().clone()),
            alloy::consensus::TxEnvelope::Eip1559(tx) => (tx.to(), tx.input().clone()),
            alloy::consensus::TxEnvelope::Eip2930(tx) => (tx.to(), tx.input().clone()),
            alloy::consensus::TxEnvelope::Legacy(tx) => (tx.to(), tx.input().clone()),
            _ => {
                tracing::warn!("Unsupported transaction type detected in mempool.");
                return Ok(());
            }
        };

        let tx_to = tx_to.ok_or_else(|| anyhow::anyhow!("Transaction has no 'to' field"))?;

        if tx_to != market_addr {
            return Ok(());
        }

        tracing::info!("🔍 New transaction detected in mempool: 0x{:x}", tx_hash);

        let submit_request_selector = IBoundlessMarket::submitRequestCall::SELECTOR;
        let input = tx_input.as_ref();

        if input.len() < 4 || input[0..4] != submit_request_selector {
            return Ok(());
        }

        let decoded = match IBoundlessMarket::submitRequestCall::abi_decode(&input[4..]) {
            Ok(call) => call,
            Err(e) => {
                tracing::error!("Failed to decode submitRequest call from mempool transaction: {}", e);
                return Ok(());
            }
        };

        // SADECE LOGLAMA KISMI
        tracing::info!("🎯 MEMPOOL ORDER DETECTED: 0x{:x} (tx: 0x{:x})",
                  decoded.request.id, tx_hash);
        tracing::info!("   - Request ID: 0x{:x}", decoded.request.id);
        tracing::info!("   - Client Address: 0x{:x}", decoded.request.client_address());

        Ok(())
    }


}


impl<P> RetryTask for MarketMonitor<P>
where
    P: Provider<Ethereum> + 'static + Clone,
{
    type Error = MarketMonitorErr;
    // fn spawn(&self, _cancel_token: CancellationToken) -> RetryRes<Self::Error> {
    //     Box::pin(async { Ok(()) })
    // }
    fn spawn(&self, cancel_token: CancellationToken) -> RetryRes<Self::Error> {
        let lookback_blocks = self.lookback_blocks;
        let market_addr = self.market_addr;
        let provider = self.provider.clone();
        let prover_addr = self.prover_addr;
        let chain_monitor = self.chain_monitor.clone();
        let new_order_tx = self.new_order_tx.clone();
        let db = self.db.clone();
        let order_stream = self.order_stream.clone();
        let order_state_tx = self.order_state_tx.clone();
        let prover = self.prover.clone();
        let config = self.config.clone();

        Box::pin(async move {
            tracing::info!("Starting up market monitor");


            tokio::try_join!(
                 Self::monitor_mempool(
                    market_addr,
                    provider.clone(),
                    new_order_tx.clone(),
                    cancel_token.clone(),
                    prover_addr.clone(),
                    db.clone(),
                    prover.clone(),
                    config.clone()
                ),
            )
                .map_err(SupervisorErr::Recover)?;

            Ok(())
        })
    }
}




