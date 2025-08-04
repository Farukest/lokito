// Copyright (c) 2025 RISC Zero, Inc.
//
// All rights reserved.

use std::sync::Arc;
use std::time::Instant;
use alloy::network::Ethereum;
use alloy::primitives::Address;
use alloy::providers::Provider;
use alloy::signers::{local::PrivateKeySigner, Signer};
use boundless_market::order_stream_client::{order_stream, OrderStreamClient};
use futures_util::StreamExt;
use anyhow::{Context, Result};
use thiserror::Error;
use tokio_util::sync::CancellationToken;
use boundless_market::BoundlessMarketService;
use crate::config::ConfigLock;
use crate::order_monitor::OrderMonitorErr;
use crate::provers::ProverObj;
use chrono::{DateTime, Utc};
use crate::{chain_monitor::ChainMonitorService, db::{DbError, DbObj},
            errors::{impl_coded_debug, CodedError}, task::{RetryRes, RetryTask, SupervisorErr},
            FulfillmentType, OrderRequest, OrderStateChange, storage::{upload_image_uri, upload_input_uri}, now_timestamp};
use crate::market_monitor::MarketMonitorErr;

#[derive(Error)]
pub enum OffchainMarketMonitorErr {
    #[error("WebSocket error: {0:?}")]
    WebSocketErr(anyhow::Error),

    #[error("{code} Receiver dropped", code = self.code())]
    ReceiverDropped,

    #[error("{code} Unexpected error: {0:?}", code = self.code())]
    UnexpectedErr(#[from] anyhow::Error),

    #[error("{code} Database error: {0:?}", code = self.code())]
    DatabaseErr(DbError),

}




impl_coded_debug!(OffchainMarketMonitorErr);

impl CodedError for OffchainMarketMonitorErr {
    fn code(&self) -> &str {
        match self {
            OffchainMarketMonitorErr::WebSocketErr(_) => "[B-OMM-001]",
            OffchainMarketMonitorErr::ReceiverDropped => "[B-OMM-002]",
            OffchainMarketMonitorErr::UnexpectedErr(_) => "[B-OMM-500]",
            OffchainMarketMonitorErr::DatabaseErr(_) => "[B-OMM-003]",
        }
    }
}

pub struct OffchainMarketMonitor<P> {
    client: OrderStreamClient,
    signer: PrivateKeySigner,
    new_order_tx: tokio::sync::mpsc::Sender<Box<OrderRequest>>,
    prover_addr : Address,
    provider: Arc<P>,
    db: DbObj,
    prover: ProverObj,
    config: ConfigLock,
}

impl<P> OffchainMarketMonitor<P> where
    P: Provider<Ethereum> + 'static + Clone,
{
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        client: OrderStreamClient,
        signer: PrivateKeySigner,
        new_order_tx: tokio::sync::mpsc::Sender<Box<OrderRequest>>,
        prover_addr : Address,
        provider: Arc<P>,
        db: DbObj,
        prover: ProverObj,
        config: ConfigLock,
    ) -> Self {
        Self {
            client, signer,
            new_order_tx,
            prover_addr,
            provider,
            db,
            prover,
            config
        }

    }

    fn format_time(dt: DateTime<Utc>) -> String {
        dt.format("%H:%M:%S%.3f").to_string()
    }

    async fn monitor_orders(
        client: OrderStreamClient,
        signer: &impl Signer,
        new_order_tx: tokio::sync::mpsc::Sender<Box<OrderRequest>>,
        cancel_token: CancellationToken,
        prover_addr : Address,
        provider: Arc<P>,
        db_obj: DbObj,
        prover: ProverObj,
        config: ConfigLock,
    ) -> Result<(), OffchainMarketMonitorErr> {
        // tracing::info!("Connecting to off-chain market: {}", client.base_url);

        let socket =
            client.connect_async(signer).await.map_err(OffchainMarketMonitorErr::WebSocketErr)?;

        let mut stream = order_stream(socket);
        // tracing::info!("OFFCHAIN START - monitor_orders - Subscribed to offchain Order stream");

        loop {
            tokio::select! {
            order_data = stream.next() => {
                match order_data {
                    Some(order_data) => {
                        let client = client.clone();
                        let prover_addr = prover_addr.clone();
                        let provider = provider.clone();
                        let db_obj = db_obj.clone();
                        let prover = prover.clone();
                        let config = config.clone();
                        let new_order_tx = new_order_tx.clone();
                        let signer = signer.clone();

                        tokio::spawn(async move {
                            // tracing::info!(
                            //     "Detected new order with stream id {:x}, request id: {:x}",
                            //     order_data.id,
                            //     order_data.order.request.id
                            // );

                            let mut new_order = OrderRequest::new(
                                order_data.order.request,
                                order_data.order.signature.as_bytes().into(),
                                FulfillmentType::LockAndFulfill,
                                client.boundless_market_address,
                                client.chain_id,
                            );

                            let order_id = new_order.id();

                            let task_id = uuid::Uuid::new_v4();
                            let start = std::time::Instant::now();

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

                            // tracing::info!(" ------------------- ORDER GELDİ: {} ", order_id);

                            // İzinli adres kontrolü (varsa)
                            if let Some(allow_addresses) = allowed_requestors_opt {

                                let client_addr = new_order.request.client_address();
                                tracing::info!(" ------------------- client_addr {} ", client_addr);
                                if !allow_addresses.contains(&client_addr) {
                                    tracing::info!("Removing order {} from {} because it is not in allowed addrs", order_id, client_addr);
                                    tracing::info!(" ------------------- ALLOWED ORDER DEĞİL {} ", order_id);
                                    return;
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

                            // DB’den commit edilmiş orderları çek
                            let committed_orders = match db_obj.get_committed_orders().await {
                                Ok(list) => list,
                                Err(e) => {
                                    tracing::error!("DB error fetching committed orders: {:?}", e);
                                    return;
                                }
                            };

                            // tracing::info!("📊 TASK-{}: Got committed_orders ({} items) at {:?}", task_id, committed_orders.len(), start.elapsed());

                            let committed_count = committed_orders.len();

                            // concurrency limiti kontrolü
                            let max_capacity = Some(1); // Burayı konfigürasyondan da alabilirsin
                            if let Some(max_capacity) = max_capacity {
                                if committed_count as u32 >= max_capacity {
                                    tracing::info!("Committed orders count ({}) reached max concurrency limit ({}), skipping lock for order {}", committed_count, max_capacity, order_id);
                                    return; // Yeni order locklama yapılmaz
                                }
                            }

                            // BoundlessMarketService oluştur
                            let boundless = BoundlessMarketService::new(client.boundless_market_address, provider.clone(), prover_addr.clone());

                            let bidding_start: u64 = new_order.request.offer.biddingStart;
                            let lock_timeout: u32 = new_order.request.offer.lockTimeout;
                            let timeout: u32 = new_order.request.offer.timeout;

                            let target_timestamp = bidding_start + (lock_timeout as u64);
                            let expire_timestamp = bidding_start + (timeout as u64);

                            // Timestampleri new_order içine set et
                            new_order.target_timestamp = Some(target_timestamp);
                            new_order.expire_timestamp = Some(expire_timestamp);

                            new_order.image_id = Some("34a5c9394fb2fd3298ece07c16ec2ed009f6029a360f90f4e93933b55e2184d4".to_string());

                            let input_id = match upload_input_uri(&prover, &new_order.request, &config).await {
                                Ok(id) => id,
                                Err(e) => {
                                    tracing::info!("Failed to upload input URI: {:?}", e);
                                    return;
                                }
                            };

                            new_order.input_id = Some(input_id);

                            // Lock timeout hesaplama ve total cycles belirleme
                            let now = now_timestamp();
                            let effective_deadline = new_order.request.offer.biddingStart + new_order.request.offer.lockTimeout as u64;

                            if let Some(existing_target_ts) = new_order.target_timestamp {
                                if existing_target_ts != effective_deadline {
                                    tracing::info!("Order {} (Type: LockAndFulfill) calculated lock_timeout_secs ({}) differs from existing target_timestamp ({}). Using calculated.", new_order.request.id, effective_deadline, existing_target_ts);
                                }
                            }

                            let current_peak_prove_khz = peak_prove_khz.unwrap_or(1); // varsayılan 1
                            let buffer_time_secs: u64 = 1 * 60; // 1 dakika
                            let available_time_for_order_secs = effective_deadline.saturating_sub(now);
                            let simulated_proof_time_secs = available_time_for_order_secs.saturating_sub(buffer_time_secs);
                            let mut simulated_total_cycles = simulated_proof_time_secs.saturating_mul(current_peak_prove_khz * 1_000);

                            simulated_total_cycles = simulated_total_cycles.max(2); // en az 2

                            if let Some(limit_mcycles) = max_mcycle_limit {
                                let limit_cycles = limit_mcycles.saturating_mul(1_000_000);
                                simulated_total_cycles = simulated_total_cycles.min(limit_cycles);
                            }

                            new_order.total_cycles = Some(simulated_total_cycles);

                            // tracing::info!("🔐 TASK-{}: About to call lock_request at {:?}", task_id, start.elapsed());

                            let pre_lock_time = chrono::Utc::now();
                            tracing::info!("                                   ");
                            tracing::info!("                                   ");
                            tracing::info!("🚀 LOCK REQUEST STARTING: {} at {}", order_id, Self::format_time(pre_lock_time));
                            tracing::info!("                                   ");
                            tracing::info!("                                   ");

                            match boundless.lock_request(&new_order.request, new_order.client_sig.clone(), lockin_priority_gas).await {
                                Ok(lock_block) => {
                                    let post_lock_time = chrono::Utc::now();
                                    tracing::info!("✅ LOCK SUCCESS: {} at {}", order_id, Self::format_time(post_lock_time));

                                    let lock_timestamp = match crate::futures_retry::retry(
                                        3,
                                        500,
                                        || async {
                                            let block = provider
                                                .get_block_by_number(lock_block.into())
                                                .await
                                                .with_context(|| format!("failed to get block {lock_block}"))?
                                                .with_context(|| format!("failed to get block {lock_block}: block not found"))?;

                                            Ok::<u64, anyhow::Error>(block.header.timestamp)
                                        },
                                        "get_block_by_number",
                                    )
                                    .await {
                                        Ok(timestamp) => timestamp,
                                        Err(e) => {
                                            tracing::error!("Failed to get block timestamp: {:?}", e);
                                            return;
                                        }
                                    };


                                    let lock_price = match new_order.request.offer.price_at(lock_timestamp) {
                                        Ok(price) => price,
                                        Err(e) => {
                                            tracing::error!("Failed to calculate lock price: {:?}", e);
                                            return;
                                        }
                                    };

                                    tracing::info!("✅ TASK-{}: Lock successful at {:?}", task_id, start.elapsed());
                                    tracing::info!("Lock transaction successful for order {}, lock price: {}", order_id, lock_price);

                                    if let Err(e) = db_obj.insert_accepted_request(&new_order, lock_price.clone()).await {
                                        tracing::error!("FATAL: Failed to insert accepted request for order {}: {:?}", order_id, e);
                                    }
                                }
                                Err(err) => {
                                    tracing::info!("❌ TASK-{}: Lock failed at {:?}: {:?}", task_id, start.elapsed(), err);
                                    tracing::error!("Failed to lock order {} due to unexpected error: {:?}", order_id, err);

                                    if let Err(e) = db_obj.insert_skipped_request(&new_order).await {
                                        tracing::error!("Failed to insert skipped request for order {}: {:?}", order_id, e);
                                    }
                                }
                            }

                            // JSON pretty print
                            // match serde_json::to_string_pretty(&new_order) {
                            //     Ok(json_str) => {
                            //         tracing::info!("New order JSON :\n{}", json_str);
                            //     }
                            //     Err(e) => {
                            //         tracing::error!("Failed to serialize order to JSON: {}", e);
                            //     }
                            // }

                            // İstersen kanala da gönderebilirsin, şu an yorumlu
                            // if let Err(e) = new_order_tx.send(Box::new(new_order)).await {
                            //     tracing::error!("Failed to send new off-chain order {} to OrderPicker: {}", order_id, e);
                            // }
                        });
                    }
                    None => {
                        // tracing::info!("NONEeeeeeeee");
                        return Err(OffchainMarketMonitorErr::WebSocketErr(anyhow::anyhow!(
                            "Offchain order stream websocket exited, polling failed"
                        )));
                    }
                }
            }
            _ = cancel_token.cancelled() => {
                // tracing::info!("Offchain market monitor received cancellation, shutting down gracefully");
                return Ok(());
            }
        }
        }
    }




}

impl<P> RetryTask for OffchainMarketMonitor<P>
where
    P: Provider<Ethereum> + 'static + Clone,
{
    type Error = OffchainMarketMonitorErr;
    // fn spawn(&self, cancel_token: CancellationToken) -> RetryRes<Self::Error> {
    //     let client = self.client.clone();
    //     let signer = self.signer.clone();
    //     let new_order_tx = self.new_order_tx.clone();
    //     let prover_addr = self.prover_addr;
    //     let provider = self.provider.clone();
    //     let db = self.db.clone();
    //     let prover = self.prover.clone();
    //     let config = self.config.clone();
    //
    //     Box::pin(async move {
    //         tracing::info!("Starting up offchain market monitor");
    //         Self::monitor_orders(client, &signer, new_order_tx, cancel_token, prover_addr, provider, db, prover, config)
    //             .await
    //             .map_err(SupervisorErr::Recover)?;
    //         Ok(())
    //     })
    // }
    fn spawn(&self, _cancel_token: CancellationToken) -> RetryRes<Self::Error> {
        Box::pin(async { Ok(()) })
    }
}
