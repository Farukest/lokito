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

use std::sync::Arc;
use crate::order_monitor::OrderMonitorErr;
use alloy::{
    network::Ethereum,
    primitives::{Address, B256},
    providers::Provider,
    sol,
    sol_types::SolCall,
};

use alloy::rpc::types::Transaction;
use alloy::consensus::Transaction as _;
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use boundless_market::{
    contracts::{
        boundless_market::BoundlessMarketService, IBoundlessMarket,
    },
};
use futures_util::{StreamExt, SinkExt};
use tokio_util::sync::CancellationToken;
use crate::{chain_monitor::ChainMonitorService, db::DbObj, errors::{impl_coded_debug, CodedError}, task::{RetryRes, RetryTask, SupervisorErr}, FulfillmentType, OrderRequest};
use thiserror::Error;
use crate::config::ConfigLock;
const BLOCK_TIME_SAMPLE_SIZE: u64 = 10;
#[derive(Error)]
pub enum MarketMonitorErr {
    #[error("{code} Mempool polling failed: {0:?}", code = self.code())]
    MempoolPollingErr(anyhow::Error),

    #[error("{code} Unexpected error: {0:?}", code = self.code())]
    UnexpectedErr(#[from] anyhow::Error),
}

impl CodedError for MarketMonitorErr {
    fn code(&self) -> &str {
        match self {
            MarketMonitorErr::MempoolPollingErr(_) => "[B-MM-501]",
            MarketMonitorErr::UnexpectedErr(_) => "[B-MM-500]",
        }
    }
}

impl_coded_debug!(MarketMonitorErr);

pub struct MarketMonitor<P> {
    market_addr: Address,
    provider: Arc<P>,
    config: ConfigLock,
    chain_monitor: Arc<ChainMonitorService<P>>,
    db_obj: DbObj,
    prover_addr: Address,
    boundless_service: BoundlessMarketService<Arc<P>>,
}

impl<P> MarketMonitor<P>
where
    P: Provider<Ethereum> + 'static + Clone,
{
    pub fn new(
        market_addr: Address,
        provider: Arc<P>,
        config: ConfigLock,
        chain_monitor: Arc<ChainMonitorService<P>>,
        db_obj: DbObj,
        prover_addr: Address,
    ) -> Self {
        let boundless_service = BoundlessMarketService::new(market_addr, provider.clone(), prover_addr);
        Self {
            market_addr,
            provider,
            config,
            chain_monitor,
            db_obj,
            prover_addr,
            boundless_service
        }
    }

    async fn start_mempool_polling(
        market_addr: Address,
        provider: Arc<P>,
        cancel_token: CancellationToken,
        config: ConfigLock,
        db_obj: DbObj,
        prover_addr: Address,
        boundless_service: &BoundlessMarketService<Arc<P>>,  // Ekle
    ) -> std::result::Result<(), MarketMonitorErr> {
        tracing::info!("🎯 Starting mempool polling for market: 0x{:x}", market_addr);

        // Config'den HTTP RPC URL'i al
        let http_rpc_url = {
            let conf = config.lock_all().context("Failed to read config")?;
            conf.market.my_rpc_url.clone()
        };

        tracing::info!("Using RPC URL: {}", http_rpc_url);

        let mut seen_tx_hashes = std::collections::HashSet::<B256>::new();

        loop {
            tokio::select! {
                _ = cancel_token.cancelled() => {
                    tracing::info!("Mempool polling cancelled");
                    return Ok(());
                }
                _ = tokio::time::sleep(tokio::time::Duration::from_millis(20)) => {
                    if let Err(e) = Self::get_mempool_content(
                        &http_rpc_url,
                        market_addr,
                        provider.clone(),
                        config.clone(),
                        db_obj.clone(),
                        prover_addr,
                        &mut seen_tx_hashes,
                        boundless_service,
                    ).await {
                        tracing::debug!("Error getting mempool content: {:?}", e);
                    }
                }
            }
        }
    }

    async fn get_mempool_content(
        http_rpc_url: &str,
        market_addr: Address,
        provider: Arc<P>,
        config: ConfigLock,
        db_obj: DbObj,
        prover_addr: Address,
        seen_tx_hashes: &mut std::collections::HashSet<B256>,
        boundless_service: &BoundlessMarketService<Arc<P>>,  // Ekle
    ) -> Result<()> {
        // HTTP request - exactly like Node.js fetch
        let client = reqwest::Client::new();

        let request_body = serde_json::json!({
        "jsonrpc": "2.0",
        "method": "eth_getBlockByNumber",
        "params": ["pending", true],
        "id": 1
    });

        let response = client
            .post(http_rpc_url)
            .header("Content-Type", "application/json")
            .json(&request_body)
            .send()
            .await?;

        let data: serde_json::Value = response.json().await?;

        if let Some(result) = data.get("result") {
            Self::process_mempool_response(
                result,
                market_addr,
                provider,
                config,
                db_obj,
                prover_addr,
                seen_tx_hashes,
                &boundless_service,
            ).await?;
        }

        Ok(())
    }

    async fn process_mempool_response(
        result: &serde_json::Value,
        market_addr: Address,
        provider: Arc<P>,
        config: ConfigLock,
        db_obj: DbObj,
        prover_addr: Address,
        seen_tx_hashes: &mut std::collections::HashSet<B256>,
        boundless_service: &BoundlessMarketService<Arc<P>>,  // Ekle
    ) -> Result<()> {
        if let Some(transactions) = result.get("transactions").and_then(|t| t.as_array()) {
            // First filter by FROM address (like Node.js FROM_FILTER)
            let allowed_requestors_opt = {
                let locked_conf = config.lock_all().context("Failed to read config")?;
                locked_conf.market.allow_requestor_addresses.clone()
            };

            for tx_data in transactions {
                // Check FROM address first (like Node.js FROM_FILTER)
                if let Some(from_addr) = tx_data.get("from").and_then(|f| f.as_str()) {
                    if let Ok(parsed_from) = from_addr.parse::<Address>() {
                        // Apply FROM filter if configured
                        if let Some(allow_addresses) = &allowed_requestors_opt {
                            if !allow_addresses.contains(&parsed_from) {
                                continue; // Skip if not in allowed FROM addresses
                            }
                        }

                        // Then check if transaction is TO our market contract
                        if let Some(to_addr) = tx_data.get("to").and_then(|t| t.as_str()) {
                            if let Ok(parsed_to) = to_addr.parse::<Address>() {
                                if parsed_to == market_addr {
                                    if let Some(hash) = tx_data.get("hash").and_then(|h| h.as_str()) {
                                        if let Ok(parsed_hash) = hash.parse::<B256>() {
                                            if !seen_tx_hashes.contains(&parsed_hash) {
                                                seen_tx_hashes.insert(parsed_hash);

                                                tracing::info!("🔥 PENDING BLOCK'DA HEDEF TX!");
                                                tracing::info!("   Hash: 0x{:x}", parsed_hash);
                                                tracing::info!("   From: 0x{:x} → To: 0x{:x}", parsed_from, parsed_to);

                                                // Process the transaction
                                                if let Err(e) = Self::process_market_tx(
                                                    tx_data,  // JSON tx data'sını direkt geç
                                                    provider.clone(),
                                                    market_addr,
                                                    config.clone(),
                                                    db_obj.clone(),
                                                    prover_addr,
                                                    &boundless_service,
                                                ).await {
                                                    tracing::error!("Failed to process market tx: {:?}", e);
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }

    async fn process_market_tx(
        tx_data: &serde_json::Value,  // JSON'dan direkt al
        provider: Arc<P>,
        market_addr: Address,
        config: ConfigLock,
        db_obj: DbObj,
        prover_addr: Address,
        boundless_service: &BoundlessMarketService<Arc<P>>,  // Ekle
    ) -> Result<()> {
        // Get transaction details
        // tx_data'dan input'u direkt al
        let input_hex = tx_data.get("input")
            .and_then(|i| i.as_str())
            .ok_or_else(|| anyhow::anyhow!("No input in tx data"))?;

        let input_bytes = hex::decode(&input_hex[2..])?; // 0x prefix'i kaldır

        // Try to decode as submitRequest
        let decoded = match IBoundlessMarket::submitRequestCall::abi_decode(&input_bytes) {
            Ok(call) => call,
            Err(_) => {
                tracing::debug!("Transaction is not submitRequest, skipping");
                return Ok(());
            }
        };

        let client_addr = decoded.request.client_address();
        let request_id = decoded.request.id;

        tracing::info!("📋 Processing submitRequest:");
        tracing::info!("   - Request ID: 0x{:x}", request_id);
        tracing::info!("   - Client: 0x{:x}", client_addr);

        // Check if client is allowed (if filter is configured)
        let (allowed_requestors_opt, lock_delay_ms) = {
            let locked_conf = config.lock_all().context("Failed to read config")?;
            (
                locked_conf.market.allow_requestor_addresses.clone(),
                locked_conf.market.lock_delay_ms
            )
        };


        if let Some(allow_addresses) = allowed_requestors_opt {
            if !allow_addresses.contains(&client_addr) {
                tracing::debug!("🚫 Client not in allowed requestors, skipping");
                return Ok(());
            }
        }

        tracing::info!("✅ Processing allowed request from: 0x{:x}", client_addr);

        // Get chain ID and create order
        let chain_id = provider.get_chain_id().await.context("Failed to get chain id")?;

        let new_order = OrderRequest::new(
            decoded.request.clone(),
            decoded.clientSignature.clone(),
            FulfillmentType::LockAndFulfill,
            market_addr,
            chain_id,
        );

        // Try to lock the request
        let lockin_priority_gas = {
            let locked_conf = config.lock_all().context("Failed to read config")?;
            locked_conf.market.lockin_priority_gas
        };

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
                    request_id
                );
                tracing::info!("return Ok(())");
                return Ok(()); // Yeni order locklama yapılmaz
            }
        }

        tracing::info!("🚀 Attempting to lock request: 0x{:x}", request_id);
        // CONFIG DELAY
        if let Some(delay) = lock_delay_ms {
            tracing::info!(" -- DELAY {} ms başlatılıyor - ORDER ID : 0x{:x}", delay, request_id);
            tokio::time::sleep(tokio::time::Duration::from_millis(delay)).await;
        }
        match boundless_service.lock_request(&decoded.request, decoded.clientSignature.clone(), lockin_priority_gas).await {
            Ok(lock_block) => {
                tracing::info!("✅ Successfully locked request: 0x{:x} at block {}", request_id, lock_block);

                // Calculate lock price and save to DB
                let lock_timestamp = provider
                    .get_block_by_number(lock_block.into())
                    .await
                    .context("Failed to get lock block")?
                    .context("Lock block not found")?
                    .header
                    .timestamp;

                let lock_price = new_order
                    .request
                    .offer
                    .price_at(lock_timestamp)
                    .context("Failed to calculate lock price")?;

                if let Err(e) = db_obj.insert_accepted_request(&new_order, lock_price).await {
                    tracing::error!("Failed to insert accepted request: {:?}", e);
                }
            }
            Err(err) => {
                tracing::warn!("❌ Failed to lock request: 0x{:x}, error: {}", request_id, err);

                if let Err(e) = db_obj.insert_skipped_request(&new_order).await {
                    tracing::error!("Failed to insert skipped request: {:?}", e);
                }
            }
        }

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

}

impl<P> RetryTask for MarketMonitor<P>
where
    P: Provider<Ethereum> + 'static + Clone,
{
    type Error = MarketMonitorErr;

    fn spawn(&self, cancel_token: CancellationToken) -> RetryRes<Self::Error> {
        let market_addr = self.market_addr;
        let provider = self.provider.clone();
        let prover_addr = self.prover_addr;
        let chain_monitor = self.chain_monitor.clone();
        let db = self.db_obj.clone();
        let config = self.config.clone();
        let boundless_service = self.boundless_service.clone();
        Box::pin(async move {
            tracing::info!("Starting market monitor");

            Self::start_mempool_polling(
                market_addr,
                provider,
                cancel_token,
                config,
                db,
                prover_addr,
                &boundless_service,  // Reference ver
            )
                .await
                .map_err(SupervisorErr::Recover)?;

            Ok(())
        })
    }
}