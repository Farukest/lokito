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
use tokio::sync::Semaphore;
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
use alloy_primitives::{Bytes, U256};
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use boundless_market::{
    contracts::{
        boundless_market::BoundlessMarketService, IBoundlessMarket,
    },
};
use futures_util::{StreamExt, SinkExt};
use tokio_util::sync::CancellationToken;
use crate::{chain_monitor::ChainMonitorService, db::DbObj, errors::{impl_coded_debug, CodedError}, task::{RetryRes, RetryTask, SupervisorErr},
            FulfillmentType, OrderRequest, storage::{upload_image_uri, upload_input_uri}};

use thiserror::Error;
use crate::config::ConfigLock;
use crate::provers::ProverObj;

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
// 🎯 GLOBAL SEMAPHORE - Max 3 concurrent order
static MARKET_PROCESSING_SEMAPHORE: Semaphore = Semaphore::const_new(3);
impl_coded_debug!(MarketMonitorErr);

pub struct MarketMonitor<P> {
    market_addr: Address,
    provider: Arc<P>,
    config: ConfigLock,
    // chain_monitor: Arc<ChainMonitorService<P>>,
    db_obj: DbObj,
    prover_addr: Address,
    boundless_service: BoundlessMarketService<Arc<P>>,
    prover: ProverObj
}

impl<P> MarketMonitor<P>
where
    P: Provider<Ethereum> + 'static + Clone,
{
    pub fn new(
        market_addr: Address,
        provider: Arc<P>,
        config: ConfigLock,
        // chain_monitor: Arc<ChainMonitorService<P>>,
        db_obj: DbObj,
        prover_addr: Address,
        prover: ProverObj
    ) -> Self {
        let boundless_service = BoundlessMarketService::new(market_addr, provider.clone(), prover_addr);
        Self {
            market_addr,
            provider,
            config,
            // chain_monitor,
            db_obj,
            prover_addr,
            boundless_service,
            prover
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
        prover: ProverObj
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
                        prover.clone(),
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
        prover : ProverObj
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
                prover,
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
        prover : ProverObj
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
                                                    prover.clone(),
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
        prover : ProverObj
    ) -> Result<()> {
        // 🔒 Permit al - max 3 order aynı anda işlensin
        let _permit = MARKET_PROCESSING_SEMAPHORE.acquire().await
            .map_err(|e| anyhow::anyhow!("Failed to acquire processing permit: {}", e))?;

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

        tracing::info!("📋 Processing submitRequest (permit acquired):");
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

        let mut new_order = OrderRequest::new(
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

        // 1. DB'den commit edilmiş orderları çek
        let committed_orders = db_obj.get_committed_orders().await
            .map_err(|e| MarketMonitorErr::UnexpectedErr(e.into()))?;

        let committed_count = committed_orders.len();

        let max_capacity = Some(3); // 🔧 3 concurrent order limit
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

        // ✅ PARALLEL LOCK ATTEMPTS
        let lock_result = Self::parallel_lock_attempts(
            boundless_service,
            decoded.request,
            decoded.clientSignature.clone(),
            lockin_priority_gas,
            request_id,
            lock_delay_ms,
        ).await;

        match lock_result {
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

                let final_order = match Self::fetch_confirmed_transaction_data_by_input(provider.clone(), input_hex).await {
                    Ok(confirmed_request) => {
                        tracing::info!("✅ Got CONFIRMED data, creating updated order: 0x{:x}", request_id);

                        // Confirmed data ile yeni order oluştur
                        let mut updated_order = OrderRequest::new(
                            confirmed_request,
                            decoded.clientSignature.clone(),
                            FulfillmentType::LockAndFulfill,
                            market_addr,
                            chain_id,
                        );
                        updated_order.target_timestamp = Some(updated_order.request.lock_expires_at());
                        updated_order.expire_timestamp = Some(updated_order.request.expires_at());

                        updated_order
                    }
                    Err(e) => {
                        tracing::warn!("⚠️ Confirmed data fetch failed: {} - using mempool data", e);
                        new_order // Eski değerler kalsın
                    }
                };

                if let Err(e) = db_obj.insert_accepted_request(&final_order, lock_price).await {
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

        // 🔓 Permit otomatik olarak drop edilir, diğer order'lar işlenebilir
        tracing::info!("📤 Processing completed for request: 0x{:x}, permit released", request_id);
        Ok(())
    }

    // ✅ PARALLEL LOCK FUNCTION - İLK BAŞARILI OLAN WINS (Fixed abort logic)
    async fn parallel_lock_attempts(
        boundless_service: &BoundlessMarketService<Arc<P>>,
        request: boundless_market::contracts::ProofRequest,
        client_signature: alloy::primitives::Bytes,
        lockin_priority_gas: Option<u64>,
        request_id: U256,
        config_delay_ms: Option<u64>,
    ) -> Result<u64> {

        // Attempt timing'leri - 2 saniyeye kadar
        // let delays = [0u64, 10, 25, 50, 100, 200, 300, 500, 750, 1000, 1500, 2000];
        let delays = [
            0, 20, 40, 60, 80, 100, 120, 140, 160, 180,
            200, 220, 240, 260, 280, 300, 320, 340, 360, 380,
            400, 420, 440, 460, 480, 500, 520, 540, 560, 580,
            600, 620, 640, 660, 680, 700, 720, 740, 760, 780,
            800, 820, 840, 860, 880, 900, 920, 940, 960, 980,
            1000, 1020, 1040, 1060, 1080, 1100
        ];

        // Config delay varsa ekle
        let base_delay = config_delay_ms.unwrap_or(0);

        let mut tasks = Vec::new();

        for (i, &delay) in delays.iter().enumerate() {
            let service = boundless_service.clone();
            let req = request.clone();
            let sig = client_signature.clone();
            let total_delay = base_delay + delay;

            let task = tokio::spawn(async move {
                if total_delay > 0 {
                    tokio::time::sleep(tokio::time::Duration::from_millis(total_delay)).await;
                }

                tracing::debug!("🚀 Lock attempt {} ({}ms delay) for: 0x{:x}", i + 1, total_delay, request_id);
                service.lock_request(&req, sig, lockin_priority_gas).await
            });

            tasks.push(task);
        }

        tracing::info!("🚀 Launched {} parallel lock attempts for: 0x{:x}", tasks.len(), request_id);

        // İlk başarılı olan win eder
        let mut last_error = None;

        loop {
            // Tüm task'ları kontrol et
            let mut any_pending = false;

            // 🔧 Finished task'ları bulup handle et
            let mut i = 0;
            while i < tasks.len() {
                if tasks[i].is_finished() {
                    let finished_task = tasks.remove(i); // Task'ı çıkar
                    match finished_task.await {
                        Ok(Ok(lock_block)) => {
                            tracing::info!("⚡ LOCK SUCCESS attempt for: 0x{:x} at block {}", request_id, lock_block);

                            // 🔧 Kalan tüm task'ları iptal et
                            for remaining_task in tasks.into_iter() {
                                remaining_task.abort();
                            }

                            return Ok(lock_block);
                        }
                        Ok(Err(err)) => {
                            tracing::debug!("❌ Lock attempt failed: {}", err);
                            last_error = Some(err);
                            // i'yi artırma çünkü remove() yaptık
                        }
                        Err(join_err) => {
                            tracing::debug!("❌ Task panicked: {}", join_err);
                            // i'yi artırma çünkü remove() yaptık
                        }
                    }
                } else {
                    any_pending = true;
                    i += 1; // Sadece pending task'larda i'yi artır
                }
            }

            // Hiç pending task yoksa çık
            if !any_pending {
                break;
            }

            // Kısa bekle
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        }

        Err(last_error.unwrap_or_else(|| anyhow::anyhow!("All lock attempts failed").into()).into())
    }
    // Helper function (aynı kalır)
    // Input hex ile çalışan alternatif fonksiyon
    async fn fetch_confirmed_transaction_data_by_input(
        provider: Arc<P>,
        input_hex: &str,
    ) -> Result<boundless_market::contracts::ProofRequest> {
        // Input'u decode et
        let input_bytes = hex::decode(&input_hex[2..])
            .context("Failed to decode input hex")?;

        let decoded_call = IBoundlessMarket::submitRequestCall::abi_decode(&input_bytes)
            .context("Failed to decode transaction input")?;

        tracing::debug!("✅ Input data decoded successfully");

        Ok(decoded_call.request)
    }

    fn format_time(dt: DateTime<Utc>) -> String {
        dt.format("%H:%M:%S%.3f").to_string()
    }


    // Her zaman 0x ile başlayan format kullan
    fn normalize_hex_data(data: &str) -> String {
        if data.starts_with("0x") {
            data.to_string()
        } else {
            format!("0x{}", data)
        }
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
        // let chain_monitor = self.chain_monitor.clone();
        let db = self.db_obj.clone();
        let config = self.config.clone();
        let prover = self.prover.clone();
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
                prover,
            )
                .await
                .map_err(SupervisorErr::Recover)?;

            Ok(())
        })
    }
}