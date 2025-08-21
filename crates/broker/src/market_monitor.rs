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
use std::sync::atomic::{AtomicU64, Ordering, AtomicBool};
use std::time::{Duration, Instant};
use std::collections::HashSet;
use tokio::time::sleep;
use crate::order_monitor::OrderMonitorErr;
use alloy::{
    network::{Ethereum, eip2718::Encodable2718},
    primitives::{Address, B256, TxKind, TxHash, U256},
    providers::Provider,
    sol,
    sol_types::SolCall,
    consensus::{TxEip1559, TxEnvelope, SignableTransaction},
    signers::{local::PrivateKeySigner, Signer},
    rpc::types::TransactionRequest,
};
use alloy::consensus::Transaction;
use alloy_primitives::Signature;
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
use serde_json::json;

const BLOCK_TIME_SAMPLE_SIZE: u64 = 10;

// Global cache'ler - offchain monitor'daki gibi
static CACHED_CHAIN_ID: AtomicU64 = AtomicU64::new(0);
static CURRENT_NONCE: AtomicU64 = AtomicU64::new(0);
static IS_PROCESSING: AtomicBool = AtomicBool::new(false);

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

// Config verileri için cache struct
#[derive(Clone)]
struct CachedConfig {
    allowed_requestor_addresses: Option<HashSet<Address>>,
    http_rpc_url: String,
    lockin_priority_gas: Option<u64>,
    wait_time_for_new_order: Option<u64>,
}

// ✅ HTTP Client wrapper - connection pooling ile
#[derive(Clone)]
struct OptimizedHttpClient {
    client: reqwest::Client,
    rpc_url: String,
}

impl OptimizedHttpClient {
    fn new(rpc_url: String) -> Self {
        // ✅ Connection pooling ve keep-alive ile optimize edilmiş client
        let client = reqwest::Client::builder()
            .pool_max_idle_per_host(10)           // Host başına max 10 idle connection
            .pool_idle_timeout(Duration::from_secs(30))  // 30s sonra idle connection'ları kapat
            .timeout(Duration::from_secs(10))      // Request timeout
            .tcp_keepalive(Duration::from_secs(60)) // TCP keep-alive
            .http2_keep_alive_interval(Some(Duration::from_secs(30))) // HTTP/2 keep-alive
            .http2_keep_alive_timeout(Duration::from_secs(10))
            .http2_keep_alive_while_idle(true)
            .build()
            .expect("Failed to create HTTP client");

        Self { client, rpc_url }
    }

    // ✅ Hızlı mempool polling
    async fn get_pending_block(&self) -> Result<serde_json::Value> {
        let request_body = serde_json::json!({
            "jsonrpc": "2.0",
            "method": "eth_getBlockByNumber",
            "params": ["pending", true],
            "id": 1
        });

        let response = self.client
            .post(&self.rpc_url)
            .header("Content-Type", "application/json")
            .json(&request_body)
            .send()
            .await
            .context("Failed to get pending block")?;

        let data: serde_json::Value = response.json().await
            .context("Failed to parse pending block response")?;

        Ok(data)
    }

    // ✅ Hızlı raw transaction gönderme
    async fn send_raw_transaction(&self, tx_encoded: &[u8]) -> Result<serde_json::Value> {
        let request_body = serde_json::json!({
            "jsonrpc": "2.0",
            "method": "eth_sendRawTransaction",
            "params": [format!("0x{}", hex::encode(tx_encoded))],
            "id": 1
        });

        let response = self.client
            .post(&self.rpc_url)
            .header("Content-Type", "application/json")
            .json(&request_body)
            .send()
            .await
            .context("Failed to send raw transaction")?;

        let result: serde_json::Value = response.json().await
            .context("Failed to parse raw transaction response")?;

        Ok(result)
    }
}

pub struct MarketMonitor<P> {
    market_addr: Address,
    provider: Arc<P>,
    config: ConfigLock,
    db_obj: DbObj,
    prover_addr: Address,
    boundless_service: BoundlessMarketService<Arc<P>>,
    prover: ProverObj,
    signer: PrivateKeySigner,
    cached_config: CachedConfig,
    http_client: OptimizedHttpClient,  // ✅ Optimize edilmiş HTTP client
}

impl<P> MarketMonitor<P>
where
    P: Provider<Ethereum> + 'static + Clone,
{
    pub fn new(
        market_addr: Address,
        provider: Arc<P>,
        config: ConfigLock,
        db_obj: DbObj,
        prover_addr: Address,
        prover: ProverObj,
        signer: PrivateKeySigner,
    ) -> Self {
        let boundless_service = BoundlessMarketService::new(market_addr, provider.clone(), prover_addr);

        // ✅ Config'i başlangıçta cache'le
        let cached_config = {
            let conf = config.lock_all().context("Failed to read config during initialization").unwrap();
            CachedConfig {
                allowed_requestor_addresses: conf.market.allow_requestor_addresses.clone(),
                http_rpc_url: conf.market.my_rpc_url.clone(),
                lockin_priority_gas: conf.market.lockin_priority_gas,
                wait_time_for_new_order: conf.market.wait_time_for_new_order,
            }
        };

        // ✅ Optimize edilmiş HTTP client oluştur
        let http_client = OptimizedHttpClient::new(cached_config.http_rpc_url.clone());

        Self {
            market_addr,
            provider,
            config,
            db_obj,
            prover_addr,
            boundless_service,
            prover,
            signer,
            cached_config,
            http_client,
        }
    }

    // ✅ Processing durumunu kontrol et
    fn is_currently_processing() -> bool {
        IS_PROCESSING.load(Ordering::Relaxed)
    }

    // ✅ Processing'i true yap
    fn set_processing_true() {
        IS_PROCESSING.store(true, Ordering::Relaxed);
        tracing::info!("🔒 Processing flag set to TRUE - blocking new orders");
    }

    // ✅ Processing'i false yap
    fn set_processing_false() {
        IS_PROCESSING.store(false, Ordering::Relaxed);
        tracing::info!("🔓 Processing flag set to FALSE - allowing new orders");
    }

    // ✅ Async dinleme servisi - committed orders'ları kontrol eder
    async fn start_committed_orders_monitor(db_obj: DbObj, cached_config: CachedConfig, config: ConfigLock) -> Result<()> {
        tracing::info!("👁️👁️ Starting committed orders monitor service...");

        // ✅ Basit unwrap_or ile default 360 saniye
        let wait_seconds = cached_config.wait_time_for_new_order.unwrap_or(360);
        let check_interval = Duration::from_millis(wait_seconds * 1000);

        loop {
            // Committed orders'ları kontrol et
            match db_obj.get_committed_orders().await {
                Ok(committed_orders) => {
                    let count = committed_orders.len();
                    tracing::debug!("📊 Committed orders count: {}", count);

                    // Eğer committed orders 0 ise, processing'i false yap ve servisi bitir
                    if count == 0 {
                        // ✅ Config'i tekrar oluştur
                        let cached_config = {
                            let conf = config.lock_all().context("Failed to read config during initialization").unwrap();
                            CachedConfig {
                                allowed_requestor_addresses: conf.market.allow_requestor_addresses.clone(),
                                http_rpc_url: conf.market.my_rpc_url.clone(),
                                lockin_priority_gas: conf.market.lockin_priority_gas,
                                wait_time_for_new_order: conf.market.wait_time_for_new_order,
                            }
                        };
                        tracing::info!("📖📖📖 CONFIG GUNCELLENIYOR 📖📖📖");
                        Duration::from_millis(5000);
                        Self::set_processing_false();
                        tracing::info!("✅ No committed orders found - monitor service stopping and proof checking continue..");
                        break; // Servis kendini iptal ediyor
                    }
                }
                Err(e) => {
                    tracing::error!("❌ Error checking committed orders: {:?}", e);
                    // DB hatası olursa 5 saniye bekle ve tekrar dene
                    tokio::time::sleep(Duration::from_millis(10000)).await;
                    continue;
                }
            }

            // Belirtilen interval kadar bekle
            tokio::time::sleep(check_interval).await;
        }

        tracing::info!("🔚 Committed orders monitor service ended");
        Ok(())
    }

    async fn start_mempool_polling(
        market_addr: Address,
        provider: Arc<P>,
        cancel_token: CancellationToken,
        db_obj: DbObj,
        prover_addr: Address,
        boundless_service: &BoundlessMarketService<Arc<P>>,
        prover: ProverObj,
        signer: PrivateKeySigner,
        cached_config: CachedConfig,
        http_client: OptimizedHttpClient,  // ✅ Optimize edilmiş client
        config: ConfigLock
    ) -> std::result::Result<(), MarketMonitorErr> {
        tracing::info!("🎯 Starting mempool polling for market: 0x{:x}", market_addr);
        tracing::info!("🚀 Using optimized HTTP client with connection pooling");

        // Chain ID ve initial nonce'u cache'le - offchain monitor'daki gibi
        let chain_id = 8453u64;
        CACHED_CHAIN_ID.store(chain_id, Ordering::Relaxed);

        let initial_nonce = provider
            .get_transaction_count(signer.address())
            .pending()
            .await
            .context("Failed to get transaction count")?;

        CURRENT_NONCE.store(initial_nonce, Ordering::Relaxed);

        tracing::info!("✅ Cache initialized - ChainId: {}, Initial Nonce: {}", chain_id, initial_nonce);

        let mut seen_tx_hashes = std::collections::HashSet::<B256>::new();

        loop {
            tokio::select! {
                _ = cancel_token.cancelled() => {
                    tracing::info!("Mempool polling cancelled");
                    return Ok(());
                }
                _ = tokio::time::sleep(tokio::time::Duration::from_millis(20)) => {


                    // ✅ İLK KONTROL: Şu anda processing yapıyor muyuz?
                    if Self::is_currently_processing() {
                        tracing::info!("⏳ Already processing an order, NO NEED TO CHECK MEMPOOL NOW");
                        return Ok(());
                    }


                    if let Err(e) = Self::get_mempool_content(
                        &http_client,  // ✅ Optimize edilmiş client kullan
                        market_addr,
                        provider.clone(),
                        db_obj.clone(),
                        prover_addr,
                        &mut seen_tx_hashes,
                        boundless_service,
                        prover.clone(),
                        signer.clone(),
                        cached_config.clone(),
                        config.clone()
                    ).await {
                        tracing::debug!("Error getting mempool content: {:?}", e);
                    }
                }
            }
        }
    }

    async fn get_mempool_content(
        http_client: &OptimizedHttpClient,  // ✅ Optimize edilmiş client
        market_addr: Address,
        provider: Arc<P>,
        db_obj: DbObj,
        prover_addr: Address,
        seen_tx_hashes: &mut std::collections::HashSet<B256>,
        boundless_service: &BoundlessMarketService<Arc<P>>,
        prover: ProverObj,
        signer: PrivateKeySigner,
        cached_config: CachedConfig,
        config: ConfigLock
    ) -> Result<()> {
        // ✅ Optimize edilmiş HTTP client kullan
        let data = http_client.get_pending_block().await?;

        if let Some(result) = data.get("result") {
            Self::process_mempool_response(
                result,
                market_addr,
                provider,
                db_obj,
                prover_addr,
                seen_tx_hashes,
                &boundless_service,
                prover,
                signer,
                cached_config,
                config
            ).await?;
        }

        Ok(())
    }

    async fn process_mempool_response(
        result: &serde_json::Value,
        market_addr: Address,
        provider: Arc<P>,
        db_obj: DbObj,
        prover_addr: Address,
        seen_tx_hashes: &mut std::collections::HashSet<B256>,
        boundless_service: &BoundlessMarketService<Arc<P>>,
        prover: ProverObj,
        signer: PrivateKeySigner,
        cached_config: CachedConfig,
        config: ConfigLock
    ) -> Result<()> {
        if let Some(transactions) = result.get("transactions").and_then(|t| t.as_array()) {
            // ✅ Cache'den allowed requestors'u al
            let allowed_requestors_opt = &cached_config.allowed_requestor_addresses;

            for tx_data in transactions {
                // Check FROM address first (like Node.js FROM_FILTER)
                if let Some(from_addr) = tx_data.get("from").and_then(|f| f.as_str()) {
                    if let Ok(parsed_from) = from_addr.parse::<Address>() {
                        // Apply FROM filter if configured
                        if let Some(allow_addresses) = allowed_requestors_opt {
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

                                                // Process the transaction
                                                if let Err(e) = Self::process_market_tx(
                                                    tx_data,  // JSON tx data'sını direkt geç
                                                    provider.clone(),
                                                    market_addr,
                                                    db_obj.clone(),
                                                    prover_addr,
                                                    &boundless_service,
                                                    prover.clone(),
                                                    signer.clone(),
                                                    cached_config.clone(),
                                                    config.clone()
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
        tx_data: &serde_json::Value,
        provider: Arc<P>,
        market_addr: Address,
        db_obj: DbObj,
        prover_addr: Address,
        boundless_service: &BoundlessMarketService<Arc<P>>,
        prover: ProverObj,
        signer: PrivateKeySigner,
        cached_config: CachedConfig,
        config: ConfigLock
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

        tracing::info!("   - Request ID: 0x{:x}", request_id);

        // ✅ Cache'den allowed requestors kontrolü - sadece bir if ile!
        if let Some(allow_addresses) = &cached_config.allowed_requestor_addresses {
            if !allow_addresses.contains(&client_addr) {
                tracing::debug!("🚫 Client not in allowed requestors, skipping");
                return Ok(());
            }
        }

        // Get chain ID from cache and create order - offchain monitor'daki gibi
        let chain_id = CACHED_CHAIN_ID.load(Ordering::Relaxed);

        let mut new_order = OrderRequest::new(
            decoded.request.clone(),
            decoded.clientSignature.clone(),
            FulfillmentType::LockAndFulfill,
            market_addr,
            chain_id,
        );

        // ✅ Cache'den lockin_priority_gas al
        let lockin_priority_gas = cached_config.lockin_priority_gas;

        // ✅ Optimize edilmiş HTTP client oluştur
        let http_client = OptimizedHttpClient::new(cached_config.http_rpc_url.clone());

        // send_private_transaction'ı optimize edilmiş client ile çağır
        match Self::send_private_transaction(
            &decoded.request,
            &decoded.clientSignature,
            &signer,
            market_addr,
            &http_client,  // ✅ Optimize edilmiş client geç
            lockin_priority_gas.unwrap_or(5000000),
            provider.clone(),
        ).await {
            Ok(lock_block) => {
                tracing::info!("✅ Successfully locked request: 0x{:x} at block {}", request_id, lock_block);

                // RPC senkronizasyonu için küçük bir gecikme ekle
                tracing::info!("⏳⏳⏳⏳⏳⏳⏳⏳⏳ Waiting for RPC to sync lock block... ⏳⏳⏳⏳⏳⏳⏳⏳");
                tokio::time::sleep(tokio::time::Duration::from_millis(5000)).await; // 500 ms bekleme

                // Calculate lock price and save to DB
                let lock_timestamp = provider
                    .get_block_by_number(lock_block.into())
                    .await
                    .context("------------------------ lock_timestamp : Failed to get lock block.....")?
                    .context("Lock block not found")?
                    .header
                    .timestamp;

                let lock_price = new_order
                    .request
                    .offer
                    .price_at(lock_timestamp)
                    .context("------------------------ lock_price : Failed to calculate lock price.....")?;

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
                        tracing::info!("⚠️ Confirmed data fetch failed: {} - using mempool data", e);
                        new_order // Eski değerler kalsın
                    }
                };

                if let Err(e) = db_obj.insert_accepted_request(&final_order, lock_price).await {
                    tracing::error!("Failed to insert accepted request: {:?}", e);
                }else{
                    // ✅ DB'ye başarılı yazıldıktan SONRA processing = true
                    Self::set_processing_true();
                    tracing::info!("💾 Order successfully saved to DB, processing flag set to TRUE");

                    // ✅ Async dinleme servisini başlat
                    let db_clone = db_obj.clone();
                    tokio::spawn(async move {
                        if let Err(e) = Self::start_committed_orders_monitor(db_clone, cached_config.clone(), config).await {
                            tracing::error!("❌ Committed orders monitor error: {:?}", e);
                            // Hata durumunda processing'i true yap cünkü lock işlemine başlamak için bir sebep olamaz. durması daha evla.
                            Self::set_processing_true();
                        }
                    });
                }
            }
            Err(err) => {
                tracing::info!("❌ Failed to lock request: 0x{:x}, error: {}", request_id, err);

                if let Err(e) = db_obj.insert_skipped_request(&new_order).await {
                    tracing::info!("Failed to insert skipped request: {:?}", e);
                }
            }
        }

        Ok(())
    }

    // ✅ Optimize edilmiş send_private_transaction - connection pooling ile
    async fn send_private_transaction(
        request: &boundless_market::contracts::ProofRequest,
        client_signature: &alloy::primitives::Bytes,
        signer: &PrivateKeySigner,
        contract_address: Address,
        http_client: &OptimizedHttpClient,  // ✅ Optimize edilmiş client
        lockin_priority_gas: u64,
        provider: Arc<P>,
    ) -> Result<u64, anyhow::Error> {
        // Cache'den chain_id ve nonce al - provider'a sormuyoruz!
        let chain_id = CACHED_CHAIN_ID.load(Ordering::Relaxed);
        let current_nonce = CURRENT_NONCE.load(Ordering::Relaxed);
        CURRENT_NONCE.store(current_nonce + 1, Ordering::Relaxed);

        let lock_call = IBoundlessMarket::lockRequestCall {
            request: request.clone(),
            clientSignature: client_signature.clone(),
        };

        let lock_calldata = lock_call.abi_encode();

        let max_priority_fee_per_gas = lockin_priority_gas.into();
        let min_competitive_gas = 60_000_000u128;
        let base_fee = min_competitive_gas;
        let max_fee_per_gas = base_fee + max_priority_fee_per_gas;

        let tx = TxEip1559 {
            chain_id,
            nonce: current_nonce,
            gas_limit: 500_000u64,
            max_fee_per_gas,
            max_priority_fee_per_gas,
            to: TxKind::Call(contract_address),
            value: U256::ZERO,
            input: lock_calldata.into(),
            access_list: Default::default(),
        };

        let signature_hash = tx.signature_hash();
        let signature = signer.sign_hash(&signature_hash).await?;
        let tx_signed = tx.into_signed(signature);
        let tx_envelope: TxEnvelope = tx_signed.into();
        let tx_encoded = tx_envelope.encoded_2718();

        let expected_tx_hash = tx_envelope.tx_hash();

        tracing::info!("------- SENDING NOW ------");
        // ✅ Optimize edilmiş HTTP client kullan - connection pooling ile
        let result = http_client.send_raw_transaction(&tx_encoded).await
            .context("Failed to send raw transaction")?;

        if let Some(error) = result.get("error") {
            let error_message = error.to_string().to_lowercase();

            if error_message.contains("nonce") {
                tracing::error!("❌ Nonce hatası: {}", error);

                // Nonce hatası varsa fresh nonce al ve cache'i güncelle
                let fresh_nonce = provider
                    .get_transaction_count(signer.address())
                    .pending()
                    .await
                    .context("Failed to get fresh transaction count")?;

                CURRENT_NONCE.store(fresh_nonce, Ordering::Relaxed);
                tracing::info!("🔄 Nonce resynchronized from {} to {}", current_nonce, fresh_nonce);

                return Err(anyhow::anyhow!("Nonce error - resynchronized: {}", error));
            }

            // Diğer hatalar için nonce'u geri al
            let prev_nonce = current_nonce;
            CURRENT_NONCE.store(prev_nonce, Ordering::Relaxed);
            tracing::warn!("⚠️ Transaction failed, rolled back nonce to: {}", prev_nonce);

            return Err(anyhow::anyhow!("Private transaction failed: {}", error));
        }

        let tx_hash = result["result"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("No transaction hash in response"))?
            .to_string();

        let tx_hash_parsed = tx_hash.parse()
            .context("Failed to parse transaction hash")?;
        tracing::info!("🎯 Private transaction hash: {}", tx_hash);

        let tx_receipt = Self::wait_for_transaction_receipt(provider.clone(), tx_hash_parsed)
            .await
            .context("Failed to get transaction receipt")?;

        if !tx_receipt.status() {
            tracing::warn!("⚠️ İşlem {} REVERT oldu. Lock alınamadı.", tx_hash);
            return Err(anyhow::anyhow!("Transaction reverted on chain"));
        }

        let lock_block = tx_receipt.block_number
            .ok_or_else(|| anyhow::anyhow!("No block number in receipt"))?;

        tracing::info!("✅ İşlem {} başarıyla onaylandı. Lock alındı. Block: {}", tx_hash, lock_block);

        Ok(lock_block)
    }

    // wait_for_transaction_receipt fonksiyonunu da ekle
    async fn wait_for_transaction_receipt(
        provider: Arc<P>,
        tx_hash: TxHash,
    ) -> Result<alloy::rpc::types::TransactionReceipt, anyhow::Error> {
        tracing::info!("⏳ İşlem onayını bekliyor: 0x{}", hex::encode(tx_hash.as_slice()));

        const RECEIPT_TIMEOUT: Duration = Duration::from_secs(60);
        const POLL_INTERVAL: Duration = Duration::from_millis(500);

        let start_time = Instant::now();

        loop {
            if start_time.elapsed() > RECEIPT_TIMEOUT {
                return Err(anyhow::anyhow!(
                    "Transaction 0x{} timeout after {} seconds",
                    hex::encode(tx_hash.as_slice()),
                    RECEIPT_TIMEOUT.as_secs()
                ));
            }

            match provider.get_transaction_receipt(tx_hash).await {
                Ok(Some(receipt)) => {
                    let elapsed = start_time.elapsed();
                    tracing::info!("✅ İşlem 0x{} başarıyla onaylandı ({:.1}s sonra)",
                                 hex::encode(tx_hash.as_slice()), elapsed.as_secs_f64());
                    return Ok(receipt);
                }
                Ok(None) => {
                    tokio::time::sleep(POLL_INTERVAL).await;
                    continue;
                }
                Err(e) => {
                    tracing::debug!("Error getting transaction receipt: {:?}, retrying...", e);
                    tokio::time::sleep(POLL_INTERVAL).await;
                    continue;
                }
            }
        }
    }

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
        let db = self.db_obj.clone();
        let prover = self.prover.clone();
        let boundless_service = self.boundless_service.clone();
        let signer = self.signer.clone();
        let cached_config = self.cached_config.clone();
        let config = self.config.clone();
        let http_client = self.http_client.clone();  // ✅ HTTP client'ı da clone'la

        Box::pin(async move {
            tracing::info!("🚀 Starting market monitor with optimized HTTP connections");

            Self::start_mempool_polling(
                market_addr,
                provider,
                cancel_token,
                db,
                prover_addr,
                &boundless_service,
                prover,
                signer,
                cached_config,
                http_client,  // ✅ Optimize edilmiş client'ı geç
                config,
            )
                .await
                .map_err(SupervisorErr::Recover)?;

            Ok(())
        })
    }
}