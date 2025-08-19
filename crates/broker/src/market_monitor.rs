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

// ✅ WebSocket imports
use tokio_tungstenite::{connect_async, tungstenite::Message};
use futures_util::stream::{SplitSink, SplitStream};
use tokio::sync::{mpsc, oneshot, Mutex};
use std::collections::HashMap;

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
}

// ✅ WebSocket Manager
#[derive(Debug)]
pub struct WebSocketRequest {
    pub method: String,
    pub params: serde_json::Value,
    pub response_tx: oneshot::Sender<Result<serde_json::Value>>,
}

pub struct WebSocketManager {
    sender: mpsc::UnboundedSender<WebSocketRequest>,
    _handle: tokio::task::JoinHandle<()>,
}

impl WebSocketManager {
    pub async fn new(ws_url: String) -> Result<Self> {
        let (tx, rx) = mpsc::unbounded_channel::<WebSocketRequest>();

        // WebSocket connection task'ı spawn et
        let handle = tokio::spawn(Self::websocket_task(ws_url, rx));

        Ok(Self {
            sender: tx,
            _handle: handle,
        })
    }

    // Persistent WebSocket connection
    async fn websocket_task(
        ws_url: String,
        mut request_rx: mpsc::UnboundedReceiver<WebSocketRequest>
    ) {
        let mut reconnect_attempts = 0;
        const MAX_RECONNECT_ATTEMPTS: u32 = 5;

        loop {
            match Self::connect_and_handle(&ws_url, &mut request_rx).await {
                Ok(_) => {
                    tracing::info!("✅ WebSocket connection ended normally");
                    break;
                }
                Err(e) => {
                    reconnect_attempts += 1;
                    tracing::error!("❌ WebSocket error (attempt {}): {}", reconnect_attempts, e);

                    if reconnect_attempts >= MAX_RECONNECT_ATTEMPTS {
                        tracing::error!("🚫 Max reconnection attempts reached");
                        break;
                    }

                    tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;
                }
            }
        }
    }

    async fn connect_and_handle(
        ws_url: &str,
        request_rx: &mut mpsc::UnboundedReceiver<WebSocketRequest>
    ) -> Result<()> {
        tracing::info!("🔗 Connecting to WebSocket: {}", ws_url);

        let (ws_stream, _) = connect_async(ws_url).await
            .context("Failed to connect to WebSocket")?;

        let (mut write, mut read) = ws_stream.split();

        let pending_requests = Arc::new(Mutex::new(HashMap::<u64, oneshot::Sender<Result<serde_json::Value>>>::new()));
        let mut request_id_counter = 1u64;

        // Response handler task - ✅ Make it mutable
        let pending_clone = pending_requests.clone();
        let mut response_handler = tokio::spawn(async move {
            while let Some(message) = read.next().await {
                match message {
                    Ok(Message::Text(text)) => {
                        if let Ok(response) = serde_json::from_str::<serde_json::Value>(&text) {
                            if let Some(id) = response.get("id").and_then(|v| v.as_u64()) {
                                let mut pending = pending_clone.lock().await;
                                if let Some(tx) = pending.remove(&id) {
                                    let _ = tx.send(Ok(response));
                                }
                            }
                        }
                    }
                    Ok(Message::Close(_)) => {
                        tracing::warn!("⚠️ WebSocket connection closed by server");
                        break;
                    }
                    Err(e) => {
                        tracing::error!("❌ WebSocket read error: {}", e);
                        break;
                    }
                    _ => {}
                }
            }
        });

        // Request handler loop
        loop {
            tokio::select! {
                // Handle incoming requests
                request = request_rx.recv() => {
                    match request {
                        Some(req) => {
                            let request_id = request_id_counter;
                            request_id_counter += 1;

                            // Store response channel
                            {
                                let mut pending = pending_requests.lock().await;
                                pending.insert(request_id, req.response_tx);
                            }

                            // Send WebSocket message
                            let ws_message = json!({
                                "jsonrpc": "2.0",
                                "method": req.method,
                                "params": req.params,
                                "id": request_id
                            });

                            if let Err(e) = write.send(Message::Text(ws_message.to_string())).await {
                                tracing::error!("❌ Failed to send WebSocket message: {}", e);
                                // Clean up pending request
                                let mut pending = pending_requests.lock().await;
                                if let Some(tx) = pending.remove(&request_id) {
                                    let _ = tx.send(Err(anyhow::anyhow!("WebSocket send failed: {}", e)));
                                }
                                break;
                            }
                        }
                        None => {
                            tracing::info!("📡 Request channel closed, stopping WebSocket");
                            break;
                        }
                    }
                }

                // Check if response handler died
                _ = &mut response_handler => {
                    tracing::error!("❌ Response handler task died");
                    break;
                }
            }
        }

        Ok(())
    }

    // Public method to send requests
    pub async fn send_request(&self, method: String, params: serde_json::Value) -> Result<serde_json::Value> {
        let (response_tx, response_rx) = oneshot::channel();

        let request = WebSocketRequest {
            method,
            params,
            response_tx,
        };

        self.sender.send(request)
            .map_err(|_| anyhow::anyhow!("WebSocket manager is dead"))?;

        // 10 saniye timeout
        match tokio::time::timeout(
            tokio::time::Duration::from_secs(10),
            response_rx
        ).await {
            Ok(Ok(response)) => response,
            Ok(Err(_)) => Err(anyhow::anyhow!("Response channel closed")),
            Err(_) => Err(anyhow::anyhow!("Request timeout")),
        }
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
    ws_manager: Option<WebSocketManager>,  // ✅ Make it optional for initialization
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
            let conf = config.lock_all().expect("Failed to read config during initialization");
            CachedConfig {
                allowed_requestor_addresses: conf.market.allow_requestor_addresses.clone(),
                http_rpc_url: conf.market.my_rpc_url.clone(),
                lockin_priority_gas: conf.market.lockin_priority_gas,
            }
        };

        tracing::info!("✅ MarketMonitor initialized (WebSocket will be created on spawn)");

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
            ws_manager: None,  // Will be created in spawn
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
    async fn start_committed_orders_monitor(db_obj: DbObj) -> Result<()> {
        tracing::info!("👁️👁️ Starting committed orders monitor service...");

        let check_interval = Duration::from_millis(180000); // 180 saniyede bir kontrol

        loop {
            // Committed orders'ları kontrol et
            match db_obj.get_committed_orders().await {
                Ok(committed_orders) => {
                    let count = committed_orders.len();
                    tracing::debug!("📊 Committed orders count: {}", count);

                    // Eğer committed orders 0 ise, processing'i false yap ve servisi bitir
                    if count == 0 {
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
        ws_manager: &WebSocketManager,  // ✅ WebSocket manager parametre
    ) -> std::result::Result<(), MarketMonitorErr> {
        tracing::info!("🎯 Starting mempool polling for market: 0x{:x}", market_addr);

        // ✅ Cache'den HTTP RPC URL'i al
        tracing::info!("Using RPC URL: {}", cached_config.http_rpc_url);

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
                    if let Err(e) = Self::get_mempool_content(
                        &cached_config.http_rpc_url,  // ✅ Cache'den kullan
                        market_addr,
                        provider.clone(),
                        db_obj.clone(),
                        prover_addr,
                        &mut seen_tx_hashes,
                        boundless_service,
                        prover.clone(),
                        signer.clone(),
                        cached_config.clone(),  // ✅ Cache'i geç
                        ws_manager,  // ✅ WebSocket manager geç
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
        db_obj: DbObj,
        prover_addr: Address,
        seen_tx_hashes: &mut std::collections::HashSet<B256>,
        boundless_service: &BoundlessMarketService<Arc<P>>,
        prover: ProverObj,
        signer: PrivateKeySigner,
        cached_config: CachedConfig,
        ws_manager: &WebSocketManager,  // ✅ WebSocket manager parametre
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
                db_obj,
                prover_addr,
                seen_tx_hashes,
                &boundless_service,
                prover,
                signer,
                cached_config,  // ✅ Cache'i geç
                ws_manager,  // ✅ WebSocket manager geç
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
        ws_manager: &WebSocketManager,  // ✅ WebSocket manager parametre
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
                                                    cached_config.clone(),  // ✅ Cache'i geç
                                                    ws_manager,  // ✅ WebSocket manager geç
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
        ws_manager: &WebSocketManager,  // ✅ WebSocket manager parametre
    ) -> Result<()> {

        // ✅ İLK KONTROL: Şu anda processing yapıyor muyuz?
        if Self::is_currently_processing() {
            tracing::info!("⏳ Already processing an order, skipping new request");
            return Ok(());
        }

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

        // ✅ WebSocket ile optimize edilmiş private transaction
        match Self::send_private_transaction_ws(
            &decoded.request,
            &decoded.clientSignature,
            &signer,
            market_addr,
            ws_manager,  // ✅ WebSocket manager kullan
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
                        if let Err(e) = Self::start_committed_orders_monitor(db_clone).await {
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

    // ✅ WebSocket ile optimize edilmiş send_private_transaction
    async fn send_private_transaction_ws(
        request: &boundless_market::contracts::ProofRequest,
        client_signature: &alloy::primitives::Bytes,
        signer: &PrivateKeySigner,
        contract_address: Address,
        ws_manager: &WebSocketManager,  // ✅ WebSocket manager
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

        // ✅ WebSocket ile gönder - HTTP yerine!
        let params = json!([{
            "tx": format!("0x{}", hex::encode(&tx_encoded)),
            "maxBlockNumber": "0x0",
            "source": "customer_farukest"
        }]);

        let start_time = std::time::Instant::now();

        let result = ws_manager.send_request(
            "eth_sendPrivateTransaction".to_string(),
            params
        ).await?;

        let send_duration = start_time.elapsed();
        tracing::info!("⚡ WebSocket send took: {:?}", send_duration);

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
        let cached_config = self.cached_config.clone();  // ✅ Cache'i clone'la

        // ✅ WebSocket manager'ı da clone'layıp move edelim
        let ws_url = cached_config.http_rpc_url
            .replace("https://", "wss://")
            .replace("http://", "ws://");

        Box::pin(async move {
            tracing::info!("Starting market monitor with WebSocket optimization");

            // ✅ Her spawn'da yeni WebSocket connection oluştur
            let ws_manager = match WebSocketManager::new(ws_url).await {
                Ok(manager) => manager,
                Err(e) => {
                    tracing::error!("❌ Failed to create WebSocket manager: {}", e);
                    return Err(SupervisorErr::Recover(MarketMonitorErr::UnexpectedErr(e)));
                }
            };

            Self::start_mempool_polling(
                market_addr,
                provider,
                cancel_token,
                db,
                prover_addr,
                &boundless_service,
                prover,
                signer,
                cached_config,  // ✅ Cache'i geç
                &ws_manager,  // ✅ WebSocket manager geç
            )
                .await
                .map_err(SupervisorErr::Recover)?;

            Ok(())
        })
    }
}