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
use alloy::rpc::types::{Log, Transaction};
use alloy::consensus::Transaction as _;
use alloy::eips::BlockNumberOrTag;
use alloy::network::TransactionResponse;
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
    market_addr: Address,
    provider: Arc<P>,
    config: ConfigLock,
    chain_monitor: Arc<ChainMonitorService<P>>,
    db_obj: DbObj,
    prover_addr: Address,
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
        market_addr: Address,
        provider: Arc<P>,
        config: ConfigLock,
        chain_monitor: Arc<ChainMonitorService<P>>,
        db_obj: DbObj,
        prover_addr: Address,
    ) -> Self {
        Self {
            market_addr,
            provider,
            config,
            chain_monitor,
            db_obj,
            prover_addr
        }
    }





    async fn monitor_mempool_enhanced(
        market_addr: Address,
        provider: Arc<P>,
        cancel_token: CancellationToken,
        config: ConfigLock,
        db_obj: DbObj,
        prover_addr: Address,
    ) -> std::result::Result<(), MarketMonitorErr>
    where
        P: Provider<Ethereum> + 'static + Clone,
    {
        tracing::info!("🎯 Starting ENHANCED mempool monitoring for market: 0x{:x}", market_addr);

        let mut interval = tokio::time::interval(tokio::time::Duration::from_millis(5));
        let mut processed_txs: HashSet<B256> = HashSet::new();
        let mut last_block_number = 0u64;

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    // STRATEGY 1: Public mempool monitoring
                    match Self::get_market_transactions_from_txpool(&provider, market_addr).await {
                        Ok(market_txs) => {
                            for tx_hash in market_txs {
                                if processed_txs.contains(&tx_hash) {
                                    continue;
                                }
                                processed_txs.insert(tx_hash);

                                if let Err(e) = Self::process_market_tx_with_config_filter(
                                    tx_hash,
                                    provider.clone(),
                                    market_addr,
                                    config.clone(),
                                ).await {
                                    tracing::error!("Market tx processing failed: {:?}", e);
                                }
                            }
                        }
                        Err(_) => {
                            // tracing::debug!("Public mempool scan failed, trying alternatives");
                        }
                    }

                    // STRATEGY 2: Latest block monitoring (private mempool catch)
                    if let Err(e) = Self::monitor_latest_blocks(
                        &provider,
                        market_addr,
                        &mut last_block_number,
                        &mut processed_txs,
                        config.clone(),
                        db_obj.clone(),
                        prover_addr.clone()
                    ).await {
                        tracing::error!("Latest block monitoring failed: {:?}", e);
                    }

                    // STRATEGY 3: Event-based detection
                    if let Err(e) = Self::monitor_contract_events(
                        &provider,
                        market_addr,
                        &mut processed_txs,
                        config.clone(),
                        db_obj.clone(),
                        prover_addr.clone()
                    ).await {
                        tracing::error!("Event monitoring failed: {:?}", e);
                    }

                    // Memory cleanup
                    if processed_txs.len() > 5000 {
                        let to_keep: Vec<_> = processed_txs.iter().skip(2500).cloned().collect();
                        processed_txs = to_keep.into_iter().collect();
                    }
                }
                _ = cancel_token.cancelled() => {
                    tracing::info!("🛑 Enhanced mempool monitoring stopped");
                    return Ok(());
                }
            }
        }
    }


    // STRATEGY 3: Contract event monitoring
    async fn monitor_contract_events(
        provider: &Arc<P>,
        market_addr: Address,
        processed_txs: &mut HashSet<B256>,
        config: ConfigLock,
        db_obj: DbObj,
        prover_addr: Address,
    ) -> Result<(), anyhow::Error>
    where
        P: Provider<Ethereum> + 'static + Clone,
    {
        // Son 3 bloktan event'leri al
        let latest_block = provider.get_block_number().await?;
        let from_block = if latest_block >= 3 { latest_block - 2 } else { 0 };

        // RequestSubmitted event'ini dinle
        let filter = Filter::new()
            .address(market_addr)
            .from_block(BlockNumberOrTag::Number(from_block))
            .to_block(BlockNumberOrTag::Latest);

        if let Ok(logs) = provider.get_logs(&filter).await {
            for log in logs {
                let tx_hash = log.transaction_hash.unwrap_or_default();

                if processed_txs.contains(&tx_hash) {
                    continue;
                }

                processed_txs.insert(tx_hash);

                tracing::info!("📋 EVENT-BASED TX DETECTION: 0x{:x}", tx_hash);

                // Event'den gelen tx'i detailed analysis yap
                if let Ok(Some(tx)) = provider.get_transaction_by_hash(tx_hash).await {
                    if let Err(e) = Self::analyze_event_based_tx(
                        tx_hash,
                        tx,
                        log,
                        provider.clone(),
                        config.clone(),
                        db_obj.clone(),
                        prover_addr.clone(),
                        market_addr.clone(),
                    ).await {
                        tracing::error!("Event-based tx analysis failed: {:?}", e);
                    }
                }
            }
        }

        Ok(())
    }

    async fn analyze_mined_market_tx(
        tx_hash: B256,
        tx: Transaction,
        provider: Arc<P>,
        config: ConfigLock,
        db_obj: DbObj,
        prover_addr: Address,
        market_addr: Address,
    ) -> Result<()>
    {
        let input = tx.input().as_ref();

        let decoded = match IBoundlessMarket::submitRequestCall::abi_decode(input) {
            Ok(call) => call,
            Err(e) => {
                tracing::warn!("Failed to decode submitRequest: {}", e);
                return Ok(());
            }
        };

        let proof_request = decoded.request.clone();
        let client_signature = decoded.clientSignature.clone();

        tracing::info!("                          ");
        tracing::info!("                          ");
        tracing::info!("✅✅✅ Decoded ProofRequest: {:?}", proof_request);
        tracing::info!("                          ");
        tracing::info!("                          ");

        let client_addr = decoded.request.client_address();
        let request_id = decoded.request.id;

        tracing::info!("✅ Decoded Request PARAMS:");
        tracing::info!("   - Request ID: 0x{:x}", request_id);
        tracing::info!("   - Client: 0x{:x}", client_addr);
        tracing::info!("                          ");
        tracing::info!("                          ");



        let chain_id = provider.get_chain_id().await.context("Failed to get chain id")?;

        let mut new_order = OrderRequest::new(
            proof_request.clone(),
            client_signature.clone(),
            FulfillmentType::LockAndFulfill,
            market_addr,
            chain_id,
        );

        // Konfigürasyon ve limit parametrelerini oku
        let (
                lockin_priority_gas,
                allowed_addresses_opt,
            ) =
            {
                let locked_conf = config.lock_all().context("Failed to read config").unwrap();

                (
                    locked_conf.market.lockin_priority_gas,
                    locked_conf.market.allow_requestor_addresses.clone())
            };

        let allowed_requestors_opt = {
            let locked_conf = config.lock_all().context("Failed to read config")?;
            locked_conf.market.allow_requestor_addresses.clone()
        };

        if let Some(allow_addresses) = allowed_requestors_opt {
            if !allow_addresses.contains(&client_addr) {
                tracing::debug!("🚫 Client 0x{:x} not in allowed requestors, skipping", client_addr);
                return Ok(());
            }
        }


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


        let boundless = BoundlessMarketService::new(market_addr, provider.clone(), prover_addr.clone());

        let pre_lock_time = chrono::Utc::now();
        println!("                                   ");
        println!("                                   ");
        println!("🚀 ORDER ID : 0x{:x} ", request_id);
        println!("🚀 CHAIN_ID : {} ", chain_id);
        println!("🚀 CUZDANIM : {} ", prover_addr);
        println!("                                   ");
        println!("🚀 LOCK REQUEST STARTING: at {}", Self::format_time(pre_lock_time));
        println!("🚀 LOCK REQUEST STARTING FOR ORDER 0x{:x}", request_id);
        println!("                                   ");
        println!("                                   ");
        match boundless.lock_request(&proof_request.clone(), client_signature.clone(), lockin_priority_gas).await {
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
                println!("🚀 LOCK SUCCESS: 0x{:x} at {}", request_id, Self::format_time(success_lock_time));
                println!("                                   ");
                println!("                                   ");

                tracing::info!(
                    "Lock transaction successful for order {}, lock price: {}",
                    request_id,
                    lock_price.clone()
                );

                if let Err(e) = db_obj.insert_accepted_request(&new_order, lock_price.clone()).await {
                    tracing::error!("FATAL: Failed to insert accepted request for order 0x{:x}: {:?}", request_id, e);
                }
            }
            Err(err) => {

                let failed_lock_time = chrono::Utc::now();
                println!("                                   ");
                println!("                                   ");
                println!("❌ FAILED LOCK SUCCESS: 0x{:x} at {} BECAUSE : {}", request_id, Self::format_time(failed_lock_time), err);
                println!("                                   ");
                println!("                                   ");

                if let Err(e) = db_obj.insert_skipped_request(&new_order).await {
                    tracing::error!("Failed to insert skipped request for order 0x{:x}: {:?}", request_id, e);
                }
            }
        }




        Ok(())
    }

    // Manuel decode fonksiyonu - ABI decode başarısız olursa
    fn manual_decode_submit_request(call_data: &[u8]) -> Result<IBoundlessMarket::submitRequestCall> {
        use alloy::sol_types::SolType;

        tracing::info!("🔧 Attempting manual decode...");

        // ABI spec'e göre manuel parsing
        if call_data.len() < 64 {
            return Err(anyhow::anyhow!("Call data too short for manual decode"));
        }

        // İlk 32 byte: request struct pointer
        // İkinci 32 byte: signature offset
        let request_offset = u64::from_be_bytes(
            call_data[24..32].try_into().unwrap()
        ) as usize;

        let signature_offset = u64::from_be_bytes(
            call_data[56..64].try_into().unwrap()
        ) as usize;

        tracing::info!("   - Request offset: {}", request_offset);
        tracing::info!("   - Signature offset: {}", signature_offset);

        // Bu noktada full decode yapmak karmaşık,
        // sadece önemli alanları çıkaralım

        // Request ID (request struct'ın ilk alanı)
        if request_offset + 32 > call_data.len() {
            return Err(anyhow::anyhow!("Invalid request offset"));
        }

        let request_id_bytes: [u8; 32] = call_data[request_offset..request_offset+32]
            .try_into()
            .map_err(|_| anyhow::anyhow!("Failed to extract request ID"))?;
        let request_id = B256::from(request_id_bytes);

        tracing::info!("   - Extracted Request ID: 0x{:x}", request_id);

        // Client address çıkarmak için daha fazla parsing gerekir
        // Şimdilik basit bir struct return edelim

        Err(anyhow::anyhow!("Manual decode not fully implemented yet"))
    }


    // Event-based transaction analysis
    async fn analyze_event_based_tx(
        tx_hash: B256,
        tx: Transaction,
        log: Log,
        provider: Arc<P>,
        config: ConfigLock,
        db_obj: DbObj,
        prover_addr: Address,
        market_addr: Address,
    ) -> Result<()>
    where
        P: Provider<Ethereum> + 'static + Clone,
    {
        tracing::info!("📊 Analyzing event-based tx: 0x{:x}", tx_hash);

        // Event data'dan request ID'yi çıkar (eğer mümkünse)
        let topics = &log.topics();
        if !topics.is_empty() {
            tracing::info!("   - Event topics: {:?}", topics);
        }

        // Transaction analysis (same as mined tx)
        Self::analyze_mined_market_tx(
            tx_hash,
            tx,
            provider.clone(),
            config.clone(),
            db_obj.clone(),
            prover_addr.clone(),
            market_addr.clone(),
        ).await


    }

    // WebSocket-based real-time monitoring (bonus strategy)
    async fn monitor_websocket_mempool(
        provider: Arc<P>,
        market_addr: Address,
        config: ConfigLock,
    ) -> Result<(), anyhow::Error>
    where
        P: Provider<Ethereum> + 'static + Clone,
    {
        tracing::info!("🌐 Starting WebSocket mempool monitoring");

        // Bu kısım provider'ın WebSocket desteği varsa kullanılabilir
        // newPendingTransactions subscription'ı ile real-time monitoring

        // Implementation depends on your provider's WebSocket capabilities
        // For now, placeholder

        Ok(())
    }

    // Gelişmiş process fonksiyonu - timing bilgisi ile
    async fn process_market_tx_with_timing(
        tx_hash: B256,
        provider: Arc<P>,
        market_addr: Address,
        config: ConfigLock,
        detection_method: &str,
    ) -> Result<()>
    where
        P: Provider<Ethereum> + 'static + Clone,
    {
        let start_time = std::time::Instant::now();

        let result = Self::process_market_tx_with_config_filter(
            tx_hash,
            provider,
            market_addr,
            config,
        ).await;

        let processing_time = start_time.elapsed();
        tracing::info!("⏱️  Processing time for {} method: {:?}", detection_method, processing_time);

        result
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


    async fn get_market_transactions_from_txpool(
        provider: &Arc<P>,
        market_addr: Address,
    ) -> Result<Vec<B256>, anyhow::Error>
    where
        P: Provider<Ethereum> + 'static + Clone,
    {
        let response: serde_json::Value = provider
            .client()
            .request("txpool_content", ())
            .await?;

        let mut market_tx_hashes = Vec::new();

        if let Some(pending) = response.get("pending").and_then(|p| p.as_object()) {
            for (from_address, nonce_map) in pending {
                if let Some(nonce_obj) = nonce_map.as_object() {
                    for (nonce, tx_data) in nonce_obj {
                        // 🎯 SADECE MARKET CONTRACT'A GİDEN TX'LERİ AL
                        if let Some(to_addr) = tx_data.get("to").and_then(|t| t.as_str()) {
                            if let Ok(parsed_to) = to_addr.parse::<Address>() {
                                if parsed_to == market_addr {
                                    if let Some(hash) = tx_data.get("hash").and_then(|h| h.as_str()) {
                                        if let Ok(parsed_hash) = hash.parse::<B256>() {
                                            market_tx_hashes.push(parsed_hash);
                                            tracing::info!("🚨 MARKET TX IN MEMPOOL: 0x{:x}", parsed_hash);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        Ok(market_tx_hashes)
    }

    // STRATEGY 2: Latest block monitoring - private mempool'dan gelen tx'leri yakalar
    async fn monitor_latest_blocks(
        provider: &Arc<P>,
        market_addr: Address,
        last_block_number: &mut u64,
        processed_txs: &mut HashSet<B256>,
        config: ConfigLock,
        db_obj: DbObj,
        prover_addr: Address,
    ) -> Result<(), anyhow::Error>
    where
        P: Provider<Ethereum> + 'static + Clone,
    {
        // En son bloğu al
        let latest_block_num = provider.get_block_number().await?;

        if *last_block_number == 0 {
            *last_block_number = latest_block_num;
            return Ok(());
        }

        // Sadece yeni blokları process et
        if latest_block_num > *last_block_number {
            tracing::debug!("🔍 Checking new blocks {} to {}", *last_block_number + 1, latest_block_num);

            // Son 3 bloğu kontrol et (private mempool tx'ler genelde hemen mine olur)
            let start_block = if latest_block_num >= 3 { latest_block_num - 2 } else { *last_block_number + 1 };

            for block_num in start_block..=latest_block_num {
                if let Ok(Some(block)) = provider.get_block_by_number(BlockNumberOrTag::Number(block_num)).await {
                    if let BlockTransactions::Full(transactions) = block.transactions {
                        for tx in transactions {
                            let tx_hash = *tx.inner.hash(); // Dereference ile owned value al

                            if processed_txs.contains(&tx_hash) {
                                continue;
                            }

                            // Market contract'a giden tx mi?
                            if let Some(to_addr) = tx.to() {
                                if to_addr == market_addr {
                                    processed_txs.insert(tx_hash);

                                    tracing::info!("🚨 BLOCK TX TO MARKET: 0x{:x} in block {}", tx_hash, block_num);

                                    // Detailed analysis
                                    if let Err(e) = Self::analyze_mined_market_tx(
                                        tx_hash,
                                        tx,
                                        provider.clone(),
                                        config.clone(),
                                        db_obj.clone(),
                                        prover_addr.clone(),
                                        market_addr.clone(),
                                    ).await {
                                        tracing::error!("Mined tx analysis failed: {:?}", e);
                                    }
                                }
                            }
                        }
                    }
                }
            }

            *last_block_number = latest_block_num;
        }

        Ok(())
    }

    // 4 CONFIG BAZLI FİLTRELEME - Allowed requestors kontrolü
    async fn process_market_tx_with_config_filter(
        tx_hash: B256,
        provider: Arc<P>,
        market_addr: Address,
        config: ConfigLock,
    ) -> Result<()>
    where
        P: Provider<Ethereum> + 'static + Clone,
    {
        tracing::debug!("⚡ Processing market tx with config filter: 0x{:x}", tx_hash);

        let tx_data = match provider.get_transaction_by_hash(tx_hash).await {
            Ok(Some(tx)) => tx,
            Ok(None) => return Ok(()),
            Err(e) => {
                tracing::warn!("Failed to get tx details: {}", e);
                return Ok(());
            }
        };

        // TX zaten market contract'a gidiyor, direkt decode et
        let (_, tx_input) = match &*tx_data.inner {
            alloy::consensus::TxEnvelope::Eip4844(tx) => (tx.to(), tx.input().clone()),
            alloy::consensus::TxEnvelope::Eip1559(tx) => (tx.to(), tx.input().clone()),
            alloy::consensus::TxEnvelope::Eip2930(tx) => (tx.to(), tx.input().clone()),
            alloy::consensus::TxEnvelope::Legacy(tx) => (tx.to(), tx.input().clone()),
            _ => return Ok(()),
        };


        let input = tx_input.as_ref();
        // let submit_request_selector = IBoundlessMarket::submitRequestCall::SELECTOR;
        //
        // if input.len() < 4 {
        //     tracing::debug!("Input too short, skipping");
        //     return Ok(());
        // }
        //
        // if &input[0..4] != submit_request_selector.as_slice() {
        //     tracing::debug!("Not submitRequest method, skipping");
        //     return Ok(());
        // }

        // Hızlı decode et (zaten çok hızlı)
        let decoded = match IBoundlessMarket::submitRequestCall::abi_decode(&input[4..]) {
            Ok(call) => call,
            Err(e) => return Ok(()),
        };

        let client_addr = decoded.request.client_address();

        let allowed_requestors_opt = {
            let locked_conf = config.lock_all().context("Failed to read config")?;
            locked_conf.market.allow_requestor_addresses.clone()
        };

        if let Some(allow_addresses) = allowed_requestors_opt {
            if !allow_addresses.contains(&client_addr) {
                tracing::debug!("🚫 Client 0x{:x} not in allowed requestors, skipping", client_addr);
                return Ok(());
            }
        }

        // 🎯🚨 TARGET HIT - ALLOWED REQUESTOR ORDER DETECTED!
        tracing::error!("🎯🚨 ALLOWED REQUESTOR ORDER FOUND: 0x{:x} (tx: 0x{:x})",
                   decoded.request.id, tx_hash);
        tracing::error!("   - Request ID: 0x{:x}", decoded.request.id);
        tracing::error!("   - Client: 0x{:x} ✅ ALLOWED", client_addr);




        // let submit_request_selector = IBoundlessMarket::submitRequestCall::SELECTOR;
        // let input = tx_input.as_ref();
        //
        // if input.len() < 4 || input[0..4] != submit_request_selector {
        //     tracing::debug!("Not a submitRequest call, skipping");
        //     return Ok(());
        // }


        // Gas price bilgisi
        // if let Ok(Some(tx_receipt)) = provider.get_transaction_by_hash(tx_hash).await {
        //     tracing::error!("   - Gas Price: {:?}", tx_receipt.gas_price());
        //     tracing::error!("   - Gas Limit: {:?}", tx_receipt.gas_limit());
        // }

        // ⚡ FRONT-RUNNING LOGIC BURAYA
        // 1. Higher gas price hesapla
        // 2. Same request'i submit et
        // 3. User tx fail edecek

        tracing::error!("🚀 EXECUTING FRONT-RUNNING STRATEGY FOR ALLOWED USER!");

        Ok(())
    }


}


// Yardımcı struct'lar
#[derive(Debug)]
pub struct DetectionStats {
    public_mempool_hits: u64,
    private_mempool_catches: u64,
    event_based_catches: u64,
    total_processed: u64,
}

impl DetectionStats {
    pub fn new() -> Self {
        Self {
            public_mempool_hits: 0,
            private_mempool_catches: 0,
            event_based_catches: 0,
            total_processed: 0,
        }
    }

    pub fn log_stats(&self) {
        tracing::info!("📊 Detection Statistics:");
        tracing::info!("   - Public mempool: {}", self.public_mempool_hits);
        tracing::info!("   - Private mempool catches: {}", self.private_mempool_catches);
        tracing::info!("   - Event-based catches: {}", self.event_based_catches);
        tracing::info!("   - Total processed: {}", self.total_processed);
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
        let market_addr = self.market_addr;
        let provider = self.provider.clone();
        let prover_addr = self.prover_addr;
        let chain_monitor = self.chain_monitor.clone();
        let db = self.db_obj.clone();
        // let prover = self.prover.clone();
        let config = self.config.clone();

        Box::pin(async move {
            tracing::info!("Starting up market monitor");


            tokio::try_join!(
                 Self::monitor_mempool_enhanced(
                    market_addr,
                    provider.clone(),
                    cancel_token.clone(),
                    config.clone(),
                    db.clone(),
                    prover_addr.clone(),
                ),
            )
                .map_err(SupervisorErr::Recover)?;

            Ok(())
        })
    }
}




