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

use std::time::Duration;

use alloy::{
    network::{EthereumWallet, TransactionBuilder},
    primitives::{
        utils::{format_units, parse_ether, parse_units},
        U256,
    },
    providers::{Provider, ProviderBuilder},
    rpc::types::TransactionRequest,
    signers::local::PrivateKeySigner,
    sol,
};
use anyhow::Result;
use boundless_market::{client::Client, Deployment};
use clap::Parser;
use url::Url;

const TX_TIMEOUT: Duration = Duration::from_secs(180);

sol! {
    #[sol(rpc)]
    contract IERC20 {
        function approve(address spender, uint256 amount) external returns (bool);
        function allowance(address owner, address spender) external view returns (uint256);
        function transfer(address to, uint256 amount) external returns (bool);
        function balanceOf(address owner) external view returns (uint256);
    }
}

/// Arguments of the order generator.
#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct MainArgs {
    /// URL of the Ethereum RPC endpoint.
    #[clap(short, long, env)]
    rpc_url: Url,
    /// Private key used to sign and submit requests.
    #[clap(long, env)]
    private_key: PrivateKeySigner,
    /// List of prover private keys
    #[clap(long, env, value_delimiter = ',')]
    prover_keys: Vec<PrivateKeySigner>,
    /// List of order generator private keys
    #[clap(long, env, value_delimiter = ',')]
    order_generator_keys: Vec<PrivateKeySigner>,
    /// Slasher private key
    #[clap(long, env)]
    slasher_key: PrivateKeySigner,
    /// If prover ETH balance is above this threshold, transfer 75% of the ETH to distributor
    #[clap(long, env, default_value = "1.0")]
    prover_eth_donate_threshold: String,
    /// If ETH balance is below this threshold, transfer ETH to address
    #[clap(long, env, default_value = "0.1")]
    eth_threshold: String,
    /// If stake balance is below this threshold, transfer stake to address
    #[clap(long, env, default_value = "1.0")]
    stake_threshold: String,
    /// Amount of ETH to transfer from distributor to account during top up
    #[clap(long, env, default_value = "0.1")]
    eth_top_up_amount: String,
    /// Amount of stake to transfer from distributor to prover during top up
    #[clap(long, env, default_value = "10")]
    stake_top_up_amount: String,
    /// Deployment to use
    #[clap(flatten, next_help_heading = "Boundless Market Deployment")]
    deployment: Option<Deployment>,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .json()
        .with_target(false)
        .with_ansi(false)
        .init();

    let args = MainArgs::parse();

    // NOTE: Using a separate `run` function to facilitate testing below.
    let result = run(&args).await;
    if let Err(e) = result {
        tracing::error!("FATAL: {:?}", e);
    }

    Ok(())
}

async fn run(args: &MainArgs) -> Result<()> {
    let distributor_wallet = EthereumWallet::from(args.private_key.clone());
    let distributor_address = distributor_wallet.default_signer().address();
    let distributor_provider =
        ProviderBuilder::new().wallet(distributor_wallet).connect_http(args.rpc_url.clone());

    let distributor_client = Client::builder()
        .with_rpc_url(args.rpc_url.clone())
        .with_private_key(args.private_key.clone())
        .with_deployment(args.deployment.clone())
        .build()
        .await?;

    // Parse thresholds
    let prover_eth_donate_threshold = parse_ether(&args.prover_eth_donate_threshold)?;
    let stake_token_decimals = distributor_client.boundless_market.stake_token_decimals().await?;
    let eth_threshold = parse_ether(&args.eth_threshold)?;
    let stake_threshold: U256 = parse_units(&args.stake_threshold, stake_token_decimals)?.into();
    let eth_top_up_amount = parse_ether(&args.eth_top_up_amount)?;
    let stake_top_up_amount: U256 =
        parse_units(&args.stake_top_up_amount, stake_token_decimals)?.into();

    // check top up amounts are greater than thresholds
    if eth_top_up_amount < eth_threshold {
        tracing::error!("ETH top up amount is less than threshold");
        return Err(anyhow::anyhow!(
            "ETH top up amount is less than threshold [top up amount: {}, threshold: {}]",
            format_units(eth_top_up_amount, "ether")?,
            format_units(eth_threshold, "ether")?
        ));
    }
    if stake_top_up_amount < stake_threshold {
        tracing::error!("Stake top up amount is less than threshold");
        return Err(anyhow::anyhow!(
            "Stake top up amount is less than threshold [top up amount: {}, threshold: {}]",
            format_units(stake_top_up_amount, stake_token_decimals)?,
            format_units(stake_threshold, stake_token_decimals)?
        ));
    }

    tracing::info!("Distributor address: {}", distributor_address);

    // Transfer ETH from provers to the distributor from provers if above threshold
    for prover_key in &args.prover_keys {
        let prover_wallet = EthereumWallet::from(prover_key.clone());
        let prover_provider =
            ProviderBuilder::new().wallet(prover_wallet.clone()).connect_http(args.rpc_url.clone());
        let prover_address = prover_wallet.default_signer().address();

        let prover_eth_balance = distributor_client.provider().get_balance(prover_address).await?;

        tracing::info!(
            "Prover {} has {} ETH balance. Threshold for donation to distributor is {}.",
            prover_address,
            format_units(prover_eth_balance, "ether")?,
            format_units(prover_eth_donate_threshold, "ether")?
        );

        if prover_eth_balance > prover_eth_donate_threshold {
            // Transfer 80% of the balance to the distributor (leave 20% for future gas)
            let transfer_amount =
                prover_eth_balance.saturating_mul(U256::from(8)).div_ceil(U256::from(10)); // Leave some for gas

            tracing::info!(
                "Transferring {} ETH from prover {} to distributor",
                format_units(transfer_amount, "ether")?,
                prover_address
            );

            let tx = TransactionRequest::default()
                .with_from(prover_address)
                .with_to(distributor_address)
                .with_value(transfer_amount);

            let pending_tx = prover_provider.send_transaction(tx).await?;

            // Wait for the transaction to be confirmed
            let receipt = pending_tx.with_timeout(Some(TX_TIMEOUT)).watch().await?;

            tracing::info!(
                "Transfer completed: {:x} from prover {} for {} ETH to distributor",
                receipt,
                prover_address,
                format_units(transfer_amount, "ether")?
            );
        }
    }

    tracing::info!("Topping up stake for provers if below threshold");

    // Top up stake for provers if below threshold
    for prover_key in &args.prover_keys {
        let prover_wallet = EthereumWallet::from(prover_key.clone());
        let prover_address = prover_wallet.default_signer().address();

        let stake_token = distributor_client.boundless_market.stake_token_address().await?;
        let stake_token_contract = IERC20::new(stake_token, distributor_provider.clone());

        let distributor_stake_balance =
            stake_token_contract.balanceOf(distributor_address).call().await?;
        let prover_stake_balance_market =
            distributor_client.boundless_market.balance_of_stake(prover_address).await?;

        tracing::info!("Account {} has {} stake balance deposited to market. Threshold for top up is {}. Distributor has {} stake balance (Stake token: 0x{:x}). ", prover_address, format_units(prover_stake_balance_market, stake_token_decimals)?, format_units(stake_threshold, stake_token_decimals)?, format_units(distributor_stake_balance, stake_token_decimals)?, stake_token);

        if prover_stake_balance_market < stake_threshold {
            let mut prover_stake_balance_contract =
                stake_token_contract.balanceOf(prover_address).call().await?;

            let transfer_amount = stake_top_up_amount.saturating_sub(prover_stake_balance_market);

            if transfer_amount > distributor_stake_balance {
                tracing::error!("[B-DIST-STK]: Distributor {} has insufficient stake balance to top up prover {} with {} stake", distributor_address, prover_address, format_units(transfer_amount, stake_token_decimals)?);
                continue;
            }

            if transfer_amount == U256::ZERO {
                tracing::error!(
                    "Misconfiguration: stake top up amount too low, or threshold too high"
                );
                continue;
            }

            tracing::info!(
                "Transferring {} stake from distributor to prover {} [stake top up amount: {}, balance on market: {}, balance on contract: {}]",
                format_units(transfer_amount, stake_token_decimals)?,
                prover_address,
                format_units(stake_top_up_amount, stake_token_decimals)?,
                format_units(prover_stake_balance_market, stake_token_decimals)?,
                format_units(prover_stake_balance_contract, stake_token_decimals)?
            );
            let pending_tx =
                stake_token_contract.transfer(prover_address, transfer_amount).send().await?;

            let receipt = pending_tx.with_timeout(Some(TX_TIMEOUT)).watch().await?;

            tracing::info!("Stake transfer completed: {:x} from distributor to prover {}. About to deposit stake", receipt, prover_address);

            // Then have the prover deposit the stake
            let prover_client = Client::builder()
                .with_rpc_url(args.rpc_url.clone())
                .with_private_key(prover_key.clone())
                .with_timeout(Some(TX_TIMEOUT))
                .build()
                .await?;

            prover_stake_balance_contract =
                stake_token_contract.balanceOf(prover_address).call().await?;

            prover_client
                .boundless_market
                .deposit_stake_with_permit(prover_stake_balance_contract, prover_key)
                .await?;
            tracing::info!("Stake deposit completed for prover {}", prover_address);
        }
    }

    // Top up ETH for all accounts if below threshold
    let all_accounts = [
        args.prover_keys.iter().collect::<Vec<_>>(),
        args.order_generator_keys.iter().collect::<Vec<_>>(),
        vec![&args.slasher_key],
    ]
    .concat();

    for key in all_accounts {
        let wallet = EthereumWallet::from(key.clone());
        let address = wallet.default_signer().address();

        let account_eth_balance = distributor_client.provider().get_balance(address).await?;
        let distributor_eth_balance =
            distributor_client.provider().get_balance(distributor_address).await?;

        tracing::info!("Account {} has {} ETH balance. Threshold for top up is {}. Distributor has {} ETH balance. ", address, format_units(account_eth_balance, "ether")?, format_units(eth_threshold, "ether")?, format_units(distributor_eth_balance, "ether")?);

        if account_eth_balance < eth_threshold {
            let transfer_amount = eth_top_up_amount.saturating_sub(account_eth_balance);

            if transfer_amount > distributor_eth_balance {
                tracing::error!("[B-DIST-ETH]: Distributor {} has insufficient ETH balance to top up {} with {} ETH.", distributor_address, address, format_units(transfer_amount, "ether")?);
                continue;
            }

            if transfer_amount == U256::ZERO {
                tracing::error!("Misconfiguration: ETH top up amount too low, or threshold too high [top up amount: {}, address 0x{:x} balance: {}, distributor balance: {}]", format_units(eth_top_up_amount, "ether")?, address, format_units(account_eth_balance, "ether")?, format_units(distributor_eth_balance, "ether")?);
                continue;
            }

            tracing::info!(
                "Transferring {} ETH from distributor to {}",
                format_units(transfer_amount, "ether")?,
                address
            );

            // Transfer ETH for gas
            let tx = TransactionRequest::default()
                .with_from(distributor_address)
                .with_to(address)
                .with_value(transfer_amount);

            let pending_tx = distributor_client.provider().send_transaction(tx).await?;

            let receipt = pending_tx.with_timeout(Some(TX_TIMEOUT)).watch().await?;

            tracing::info!(
                "ETH transfer completed: {:x}. {} ETH from distributor to {}",
                receipt,
                format_units(transfer_amount, "ether")?,
                address
            );
        }
    }

    Ok(())
}


