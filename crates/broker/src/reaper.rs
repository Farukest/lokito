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

use async_trait::async_trait;
use thiserror::Error;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::{
    config::{ConfigErr, ConfigLock},
    db::{DbError, DbObj},
    errors::CodedError,
    provers::ProverObj,
    task::{RetryRes, RetryTask, SupervisorErr},
    utils::cancel_proof_and_fail_order,
};

#[derive(Error, Debug)]
pub enum ReaperError {
    #[error("{code} DB error: {0}", code = self.code())]
    DbError(#[from] DbError),

    #[error("{code} Config error {0}", code = self.code())]
    ConfigReadErr(#[from] ConfigErr),

    #[error("{code} Failed to update expired order status: {0}", code = self.code())]
    UpdateFailed(anyhow::Error),
}

impl CodedError for ReaperError {
    fn code(&self) -> &str {
        match self {
            ReaperError::DbError(_) => "[B-REAP-001]",
            ReaperError::ConfigReadErr(_) => "[B-REAP-002]",
            ReaperError::UpdateFailed(_) => "[B-REAP-003]",
        }
    }
}

#[derive(Clone)]
pub struct ReaperTask {
    db: DbObj,
    config: ConfigLock,
    prover: ProverObj,
}

impl ReaperTask {
    pub fn new(db: DbObj, config: ConfigLock, prover: ProverObj) -> Self {
        Self { db, config, prover }
    }

    async fn check_expired_orders(&self) -> Result<(), ReaperError> {
        let grace_period = {
            let config = self.config.lock_all()?;
            config.prover.reaper_grace_period_secs
        };

        let expired_orders = self.db.get_expired_committed_orders(grace_period.into()).await?;

        if !expired_orders.is_empty() {
            info!("[B-REAP-100] Found {} expired committed orders", expired_orders.len());

            for order in expired_orders {
                let order_id = order.id();
                debug!("Setting expired order {} to failed", order_id);

                cancel_proof_and_fail_order(
                    &self.prover,
                    &self.db,
                    &order,
                    "Order expired in reaper",
                )
                .await;
                match self.db.set_order_failure(&order_id, "Order expired").await {
                    Ok(()) => {
                        warn!("Order {} has expired, marked as failed", order_id);
                    }
                    Err(err) => {
                        error!("Failed to update status for expired order {}: {}", order_id, err);
                        return Err(ReaperError::UpdateFailed(err.into()));
                    }
                }
            }
        }

        Ok(())
    }

    async fn run_reaper_loop(&self, cancel_token: CancellationToken) -> Result<(), ReaperError> {
        let interval = {
            let config = self.config.lock_all()?;
            config.prover.reaper_interval_secs
        };

        loop {
            // Wait to run the reaper on startup to allow other tasks to start.
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs(interval.into())) => {},
                _ = cancel_token.cancelled() => {
                    tracing::info!("Reaper task received cancellation, shutting down gracefully");
                    return Ok(());
                }
            }

            if let Err(err) = self.check_expired_orders().await {
                warn!("Error checking expired orders: {}", err);
            }
        }
    }
}

#[async_trait]
impl RetryTask for ReaperTask {
    type Error = ReaperError;

    fn spawn(&self, cancel_token: CancellationToken) -> RetryRes<Self::Error> {
        let this = self.clone();
        Box::pin(async move {
            this.run_reaper_loop(cancel_token).await.map_err(SupervisorErr::Recover)?;
            Ok(())
        })
    }
}
