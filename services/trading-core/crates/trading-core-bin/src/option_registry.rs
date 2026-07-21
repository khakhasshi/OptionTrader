//! Trusted ThetaData option proof registry.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::{DateTime, Utc};
use optiontrader_proto::execution_v1::{CandidateTradePlan, OptionRight as ExecutionRight};
use optiontrader_proto::market_v1::{
    theta_data_sdk_service_client::ThetaDataSdkServiceClient, ThetaOptionContractRequest,
    ThetaOptionRight, ThetaOptionSnapshot, ThetaOptionSnapshotBatch, ThetaOptionSnapshotRequest,
};
use prost::Message;
use rust_decimal::Decimal;
use sha2::{Digest, Sha256};

#[derive(Clone)]
pub enum OptionRegistryAuthority {
    Sdk(TrustedOptionRegistry),
    #[cfg(test)]
    Fixture,
}

impl OptionRegistryAuthority {
    pub fn from_endpoint(endpoint: String) -> Self {
        Self::Sdk(TrustedOptionRegistry::new(endpoint))
    }

    pub async fn verify(
        &self,
        plan: &CandidateTradePlan,
        now: DateTime<Utc>,
        force_refresh: bool,
    ) -> bool {
        match self {
            Self::Sdk(registry) => registry.verify(plan, now, force_refresh).await,
            #[cfg(test)]
            Self::Fixture => true,
        }
    }
}

#[derive(Clone)]
pub struct TrustedOptionRegistry {
    endpoint: String,
    batches: Arc<Mutex<BTreeMap<String, ThetaOptionSnapshotBatch>>>,
}

impl TrustedOptionRegistry {
    pub fn new(endpoint: String) -> Self {
        Self {
            endpoint,
            batches: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    async fn verify(
        &self,
        plan: &CandidateTradePlan,
        now: DateTime<Utc>,
        force_refresh: bool,
    ) -> bool {
        if plan.legs.is_empty() || plan.legs.len() > 4 {
            return false;
        }
        let expected_id = match plan
            .legs
            .iter()
            .map(|leg| {
                leg.quote
                    .as_ref()
                    .map(|quote| quote.chain_snapshot_id.as_str())
            })
            .collect::<Option<Vec<_>>>()
        {
            Some(ids) if ids.iter().all(|id| !id.is_empty() && *id == ids[0]) => ids[0],
            _ => return false,
        };
        if let Ok(mut cache) = self.batches.lock() {
            cache.retain(|_, batch| batch_valid(batch, now));
            if !force_refresh {
                if let Some(batch) = cache.get(expected_id) {
                    return cached_batch_matches(plan, batch, now);
                }
            }
        } else {
            return false;
        }

        let contracts = match plan
            .legs
            .iter()
            .map(|leg| {
                let right = match ExecutionRight::try_from(leg.option_right).ok()? {
                    ExecutionRight::Call => ThetaOptionRight::Call,
                    ExecutionRight::Put => ThetaOptionRight::Put,
                    ExecutionRight::Unspecified => return None,
                };
                Some(ThetaOptionContractRequest {
                    contract_id: leg.contract_id.clone(),
                    symbol: leg.symbol.clone(),
                    expiration: leg.expiry.clone(),
                    strike: leg.strike.clone(),
                    right: right as i32,
                })
            })
            .collect::<Option<Vec<_>>>()
        {
            Some(value) => value,
            None => return false,
        };
        let mut client = match tokio::time::timeout(
            Duration::from_secs(2),
            ThetaDataSdkServiceClient::connect(self.endpoint.clone()),
        )
        .await
        {
            Ok(Ok(client)) => client,
            _ => return false,
        };
        let response = match tokio::time::timeout(
            Duration::from_secs(2),
            client.get_option_snapshots(ThetaOptionSnapshotRequest { contracts }),
        )
        .await
        {
            Ok(Ok(response)) => response.into_inner(),
            _ => return false,
        };
        if !batch_valid(&response, now)
            || response.chain_snapshot_id != expected_id
            || !snapshots_match(plan, &response.snapshots)
        {
            return false;
        }
        self.batches
            .lock()
            .map(|mut cache| {
                while cache.len() >= 256 {
                    cache.pop_first();
                }
                cache.insert(response.chain_snapshot_id.clone(), response);
                true
            })
            .unwrap_or(false)
    }
}

fn cached_batch_matches(
    plan: &CandidateTradePlan,
    batch: &ThetaOptionSnapshotBatch,
    now: DateTime<Utc>,
) -> bool {
    batch_valid(batch, now) && snapshots_match(plan, &batch.snapshots)
}

fn batch_valid(batch: &ThetaOptionSnapshotBatch, now: DateTime<Utc>) -> bool {
    if batch.provider != "THETADATA" || !batch.fetched_at_utc.ends_with('Z') {
        return false;
    }
    let Ok(fetched) = DateTime::parse_from_rfc3339(&batch.fetched_at_utc) else {
        return false;
    };
    let age = now.timestamp_millis() - fetched.timestamp_millis();
    if !(-5_000..=10_000).contains(&age) {
        return false;
    }
    let mut digest = Sha256::new();
    for snapshot in &batch.snapshots {
        digest.update(snapshot.encode_to_vec());
    }
    batch.chain_snapshot_id == format!("thetaopt_{:x}", digest.finalize())
}

fn decimal_equal(left: &str, right: &str) -> bool {
    Decimal::from_str_exact(left)
        .ok()
        .zip(Decimal::from_str_exact(right).ok())
        .is_some_and(|(left, right)| left == right)
}

fn snapshots_match(plan: &CandidateTradePlan, snapshots: &[ThetaOptionSnapshot]) -> bool {
    plan.legs.len() == snapshots.len()
        && plan.legs.iter().zip(snapshots).all(|(leg, snapshot)| {
            let Some(quote) = leg.quote.as_ref() else {
                return false;
            };
            snapshot.provider == "THETADATA"
                && snapshot.contract_id == leg.contract_id
                && snapshot.symbol == leg.symbol
                && snapshot.expiration == leg.expiry
                && decimal_equal(&snapshot.strike, &leg.strike)
                && snapshot.right
                    == match ExecutionRight::try_from(leg.option_right).ok() {
                        Some(ExecutionRight::Call) => ThetaOptionRight::Call as i32,
                        Some(ExecutionRight::Put) => ThetaOptionRight::Put as i32,
                        _ => return false,
                    }
                && decimal_equal(&snapshot.bid, &quote.bid)
                && decimal_equal(&snapshot.ask, &quote.ask)
                && snapshot.bid_size == quote.bid_size
                && snapshot.ask_size == quote.ask_size
                && snapshot.occurred_at_utc == quote.occurred_at_utc
                && decimal_equal(&snapshot.delta, &quote.delta)
                && decimal_equal(&snapshot.gamma, &quote.gamma)
                && decimal_equal(&snapshot.theta, &quote.theta)
                && decimal_equal(&snapshot.vega, &quote.vega)
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use optiontrader_proto::execution_v1::{CandidateLeg, OptionQuoteProof, OrderSide};

    #[test]
    fn cached_snapshot_requires_exact_quote_and_greeks_content() {
        let snapshot = ThetaOptionSnapshot {
            contract_id: "QQQ-C-500".into(),
            symbol: "QQQ".into(),
            expiration: "2026-07-21".into(),
            strike: "500".into(),
            right: ThetaOptionRight::Call as i32,
            bid: "2.4".into(),
            ask: "2.5".into(),
            bid_size: 20,
            ask_size: 25,
            occurred_at_utc: "2026-07-21T14:30:00.000Z".into(),
            delta: "0.52".into(),
            gamma: "0.08".into(),
            theta: "-0.12".into(),
            vega: "0.05".into(),
            provider: "THETADATA".into(),
        };
        let mut plan = CandidateTradePlan {
            legs: vec![CandidateLeg {
                side: OrderSide::Buy as i32,
                option_right: ExecutionRight::Call as i32,
                contract_id: snapshot.contract_id.clone(),
                expiry: snapshot.expiration.clone(),
                strike: snapshot.strike.clone(),
                quantity: 1,
                quote: Some(OptionQuoteProof {
                    bid: snapshot.bid.clone(),
                    ask: snapshot.ask.clone(),
                    bid_size: 20,
                    ask_size: 25,
                    occurred_at_utc: snapshot.occurred_at_utc.clone(),
                    delta: snapshot.delta.clone(),
                    gamma: snapshot.gamma.clone(),
                    theta: snapshot.theta.clone(),
                    vega: snapshot.vega.clone(),
                    chain_snapshot_id: "thetaopt_x".into(),
                    provider: "THETADATA".into(),
                }),
                broker_contract_id: "123".into(),
                symbol: "QQQ".into(),
                exchange: "SMART".into(),
            }],
            ..CandidateTradePlan::default()
        };
        assert!(snapshots_match(&plan, std::slice::from_ref(&snapshot)));
        plan.legs[0].quote.as_mut().unwrap().delta = "0.51".into();
        assert!(!snapshots_match(&plan, &[snapshot]));
    }

    #[test]
    fn batch_identity_and_wall_clock_freshness_are_both_authoritative() {
        let snapshot = ThetaOptionSnapshot {
            contract_id: "QQQ-C-500".into(),
            symbol: "QQQ".into(),
            expiration: "2026-07-21".into(),
            strike: "500".into(),
            right: ThetaOptionRight::Call as i32,
            bid: "2.4".into(),
            ask: "2.5".into(),
            bid_size: 20,
            ask_size: 25,
            occurred_at_utc: "2026-07-21T14:30:00.000Z".into(),
            delta: "0.52".into(),
            gamma: "0.08".into(),
            theta: "-0.12".into(),
            vega: "0.05".into(),
            provider: "THETADATA".into(),
        };
        let id = format!("thetaopt_{:x}", Sha256::digest(snapshot.encode_to_vec()));
        let now: DateTime<Utc> = "2026-07-21T14:30:01Z".parse().unwrap();
        let mut batch = ThetaOptionSnapshotBatch {
            chain_snapshot_id: id.clone(),
            fetched_at_utc: "2026-07-21T14:30:00.000Z".into(),
            snapshots: vec![snapshot],
            provider: "THETADATA".into(),
        };
        assert!(batch_valid(&batch, now));
        batch.chain_snapshot_id = "thetaopt_forged".into();
        assert!(!batch_valid(&batch, now));
        batch.chain_snapshot_id = id;
        batch.fetched_at_utc = "2026-07-21T14:29:00.000Z".into();
        assert!(!batch_valid(&batch, now));
    }

    #[tokio::test]
    async fn confirm_refresh_bypasses_valid_cache_and_stale_cache_is_evicted() {
        let snapshot = ThetaOptionSnapshot {
            contract_id: "QQQ-C-500".into(),
            symbol: "QQQ".into(),
            expiration: "2026-07-21".into(),
            strike: "500".into(),
            right: ThetaOptionRight::Call as i32,
            bid: "2.4".into(),
            ask: "2.5".into(),
            bid_size: 20,
            ask_size: 25,
            occurred_at_utc: "2026-07-21T14:30:00.000Z".into(),
            delta: "0.52".into(),
            gamma: "0.08".into(),
            theta: "-0.12".into(),
            vega: "0.05".into(),
            provider: "THETADATA".into(),
        };
        let id = format!("thetaopt_{:x}", Sha256::digest(snapshot.encode_to_vec()));
        let plan = CandidateTradePlan {
            legs: vec![CandidateLeg {
                side: OrderSide::Buy as i32,
                option_right: ExecutionRight::Call as i32,
                contract_id: snapshot.contract_id.clone(),
                expiry: snapshot.expiration.clone(),
                strike: snapshot.strike.clone(),
                quantity: 1,
                quote: Some(OptionQuoteProof {
                    bid: snapshot.bid.clone(),
                    ask: snapshot.ask.clone(),
                    bid_size: snapshot.bid_size,
                    ask_size: snapshot.ask_size,
                    occurred_at_utc: snapshot.occurred_at_utc.clone(),
                    delta: snapshot.delta.clone(),
                    gamma: snapshot.gamma.clone(),
                    theta: snapshot.theta.clone(),
                    vega: snapshot.vega.clone(),
                    chain_snapshot_id: id.clone(),
                    provider: "THETADATA".into(),
                }),
                broker_contract_id: "123".into(),
                symbol: "QQQ".into(),
                exchange: "SMART".into(),
            }],
            ..CandidateTradePlan::default()
        };
        let batch = ThetaOptionSnapshotBatch {
            chain_snapshot_id: id.clone(),
            fetched_at_utc: "2026-07-21T14:30:00.000Z".into(),
            snapshots: vec![snapshot],
            provider: "THETADATA".into(),
        };
        let registry = TrustedOptionRegistry::new("http://127.0.0.1:1".into());
        registry.batches.lock().unwrap().insert(id.clone(), batch);
        let now: DateTime<Utc> = "2026-07-21T14:30:01Z".parse().unwrap();
        assert!(registry.verify(&plan, now, false).await);
        assert!(!registry.verify(&plan, now, true).await);

        let stale_now: DateTime<Utc> = "2026-07-21T14:30:11Z".parse().unwrap();
        assert!(!registry.verify(&plan, stale_now, false).await);
        assert!(!registry.batches.lock().unwrap().contains_key(&id));
    }
}
