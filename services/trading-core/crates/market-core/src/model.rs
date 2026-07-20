//! Provider-neutral normalized market records owned by Market Core.

use crate::FeatureError;
use serde::{Deserialize, Serialize};

pub const SESSION_OPEN_MINUTE_ET: u16 = 9 * 60 + 30;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MarketBar {
    pub occurred_at_utc_ms: i64,
    pub minute_et: u16,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: u64,
    pub vwap: Option<f64>,
}

impl MarketBar {
    pub fn validate(&self) -> Result<(), FeatureError> {
        let prices = [self.open, self.high, self.low, self.close];
        if prices
            .iter()
            .any(|value| !value.is_finite() || *value <= 0.0)
        {
            return Err(FeatureError::InvalidMarket(
                "OHLC must be positive and finite",
            ));
        }
        if self.high < self.open.max(self.close).max(self.low)
            || self.low > self.open.min(self.close).min(self.high)
        {
            return Err(FeatureError::InvalidMarket("invalid OHLC relationship"));
        }
        if self.minute_et >= 24 * 60 {
            return Err(FeatureError::InvalidMarket("minute_et is outside the day"));
        }
        if self
            .vwap
            .is_some_and(|value| !value.is_finite() || value <= 0.0)
        {
            return Err(FeatureError::InvalidMarket(
                "VWAP must be positive and finite",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum OptionRight {
    C,
    P,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OptionQuote {
    pub underlying: String,
    pub expiry: String,
    pub strike: f64,
    pub right: OptionRight,
    pub occurred_at_utc_ms: i64,
    pub bid: f64,
    pub ask: f64,
}

impl OptionQuote {
    pub fn validate(&self) -> Result<(), FeatureError> {
        if self.underlying.is_empty() || self.expiry.is_empty() {
            return Err(FeatureError::InvalidMarket("option identity is incomplete"));
        }
        if !self.strike.is_finite() || self.strike <= 0.0 {
            return Err(FeatureError::InvalidMarket(
                "strike must be positive and finite",
            ));
        }
        if !self.bid.is_finite() || !self.ask.is_finite() || self.bid < 0.0 || self.ask < self.bid {
            return Err(FeatureError::InvalidMarket(
                "invalid or crossed option market",
            ));
        }
        Ok(())
    }
}

pub fn normalize_bars(mut bars: Vec<MarketBar>) -> Result<Vec<MarketBar>, FeatureError> {
    for bar in &bars {
        bar.validate()?;
    }
    bars.sort_by_key(|bar| bar.occurred_at_utc_ms);
    let mut normalized: Vec<MarketBar> = Vec::with_capacity(bars.len());
    for bar in bars {
        if let Some(previous) = normalized.last() {
            if previous.occurred_at_utc_ms == bar.occurred_at_utc_ms {
                if previous != &bar {
                    return Err(FeatureError::ConflictingDuplicate);
                }
                continue;
            }
        }
        normalized.push(bar);
    }
    Ok(normalized)
}
