//! Deterministic low-level features. Decision labels remain in Python.

use crate::model::{MarketBar, OptionQuote, OptionRight, SESSION_OPEN_MINUTE_ET};
use crate::{DataHealth, FeatureError};

const TRADING_DAYS: f64 = 252.0;

#[derive(Debug, Clone, PartialEq)]
pub struct FeatureValue<T> {
    pub value: T,
    pub data_health: DataHealth,
    pub reasons: Vec<&'static str>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct OpeningRange {
    pub high: f64,
    pub low: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct StraddleMark {
    pub underlying: String,
    pub expiry: String,
    pub strike: f64,
    pub occurred_at_utc_ms: i64,
    pub call_mid: f64,
    pub put_mid: f64,
}

impl StraddleMark {
    pub fn mark(&self) -> f64 {
        self.call_mid + self.put_mid
    }
}

pub fn assess_bar_health(bars: &[MarketBar]) -> Result<FeatureValue<()>, FeatureError> {
    if bars.is_empty() {
        return Err(FeatureError::EmptyInput);
    }
    for bar in bars {
        bar.validate()?;
    }
    let mut reasons: Vec<&'static str> = Vec::new();
    if bars[0].minute_et != SESSION_OPEN_MINUTE_ET {
        reasons.push("session_open_missing");
    }
    if bars.windows(2).any(|pair| {
        pair[1].occurred_at_utc_ms - pair[0].occurred_at_utc_ms != 60_000
            || pair[1].minute_et != pair[0].minute_et + 1
    }) {
        reasons.push("minute_gap_or_duplicate");
    }
    if bars.iter().any(|bar| bar.vwap.is_none()) {
        reasons.push("provider_bar_vwap_missing");
    }
    Ok(FeatureValue {
        value: (),
        data_health: if reasons.is_empty() {
            DataHealth::Healthy
        } else {
            DataHealth::Degraded
        },
        reasons,
    })
}

pub fn session_vwap(bars: &[MarketBar]) -> Result<FeatureValue<f64>, FeatureError> {
    if bars.is_empty() {
        return Err(FeatureError::EmptyInput);
    }
    let mut cumulative_volume = 0_u64;
    let mut cumulative_price_volume = 0.0;
    let mut fallback = false;
    for bar in bars {
        bar.validate()?;
        let price = match bar.vwap {
            Some(value) => value,
            None => {
                fallback = true;
                bar.close
            }
        };
        cumulative_volume = cumulative_volume.saturating_add(bar.volume);
        cumulative_price_volume += price * bar.volume as f64;
    }
    let value = if cumulative_volume == 0 {
        fallback = true;
        bars.iter().map(|bar| bar.close).sum::<f64>() / bars.len() as f64
    } else {
        cumulative_price_volume / cumulative_volume as f64
    };
    Ok(FeatureValue {
        value,
        data_health: if fallback {
            DataHealth::Degraded
        } else {
            DataHealth::Healthy
        },
        reasons: if fallback {
            vec!["provider_bar_vwap_missing"]
        } else {
            Vec::new()
        },
    })
}

pub fn opening_range(bars: &[MarketBar], minutes: u16) -> Result<OpeningRange, FeatureError> {
    if minutes == 0 {
        return Err(FeatureError::InvalidArgument(
            "opening range minutes must be positive",
        ));
    }
    let mut window: Vec<&MarketBar> = bars
        .iter()
        .filter(|bar| {
            bar.minute_et >= SESSION_OPEN_MINUTE_ET
                && bar.minute_et < SESSION_OPEN_MINUTE_ET + minutes
        })
        .collect();
    window.sort_by_key(|bar| bar.minute_et);
    if window.len() != usize::from(minutes)
        || window
            .iter()
            .enumerate()
            .any(|(index, bar)| bar.minute_et != SESSION_OPEN_MINUTE_ET + index as u16)
    {
        return Err(FeatureError::IncompleteOpeningRange);
    }
    Ok(OpeningRange {
        high: window
            .iter()
            .map(|bar| bar.high)
            .fold(f64::NEG_INFINITY, f64::max),
        low: window
            .iter()
            .map(|bar| bar.low)
            .fold(f64::INFINITY, f64::min),
    })
}

pub fn historical_volatility(daily_closes: &[f64], window: usize) -> Result<f64, FeatureError> {
    if window < 2 {
        return Err(FeatureError::InvalidArgument(
            "HV window must be at least two",
        ));
    }
    if daily_closes.len() < window + 1 {
        return Err(FeatureError::InsufficientHistory);
    }
    if daily_closes
        .iter()
        .any(|value| !value.is_finite() || *value <= 0.0)
    {
        return Err(FeatureError::InvalidMarket(
            "daily closes must be positive and finite",
        ));
    }
    let closes = &daily_closes[daily_closes.len() - window - 1..];
    let returns: Vec<f64> = closes
        .windows(2)
        .map(|pair| (pair[1] / pair[0]).ln())
        .collect();
    let mean = returns.iter().sum::<f64>() / returns.len() as f64;
    let variance = returns
        .iter()
        .map(|value| (value - mean).powi(2))
        .sum::<f64>()
        / (returns.len() - 1) as f64;
    Ok(variance.sqrt() * TRADING_DAYS.sqrt())
}

pub fn hv20_hv60(daily_closes: &[f64]) -> Result<(f64, f64), FeatureError> {
    Ok((
        historical_volatility(daily_closes, 20)?,
        historical_volatility(daily_closes, 60)?,
    ))
}

pub fn bid_ask_spread(quote: &OptionQuote) -> Result<f64, FeatureError> {
    quote.validate()?;
    Ok(quote.ask - quote.bid)
}

pub fn quote_age_ms(quote: &OptionQuote, as_of_utc_ms: i64) -> Result<u64, FeatureError> {
    if quote.occurred_at_utc_ms > as_of_utc_ms {
        return Err(FeatureError::FutureQuote);
    }
    Ok((as_of_utc_ms - quote.occurred_at_utc_ms) as u64)
}

pub fn atm_straddle(
    quotes: &[OptionQuote],
    spot: f64,
    underlying: &str,
    expiry: &str,
    as_of_utc_ms: i64,
    max_quote_age_ms: u64,
) -> Result<StraddleMark, FeatureError> {
    if !spot.is_finite() || spot <= 0.0 {
        return Err(FeatureError::InvalidArgument(
            "spot must be positive and finite",
        ));
    }
    let mut eligible: Vec<&OptionQuote> = Vec::new();
    for quote in quotes {
        quote.validate()?;
        if quote.underlying.eq_ignore_ascii_case(underlying)
            && quote.expiry == expiry
            && quote_age_ms(quote, as_of_utc_ms)? <= max_quote_age_ms
        {
            eligible.push(quote);
        }
    }
    let strike = eligible
        .iter()
        .min_by(|left, right| {
            (left.strike - spot)
                .abs()
                .total_cmp(&(right.strike - spot).abs())
        })
        .map(|quote| quote.strike)
        .ok_or(FeatureError::MissingOptionLeg)?;
    let legs: Vec<&OptionQuote> = eligible
        .into_iter()
        .filter(|quote| quote.strike == strike)
        .collect();
    let calls: Vec<&OptionQuote> = legs
        .iter()
        .copied()
        .filter(|quote| quote.right == OptionRight::C)
        .collect();
    let puts: Vec<&OptionQuote> = legs
        .iter()
        .copied()
        .filter(|quote| quote.right == OptionRight::P)
        .collect();
    if calls.len() != 1 || puts.len() != 1 {
        return Err(FeatureError::MissingOptionLeg);
    }
    let call = calls[0];
    let put = puts[0];
    if call.occurred_at_utc_ms != put.occurred_at_utc_ms {
        return Err(FeatureError::MismatchedQuoteTime);
    }
    Ok(StraddleMark {
        underlying: underlying.to_ascii_uppercase(),
        expiry: expiry.to_owned(),
        strike,
        occurred_at_utc_ms: call.occurred_at_utc_ms,
        call_mid: (call.bid + call.ask) / 2.0,
        put_mid: (put.bid + put.ask) / 2.0,
    })
}
