//! ThetaData v3 stock TRADE stream parsing and deterministic one-minute bars.
//!
//! Transport remains in trading-core-bin. This module accepts only official
//! JSON message shapes and never trusts a header alone as market data.

use chrono::{NaiveDate, SecondsFormat, TimeZone, Timelike, Utc};
use chrono_tz::America::New_York;
use serde::Deserialize;
use serde_json::{json, Value};

use crate::{FeatureError, ReplayBar};

const RTH_OPEN_MINUTE: u16 = 9 * 60 + 30;
const RTH_CLOSE_MINUTE: u16 = 16 * 60;

#[derive(Debug, Clone, PartialEq)]
pub enum ThetaStreamEvent {
    Connected,
    Disconnected,
    Trade(ThetaTrade),
    Ignored,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ThetaTrade {
    pub date: u32,
    pub ms_of_day: u32,
    pub sequence: u64,
    pub size: u64,
    pub price: f64,
}

#[derive(Deserialize)]
struct OhlcRow {
    timestamp: String,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    volume: u64,
    vwap: f64,
}

/// Parse ThetaData v3 stock history OHLC JSON. Only regular-session rows enter
/// the trading permission chain.
pub fn parse_ohlc_backfill(
    raw: &str,
    expected_date: NaiveDate,
) -> Result<Vec<ReplayBar>, FeatureError> {
    let rows: Vec<OhlcRow> = serde_json::from_str(raw)
        .map_err(|_| FeatureError::InvalidArgument("malformed ThetaData OHLC backfill json"))?;
    let mut bars = Vec::with_capacity(rows.len());
    for row in rows {
        let naive =
            chrono::NaiveDateTime::parse_from_str(&row.timestamp, "%Y-%m-%dT%H:%M:%S%.f")
                .map_err(|_| FeatureError::InvalidArgument("invalid ThetaData OHLC timestamp"))?;
        if naive.date() != expected_date {
            return Err(FeatureError::InvalidMarket("ThetaData OHLC date mismatch"));
        }
        let minute_et = (naive.time().hour() * 60 + naive.time().minute()) as u16;
        if !(RTH_OPEN_MINUTE..RTH_CLOSE_MINUTE).contains(&minute_et) {
            continue;
        }
        if ![row.open, row.high, row.low, row.close, row.vwap]
            .into_iter()
            .all(|value| value.is_finite() && value > 0.0)
            || row.low > row.high
            || row.open < row.low
            || row.open > row.high
            || row.close < row.low
            || row.close > row.high
        {
            return Err(FeatureError::InvalidMarket("invalid ThetaData OHLC row"));
        }
        let local =
            New_York
                .from_local_datetime(&naive)
                .single()
                .ok_or(FeatureError::InvalidArgument(
                    "ambiguous ThetaData OHLC time",
                ))?;
        let utc = local.with_timezone(&Utc);
        bars.push(ReplayBar {
            occurred_at_utc: utc.to_rfc3339_opts(SecondsFormat::Secs, true),
            timestamp_et: local.to_rfc3339_opts(SecondsFormat::Secs, false),
            occurred_at_utc_ms: utc.timestamp_millis(),
            minute_et,
            open: row.open,
            high: row.high,
            low: row.low,
            close: row.close,
            volume: row.volume,
            vwap: Some(row.vwap),
        });
    }
    bars.sort_by_key(|bar| bar.occurred_at_utc_ms);
    if bars
        .windows(2)
        .any(|pair| pair[0].minute_et >= pair[1].minute_et)
    {
        return Err(FeatureError::InvalidMarket(
            "unordered ThetaData OHLC backfill",
        ));
    }
    Ok(bars)
}

#[derive(Deserialize)]
struct Header {
    #[serde(rename = "type")]
    kind: String,
    status: String,
}

#[derive(Deserialize)]
struct Contract {
    security_type: String,
    root: String,
}

#[derive(Deserialize)]
struct TradePayload {
    date: u32,
    ms_of_day: u32,
    sequence: u64,
    size: u64,
    price: f64,
}

#[derive(Deserialize)]
struct Message {
    header: Header,
    contract: Option<Contract>,
    trade: Option<TradePayload>,
}

pub fn subscribe_trade_request(symbol: &str, request_id: u64) -> Value {
    json!({
        "msg_type": "STREAM",
        "sec_type": "STOCK",
        "req_type": "TRADE",
        "add": true,
        "id": request_id,
        "contract": { "root": symbol }
    })
}

pub fn parse_stream_message(
    raw: &str,
    expected_symbol: &str,
) -> Result<ThetaStreamEvent, FeatureError> {
    let message: Message = serde_json::from_str(raw)
        .map_err(|_| FeatureError::InvalidArgument("malformed ThetaData stream json"))?;
    match message.header.status.as_str() {
        "DISCONNECTED" => return Ok(ThetaStreamEvent::Disconnected),
        "CONNECTED" => {}
        _ => {
            return Err(FeatureError::InvalidMarket(
                "unknown ThetaData connection status",
            ))
        }
    }
    if message.header.kind != "TRADE" {
        return Ok(ThetaStreamEvent::Ignored);
    }
    let (Some(contract), Some(trade)) = (message.contract, message.trade) else {
        return Ok(ThetaStreamEvent::Connected);
    };
    if contract.security_type != "STOCK" || contract.root != expected_symbol {
        return Err(FeatureError::InvalidMarket("unexpected ThetaData contract"));
    }
    if trade.ms_of_day >= 86_400_000
        || trade.size == 0
        || !trade.price.is_finite()
        || trade.price <= 0.0
    {
        return Err(FeatureError::InvalidMarket("invalid ThetaData stock trade"));
    }
    Ok(ThetaStreamEvent::Trade(ThetaTrade {
        date: trade.date,
        ms_of_day: trade.ms_of_day,
        sequence: trade.sequence,
        size: trade.size,
        price: trade.price,
    }))
}

#[derive(Debug, Clone)]
struct PendingBar {
    date: u32,
    minute_et: u16,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    volume: u64,
    notional: f64,
}

impl PendingBar {
    fn new(trade: &ThetaTrade) -> Self {
        PendingBar {
            date: trade.date,
            minute_et: (trade.ms_of_day / 60_000) as u16,
            open: trade.price,
            high: trade.price,
            low: trade.price,
            close: trade.price,
            volume: trade.size,
            notional: trade.price * trade.size as f64,
        }
    }

    fn update(&mut self, trade: &ThetaTrade) {
        self.high = self.high.max(trade.price);
        self.low = self.low.min(trade.price);
        self.close = trade.price;
        self.volume = self.volume.saturating_add(trade.size);
        self.notional += trade.price * trade.size as f64;
    }

    fn finalize(self) -> Result<ReplayBar, FeatureError> {
        let date = NaiveDate::parse_from_str(&self.date.to_string(), "%Y%m%d")
            .map_err(|_| FeatureError::InvalidArgument("invalid ThetaData trade date"))?;
        let hour = u32::from(self.minute_et / 60);
        let minute = u32::from(self.minute_et % 60);
        let naive = date
            .and_hms_opt(hour, minute, 0)
            .ok_or(FeatureError::InvalidArgument(
                "invalid ThetaData trade time",
            ))?;
        let local =
            New_York
                .from_local_datetime(&naive)
                .single()
                .ok_or(FeatureError::InvalidArgument(
                    "ambiguous ThetaData trade time",
                ))?;
        let utc = local.with_timezone(&Utc);
        Ok(ReplayBar {
            occurred_at_utc: utc.to_rfc3339_opts(SecondsFormat::Secs, true),
            timestamp_et: local.to_rfc3339_opts(SecondsFormat::Secs, false),
            occurred_at_utc_ms: utc.timestamp_millis(),
            minute_et: self.minute_et,
            open: self.open,
            high: self.high,
            low: self.low,
            close: self.close,
            volume: self.volume,
            vwap: Some(self.notional / self.volume as f64),
        })
    }
}

#[derive(Default)]
pub struct ThetaBarAggregator {
    pending: Option<PendingBar>,
    last_sequence: Option<u64>,
}

impl ThetaBarAggregator {
    /// Consume one trade. Returns the previous finalized minute when the stream
    /// crosses a minute boundary. Duplicate sequences are ignored.
    pub fn push(&mut self, trade: ThetaTrade) -> Result<Option<ReplayBar>, FeatureError> {
        if self.last_sequence == Some(trade.sequence) {
            return Ok(None);
        }
        if self
            .last_sequence
            .is_some_and(|sequence| trade.sequence < sequence)
        {
            return Err(FeatureError::InvalidMarket(
                "out-of-order ThetaData trade sequence",
            ));
        }
        self.last_sequence = Some(trade.sequence);
        let minute = (trade.ms_of_day / 60_000) as u16;
        if !(RTH_OPEN_MINUTE..RTH_CLOSE_MINUTE).contains(&minute) {
            return Ok(None);
        }
        let key = (trade.date, minute);
        match self.pending.as_mut() {
            None => {
                self.pending = Some(PendingBar::new(&trade));
                Ok(None)
            }
            Some(pending) if (pending.date, pending.minute_et) == key => {
                pending.update(&trade);
                Ok(None)
            }
            Some(pending) if (pending.date, pending.minute_et) < key => {
                let completed = self.pending.take().expect("pending exists").finalize()?;
                self.pending = Some(PendingBar::new(&trade));
                Ok(Some(completed))
            }
            Some(_) => Err(FeatureError::InvalidMarket(
                "out-of-order ThetaData trade time",
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn trade_json(symbol: &str, ms: u32, seq: u64, price: f64, size: u64) -> String {
        json!({
            "header": {"type": "TRADE", "status": "CONNECTED"},
            "contract": {"security_type": "STOCK", "root": symbol},
            "trade": {
                "ms_of_day": ms, "sequence": seq, "size": size,
                "condition": 0, "price": price, "exchange": 57, "date": 20260720
            }
        })
        .to_string()
    }

    #[test]
    fn official_stock_trade_shape_parses_strictly() {
        let event =
            parse_stream_message(&trade_json("QQQ", 34_200_001, 10, 500.25, 2), "QQQ").unwrap();
        let ThetaStreamEvent::Trade(trade) = event else {
            panic!("expected trade")
        };
        assert_eq!(trade.sequence, 10);
        assert_eq!(trade.price, 500.25);
        assert!(parse_stream_message(&trade_json("SPY", 34_200_001, 11, 600.0, 1), "QQQ").is_err());
    }

    #[test]
    fn trades_aggregate_to_provider_vwap_minute_bar() {
        let mut aggregator = ThetaBarAggregator::default();
        let inputs = [
            ThetaTrade {
                date: 20260720,
                ms_of_day: 34_200_001,
                sequence: 1,
                size: 2,
                price: 500.0,
            },
            ThetaTrade {
                date: 20260720,
                ms_of_day: 34_230_000,
                sequence: 2,
                size: 1,
                price: 503.0,
            },
            ThetaTrade {
                date: 20260720,
                ms_of_day: 34_260_000,
                sequence: 3,
                size: 1,
                price: 501.0,
            },
        ];
        assert!(aggregator.push(inputs[0].clone()).unwrap().is_none());
        assert!(aggregator.push(inputs[1].clone()).unwrap().is_none());
        let bar = aggregator.push(inputs[2].clone()).unwrap().unwrap();
        assert_eq!(bar.minute_et, 570);
        assert_eq!(bar.open, 500.0);
        assert_eq!(bar.high, 503.0);
        assert_eq!(bar.close, 503.0);
        assert_eq!(bar.volume, 3);
        assert!((bar.vwap.unwrap() - 501.0).abs() < 1e-12);
        assert_eq!(bar.occurred_at_utc, "2026-07-20T13:30:00Z");
    }

    #[test]
    fn duplicate_and_out_of_order_sequences_fail_safely() {
        let mut aggregator = ThetaBarAggregator::default();
        let first = ThetaTrade {
            date: 20260720,
            ms_of_day: 34_200_000,
            sequence: 10,
            size: 1,
            price: 500.0,
        };
        assert!(aggregator.push(first.clone()).unwrap().is_none());
        assert!(aggregator.push(first).unwrap().is_none());
        let old = ThetaTrade {
            date: 20260720,
            ms_of_day: 34_200_100,
            sequence: 9,
            size: 1,
            price: 500.1,
        };
        assert!(aggregator.push(old).is_err());
    }

    #[test]
    fn official_ohlc_backfill_shape_parses_to_rth_bars() {
        let raw = json!([
            {"timestamp":"2026-07-20T09:29:00.000","open":499.0,"high":499.0,"low":499.0,"close":499.0,"volume":10,"count":1,"vwap":499.0},
            {"timestamp":"2026-07-20T09:30:00.000","open":500.0,"high":501.0,"low":499.5,"close":500.5,"volume":100,"count":20,"vwap":500.2}
        ])
        .to_string();
        let bars =
            parse_ohlc_backfill(&raw, NaiveDate::from_ymd_opt(2026, 7, 20).unwrap()).unwrap();
        assert_eq!(bars.len(), 1);
        assert_eq!(bars[0].minute_et, 570);
        assert_eq!(bars[0].occurred_at_utc, "2026-07-20T13:30:00Z");
        assert_eq!(bars[0].vwap, Some(500.2));
    }
}
