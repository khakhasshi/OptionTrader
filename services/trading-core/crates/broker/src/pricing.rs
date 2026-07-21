use chrono::{DateTime, Utc};
use rust_decimal::Decimal;

use crate::{AdaptiveLimitPolicy, OrderSide, QuoteProof};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdaptivePriceError {
    InvalidPolicy,
    InvalidQuote,
    StaleQuote,
    CrossedQuote,
    SpreadTooWide,
    InvalidProtectionPrice,
}

/// Price one adaptive-limit attempt between mid and the opposite touch.
///
/// `protection_price` is an immutable worst acceptable price approved by risk:
/// a BUY never exceeds it and a SELL never goes below it. Bad quote data never
/// falls back to a touch or market order.
pub fn price_adaptive_limit(
    side: OrderSide,
    quote: &QuoteProof,
    policy: &AdaptiveLimitPolicy,
    attempt: u32,
    protection_price: Decimal,
    now: DateTime<Utc>,
) -> Result<Decimal, AdaptivePriceError> {
    if policy.initial_aggressiveness_bps > 10_000
        || policy.max_attempts == 0
        || attempt >= policy.max_attempts
        || policy.max_quote_age_ms == 0
        || policy.max_spread_bps == 0
    {
        return Err(AdaptivePriceError::InvalidPolicy);
    }
    if quote.bid <= Decimal::ZERO
        || quote.ask <= Decimal::ZERO
        || quote.tick_size <= Decimal::ZERO
        || quote.occurred_at > now
    {
        return Err(AdaptivePriceError::InvalidQuote);
    }
    if quote.bid > quote.ask {
        return Err(AdaptivePriceError::CrossedQuote);
    }
    let age_ms = (now - quote.occurred_at).num_milliseconds();
    if age_ms < 0 || age_ms as u64 > policy.max_quote_age_ms {
        return Err(AdaptivePriceError::StaleQuote);
    }
    let mid = (quote.bid + quote.ask) / Decimal::TWO;
    let spread = quote.ask - quote.bid;
    if mid <= Decimal::ZERO
        || spread * Decimal::from(10_000u32) > mid * Decimal::from(policy.max_spread_bps)
    {
        return Err(AdaptivePriceError::SpreadTooWide);
    }
    if protection_price <= Decimal::ZERO {
        return Err(AdaptivePriceError::InvalidProtectionPrice);
    }

    let start = Decimal::from(policy.initial_aggressiveness_bps) / Decimal::from(10_000u32);
    let aggressiveness = if policy.max_attempts == 1 {
        start
    } else {
        let progress = Decimal::from(attempt) / Decimal::from(policy.max_attempts - 1);
        start + (Decimal::ONE - start) * progress
    };
    let half_spread = spread / Decimal::TWO;
    let raw = match side {
        OrderSide::Buy => mid + aggressiveness * half_spread,
        OrderSide::Sell => mid - aggressiveness * half_spread,
    };
    let rounded = round_aggressively(raw, quote.tick_size, side);
    let protected = match side {
        OrderSide::Buy => rounded.min(protection_price),
        OrderSide::Sell => rounded.max(protection_price),
    };
    if protected <= Decimal::ZERO {
        return Err(AdaptivePriceError::InvalidProtectionPrice);
    }
    Ok(protected)
}

fn round_aggressively(price: Decimal, tick: Decimal, side: OrderSide) -> Decimal {
    let units = price / tick;
    let whole = match side {
        OrderSide::Buy => units.ceil(),
        OrderSide::Sell => units.floor(),
    };
    whole * tick
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeDelta;

    fn now() -> DateTime<Utc> {
        "2026-07-21T14:30:00Z".parse().unwrap()
    }

    fn quote() -> QuoteProof {
        QuoteProof {
            bid: Decimal::new(200, 2),
            ask: Decimal::new(240, 2),
            tick_size: Decimal::new(1, 2),
            occurred_at: now() - TimeDelta::milliseconds(50),
        }
    }

    fn policy() -> AdaptiveLimitPolicy {
        AdaptiveLimitPolicy {
            initial_aggressiveness_bps: 3_000,
            max_attempts: 3,
            max_quote_age_ms: 500,
            max_spread_bps: 2_000,
        }
    }

    #[test]
    fn buy_and_sell_walk_from_mid_toward_touch() {
        let buy0 = price_adaptive_limit(
            OrderSide::Buy,
            &quote(),
            &policy(),
            0,
            Decimal::new(240, 2),
            now(),
        )
        .unwrap();
        let buy2 = price_adaptive_limit(
            OrderSide::Buy,
            &quote(),
            &policy(),
            2,
            Decimal::new(240, 2),
            now(),
        )
        .unwrap();
        let sell0 = price_adaptive_limit(
            OrderSide::Sell,
            &quote(),
            &policy(),
            0,
            Decimal::new(200, 2),
            now(),
        )
        .unwrap();
        let sell2 = price_adaptive_limit(
            OrderSide::Sell,
            &quote(),
            &policy(),
            2,
            Decimal::new(200, 2),
            now(),
        )
        .unwrap();
        assert_eq!(buy0, Decimal::new(226, 2));
        assert_eq!(buy2, Decimal::new(240, 2));
        assert_eq!(sell0, Decimal::new(214, 2));
        assert_eq!(sell2, Decimal::new(200, 2));
    }

    #[test]
    fn protection_price_is_never_crossed() {
        let buy = price_adaptive_limit(
            OrderSide::Buy,
            &quote(),
            &policy(),
            2,
            Decimal::new(230, 2),
            now(),
        )
        .unwrap();
        let sell = price_adaptive_limit(
            OrderSide::Sell,
            &quote(),
            &policy(),
            2,
            Decimal::new(210, 2),
            now(),
        )
        .unwrap();
        assert_eq!(buy, Decimal::new(230, 2));
        assert_eq!(sell, Decimal::new(210, 2));
    }

    #[test]
    fn malformed_stale_crossed_and_wide_quotes_fail_closed() {
        let mut stale = quote();
        stale.occurred_at = now() - TimeDelta::seconds(1);
        assert_eq!(
            price_adaptive_limit(
                OrderSide::Buy,
                &stale,
                &policy(),
                0,
                Decimal::new(240, 2),
                now()
            ),
            Err(AdaptivePriceError::StaleQuote)
        );
        let mut crossed = quote();
        crossed.bid = Decimal::new(250, 2);
        assert_eq!(
            price_adaptive_limit(
                OrderSide::Buy,
                &crossed,
                &policy(),
                0,
                Decimal::new(240, 2),
                now()
            ),
            Err(AdaptivePriceError::CrossedQuote)
        );
        let mut wide_policy = policy();
        wide_policy.max_spread_bps = 100;
        assert_eq!(
            price_adaptive_limit(
                OrderSide::Buy,
                &quote(),
                &wide_policy,
                0,
                Decimal::new(240, 2),
                now()
            ),
            Err(AdaptivePriceError::SpreadTooWide)
        );
    }
}
