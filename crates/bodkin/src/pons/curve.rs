use crate::chain::BPS;
use alloy::primitives::U256;
use serde::{Deserialize, Serialize};

/// Curve pricing in the protocol's integer order. Fees come off the input on a buy
/// and off the output on a sell; the opening tax only ever applies to buys.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CurveState {
    pub quote_reserve: U256,
    pub token_reserve: U256,
    pub real_quote_reserve: U256,
    pub sellable_tokens: U256,
    pub reserved_tokens: U256,
    pub graduation_threshold: U256,
    pub fee_bps: U256,
    pub creator_tax_bps: U256,
    pub opening_tax_bps: U256,
    pub graduated: bool,
    pub ready_to_graduate: bool,
    pub launched_at: u64,
    pub read_at_ms: u64,
    #[serde(default)]
    pub read_block: u64,
    #[serde(default)]
    pub read_chain_ts: u64,
    /// Snapshotted on the curve in `initialize`. Factory params are owner-mutable.
    pub snipe_tax_start_bps: U256,
    pub snipe_tax_seconds: U256,
}

impl Default for CurveState {
    fn default() -> Self {
        Self {
            quote_reserve: U256::ZERO,
            token_reserve: U256::ZERO,
            real_quote_reserve: U256::ZERO,
            sellable_tokens: U256::ZERO,
            reserved_tokens: U256::ZERO,
            graduation_threshold: U256::ZERO,
            fee_bps: U256::from(100u64),
            creator_tax_bps: U256::ZERO,
            opening_tax_bps: U256::ZERO,
            graduated: false,
            ready_to_graduate: false,
            launched_at: 0,
            read_at_ms: 0,
            read_block: 0,
            read_chain_ts: 0,
            snipe_tax_start_bps: U256::from(9900u64),
            snipe_tax_seconds: U256::from(3u64),
        }
    }
}

pub fn amount_out(in_amount: U256, reserve_in: U256, reserve_out: U256) -> U256 {
    in_amount * reserve_out / (reserve_in + in_amount)
}

pub fn amount_in(out_amount: U256, reserve_in: U256, reserve_out: U256) -> U256 {
    out_amount * reserve_in / (reserve_out - out_amount) + U256::from(1u64)
}

fn ceil_div(a: U256, b: U256) -> U256 {
    (a + b - U256::from(1u64)) / b
}

/// The opening tax is capped so a buyer always nets at least 1% of the spend.
pub fn effective_opening_bps(s: &CurveState) -> U256 {
    if s.opening_tax_bps.is_zero() {
        return U256::ZERO;
    }
    let max = U256::from(BPS) - s.fee_bps - s.creator_tax_bps - U256::from(100u64);
    if s.opening_tax_bps > max {
        max
    } else {
        s.opening_tax_bps
    }
}

/// A snapshot read before the entry second still carries the launch-time tax;
/// normalize it to the tax modeled for the actual entry second before quoting.
pub fn with_entry_tax(s: &CurveState, entry_tax_bps: u64) -> CurveState {
    let mut s = s.clone();
    s.opening_tax_bps = U256::from(entry_tax_bps);
    s
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BuyQuote {
    pub tokens_out: U256,
    pub spent: U256,
    pub refund: U256,
    pub total_input_bps: U256,
    pub clamped: bool,
}

pub fn quote_buy(s: &CurveState, quote_in: U256) -> BuyQuote {
    let open_bps = effective_opening_bps(s);
    let mut spent = quote_in;
    let fee = spent * s.fee_bps / U256::from(BPS);
    let tax = spent * s.creator_tax_bps / U256::from(BPS);
    let opening = spent * open_bps / U256::from(BPS);
    let mut tokens_out = amount_out(
        spent - fee - tax - opening,
        s.quote_reserve,
        s.token_reserve,
    );
    let mut clamped = false;
    if tokens_out > s.sellable_tokens {
        clamped = true;
        tokens_out = s.sellable_tokens;
        let net = amount_in(s.sellable_tokens, s.quote_reserve, s.token_reserve);
        let denom = U256::from(BPS) - s.fee_bps - s.creator_tax_bps - open_bps;
        let grossed = ceil_div(net * U256::from(BPS), denom);
        spent = if grossed < quote_in {
            grossed
        } else {
            quote_in
        };
    }
    BuyQuote {
        tokens_out,
        spent,
        refund: quote_in - spent,
        total_input_bps: s.fee_bps + s.creator_tax_bps + open_bps,
        clamped,
    }
}

pub fn quote_sell(s: &CurveState, tokens_in: U256) -> U256 {
    let gross = amount_out(tokens_in, s.token_reserve, s.quote_reserve);
    let fee = gross * s.fee_bps / U256::from(BPS);
    let tax = gross * s.creator_tax_bps / U256::from(BPS);
    gross - fee - tax
}

/// `minTokensOut` bounds the rate, not the quantity: a clamped fill at the accepted rate still settles.
pub fn min_out_from_rate(quote: U256, slippage_bps: u64) -> U256 {
    quote * U256::from(BPS - slippage_bps) / U256::from(BPS)
}

/// Size `minTokensOut` as if this buy is last in the entry block (same-block siblings have already taken).
pub fn min_out_as_last_in_block(
    s: &CurveState,
    our_quote: U256,
    sibling_quote: U256,
    slippage_bps: u64,
) -> U256 {
    let after_siblings = {
        let q = quote_buy(s, sibling_quote);
        let mut next = s.clone();
        let spent = q.spent;
        let fee_tax =
            spent * (s.fee_bps + s.creator_tax_bps + effective_opening_bps(s)) / U256::from(BPS);
        next.quote_reserve += spent - fee_tax;
        next.token_reserve = next.token_reserve.saturating_sub(q.tokens_out);
        next.sellable_tokens = next.sellable_tokens.saturating_sub(q.tokens_out);
        next
    };
    let q = quote_buy(&after_siblings, our_quote);
    min_out_from_rate(q.tokens_out, slippage_bps)
}

pub fn spot_price(s: &CurveState) -> f64 {
    if s.token_reserve.is_zero() {
        return 0.0;
    }
    u256_to_f64(s.quote_reserve) / u256_to_f64(s.token_reserve)
}

pub fn fdv_quote(s: &CurveState) -> f64 {
    let supply = 1_000_000_000f64;
    spot_price(s) * supply
}

pub fn progress(s: &CurveState) -> f64 {
    if s.graduation_threshold.is_zero() {
        return 0.0;
    }
    let p = u256_to_f64(s.real_quote_reserve) / u256_to_f64(s.graduation_threshold);
    if p > 1.0 { 1.0 } else { p }
}

fn u256_to_f64(value: U256) -> f64 {
    f64::from(value)
}
