use alloy::primitives::{U256, address};
use bodkin::pons::curve::{
    CurveState, amount_out, effective_opening_bps, fdv_quote, min_out_as_last_in_block,
    min_out_from_rate, progress, quote_buy, quote_sell, spot_price, with_entry_tax,
};

fn fresh(over: impl FnOnce(&mut CurveState)) -> CurveState {
    let mut s = CurveState {
        quote_reserve: U256::from(1_680_000_000_000_000_000u128),
        token_reserve: U256::from(1_000_000_000u128) * U256::from(10u128.pow(18)),
        real_quote_reserve: U256::ZERO,
        sellable_tokens: "714285714285714285714285715".parse().unwrap(),
        reserved_tokens: "285714285714285714285714285".parse().unwrap(),
        graduation_threshold: U256::from(4_200_000_000_000_000_000u128),
        fee_bps: U256::from(100u64),
        creator_tax_bps: U256::from(100u64),
        opening_tax_bps: U256::ZERO,
        graduated: false,
        ready_to_graduate: false,
        launched_at: 0,
        read_at_ms: 0,
        read_block: 0,
        read_chain_ts: 0,
        snipe_tax_start_bps: U256::from(9900u64),
        snipe_tax_seconds: U256::from(3u64),
    };
    over(&mut s);
    s
}

#[test]
fn builder_buy_is_about_3_percent() {
    let q = quote_buy(&fresh(|_| {}), U256::from(53_519_145_802_650_970u128));
    let share = bodkin::fmt::wei_to_f64(q.tokens_out) / 1e27;
    assert!(share > 0.0295 && share < 0.0305, "got {share}");
    assert!(q.refund.is_zero());
    assert!(!q.clamped);
}

#[test]
fn round_trip_loses_more_than_the_fee() {
    let s = fresh(|_| {});
    let spend = U256::from(10u128.pow(17));
    let q = quote_buy(&s, spend);
    let mut after = s.clone();
    after.quote_reserve =
        s.quote_reserve + spend - (spend * U256::from(200u64) / U256::from(10_000u64));
    after.token_reserve = s.token_reserve - q.tokens_out;
    let back = quote_sell(&after, q.tokens_out);
    assert!(back < spend);
    assert!(back > spend * U256::from(95u64) / U256::from(100u64));
}

#[test]
fn opening_tax_capped_so_buyer_nets_1_percent() {
    assert_eq!(
        effective_opening_bps(&fresh(|s| s.opening_tax_bps = U256::from(9_900u64))),
        U256::from(9_700u64)
    );
    assert_eq!(
        effective_opening_bps(&fresh(|s| s.opening_tax_bps = U256::from(250u64))),
        U256::from(250u64)
    );
    assert_eq!(
        effective_opening_bps(&fresh(|s| s.opening_tax_bps = U256::ZERO)),
        U256::ZERO
    );
}

#[test]
fn ninety_nine_percent_tax_nearly_worthless() {
    let taxed = quote_buy(
        &fresh(|s| s.opening_tax_bps = U256::from(9_900u64)),
        U256::from(10u128.pow(17)),
    );
    let clean = quote_buy(&fresh(|_| {}), U256::from(10u128.pow(17)));
    assert!(taxed.tokens_out * U256::from(20u64) < clean.tokens_out);
}

#[test]
fn min_out_uses_modeled_entry_tax_not_stale_opening_tax() {
    let spend = U256::from(10u128.pow(17));
    let sibling = U256::from(100_000_000_000_000_000u128);
    let stale = fresh(|s| s.opening_tax_bps = U256::from(9_900u64));
    let normalized = with_entry_tax(&stale, 19);
    let min_out = min_out_as_last_in_block(&normalized, spend, sibling, 300);
    let at_19 = min_out_as_last_in_block(
        &fresh(|s| s.opening_tax_bps = U256::from(19u64)),
        spend,
        sibling,
        300,
    );
    let at_9900 = min_out_as_last_in_block(&stale, spend, sibling, 300);
    // Normalized to the +2 tax the bound equals the 19 bps quote × (1 − slip);
    // the stale 9900 read would bound it at ~1% of that.
    assert_eq!(min_out, at_19);
    assert!(at_9900 * U256::from(20u64) < min_out);
}

#[test]
fn clamp_and_refund() {
    let s = fresh(|st| st.sellable_tokens = U256::from(1_000_000u128) * U256::from(10u128.pow(18)));
    let q = quote_buy(&s, U256::from(10u128.pow(18)));
    assert!(q.clamped);
    assert_eq!(q.tokens_out, s.sellable_tokens);
    assert!(q.refund > U256::ZERO);
    assert_eq!(q.spent + q.refund, U256::from(10u128.pow(18)));
}

#[test]
fn min_out_progress_fdv() {
    assert_eq!(
        min_out_from_rate(U256::from(10_000u64), 300),
        U256::from(9_700u64)
    );
    assert!(
        (progress(&fresh(
            |s| s.real_quote_reserve = U256::from(2_100_000_000_000_000_000u128)
        )) - 0.5)
            .abs()
            < 1e-9
    );
    assert_eq!(
        progress(&fresh(
            |s| s.real_quote_reserve = U256::from(9u128) * U256::from(10u128.pow(18))
        )),
        1.0
    );
    let px = spot_price(&fresh(|_| {}));
    assert!((px - 1.68e-9).abs() < 1e-12);
    assert!((fdv_quote(&fresh(|_| {})) - 1.68).abs() < 1e-6);
    assert_eq!(
        amount_out(U256::from(100u64), U256::from(1000u64), U256::from(1000u64)),
        U256::from(90u64)
    );
    let _ = address!("0x0000000000000000000000000000000000000000");
}
