use alloy::primitives::{U256, uint};
use bodkin::pons::curve::{CurveState, min_out_as_last_in_block, quote_buy};
use std::sync::OnceLock;

fn main() {
    divan::main();
}

fn curve() -> CurveState {
    CurveState {
        quote_reserve: uint!(1680000000000000000_U256),
        token_reserve: uint!(1000000000000000000000000000_U256),
        real_quote_reserve: U256::ZERO,
        sellable_tokens: uint!(714285714285714285714285715_U256),
        reserved_tokens: uint!(285714285714285714285714285_U256),
        graduation_threshold: uint!(4200000000000000000_U256),
        fee_bps: U256::from(100),
        creator_tax_bps: U256::from(100),
        opening_tax_bps: U256::from(19),
        graduated: false,
        ready_to_graduate: false,
        launched_at: 0,
        read_at_ms: 0,
        read_block: 0,
        read_chain_ts: 0,
        snipe_tax_start_bps: U256::from(9_900),
        snipe_tax_seconds: U256::from(3),
    }
}

#[divan::bench]
fn curve_quote(bencher: divan::Bencher) {
    let state = curve();
    bencher.bench_local(|| {
        quote_buy(
            divan::black_box(&state),
            U256::from(10_000_000_000_000_000u64),
        )
    });
}

#[divan::bench]
fn last_in_block_min_out(bencher: divan::Bencher) {
    let state = curve();
    bencher.bench_local(|| {
        min_out_as_last_in_block(
            divan::black_box(&state),
            U256::from(10_000_000_000_000_000u64),
            U256::from(50_000_000_000_000_000u64),
            300,
        )
    });
}

fn http_client() -> reqwest::Client {
    reqwest::Client::builder()
        .pool_max_idle_per_host(4)
        .tcp_nodelay(true)
        .http1_only()
        .build()
        .unwrap()
}

#[divan::bench]
fn cold_http_client() -> reqwest::Client {
    http_client()
}

#[divan::bench]
fn persistent_http_client_clone() -> reqwest::Client {
    static CLIENT: OnceLock<reqwest::Client> = OnceLock::new();
    CLIENT.get_or_init(http_client).clone()
}

#[divan::bench]
fn sanitize_terminal_text(bencher: divan::Bencher) {
    bencher.bench_local(|| {
        bodkin::fmt::clean_text(
            divan::black_box("\u{1b}[31m👨‍👩‍👧‍👦 launch 名称\u{1b}[0m\nignored"),
            40,
        )
    });
}
