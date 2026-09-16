use alloy::primitives::{Address, B256, U256, uint};
use bodkin::pons::curve::{CurveState, min_out_as_last_in_block, quote_buy};
use bodkin::pons::deployer::{DeployerIndex, HistoryCoverage};
use bodkin::pons::launches::LaunchEvent;
use bodkin::trade::journal::{OperationSpec, TxJournal};
use bodkin::trade::scheduler::EntryDeadline;
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

fn address(id: u64) -> Address {
    let mut bytes = [0u8; 20];
    bytes[12..].copy_from_slice(&id.to_be_bytes());
    Address::from(bytes)
}

fn deployer_index() -> &'static DeployerIndex {
    static INDEX: OnceLock<DeployerIndex> = OnceLock::new();
    INDEX.get_or_init(|| {
        let mut index = DeployerIndex::new(20_000);
        for id in 1..=10_000 {
            let event = LaunchEvent {
                token: address(id + 10_000),
                curve: address(id + 20_000),
                deployer: address(id % 100),
                pair_token: Address::ZERO,
                launch_config_id: U256::ZERO,
                graduation_threshold: U256::ZERO,
                block_number: id,
                tx_hash: B256::from(U256::from(id)),
                log_index: 0,
                detected_at_ms: 0,
                source: "bench",
            };
            index.note(&event);
            if id.is_multiple_of(4) {
                index.mark_graduated(event.token, id + 1);
            }
        }
        index.mark_ready(HistoryCoverage {
            chain_id: bodkin::CHAIN_ID,
            factory: bodkin::ADDR.pons_factory,
            from_block: 1,
            to_block: 10_001,
            anchor: B256::from([1; 32]),
        });
        index
    })
}

#[divan::bench(args = [0, 50, 99])]
fn deployer_history_lookup(deployer: u64) -> Option<(u32, u32)> {
    deployer_index().quick(divan::black_box(address(deployer)), 10_002)
}

#[divan::bench(args = [40, 150, 400])]
fn entry_deadline(lead_ms: u64) -> EntryDeadline {
    EntryDeadline::new(1_750_000_000, 2, divan::black_box(lead_ms)).unwrap()
}

#[divan::bench(sample_count = 20, sample_size = 1, skip_ext_time)]
fn journal_begin(bencher: divan::Bencher) {
    bencher
        .with_inputs(|| {
            let directory = tempfile::tempdir().unwrap();
            let journal = TxJournal::open(directory.path()).unwrap();
            (directory, journal)
        })
        .bench_values(|(directory, journal)| {
            let operation = journal
                .begin(
                    address(1),
                    OperationSpec::Entry {
                        token: address(2),
                        curve: address(3),
                        symbol: "BENCH".into(),
                        name: "Benchmark".into(),
                        value: MILLI_ETH.to_string(),
                    },
                )
                .unwrap();
            divan::black_box((directory, operation))
        });
}

const MILLI_ETH: u64 = 1_000_000_000_000_000;

#[divan::bench]
fn launch_telemetry_encoding() -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({
        "kind": "launch",
        "token": format!("{:#x}", address(1)),
        "curve": format!("{:#x}", address(2)),
        "historyStatus": "ready",
        "timing": {
            "detectionLagMs": 73,
            "entryQueueMs": 2,
            "enrichWaitMs": 0,
            "enrichMs": 41,
            "decisionLagMs": 114,
        },
    }))
    .unwrap()
}
