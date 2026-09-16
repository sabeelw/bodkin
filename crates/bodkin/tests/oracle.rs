//! Ports of legacy/test/{score,engine,v4,links}.test.ts. If these fail, the rewrite drifted.

use alloy::primitives::{Address, B256, U256, address};
use bodkin::engine::{decide, live_gate, rules_from_env};
use bodkin::links::{Links, osc, ref_line};
use bodkin::pons::curve::CurveState;
use bodkin::pons::deployer::{DeployerIndex, HistoryCoverage};
use bodkin::pons::enrich::{LaunchIntel, LaunchRecord, LaunchTx, PairInfo, Socials, TokenMeta};
use bodkin::pons::fingerprint::FarmDetector;
use bodkin::pons::launches::LaunchEvent;
use bodkin::pons::stream::{FlowSnapshot, FlowTracker};
use bodkin::score::{ScoreContext, Verdict, score_launch};
use bodkin::trade::positions::{ExitLadder, ExitRules, Position, exit_reason};
use bodkin::trade::v4::{pons_pool_key, pool_id};

const ZERO: Address = Address::ZERO;
const A: Address = address!("0x1111111111111111111111111111111111111111");
const B: Address = address!("0x2222222222222222222222222222222222222222");

fn ev() -> LaunchEvent {
    LaunchEvent {
        token: address!("0x3333333333333333333333333333333333333333"),
        curve: address!("0x4444444444444444444444444444444444444444"),
        deployer: A,
        pair_token: ZERO,
        launch_config_id: U256::ZERO,
        graduation_threshold: U256::from(42u64) * U256::from(10u128.pow(17)),
        block_number: 53_000_000,
        tx_hash: Default::default(),
        log_index: 0,
        detected_at_ms: 0,
        source: "unknown",
    }
}

fn builder() -> LaunchIntel {
    LaunchIntel {
        ev: ev(),
        meta: Some(TokenMeta {
            name: "Night Shift Harness".into(),
            symbol: "SHIFT".into(),
            description: "A local agent harness that works the night shift on your tasks so you do not have to".into(),
            socials: Socials {
                twitter: "https://x.com/example/status/1".into(),
                telegram: String::new(),
                website: "https://github.com/example/shift".into(),
            },
        }),
        record: Some(LaunchRecord {
            creator_fee_recipient: B,
            creator_tax_bps: 100,
            phase: 0,
        }),
        tx: Some(LaunchTx {
            from: A,
            dev_buy_wei: U256::from(53_519_145_802_650_970u128),
            dev_tokens: U256::from(30_000_000u128) * U256::from(10u128.pow(18)),
            exemptions: vec![A, B, ZERO, ZERO],
            recipient: A,
            timestamp: 1_788_397_521,
        }),
        curve: Some(CurveState {
            quote_reserve: U256::from(1_733_000_000_000_000_000u128),
            token_reserve: U256::from(970_000_000u128) * U256::from(10u128.pow(18)),
            real_quote_reserve: U256::from(53_000_000_000_000_000u128),
            sellable_tokens: U256::from(684_285_714u128) * U256::from(10u128.pow(18)),
            reserved_tokens: U256::from(285_714_285u128) * U256::from(10u128.pow(18)),
            graduation_threshold: U256::from(42u64) * U256::from(10u128.pow(17)),
            fee_bps: U256::from(100u64),
            creator_tax_bps: U256::from(100u64),
            opening_tax_bps: U256::ZERO,
            graduated: false,
            ready_to_graduate: false,
            launched_at: 1_788_397_521,
            read_at_ms: 0,
            read_block: 0,
            read_chain_ts: 0,
            snipe_tax_start_bps: U256::from(9900u64),
            snipe_tax_seconds: U256::from(3u64),
        }),
        pair: PairInfo::eth(),
        errors: vec![],
        fee_recipient_is_contract: None,
        fee_check_ms: 0,
    }
}

#[test]
fn builder_shaped_launch_scores_fire() {
    let s = score_launch(
        &builder(),
        &ScoreContext {
            deployer: Some((0, 0)),
            ..Default::default()
        },
    );
    assert_eq!(s.verdict, Verdict::Fire);
    assert!(s.reasons.iter().any(|r| r.contains("third party")));
    assert!(s.reasons.iter().any(|r| r.contains("declared bundle")));
}

#[test]
fn serial_deployer_no_socials_is_skip() {
    let mut i = builder();
    if let Some(m) = &mut i.meta {
        m.socials = Socials::default();
    }
    let s = score_launch(
        &i,
        &ScoreContext {
            deployer: Some((185, 0)),
            ..Default::default()
        },
    );
    assert_eq!(s.verdict, Verdict::Skip);
}

#[test]
fn dev_share_over_10_costs_25() {
    let mut heavy = builder();
    if let Some(tx) = &mut heavy.tx {
        tx.dev_tokens = U256::from(181_600_000u128) * U256::from(10u128.pow(18));
    }
    let a = score_launch(&builder(), &ScoreContext::default()).total;
    let b = score_launch(&heavy, &ScoreContext::default()).total;
    assert_eq!(a - b, 40);
}

#[test]
fn telegram_scores_same_as_website() {
    let mut i = builder();
    if let Some(m) = &mut i.meta {
        m.socials.website.clear();
        m.socials.twitter.clear();
        m.socials.telegram = "https://t.me/x".into();
    }
    let s = score_launch(&i, &ScoreContext::default());
    assert!(s.reasons.iter().any(|r| r.contains("+8 has telegram")));
}

#[test]
fn sniper_refuses_four_exempt_by_default() {
    let rules = rules_from_env();
    let s = score_launch(
        &builder(),
        &ScoreContext {
            deployer: Some((0, 0)),
            ..Default::default()
        },
    );
    let d = decide(&builder(), &s, &rules, 0, 0, U256::ZERO);
    assert!(!d.fire);
    assert!(d.why.iter().any(|w| w.contains("exempt wallets")));
}

#[test]
fn fires_when_bundle_relaxed_stops_at_cap() {
    let mut rules = rules_from_env();
    rules.max_exempt_wallets = 4;
    let s = score_launch(
        &builder(),
        &ScoreContext {
            deployer: Some((0, 0)),
            ..Default::default()
        },
    );
    assert!(decide(&builder(), &s, &rules, 0, 0, U256::ZERO).fire);
    assert!(!decide(&builder(), &s, &rules, 3, 0, U256::ZERO).fire);
    let mut usd = builder();
    usd.pair = PairInfo {
        address: A,
        symbol: "USDG".into(),
        decimals: 6,
        usd_per_unit: Some(1.0),
    };
    assert!(!decide(&usd, &s, &rules, 0, 0, U256::ZERO).fire);
}

#[tokio::test]
async fn limiter_runs_at_most_n() {
    use bodkin::pons::deployer::Limiter;
    let limit = Limiter::new(2);
    let active = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0));
    let peak = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0));
    let mut futs = Vec::new();
    for i in 1..=5 {
        let active = active.clone();
        let peak = peak.clone();
        futs.push(limit.run(move || {
            let active = active.clone();
            let peak = peak.clone();
            async move {
                let n = active.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
                peak.fetch_max(n, std::sync::atomic::Ordering::SeqCst);
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                active.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
                i
            }
        }));
    }
    let out = futures_util::future::join_all(futs).await;
    assert_eq!(out, vec![1, 2, 3, 4, 5]);
    assert_eq!(peak.load(std::sync::atomic::Ordering::SeqCst), 2);
}

#[test]
fn deployer_index_from_memory() {
    let mut idx = DeployerIndex::new(400_000);
    let ev = |token: Address, block: u64| LaunchEvent {
        token,
        curve: ZERO,
        deployer: A,
        pair_token: ZERO,
        launch_config_id: U256::ZERO,
        graduation_threshold: U256::ZERO,
        block_number: block,
        tx_hash: Default::default(),
        log_index: 0,
        detected_at_ms: 0,
        source: "unknown",
    };
    assert!(idx.quick(A, 100).is_none());
    idx.mark_ready(HistoryCoverage {
        chain_id: bodkin::CHAIN_ID,
        factory: bodkin::ADDR.pons_factory,
        from_block: 0,
        to_block: 100,
        anchor: B256::ZERO,
    });
    idx.note(&ev(
        address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
        10,
    ));
    idx.note(&ev(
        address!("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
        20,
    ));
    idx.note(&ev(
        address!("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
        20,
    ));
    idx.note(&ev(
        address!("0xcccccccccccccccccccccccccccccccccccccccc"),
        30,
    ));
    idx.mark_graduated(address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), 15);
    assert_eq!(idx.quick(A, 25), Some((2, 1)));
    assert_eq!(idx.quick(A, 5), Some((0, 0)));
    assert_eq!(idx.size(), (1, 3, 1));
}

#[test]
fn launch_farm_three_wallets() {
    let base = {
        let mut i = builder();
        i.curve = None;
        if let Some(m) = &mut i.meta {
            m.name = "x".into();
            m.symbol = "X".into();
            m.description.clear();
            m.socials.twitter = "https://x.com/a".into();
            m.socials.website = "https://a.b".into();
        }
        if let Some(r) = &mut i.record {
            r.creator_fee_recipient = A;
            r.creator_tax_bps = 200;
        }
        if let Some(tx) = &mut i.tx {
            tx.dev_buy_wei = U256::from(600_000_000_000_000u64);
            tx.dev_tokens = U256::ZERO;
            tx.exemptions.clear();
        }
        i
    };
    let mut farms = FarmDetector::default();
    let other = |d: Address| {
        let mut i = base.clone();
        i.ev.deployer = d;
        i
    };
    assert_eq!(farms.note(&base, 1_000).0, 0);
    // Same (deployer, token) noted again — a backfill/live double-delivery —
    // counts once, not as a second farm twin.
    assert_eq!(farms.note(&base, 2_000).0, 0);
    assert_eq!(
        farms
            .note(
                &other(address!("0x2222222222222222222222222222222222222222")),
                3_000
            )
            .0,
        1
    );
    assert_eq!(
        farms
            .note(
                &other(address!("0x3333333333333333333333333333333333333333")),
                4_000
            )
            .0,
        2
    );
    assert_eq!(
        farms
            .note(
                &other(address!("0x4444444444444444444444444444444444444444")),
                4_000 + 31 * 60_000
            )
            .0,
        0
    );
    let s = score_launch(
        &base,
        &ScoreContext {
            farm_twins: 3,
            ..Default::default()
        },
    );
    assert!(s.reasons.iter().any(|r| r.starts_with("-25 launch farm")));
    assert!(
        decide(&base, &s, &rules_from_env(), 0, 3, U256::ZERO)
            .why
            .iter()
            .any(|w| w.starts_with("launch farm"))
    );
    assert!(
        !decide(&base, &s, &rules_from_env(), 0, 1, U256::ZERO)
            .why
            .iter()
            .any(|w| w.starts_with("launch farm"))
    );
}

#[test]
fn session_budget_stops_firing() {
    let mut base = builder();
    if let Some(m) = &mut base.meta {
        m.name = "x".into();
        m.symbol = "X".into();
        m.description = "a real description of a real thing that is long enough".into();
        m.socials.twitter = "https://x.com/a".into();
        m.socials.website = "https://a.b".into();
        m.socials.telegram.clear();
    }
    if let Some(r) = &mut base.record {
        r.creator_fee_recipient = A;
        r.creator_tax_bps = 100;
    }
    if let Some(tx) = &mut base.tx {
        tx.exemptions.clear();
        tx.dev_tokens = U256::from(30_000_000u128) * U256::from(10u128.pow(18));
        tx.dev_buy_wei = U256::from(53_519_145_802_650_970u128);
    }
    let mut rules = rules_from_env();
    rules.session_budget_wei = U256::from(25u64) * U256::from(10u128.pow(15));
    rules.eth_per_buy = U256::from(10u128.pow(16));
    let s = score_launch(
        &base,
        &ScoreContext {
            deployer: Some((0, 0)),
            ..Default::default()
        },
    );
    assert!(decide(&base, &s, &rules, 0, 0, U256::ZERO).fire);
    assert!(decide(&base, &s, &rules, 0, 0, U256::from(10u128.pow(16))).fire);
    let third = decide(
        &base,
        &s,
        &rules,
        0,
        0,
        U256::from(2u128) * U256::from(10u128.pow(16)),
    );
    assert!(!third.fire);
    assert!(third.why[0].starts_with("session budget"));
}

#[test]
fn unreadable_launch_refused_as_unreadable() {
    let intel = LaunchIntel {
        ev: {
            let mut e = ev();
            e.curve = ZERO;
            e.block_number = 1;
            e
        },
        meta: None,
        record: None,
        tx: None,
        curve: None,
        pair: PairInfo::eth(),
        errors: vec!["tx: HTTP 429 after 5 tries".into()],
        fee_recipient_is_contract: None,
        fee_check_ms: 0,
    };
    let d = decide(
        &intel,
        &score_launch(&intel, &ScoreContext::default()),
        &rules_from_env(),
        0,
        0,
        U256::ZERO,
    );
    assert!(!d.fire);
    assert_eq!(d.why.len(), 1);
    assert!(d.why[0].starts_with("unreadable: tx: HTTP 429"));
}

#[test]
fn exit_rules_tp_sl_trail_hold() {
    let base = Position {
        id: "p".into(),
        token: address!("0x3333333333333333333333333333333333333333"),
        curve: address!("0x3333333333333333333333333333333333333333"),
        symbol: "T".into(),
        name: "T".into(),
        opened_at: 1_000,
        entry_tx: None,
        dry_run: true,
        chain_id: 0,
        wallet: String::new(),
        entry_eth: U256::from(10u128.pow(18)).to_string(),
        entry_gas_wei: None,
        basis_gas_wei: None,
        overhead_gas_wei: None,
        overhead_operations: Vec::new(),
        tokens: "1".into(),
        basis_eth: None,
        peak_eth: U256::from(10u128.pow(18)).to_string(),
        last_eth: U256::from(10u128.pow(18)).to_string(),
        last_at: 1_000,
        status: "open".into(),
        exits: vec![],
    };
    let rules = ExitRules {
        take_profit_pct: 80.0,
        stop_loss_pct: 35.0,
        trailing_pct: 25.0,
        max_hold_min: 45.0,
    };
    assert!(exit_reason(&base, U256::from(10u128.pow(18)), &rules, 1_100).is_none());
    assert!(
        exit_reason(
            &base,
            U256::from(19u64) * U256::from(10u128.pow(17)),
            &rules,
            1_100
        )
        .unwrap()
        .contains("take profit")
    );
    assert!(
        exit_reason(
            &base,
            U256::from(6u64) * U256::from(10u128.pow(17)),
            &rules,
            1_100
        )
        .unwrap()
        .contains("stop loss")
    );
    let mut peaked = base.clone();
    peaked.peak_eth = (U256::from(16u64) * U256::from(10u128.pow(17))).to_string();
    assert!(
        exit_reason(
            &peaked,
            U256::from(11u64) * U256::from(10u128.pow(17)),
            &rules,
            1_100
        )
        .unwrap()
        .contains("trailing")
    );
    assert!(
        exit_reason(&base, U256::from(10u128.pow(18)), &rules, 1_000 + 46 * 60)
            .unwrap()
            .contains("max hold")
    );
}

#[test]
fn ladder_and_stale_and_insider() {
    let mut rules = rules_from_env();
    rules.ladder = ExitLadder {
        rungs: vec![(34, 100), (33, 300)],
    };
    rules.stale_sec = 90;
    rules.stale_min_progress = 0.02;
    let pos = Position {
        id: "p".into(),
        token: A,
        curve: A,
        symbol: "T".into(),
        name: "T".into(),
        opened_at: 1_000,
        entry_tx: None,
        dry_run: true,
        chain_id: 0,
        wallet: String::new(),
        entry_eth: U256::from(10u128.pow(18)).to_string(),
        entry_gas_wei: None,
        basis_gas_wei: None,
        overhead_gas_wei: None,
        overhead_operations: Vec::new(),
        tokens: "1".into(),
        basis_eth: None,
        peak_eth: U256::from(10u128.pow(18)).to_string(),
        last_eth: U256::from(10u128.pow(18)).to_string(),
        last_at: 1_000,
        status: "open".into(),
        exits: vec![],
    };
    let hit = rules
        .ladder
        .hit(&pos, U256::from(2u64) * U256::from(10u128.pow(18)))
        .unwrap();
    assert!(hit.reason.contains("ladder +100%"));
    assert_eq!(hit.fraction_bps, 3400);
    let flow = FlowSnapshot {
        insider_sold: true,
        ..Default::default()
    };
    let act = bodkin::engine::pick_exit(
        &pos,
        U256::from(10u128.pow(18)),
        &rules,
        Some(&flow),
        1_010,
        false,
        0.01,
    )
    .unwrap();
    assert_eq!(act.reason, "insider sold");
    let flow = FlowSnapshot {
        insider_sold: false,
        ..Default::default()
    };
    let stale = bodkin::engine::pick_exit(
        &pos,
        U256::from(10u128.pow(18)),
        &rules,
        Some(&flow),
        1_000 + 91,
        false,
        0.01,
    )
    .unwrap();
    assert!(stale.reason.starts_with("stale"));
}

#[test]
fn live_gate_taxed_and_exempt() {
    let mut rules = rules_from_env();
    rules.min_taxed_buyers_s1 = 3;
    rules.max_exempt_buys_s0 = 1;
    rules.abort_if_insider_sold = true;
    let ok = FlowSnapshot {
        taxed_buyers_s1: 3,
        exempt_buys_s0: 0,
        insider_sold: false,
        ..Default::default()
    };
    assert!(live_gate(&rules, &ok).fire);
    let no = FlowSnapshot {
        taxed_buyers_s1: 1,
        exempt_buys_s0: 0,
        insider_sold: false,
        ..Default::default()
    };
    assert!(!live_gate(&rules, &no).fire);
    let sold = FlowSnapshot {
        taxed_buyers_s1: 3,
        exempt_buys_s0: 0,
        insider_sold: true,
        ..Default::default()
    };
    assert!(
        live_gate(&rules, &sold)
            .why
            .iter()
            .any(|w| w.contains("insider"))
    );
}

#[test]
fn pons_pool_key_eth_is_currency0() {
    let token = address!("0x3333333333333333333333333333333333333333");
    let k = pons_pool_key(token, ZERO, 200);
    assert_eq!(k.currency0, ZERO);
    assert_eq!(k.currency1, token);
    assert_eq!(k.fee, 0);
    assert_eq!(k.tick_spacing, 200);
    let id = pool_id(&k);
    assert_ne!(
        id,
        pool_id(&bodkin::trade::v4::pons_pool_key(token, ZERO, 60))
    );
}

#[test]
fn axiom_keyed_by_curve() {
    let token = "0x6219c797646FD54EdDE1497f66d3F76a2Bb67F81";
    let curve = "0x38B9A9d9DB16c302c30a1046AB2d4B11Fc1f668B";
    assert_eq!(
        Links::axiom(curve),
        "https://axiom.trade/meme/0x38b9a9d9db16c302c30a1046ab2d4b11fc1f668b?chain=robinhood"
    );
    assert_eq!(
        Links::fomo(token),
        "https://fomo.family/tokens/robinhood/0x6219c797646fd54edde1497f66d3f76a2bb67f81"
    );
    assert_eq!(
        Links::pons(token),
        format!("https://www.ponsfamily.com/launchpad/{token}")
    );
}

#[test]
fn empty_handles_drop_signup() {
    let links = Links {
        axiom_handle: String::new(),
        fomo_handle: String::new(),
    };
    assert!(links.axiom_ref().is_empty());
    assert!(links.fomo_ref().is_empty());
    assert!(ref_line(&links).is_empty());
    let _ = osc("x", "");
}

#[test]
fn tax_staircase_live() {
    assert_eq!(bodkin::pons::tax::live_table(0), 9900);
    assert_eq!(bodkin::pons::tax::live_table(1), 618);
    assert_eq!(bodkin::pons::tax::live_table(2), 19);
    assert_eq!(bodkin::pons::tax::live_table(3), 0);
}

#[test]
fn flow_tracker_compiles_watch() {
    let mut f = FlowTracker::default();
    f.watch(A, 100, 0, [A]);
    let s = f.snapshot(A);
    assert_eq!(s.buys, 0);
}
