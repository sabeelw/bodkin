use alloy::primitives::{Address, U256};
use alloy::sol_types::{SolCall, SolEvent};
use anyhow::Context;
use bodkin::abi::{escrow, factory};
use bodkin::alerts::eth_usd;
use bodkin::chain::{ADDR, CHAIN_ID};
use bodkin::config::{parse_ether, Config};
use bodkin::engine::{rules_from_env, SnipeRules};
use bodkin::fmt::{eth, hhmmss, iso, pad, short, usd};
use bodkin::links::{ref_line, Links};
use bodkin::outcomes::{print_summary, summarize, OutcomeLog};
use bodkin::pons::deployer::DeployerIndex;
use bodkin::pons::enrich::{enrich_launch, read_token_meta};
use bodkin::pons::fees::fee_forensics;
use bodkin::pons::fingerprint::FarmDetector;
use bodkin::pons::launches::{find_launch, recent_launches, watch_launches};
use bodkin::pons::tax::snipe_tax_bps;
use bodkin::replay::{print_report, replay, ReplayAlt, ReplayLaunch};
use bodkin::rpc::{Lane, Rpc};
use bodkin::run::start_engine;
use bodkin::score::{score_launch, ScoreContext};
use bodkin::style::{banner, error, hr, info, loss, muted, neon, on_neon, warn, white};
use bodkin::trade::burst::BurstCtl;
use bodkin::trade::curve::{buy_on_curve, read_curve_state as read_cs};
use bodkin::trade::pool::{buy_on_pool, sell_anywhere, token_balance, value_now};
use bodkin::trade::positions::PositionStore;
use bodkin::trade::submitter::Submitter;
use bodkin::trade::v4::{detect_router_layout, pool_key_for, pool_state, quote_v4};
use bodkin::trade::wallet::Wallet;
use bodkin::view::{launch_card, launch_update, ViewCtx};
use clap::{Parser, Subcommand};
use std::str::FromStr;
use std::sync::Arc;

#[derive(Parser)]
#[command(name = "bodkin", version, about = "The sniper terminal for pons v2 on Robinhood Chain. Local, open, non-custodial.")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Check RPC, chain id, pons contracts, sequencer RTT, helper bytecode
    Doctor {
        #[arg(long)]
        probe: bool,
    },
    /// Live feed of pons v2 launches
    Hunt {
        #[arg(long, default_value = "3")]
        backfill: u64,
        #[arg(long, default_value = "0")]
        min_score: i32,
        #[arg(long)]
        fire_only: bool,
        #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
        follow: bool,
        #[arg(long)]
        json: bool,
        #[arg(long)]
        poll: Option<u64>,
        #[arg(long)]
        r#for: Option<u64>,
    },
    /// Everything on chain about one token
    Scan { token: String },
    /// Follow one token live
    Watch {
        token: String,
        #[arg(long, default_value = "5")]
        every: u64,
        #[arg(long)]
        r#for: Option<u64>,
    },
    /// Creator-fee forensics
    Fees { token: String },
    /// Deployer history
    Dev {
        address: String,
        #[arg(long, default_value = "1728000")]
        blocks: u64,
    },
    /// Auto-buy launches that pass the rules
    Snipe {
        #[arg(long)]
        live: bool,
        #[arg(long)]
        eth: Option<String>,
        #[arg(long)]
        min_score: Option<i32>,
        #[arg(long)]
        max_tax_bps: Option<u64>,
        #[arg(long)]
        slippage: Option<u64>,
        #[arg(long)]
        keyword: Option<String>,
        #[arg(long)]
        deployer: Vec<String>,
        #[arg(long)]
        max_open: Option<usize>,
        #[arg(long)]
        allow_pairs: bool,
        #[arg(long)]
        budget: Option<String>,
        #[arg(long)]
        yes: bool,
        #[arg(long)]
        r#for: Option<u64>,
    },
    Wallet,
    Claim {
        #[arg(long)]
        live: bool,
    },
    Buy {
        token: String,
        eth: String,
        #[arg(long)]
        live: bool,
        #[arg(long, default_value = "300")]
        slippage: u64,
    },
    Sell {
        token: String,
        pct: Option<String>,
        #[arg(long)]
        live: bool,
        #[arg(long, default_value = "300")]
        slippage: u64,
    },
    Positions,
    Board {
        #[arg(long)]
        port: Option<u16>,
        #[arg(long)]
        live: bool,
        #[arg(long)]
        eth: Option<String>,
        #[arg(long)]
        min_score: Option<i32>,
        #[arg(long)]
        keyword: Option<String>,
        #[arg(long)]
        allow_pairs: bool,
        #[arg(long)]
        budget: Option<String>,
        #[arg(long)]
        yes: bool,
    },
    #[command(subcommand)]
    Helper(HelperCmd),
    /// Print outcomes.jsonl summary
    Outcomes,
    /// Replay sampled launches against alternate rules
    Replay {
        #[arg(long, default_value = "6")]
        hours: u64,
        #[arg(long)]
        sample: Option<u64>,
        #[arg(long, default_value = "2")]
        entry_second: u64,
    },
}

#[derive(Subcommand)]
enum HelperCmd {
    Deploy {
        #[arg(long)]
        live: bool,
    },
    Check,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt().with_env_filter(tracing_subscriber::EnvFilter::from_default_env()).with_target(false).init();
    let cli = Cli::parse();
    if let Err(e) = run(cli).await {
        error(format!("{}", first_line(&format!("{e:#}"))));
        std::process::exit(1);
    }
}

async fn run(cli: Cli) -> anyhow::Result<()> {
    let cfg = Config::from_env();
    let rpc = Arc::new(Rpc::new(&cfg)?);
    match cli.cmd {
        Cmd::Doctor { probe } => doctor(rpc, &cfg, probe).await,
        Cmd::Hunt { backfill, min_score, fire_only, follow, json, poll, r#for } => hunt(rpc, cfg, backfill, min_score, fire_only, follow, json, poll, r#for).await,
        Cmd::Scan { token } => scan(rpc, addr(&token)?).await,
        Cmd::Watch { token, every, r#for } => watch(rpc, addr(&token)?, every, r#for).await,
        Cmd::Fees { token } => fees(rpc, addr(&token)?).await,
        Cmd::Dev { address, blocks } => dev(rpc, addr(&address)?, blocks).await,
        Cmd::Snipe { live, eth, min_score, max_tax_bps, slippage, keyword, deployer, max_open, allow_pairs, budget, yes, r#for } => {
            let rules = snipe_rules(eth, min_score, max_tax_bps, slippage, keyword, deployer, max_open, allow_pairs, budget)?;
            banner("the sniper terminal for pons v2 on Robinhood Chain");
            if live {
                arm_live(&rpc, &rules, yes).await?;
            }
            let emit = Arc::new(|e: serde_json::Value| {
                if e.get("kind").and_then(|k| k.as_str()) == Some("fire") || e.get("kind").and_then(|k| k.as_str()) == Some("exit") {
                    tracing::info!("{e}");
                }
            });
            let engine = start_engine(rpc, cfg, rules, live, false, emit);
            if let Some(sec) = r#for {
                tokio::time::sleep(std::time::Duration::from_secs(sec)).await;
                engine.stop();
            } else {
                tokio::signal::ctrl_c().await.ok();
                engine.stop();
            }
            info(muted("bodkin stopped"));
            Ok(())
        }
        Cmd::Wallet => wallet_cmd(rpc).await,
        Cmd::Claim { live } => claim_cmd(rpc, live).await,
        Cmd::Buy { token, eth, live, slippage } => buy_cmd(rpc, addr(&token)?, parse_ether(&eth)?, live, slippage).await,
        Cmd::Sell { token, pct, live, slippage } => sell_cmd(rpc, addr(&token)?, pct.unwrap_or_else(|| "100".into()), live, slippage).await,
        Cmd::Positions => positions_cmd(rpc).await,
        Cmd::Board { port, live, eth, min_score, keyword, allow_pairs, budget, yes } => {
            let rules = snipe_rules(eth, min_score, None, None, keyword, vec![], None, allow_pairs, budget)?;
            banner("the sniper terminal for pons v2 on Robinhood Chain");
            if live {
                arm_live(&rpc, &rules, yes).await?;
            }
            let port = port.unwrap_or(cfg.board_port);
            bodkin::board::start_board(rpc, cfg, port, live, rules).await
        }
        Cmd::Helper(HelperCmd::Deploy { live }) => helper_deploy(rpc, &cfg, live).await,
        Cmd::Helper(HelperCmd::Check) => helper_check(rpc, &cfg).await,
        Cmd::Outcomes => {
            let evs = OutcomeLog::open("data").read_all();
            print_summary(&summarize(&evs));
            Ok(())
        }
        Cmd::Replay { hours, sample, entry_second } => replay_cmd(rpc, hours, sample, entry_second).await,
    }
}

fn addr(s: &str) -> anyhow::Result<Address> {
    Address::from_str(s).map_err(|_| anyhow::anyhow!("not an address: {s}"))
}

fn snipe_rules(
    eth: Option<String>,
    min_score: Option<i32>,
    max_tax: Option<u64>,
    slippage: Option<u64>,
    keyword: Option<String>,
    deployers: Vec<String>,
    max_open: Option<usize>,
    allow_pairs: bool,
    budget: Option<String>,
) -> anyhow::Result<SnipeRules> {
    let mut r = rules_from_env();
    if let Some(e) = eth {
        r.eth_per_buy = parse_ether(&e)?;
    }
    if let Some(s) = min_score {
        r.min_score = s;
    }
    if let Some(t) = max_tax {
        r.max_opening_tax_bps = t;
    }
    if let Some(s) = slippage {
        r.slippage_bps = s;
    }
    if let Some(k) = keyword {
        r.keyword = Some(regex::Regex::new(&format!("(?i){k}"))?);
    }
    if !deployers.is_empty() {
        r.deployers = deployers.into_iter().map(|d| d.to_ascii_lowercase()).collect();
    }
    if let Some(n) = max_open {
        r.max_open_positions = n;
    }
    if allow_pairs {
        r.eth_pairs_only = false;
    }
    if let Some(b) = budget {
        r.session_budget_wei = parse_ether(&b)?;
    }
    Ok(r)
}

async fn arm_live(rpc: &Rpc, rules: &SnipeRules, yes: bool) -> anyhow::Result<()> {
    let acct = Wallet::require()?;
    let bal = rpc.get_balance(Lane::Hot, acct.address()).await?;
    let price = eth_usd().await;
    info(on_neon(" LIVE "));
    info(format!("   wallet     {:#x}", acct.address()));
    info(format!("   balance    {} ETH {}", eth(bal), muted(usd(price.map(|p| wei_eth(bal) * p)))));
    info(format!("   per buy    {} ETH {}   max open {}", eth(rules.eth_per_buy), muted(usd(price.map(|p| wei_eth(rules.eth_per_buy) * p))), rules.max_open_positions));
    info(format!("   budget     {} ETH {}", eth(rules.session_budget_wei), muted(usd(price.map(|p| wei_eth(rules.session_budget_wei) * p)))));
    if bal < rules.eth_per_buy {
        anyhow::bail!("balance {} ETH does not cover one buy", eth(bal));
    }
    if yes {
        return Ok(());
    }
    eprint!("   type arm to continue, anything else to stay in dry run: ");
    use std::io::Write;
    let _ = std::io::stderr().flush();
    let mut line = String::new();
    std::io::stdin().read_line(&mut line)?;
    if line.trim().eq_ignore_ascii_case("arm") {
        Ok(())
    } else {
        anyhow::bail!("not armed");
    }
}

fn wei_eth(w: U256) -> f64 {
    bodkin::fmt::wei_to_f64(w) / 1e18
}

async fn doctor(rpc: Arc<Rpc>, cfg: &Config, probe: bool) -> anyhow::Result<()> {
    let t0 = std::time::Instant::now();
    let chain = rpc.chain_id(Lane::Hot).await?;
    let block = rpc.block_number(Lane::Hot).await?;
    let gas = rpc.gas_price(Lane::Hot).await?;
    let rtt = t0.elapsed().as_millis();
    let ok = |b: bool| if b { neon("ok") } else { loss("FAIL") };
    info(format!("{}        {}  {rtt} ms round trip  {} chain {chain}, block {block}, gas {} ETH", white("rpc"), cfg.http_labels(), ok(chain == CHAIN_ID), eth(gas)));
    info(format!("{}  {}", white("websocket"), muted(cfg.ws_labels())));
    let hook: Address = rpc.eth_call(Lane::Enrich, ADDR.pons_factory, factory::memeHookCall {}, None).await?;
    let escrow_a: Address = rpc.eth_call(Lane::Enrich, ADDR.pons_factory, factory::feeEscrowCall {}, None).await?;
    let pm: Address = rpc.eth_call(Lane::Enrich, ADDR.pons_factory, factory::poolManagerCall {}, None).await?;
    let fee: U256 = rpc.eth_call(Lane::Enrich, ADDR.pons_factory, factory::launchFeeCall {}, None).await?;
    let tax: U256 = rpc.eth_call(Lane::Enrich, ADDR.pons_factory, factory::snipeTaxStartBpsCall {}, None).await?;
    let tax_sec: U256 = rpc.eth_call(Lane::Enrich, ADDR.pons_factory, factory::snipeTaxSecondsCall {}, None).await?;
    let max_tax: U256 = rpc.eth_call(Lane::Enrich, ADDR.pons_factory, factory::maxCreatorTaxBpsCall {}, None).await?;
    let enabled: bool = rpc.eth_call(Lane::Enrich, ADDR.pons_factory, factory::launchEnabledCall {}, None).await?;
    info(format!("{}    {:#x}  launches {}", white("factory"), ADDR.pons_factory, if enabled { "enabled" } else { "DISABLED" }));
    info(format!("   hook {}  escrow {}  poolManager {}", ok(hook == ADDR.pons_hook), ok(escrow_a == ADDR.pons_escrow), ok(pm == ADDR.v4_pool_manager)));
    let start: u64 = tax.try_into().unwrap_or(0);
    let win: u64 = tax_sec.try_into().unwrap_or(3);
    info(format!(
        "   launch fee {} ETH   opening tax {} decaying over {win}s ({} / {} / {} / 0)   max creator tax {}",
        eth(fee),
        format!("{:.2}%", start as f64 / 100.0),
        snipe_tax_bps(start, win, 0),
        snipe_tax_bps(start, win, 1),
        snipe_tax_bps(start, win, 2),
        format!("{:.2}%", u64::try_from(max_tax).unwrap_or(0) as f64 / 100.0)
    ));
    match recent_launches(&rpc, 3_000, None, None).await {
        Ok(r) => info(format!("{}      {} launches in the last 3000 blocks (~5 min)", white("tempo"), r.len())),
        Err(e) => warn(format!("tempo: {e}")),
    }
    info(format!("{}    {}", white("eth/usd"), eth_usd().await.map(|p| format!("${p:.0}")).unwrap_or_else(|| muted("offline"))));
    info(format!("{}     {}", white("wallet"), if cfg.private_key.is_some() { "key present (used only by live trades)" } else { "no key, analytics and dry-run only" }));
    if let Some(h) = cfg.helper {
        let code = rpc.get_code(Lane::Background, h).await?;
        info(format!("{}    {:#x}  {} ({} bytes)", white("helper"), h, if code.is_empty() { loss("NO CODE") } else { neon("ok") }, code.len()));
    } else {
        info(format!("{}    {}", white("helper"), muted("HELPER_ADDRESS unset")));
    }
    if let Ok(sub) = Submitter::new(cfg) {
        match sub.resolve_and_pin().await {
            Ok(ips) => {
                for ip in &ips {
                    info(format!("{}   {}  {} ms", white("sequencer"), ip.ip, ip.rtt_ms));
                }
            }
            Err(e) => warn(format!("sequencer resolve: {e}")),
        }
    }
    if probe {
        if let Ok(sub) = Submitter::new(cfg) {
            // Dummy future timestampMin — if reject is immediate -32003, Conditional is usable as a cheap reject.
            let dummy = alloy::primitives::Bytes::from(vec![0u8; 1]);
            let future = chrono::Utc::now().timestamp() as u64 + 120;
            match sub.send_conditional(&dummy, serde_json::json!({"timestampMin": future})).await {
                Err((-32003, msg)) => info(format!("{}  immediate -32003 ({}); Conditional is a cheap reject, not a park", white("conditional"), first_line(&msg))),
                Err((code, msg)) => info(format!("{}  {code} {}", white("conditional"), first_line(&msg))),
                Ok(_) => warn("conditional accepted a dummy; do not put timestampMin on the fire path"),
            }
        }
        let head = rpc.block_number(Lane::Background).await?;
        let logs = rpc
            .get_logs(
                Lane::Background,
                alloy::rpc::types::Filter::new()
                    .address(ADDR.pons_factory)
                    .event_signature(bodkin::abi::topics::pool_graduated())
                    .from_block(head.saturating_sub(200_000))
                    .to_block(head),
            )
            .await
            .unwrap_or_default();
        let mut picked = None;
        for l in logs.iter().rev().take(25) {
            if let Ok(g) = factory::PoolGraduated::decode_log(l.as_ref()) {
                if let Ok((key, pair, _)) = pool_key_for(&rpc, g.token).await {
                    if pair == Address::ZERO {
                        picked = Some((g.token, key));
                        break;
                    }
                }
            }
        }
        if let Some((tok, key)) = picked {
            let q = quote_v4(&rpc, &key, true, U256::from(10u64.pow(15))).await.unwrap_or(U256::ZERO);
            let st = pool_state(&rpc, &key).await.ok();
            let layout = detect_router_layout(&rpc, &key).await.ok();
            info(format!(
                "{}   {}: 0.001 ETH → {} tokens, liquidity {}, router {:?}",
                white("v4 probe"),
                short(&format!("{tok:#x}"), 4),
                bodkin::fmt::tokens(q),
                st.map(|s| s.liquidity.to_string()).unwrap_or_else(|| "?".into()),
                layout
            ));
        } else {
            warn("no ETH-paired graduation in the last 200k blocks to probe against");
        }
        let _ = BurstCtl::from_env();
    }
    Ok(())
}

async fn hunt(rpc: Arc<Rpc>, cfg: Config, backfill: u64, min_score: i32, fire_only: bool, follow: bool, json: bool, poll: Option<u64>, for_s: Option<u64>) -> anyhow::Result<()> {
    let mut cfg = cfg;
    if let Some(p) = poll {
        cfg.poll_ms = p;
    }
    let price = eth_usd().await;
    let links = Links::from_env();
    if !json {
        banner("the sniper terminal for pons v2 on Robinhood Chain");
        info(format!("{} {}", neon("bodkin"), muted(format!("hunt · pons v2 · Robinhood Chain ({CHAIN_ID}) · {}", cfg.ws_labels()))));
        let rl = ref_line(&links);
        if !rl.is_empty() {
            info(rl);
        }
        info(hr(72));
    }
    let index = Arc::new(tokio::sync::Mutex::new(DeployerIndex::default()));
    {
        let rpc = rpc.clone();
        let index = index.clone();
        tokio::spawn(async move {
            if let Ok(evs) = recent_launches(&rpc, 1_728_000, None, None).await {
                let mut idx = index.lock().await;
                for e in &evs {
                    idx.note(e);
                }
                idx.mark_ready();
            }
        });
    }
    let farms = Arc::new(parking_lot::Mutex::new(FarmDetector::default()));
    if backfill > 0 {
        if let Ok(recent) = recent_launches(&rpc, 3_000, None, None).await {
            for ev in recent.into_iter().rev().take(backfill as usize).collect::<Vec<_>>().into_iter().rev() {
                present(&rpc, ev, price, min_score, fire_only, json, &index, &farms, &links).await;
            }
            if !json {
                info(hr(72));
            }
        }
    }
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let watch = watch_launches(rpc.clone(), cfg, tx);
    let deadline = for_s.map(|s| tokio::time::Instant::now() + std::time::Duration::from_secs(s));
    loop {
        let ev = tokio::select! {
            _ = tokio::signal::ctrl_c() => break,
            _ = async { if let Some(d) = deadline { tokio::time::sleep_until(d).await } else { std::future::pending::<()>().await } } => break,
            Some(ev) = rx.recv() => ev,
            else => break,
        };
        let intel = present(&rpc, ev.clone(), price, min_score, fire_only, json, &index, &farms, &links).await;
        if follow {
            if let Some(intel) = intel {
                let rpc = rpc.clone();
                let dq = index.lock().await.quick(ev.deployer, ev.block_number);
                tokio::spawn(async move { follow_launch(&rpc, intel, dq, price).await });
            }
        }
    }
    watch.stop();
    info(muted("bodkin stopped"));
    Ok(())
}

async fn present(
    rpc: &Rpc,
    ev: bodkin::pons::launches::LaunchEvent,
    price: Option<f64>,
    min_score: i32,
    fire_only: bool,
    json: bool,
    index: &tokio::sync::Mutex<DeployerIndex>,
    farms: &parking_lot::Mutex<FarmDetector>,
    links: &Links,
) -> Option<bodkin::pons::enrich::LaunchIntel> {
    index.lock().await.note(&ev);
    let intel = enrich_launch(rpc, ev.clone(), bodkin::chain::DEAD).await;
    let dq = index.lock().await.quick(ev.deployer, ev.block_number);
    let twins = farms.lock().note(&intel, bodkin::pons::clock::now_ms()).0;
    let score = score_launch(&intel, &ScoreContext { deployer: dq, farm_twins: twins, ..Default::default() });
    if score.total < min_score || (fire_only && score.verdict != bodkin::score::Verdict::Fire) {
        return None;
    }
    if json {
        println!("{}", serde_json::json!({"t": chrono::Utc::now().to_rfc3339(), "token": format!("{:#x}", ev.token), "score": score.total, "verdict": score.verdict.as_str()}));
    } else {
        println!("{}", launch_card(&intel, &score, &ViewCtx { eth_usd: price, deployer: dq, activity: None }, links));
        println!("   {}", muted(format!("read · {}", intel.errors.join("; "))));
    }
    if let Ok(mut f) = std::fs::OpenOptions::new().create(true).append(true).open("data/launches.jsonl") {
        use std::io::Write;
        let _ = writeln!(f, "{}", serde_json::json!({"t": bodkin::pons::clock::now_ms(), "token": format!("{:#x}", ev.token), "score": score.total}));
    }
    Some(intel)
}

async fn follow_launch(rpc: &Rpc, intel: bodkin::pons::enrich::LaunchIntel, dq: Option<(u32, u32)>, price: Option<f64>) {
    let launch_ts = intel.tx.as_ref().map(|t| t.timestamp).unwrap_or(bodkin::pons::clock::now_ms() / 1000);
    for at in [15u64, 60] {
        let wait = (launch_ts + at) * 1000;
        let now = bodkin::pons::clock::now_ms();
        if wait > now {
            tokio::time::sleep(std::time::Duration::from_millis(wait - now)).await;
        }
        if let (Ok(act), Ok(curve)) = (curve_activity_safe(rpc, intel.ev.curve, intel.ev.block_number).await, read_cs(rpc, intel.ev.curve, bodkin::chain::DEAD).await) {
            let mut fresh = intel.clone();
            fresh.curve = Some(curve);
            let score = score_launch(&fresh, &ScoreContext { deployer: dq, activity: Some(act.clone()), age_sec: Some(at), ..Default::default() });
            println!("{}", launch_update(&fresh, &score, &act, at, &ViewCtx { eth_usd: price, deployer: dq, activity: Some(act.clone()) }));
        }
    }
}

async fn curve_activity_safe(rpc: &Rpc, curve: Address, from: u64) -> anyhow::Result<bodkin::pons::enrich::CurveActivity> {
    bodkin::pons::enrich::curve_activity(rpc, curve, from, None).await
}

async fn scan(rpc: Arc<Rpc>, token: Address) -> anyhow::Result<()> {
    let price = eth_usd().await;
    let ev = find_launch(&rpc, token).await?.ok_or_else(|| anyhow::anyhow!("no pons v2 TokenLaunched event found"))?;
    let intel = enrich_launch(&rpc, ev.clone(), bodkin::chain::DEAD).await;
    let act = curve_activity_safe(&rpc, ev.curve, ev.block_number).await.ok();
    let score = score_launch(&intel, &ScoreContext { activity: act.clone(), age_sec: intel.tx.as_ref().map(|t| bodkin::pons::clock::now_ms() / 1000 - t.timestamp), ..Default::default() });
    let links = Links::from_env();
    println!("{}", launch_card(&intel, &score, &ViewCtx { eth_usd: price, deployer: None, activity: act }, &links));
    info(hr(72));
    info(format!("{}   {}  block {}  tx {}", white("launched"), intel.tx.as_ref().map(|t| iso(t.timestamp)).unwrap_or_else(|| "?".into()), ev.block_number, muted(format!("{:#x}", ev.tx_hash))));
    info(format!("{}   {:#x}", white("explorer"), token));
    Ok(())
}

async fn watch(rpc: Arc<Rpc>, token: Address, every: u64, for_s: Option<u64>) -> anyhow::Result<()> {
    let ev = find_launch(&rpc, token).await?.ok_or_else(|| anyhow::anyhow!("no TokenLaunched"))?;
    let intel = enrich_launch(&rpc, ev.clone(), bodkin::chain::DEAD).await;
    let sym = intel.meta.as_ref().map(|m| format!("${}", m.symbol)).unwrap_or_else(|| short(&format!("{token:#x}"), 4));
    info(format!("{} {}  {:#x}", white(intel.meta.as_ref().map(|m| m.name.as_str()).unwrap_or("?")), neon(&sym), token));
    let t0 = std::time::Instant::now();
    loop {
        if let Ok(rec) = rpc.eth_call(Lane::Hot, ADDR.pons_factory, factory::getLaunchedTokenCall { token }, None).await {
            if rec.phase == 0 {
                if let Ok(c) = read_cs(&rpc, ev.curve, bodkin::chain::DEAD).await {
                    let p = bodkin::pons::curve::progress(&c);
                    info(format!("{}  curve {:.1}%  {}/{} ETH  tax {}", muted(hhmmss(None)), p * 100.0, eth(c.real_quote_reserve), eth(c.graduation_threshold), bodkin::fmt::bps(c.opening_tax_bps.try_into().unwrap_or(0))));
                }
            } else if rec.phase == 2 {
                if let Ok((key, pair, _)) = pool_key_for(&rpc, token).await {
                    if pair == Address::ZERO {
                        if let Ok(out) = quote_v4(&rpc, &key, false, U256::from(10u128.pow(24))).await {
                            info(format!("{}  pool  1M tokens → {} ETH", muted(hhmmss(None)), eth(out)));
                        }
                    }
                }
            }
        }
        if for_s.is_some_and(|s| t0.elapsed().as_secs() > s) {
            break;
        }
        tokio::select! {
            _ = tokio::signal::ctrl_c() => break,
            _ = tokio::time::sleep(std::time::Duration::from_secs(every)) => {}
        }
    }
    Ok(())
}

async fn fees(rpc: Arc<Rpc>, token: Address) -> anyhow::Result<()> {
    let ev = find_launch(&rpc, token).await?.ok_or_else(|| anyhow::anyhow!("no TokenLaunched"))?;
    let rec = rpc.eth_call(Lane::Background, ADDR.pons_factory, factory::getLaunchedTokenCall { token }, None).await?;
    let fees = fee_forensics(&rpc, rec.creatorFeeRecipient, ev.curve, ev.block_number).await?;
    info(format!("{}  recipient {:#x}  credited {} ETH  claimed {} ETH  pending {} ETH", white("fees"), fees.recipient, eth(fees.total_credited), eth(fees.total_claimed), eth(fees.pending)));
    for c in &fees.claims {
        info(format!("   {}  {}  {:#x}", if c.timestamp > 0 { iso(c.timestamp) } else { format!("block {}", c.block) }, pad(&format!("{} ETH", eth(c.amount)), 14), c.tx));
    }
    Ok(())
}

async fn dev(rpc: Arc<Rpc>, deployer: Address, blocks: u64) -> anyhow::Result<()> {
    let evs = recent_launches(&rpc, blocks, Some(deployer), None).await?;
    info(format!("{:#x}  {} launches in {blocks} blocks", deployer, evs.len()));
    info(hr(72));
    for ev in evs.iter().rev().take(25) {
        let meta = read_token_meta(&rpc, ev.token).await.ok();
        info(format!("   {}  {}  {:#x}", pad(&meta.as_ref().map(|m| format!("${}", m.symbol)).unwrap_or_else(|| "?".into()), 12), pad(&meta.as_ref().map(|m| m.name.clone()).unwrap_or_default(), 28), ev.token));
    }
    Ok(())
}

async fn wallet_cmd(rpc: Arc<Rpc>) -> anyhow::Result<()> {
    let Some(w) = Wallet::from_env()? else {
        info(muted("no PRIVATE_KEY in .env"));
        return Ok(());
    };
    let bal = rpc.get_balance(Lane::Hot, w.address()).await?;
    let pending: U256 = rpc.eth_call(Lane::Background, ADDR.pons_escrow, escrow::balanceOfCall { recipient: w.address() }, None).await.unwrap_or(U256::ZERO);
    let price = eth_usd().await;
    info(format!("{}   {:#x}", white("address"), w.address()));
    info(format!("{}   {} ETH {}", white("balance"), eth(bal), muted(usd(price.map(|p| wei_eth(bal) * p)))));
    info(format!("{}      {} ETH unclaimed", white("fees"), eth(pending)));
    Ok(())
}

async fn claim_cmd(rpc: Arc<Rpc>, live: bool) -> anyhow::Result<()> {
    let w = Wallet::require()?;
    let pending: U256 = rpc.eth_call(Lane::Hot, ADDR.pons_escrow, escrow::balanceOfCall { recipient: w.address() }, None).await?;
    if pending.is_zero() {
        info(muted(format!("nothing to claim for {:#x}", w.address())));
        return Ok(());
    }
    info(format!("{} {} ETH for {:#x}", if live { "claiming" } else { "dry run: would claim" }, eth(pending), w.address()));
    if !live {
        return Ok(());
    }
    let data = escrow::claimCall {}.abi_encode();
    let nonce = rpc.get_transaction_count(Lane::Hot, w.address(), true).await?;
    let clock = bodkin::pons::clock::ChainClock::default();
    let (hash, _) = w.sign_eip1559(ADDR.pons_escrow, U256::ZERO, data.into(), nonce, &clock)?;
    info(format!("signed claim {hash:#x}"));
    Ok(())
}

async fn buy_cmd(rpc: Arc<Rpc>, token: Address, amount: U256, live: bool, slip: u64) -> anyhow::Result<()> {
    let rec = rpc.eth_call(Lane::Enrich, ADDR.pons_factory, factory::getLaunchedTokenCall { token }, None).await?;
    if !rec.exists {
        anyhow::bail!("not a pons v2 token");
    }
    let w = if live { Some(Wallet::require()?) } else { None };
    let res = if rec.phase == 0 {
        buy_on_curve(&rpc, w.as_ref(), rec.curve, amount, slip, !live, None).await?
    } else {
        buy_on_pool(&rpc, w.as_ref(), token, amount, slip, !live).await?
    };
    info(format!("{} {} ETH → {} tokens on the {}", if live { "bought" } else { "dry run:" }, eth(res.eth_in), bodkin::fmt::tokens(res.tokens_out.unwrap_or(res.tokens_quoted)), res.venue));
    Ok(())
}

async fn sell_cmd(rpc: Arc<Rpc>, token: Address, pct: String, live: bool, slip: u64) -> anyhow::Result<()> {
    let w = Wallet::require()?;
    let bal = token_balance(&rpc, token, w.address()).await?;
    let pct: f64 = pct.parse().unwrap_or(100.0);
    let amount = bal * U256::from((pct * 100.0) as u64) / U256::from(10_000u64);
    if amount.is_zero() {
        anyhow::bail!("nothing to sell");
    }
    let res = sell_anywhere(&rpc, Some(&w), token, amount, slip, !live).await?;
    info(format!("{} {} tokens → {} ETH on the {}", if live { "sold" } else { "dry run:" }, bodkin::fmt::tokens(res.tokens_in), eth(res.eth_out.unwrap_or(res.eth_quoted)), res.venue));
    Ok(())
}

async fn positions_cmd(rpc: Arc<Rpc>) -> anyhow::Result<()> {
    let list = PositionStore::open("data").load();
    if list.is_empty() {
        info(muted("no positions yet"));
        return Ok(());
    }
    let price = eth_usd().await;
    for p in list {
        let entry: U256 = p.entry_eth.parse().unwrap_or(U256::ZERO);
        let mut mark: U256 = p.last_eth.parse().unwrap_or(entry);
        if p.status == "open" {
            if let Ok(Some((e, _))) = value_now(&rpc, p.token, p.tokens.parse().unwrap_or(U256::ZERO)).await {
                mark = e;
            }
        }
        let pnl = if !entry.is_zero() {
            let g: u64 = (mark * U256::from(10_000u64) / entry).try_into().unwrap_or(0);
            g as f64 / 100.0 - 100.0
        } else {
            0.0
        };
        info(format!(
            "{} ${} {}  in {} ETH  {} {} ETH  {:+.1}%  {}",
            if p.status == "open" { neon("open  ") } else { muted("closed") },
            pad(&p.symbol, 8),
            if p.dry_run { muted("dry") } else { white("live") },
            eth(entry),
            if p.status == "open" { "now" } else { "out" },
            eth(mark),
            pnl,
            muted(iso(p.opened_at))
        ));
        let _ = price;
    }
    Ok(())
}

async fn helper_deploy(rpc: Arc<Rpc>, cfg: &Config, live: bool) -> anyhow::Result<()> {
    let bytecode = helper_bytecode().context("compile contracts/ with `forge build`, or set BODKIN_HELPER_BYTECODE")?;
    info(format!("BodkinBuyOnce bytecode {} bytes", bytecode.len()));
    if !live {
        info(muted("dry run: pass --live to broadcast. After deploy, set HELPER_ADDRESS."));
        return Ok(());
    }
    let w = Wallet::require()?;
    let nonce = rpc.get_transaction_count(Lane::Hot, w.address(), true).await?;
    let clock = bodkin::pons::clock::ChainClock::default();
    if let Ok(h) = rpc.latest_header(Lane::Hot).await {
        clock.note_header(h.timestamp, bodkin::pons::clock::now_ms(), h.base_fee, h.number);
    }
    let (hash, raw) = w.sign_create(bytecode.into(), nonce, &clock)?;
    let sub = Submitter::new(cfg)?;
    match sub.send_raw(&raw).await {
        bodkin::trade::submitter::SendOutcome::Hash(h) => info(format!("{} deployed {h:#x}  set HELPER_ADDRESS to the create address", neon("ok"))),
        other => anyhow::bail!("deploy send: {other:?}"),
    }
    info(format!("signed create {hash:#x} from {:#x} nonce {nonce}", w.address()));
    Ok(())
}

async fn helper_check(rpc: Arc<Rpc>, cfg: &Config) -> anyhow::Result<()> {
    let Some(h) = cfg.helper else {
        anyhow::bail!("HELPER_ADDRESS unset");
    };
    let code = rpc.get_code(Lane::Hot, h).await?;
    if code.is_empty() {
        anyhow::bail!("no code at {h:#x}");
    }
    info(format!("{} {:#x}  {} bytes", neon("ok"), h, code.len()));
    Ok(())
}

fn helper_bytecode() -> anyhow::Result<alloy::primitives::Bytes> {
    if let Some(hex) = bodkin::config::env_str("BODKIN_HELPER_BYTECODE") {
        let raw = hex::decode(hex.trim().trim_start_matches("0x"))?;
        anyhow::ensure!(!raw.is_empty(), "BODKIN_HELPER_BYTECODE is empty");
        return Ok(raw.into());
    }
    for path in [
        "contracts/out/BodkinBuyOnce.sol/BodkinBuyOnce.json",
        "crates/bodkin/bytecode/BodkinBuyOnce.json",
    ] {
        if let Ok(text) = std::fs::read_to_string(path) {
            let v: serde_json::Value = serde_json::from_str(&text)?;
            if let Some(obj) = v.get("bytecode").and_then(|b| b.get("object")).and_then(|s| s.as_str()).or_else(|| v.get("bytecode").and_then(|s| s.as_str())) {
                let raw = hex::decode(obj.trim_start_matches("0x"))?;
                if !raw.is_empty() {
                    return Ok(raw.into());
                }
            }
        }
    }
    anyhow::bail!("no BodkinBuyOnce bytecode (run `forge build` in contracts/)")
}

async fn replay_cmd(rpc: Arc<Rpc>, hours: u64, sample: Option<u64>, entry_second: u64) -> anyhow::Result<()> {
    let blocks = hours * 3_600 * 10;
    let evs = recent_launches(&rpc, blocks, None, None).await.context("eth_getLogs (sampled by default; export a log file for full days)")?;
    let take = sample.unwrap_or(evs.len() as u64 / 10 + 1) as usize;
    let mut launches = Vec::new();
    for ev in evs.into_iter().rev().take(take) {
        launches.push(ReplayLaunch {
            token: format!("{:#x}", ev.token),
            launched_at: 0,
            start_bps: 9900,
            window: 3,
            taxed_buyers_s1: 0,
            exempt_buys_s0: 0,
            graduated: false,
            peak_progress: 0.0,
            intel: None,
        });
    }
    let rules = rules_from_env();
    let r = replay(&launches, &rules, &ReplayAlt { entry_second, min_taxed_s1: 0 });
    print_report(&r, &format!("entry +{entry_second}"));
    Ok(())
}

fn first_line(s: &str) -> String {
    s.lines().next().unwrap_or(s).to_string()
}
