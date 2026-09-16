use alloy::primitives::{Address, U256};
use alloy::sol_types::{SolCall, SolEvent};
use anyhow::Context;
use bodkin::abi::{escrow, factory};
use bodkin::alerts::eth_usd;
use bodkin::chain::{ADDR, CHAIN_ID};
use bodkin::config::{Config, env_str, parse_ether};
use bodkin::engine::{SnipeRules, try_rules_from_env};
use bodkin::fmt::{clean_text, eth, first_line, hhmmss, iso, pad, short, usd};
use bodkin::links::{Links, ref_line};
use bodkin::outcomes::{OutcomeLog, print_summary, summarize};
use bodkin::pons::deployer::{DeployerIndex, HistoryCoverage};
use bodkin::pons::enrich::{enrich_launch, read_token_meta};
use bodkin::pons::fees::fee_forensics;
use bodkin::pons::fingerprint::FarmDetector;
use bodkin::pons::launches::{
    FeedHealth, LaunchEvent, find_launch, graduations_in_range, launches_in_range, recent_launches,
    watch_launches,
};
use bodkin::pons::tax::snipe_tax_bps;
use bodkin::research::{
    CanonicalObservation, CaptureLimits, CaptureRecordStatus, CaptureStopReason, ResearchEventKind,
    ResearchProfile, ResearchRecorder,
};
use bodkin::rpc::{Lane, Rpc};
use bodkin::run::{EngineOptions, start_engine};
use bodkin::score::{ScoreContext, score_launch};
use bodkin::style::{banner, error, hr, info, loss, muted, neon, on_neon, warn, white};
use bodkin::trade::burst::BurstCtl;
use bodkin::trade::curve::{buy_on_curve, read_curve_state as read_cs};
use bodkin::trade::exec::{ExecKit, TxFinal};
use bodkin::trade::pool::{buy_on_pool, sell_anywhere, token_balance, value_now};
use bodkin::trade::positions::PositionStore;
use bodkin::trade::submitter::Submitter;
use bodkin::trade::v4::{detect_router_layout, pool_key_for, pool_liquidity, quote_v4};
use bodkin::trade::wallet::Wallet;
use bodkin::view::{ViewCtx, launch_card, launch_update};
use clap::{Parser, Subcommand};
use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::Arc;

#[derive(Parser)]
#[command(
    name = "bodkin",
    version,
    about = "The sniper terminal for pons v2 on Robinhood Chain. Local, open, non-custodial."
)]
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
    Scan {
        token: Address,
    },
    /// Follow one token live
    Watch {
        token: Address,
        #[arg(long, default_value = "5")]
        every: u64,
        #[arg(long)]
        r#for: Option<u64>,
    },
    /// Creator-fee forensics
    Fees {
        token: Address,
    },
    /// Deployer history
    Dev {
        address: Address,
        #[arg(long, default_value = "1728000")]
        blocks: u64,
    },
    /// Auto-buy launches that pass the rules
    Snipe {
        #[arg(long)]
        live: bool,
        #[arg(long, value_parser = parse_ether)]
        eth: Option<U256>,
        #[arg(long)]
        min_score: Option<i32>,
        #[arg(long)]
        max_tax_bps: Option<u64>,
        #[arg(long)]
        slippage: Option<u64>,
        #[arg(long)]
        keyword: Option<String>,
        #[arg(long)]
        deployer: Vec<Address>,
        #[arg(long)]
        max_open: Option<usize>,
        #[arg(long)]
        allow_pairs: bool,
        #[arg(long, value_parser = parse_ether)]
        budget: Option<U256>,
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
        token: Address,
        #[arg(value_parser = parse_ether)]
        eth: U256,
        #[arg(long)]
        live: bool,
        #[arg(long, default_value = "300")]
        slippage: u64,
    },
    Sell {
        token: Address,
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
        research: bool,
        #[arg(long, value_name = "DIR")]
        data_dir: Option<PathBuf>,
        #[arg(long, value_parser = parse_ether)]
        eth: Option<U256>,
        #[arg(long)]
        min_score: Option<i32>,
        #[arg(long)]
        keyword: Option<String>,
        #[arg(long)]
        allow_pairs: bool,
        #[arg(long, value_parser = parse_ether)]
        budget: Option<U256>,
        #[arg(long)]
        yes: bool,
    },
    #[command(subcommand)]
    Helper(HelperCmd),
    /// Print outcomes.jsonl summary
    Outcomes,
    Capture {
        #[arg(long, default_value = "86400")]
        r#for: u64,
        #[arg(long, default_value = "1073741824")]
        max_bytes: u64,
        #[arg(long, value_name = "DIR")]
        output: PathBuf,
    },
    /// Replay sampled launches against alternate rules
    Replay {
        #[arg(long, default_value = "6")]
        hours: u64,
        #[arg(long)]
        sample: Option<u64>,
        #[arg(long, default_value = "2")]
        entry_second: u64,
        #[arg(long, value_name = "DIR")]
        dataset: Option<PathBuf>,
        #[arg(long)]
        research: bool,
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
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_target(false)
        .init();
    let cli = Cli::parse();
    if let Err(e) = run(cli).await {
        error(first_line(&format!("{e:#}")));
        std::process::exit(1);
    }
}

async fn run(cli: Cli) -> anyhow::Result<()> {
    let cfg = Config::from_env()?;
    let rpc = Arc::new(Rpc::new(&cfg)?);
    match cli.cmd {
        Cmd::Doctor { probe } => doctor(rpc, &cfg, probe).await,
        Cmd::Hunt {
            backfill,
            min_score,
            fire_only,
            follow,
            json,
            poll,
            r#for,
        } => {
            hunt(
                rpc, cfg, backfill, min_score, fire_only, follow, json, poll, r#for,
            )
            .await
        }
        Cmd::Scan { token } => scan(rpc, token).await,
        Cmd::Watch {
            token,
            every,
            r#for,
        } => watch(rpc, token, every, r#for).await,
        Cmd::Fees { token } => fees(rpc, token).await,
        Cmd::Dev { address, blocks } => dev(rpc, address, blocks).await,
        Cmd::Snipe {
            live,
            eth,
            min_score,
            max_tax_bps,
            slippage,
            keyword,
            deployer,
            max_open,
            allow_pairs,
            budget,
            yes,
            r#for,
        } => {
            let rules = snipe_rules(
                eth,
                min_score,
                max_tax_bps,
                slippage,
                keyword,
                deployer,
                max_open,
                allow_pairs,
                budget,
            )?;
            banner("the sniper terminal for pons v2 on Robinhood Chain");
            if live {
                arm_live(&rpc, &cfg, &rules, yes).await?;
            }
            let emit = Arc::new(|e: serde_json::Value| {
                if e.get("kind").and_then(|k| k.as_str()) == Some("fire")
                    || e.get("kind").and_then(|k| k.as_str()) == Some("exit")
                {
                    tracing::info!("{e}");
                }
            });
            let engine = start_engine(rpc, cfg, rules, live, false, emit).await?;
            let stopped = async {
                while !engine.stopped() {
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                }
            };
            if let Some(sec) = r#for {
                tokio::select! {
                    _ = tokio::time::sleep(std::time::Duration::from_secs(sec)) => {}
                    _ = stopped => {}
                }
            } else {
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => {}
                    _ = stopped => {}
                }
            }
            engine.shutdown().await;
            info(muted("bodkin stopped"));
            Ok(())
        }
        Cmd::Wallet => wallet_cmd(rpc).await,
        Cmd::Claim { live } => claim_cmd(rpc, &cfg, live).await,
        Cmd::Buy {
            token,
            eth,
            live,
            slippage,
        } => buy_cmd(rpc, &cfg, token, eth, live, slippage).await,
        Cmd::Sell {
            token,
            pct,
            live,
            slippage,
        } => {
            sell_cmd(
                rpc,
                &cfg,
                token,
                pct.unwrap_or_else(|| "100".into()),
                live,
                slippage,
            )
            .await
        }
        Cmd::Positions => positions_cmd(rpc).await,
        Cmd::Board {
            port,
            live,
            research,
            data_dir,
            eth,
            min_score,
            keyword,
            allow_pairs,
            budget,
            yes,
        } => {
            anyhow::ensure!(
                !(research && live),
                "--research cannot be combined with --live"
            );
            anyhow::ensure!(
                research || data_dir.is_none(),
                "--data-dir requires --research"
            );
            let rules = snipe_rules(
                eth,
                min_score,
                None,
                None,
                keyword,
                vec![],
                None,
                allow_pairs,
                budget,
            )?;
            banner("the sniper terminal for pons v2 on Robinhood Chain");
            if live {
                arm_live(&rpc, &cfg, &rules, yes).await?;
            }
            let port = port.unwrap_or(cfg.board_port);
            if research {
                let data_dir = data_dir
                    .ok_or_else(|| anyhow::anyhow!("--research requires an explicit --data-dir"))?;
                let options = EngineOptions::research(ResearchProfile::default(), data_dir)?;
                bodkin::board::start_board_with_options(rpc, cfg, port, rules, options).await
            } else {
                bodkin::board::start_board(rpc, cfg, port, live, rules).await
            }
        }
        Cmd::Helper(HelperCmd::Deploy { live }) => helper_deploy(rpc, &cfg, live).await,
        Cmd::Helper(HelperCmd::Check) => helper_check(rpc, &cfg).await,
        Cmd::Outcomes => {
            let evs = OutcomeLog::open("data").read_all()?;
            print_summary(&summarize(&evs));
            Ok(())
        }
        Cmd::Capture {
            r#for,
            max_bytes,
            output,
        } => capture_cmd(rpc, cfg, r#for, max_bytes, output).await,
        Cmd::Replay {
            hours,
            sample,
            entry_second,
            dataset,
            research,
        } => {
            anyhow::ensure!(
                research || dataset.is_none(),
                "--dataset requires --research"
            );
            if research {
                let dataset =
                    dataset.ok_or_else(|| anyhow::anyhow!("--research requires --dataset"))?;
                let report = bodkin::replay::research_dataset_report(dataset)?;
                println!("{}", serde_json::to_string_pretty(&report)?);
                Ok(())
            } else {
                replay_cmd(rpc, hours, sample, entry_second).await
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn snipe_rules(
    eth: Option<U256>,
    min_score: Option<i32>,
    max_tax: Option<u64>,
    slippage: Option<u64>,
    keyword: Option<String>,
    deployers: Vec<Address>,
    max_open: Option<usize>,
    allow_pairs: bool,
    budget: Option<U256>,
) -> anyhow::Result<SnipeRules> {
    let mut r = try_rules_from_env()?;
    if let Some(eth) = eth {
        r.eth_per_buy = eth;
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
        r.deployers = deployers
            .into_iter()
            .map(|deployer| format!("{deployer:#x}"))
            .collect();
    }
    if let Some(n) = max_open {
        r.max_open_positions = n;
    }
    if allow_pairs {
        r.eth_pairs_only = false;
    }
    if let Some(budget) = budget {
        r.session_budget_wei = budget;
    }
    r.validate()?;
    Ok(r)
}

/// Live-mode preflight: rules must validate, a key and exact helper must exist,
/// and balance/budget must cover one buy plus worst-case burst gas before arm.
async fn arm_live(rpc: &Rpc, cfg: &Config, rules: &SnipeRules, yes: bool) -> anyhow::Result<()> {
    rules.validate()?;
    let acct = Wallet::require()?;
    let Some(helper) = cfg.helper else {
        anyhow::bail!("HELPER_ADDRESS unset — deploy it first (`bodkin helper deploy --live`)");
    };
    verify_helper_code(rpc, helper).await?;
    let header = rpc.latest_header(Lane::Hot).await?;
    let clock = bodkin::pons::clock::ChainClock::default();
    clock.note_header(
        header.timestamp,
        bodkin::pons::clock::now_ms(),
        header.base_fee,
        header.number,
    );
    let burst_attempts = BurstCtl::from_env().max;
    let gas_reserve = acct.worst_case_gas_wei(&clock, burst_attempts);
    let required = rules.eth_per_buy.saturating_add(gas_reserve);
    let bal = rpc.get_balance(Lane::Hot, acct.address()).await?;
    let price = eth_usd().await;
    info(on_neon(" LIVE "));
    info(format!("   wallet     {:#x}", acct.address()));
    info(format!(
        "   balance    {} ETH {}",
        eth(bal),
        muted(usd(price.map(|p| wei_eth(bal) * p)))
    ));
    info(format!(
        "   per buy    {} ETH {}   max open {}",
        eth(rules.eth_per_buy),
        muted(usd(price.map(|p| wei_eth(rules.eth_per_buy) * p))),
        rules.max_open_positions
    ));
    info(format!(
        "   burst gas  up to {} ETH across {burst_attempts} attempts",
        eth(gas_reserve)
    ));
    info(format!(
        "   budget     {} ETH {}",
        eth(rules.session_budget_wei),
        muted(usd(price.map(|p| wei_eth(rules.session_budget_wei) * p)))
    ));
    if bal < required {
        anyhow::bail!(
            "balance {} ETH does not cover one buy plus reserved burst gas ({})",
            eth(bal),
            eth(required)
        );
    }
    if rules.session_budget_wei < required {
        anyhow::bail!(
            "session budget is below one buy plus reserved burst gas ({})",
            eth(required)
        );
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
    info(format!(
        "{}        {}  {rtt} ms round trip  {} chain {chain}, block {block}, gas {} ETH",
        white("rpc"),
        cfg.http_labels(),
        ok(chain == CHAIN_ID),
        eth(gas)
    ));
    info(format!(
        "{}  {}",
        white("websocket"),
        muted(cfg.ws_labels())
    ));
    let hook: Address = rpc
        .eth_call(
            Lane::Enrich,
            ADDR.pons_factory,
            factory::memeHookCall {},
            None,
        )
        .await?;
    let escrow_a: Address = rpc
        .eth_call(
            Lane::Enrich,
            ADDR.pons_factory,
            factory::feeEscrowCall {},
            None,
        )
        .await?;
    let pm: Address = rpc
        .eth_call(
            Lane::Enrich,
            ADDR.pons_factory,
            factory::poolManagerCall {},
            None,
        )
        .await?;
    let fee: U256 = rpc
        .eth_call(
            Lane::Enrich,
            ADDR.pons_factory,
            factory::launchFeeCall {},
            None,
        )
        .await?;
    let tax: U256 = rpc
        .eth_call(
            Lane::Enrich,
            ADDR.pons_factory,
            factory::snipeTaxStartBpsCall {},
            None,
        )
        .await?;
    let tax_sec: U256 = rpc
        .eth_call(
            Lane::Enrich,
            ADDR.pons_factory,
            factory::snipeTaxSecondsCall {},
            None,
        )
        .await?;
    let max_tax: U256 = rpc
        .eth_call(
            Lane::Enrich,
            ADDR.pons_factory,
            factory::maxCreatorTaxBpsCall {},
            None,
        )
        .await?;
    let enabled: bool = rpc
        .eth_call(
            Lane::Enrich,
            ADDR.pons_factory,
            factory::launchEnabledCall {},
            None,
        )
        .await?;
    info(format!(
        "{}    {:#x}  launches {}",
        white("factory"),
        ADDR.pons_factory,
        if enabled { "enabled" } else { "DISABLED" }
    ));
    info(format!(
        "   hook {}  escrow {}  poolManager {}",
        ok(hook == ADDR.pons_hook),
        ok(escrow_a == ADDR.pons_escrow),
        ok(pm == ADDR.v4_pool_manager)
    ));
    let start: u64 = tax.try_into().unwrap_or(0);
    let win: u64 = tax_sec.try_into().unwrap_or(3);
    let opening_tax = start as f64 / 100.0;
    let max_creator_tax = u64::try_from(max_tax).unwrap_or(0) as f64 / 100.0;
    info(format!(
        "   launch fee {} ETH   opening tax {opening_tax:.2}% decaying over {win}s ({} / {} / {} / 0)   max creator tax {max_creator_tax:.2}%",
        eth(fee),
        snipe_tax_bps(start, win, 0),
        snipe_tax_bps(start, win, 1),
        snipe_tax_bps(start, win, 2),
    ));
    match recent_launches(&rpc, 3_000, None, None).await {
        Ok(r) => info(format!(
            "{}      {} launches in the last 3000 blocks (~5 min)",
            white("tempo"),
            r.len()
        )),
        Err(e) => warn(format!("tempo: {e}")),
    }
    info(format!(
        "{}    {}",
        white("eth/usd"),
        eth_usd()
            .await
            .map(|p| format!("${p:.0}"))
            .unwrap_or_else(|| muted("offline"))
    ));
    info(format!(
        "{}     {}",
        white("wallet"),
        if env_str("PRIVATE_KEY").is_some() {
            "key present (used only by live trades)"
        } else {
            "no key, analytics and dry-run only"
        }
    ));
    if let Some(h) = cfg.helper {
        let code = rpc.get_code(Lane::Background, h).await?;
        info(format!(
            "{}    {:#x}  {} ({} bytes)",
            white("helper"),
            h,
            if code.is_empty() {
                loss("NO CODE")
            } else {
                neon("ok")
            },
            code.len()
        ));
    } else {
        info(format!(
            "{}    {}",
            white("helper"),
            muted("HELPER_ADDRESS unset")
        ));
    }
    if let Ok(sub) = Submitter::new(cfg) {
        match sub.resolve_and_pin().await {
            Ok(ips) => {
                for ip in &ips {
                    info(format!(
                        "{}   {}  {} ms",
                        white("sequencer"),
                        ip.ip,
                        ip.rtt_ms
                    ));
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
            match sub
                .send_conditional(&dummy, serde_json::json!({"timestampMin": future}))
                .await
            {
                Err((-32003, msg)) => info(format!(
                    "{}  immediate -32003 ({}); Conditional is a cheap reject, not a park",
                    white("conditional"),
                    first_line(&msg)
                )),
                Err((code, msg)) => info(format!(
                    "{}  {code} {}",
                    white("conditional"),
                    first_line(&msg)
                )),
                Ok(_) => {
                    warn("conditional accepted a dummy; do not put timestampMin on the fire path")
                }
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
            if let Ok(g) = factory::PoolGraduated::decode_log(l.as_ref())
                && let Ok((key, pair, _)) = pool_key_for(&rpc, g.token).await
                && pair == Address::ZERO
            {
                picked = Some((g.token, key));
                break;
            }
        }
        if let Some((tok, key)) = picked {
            let q = quote_v4(&rpc, &key, true, U256::from(10u64.pow(15)))
                .await
                .unwrap_or(U256::ZERO);
            let st = pool_liquidity(&rpc, &key).await.ok();
            let layout = detect_router_layout(&rpc, &key).await.ok();
            info(format!(
                "{}   {}: 0.001 ETH → {} tokens, liquidity {}, router {:?}",
                white("v4 probe"),
                short(&format!("{tok:#x}"), 4),
                bodkin::fmt::tokens(q),
                st.map(|liquidity| liquidity.to_string())
                    .unwrap_or_else(|| "?".into()),
                layout
            ));
        } else {
            warn("no ETH-paired graduation in the last 200k blocks to probe against");
        }
        let _ = BurstCtl::from_env();
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn hunt(
    rpc: Arc<Rpc>,
    cfg: Config,
    backfill: u64,
    min_score: i32,
    fire_only: bool,
    follow: bool,
    json: bool,
    poll: Option<u64>,
    for_s: Option<u64>,
) -> anyhow::Result<()> {
    let mut cfg = cfg;
    if let Some(poll_ms) = poll {
        anyhow::ensure!(
            (10..=60_000).contains(&poll_ms),
            "--poll must be 10..=60000 ms"
        );
        cfg.poll_ms = poll_ms;
    }
    let price = eth_usd().await;
    let links = Links::from_env();
    if !json {
        banner("the sniper terminal for pons v2 on Robinhood Chain");
        info(format!(
            "{} {}",
            neon("bodkin"),
            muted(format!(
                "hunt · pons v2 · Robinhood Chain ({CHAIN_ID}) · {}",
                cfg.ws_labels()
            ))
        ));
        let rl = ref_line(&links);
        if !rl.is_empty() {
            info(rl);
        }
        info(hr(72));
    }
    let index = Arc::new(tokio::sync::Mutex::new(DeployerIndex::default()));
    let index_task = {
        let rpc = rpc.clone();
        let index = index.clone();
        tokio::spawn(async move {
            index.lock().await.begin_history();
            let scanned = async {
                let head = rpc.block_number(Lane::Background).await?;
                let from = head.saturating_sub(1_728_000);
                let anchor = rpc.block_hash(Lane::Background, head).await?;
                let events = launches_in_range(&rpc, from, head, None, None).await?;
                let graduations = graduations_in_range(&rpc, from, head).await?;
                anyhow::ensure!(
                    rpc.block_hash(Lane::Background, head).await? == anchor,
                    "deployer history anchor changed during scan"
                );
                anyhow::Ok((head, from, anchor, events, graduations))
            }
            .await;
            match scanned {
                Ok((head, from, anchor, events, graduations)) => {
                    let mut idx = index.lock().await;
                    for event in &events {
                        idx.note(event);
                    }
                    for (token, block) in graduations {
                        idx.mark_graduated(token, block);
                    }
                    idx.mark_ready(HistoryCoverage {
                        chain_id: CHAIN_ID,
                        factory: ADDR.pons_factory,
                        from_block: from,
                        to_block: head,
                        anchor,
                    });
                }
                Err(error) => index
                    .lock()
                    .await
                    .mark_failed(first_line(&error.to_string())),
            }
        })
    };
    let farms = Arc::new(parking_lot::Mutex::new(FarmDetector::default()));
    if backfill > 0
        && let Ok(recent) = recent_launches(&rpc, 3_000, None, None).await
    {
        for ev in recent
            .into_iter()
            .rev()
            .take(backfill as usize)
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
        {
            present(
                &rpc, ev, price, min_score, fire_only, json, &index, &farms, &links,
            )
            .await;
        }
        if !json {
            info(hr(72));
        }
    }
    let (tx, mut rx) = tokio::sync::mpsc::channel(1_024);
    let feed = Arc::new(parking_lot::Mutex::new(FeedHealth::default()));
    let watch = watch_launches(rpc.clone(), cfg, tx, feed);
    let deadline = for_s.map(|s| tokio::time::Instant::now() + std::time::Duration::from_secs(s));
    let mut follows = tokio::task::JoinSet::new();
    loop {
        let ev = tokio::select! {
            _ = tokio::signal::ctrl_c() => break,
            _ = async { if let Some(d) = deadline { tokio::time::sleep_until(d).await } else { std::future::pending::<()>().await } } => break,
            Some(ev) = rx.recv() => ev,
            else => break,
        };
        let intel = present(
            &rpc,
            ev.clone(),
            price,
            min_score,
            fire_only,
            json,
            &index,
            &farms,
            &links,
        )
        .await;
        while follows.try_join_next().is_some() {}
        if follow && let Some(intel) = intel {
            if follows.len() >= 64 {
                warn("follow queue full; launch remains in the main feed");
                continue;
            }
            let rpc = rpc.clone();
            let dq = index.lock().await.quick(ev.deployer, ev.block_number);
            follows.spawn(async move { follow_launch(&rpc, intel, dq, price).await });
        }
    }
    watch.shutdown().await;
    follows.abort_all();
    while follows.join_next().await.is_some() {}
    index_task.abort();
    let _ = index_task.await;
    info(muted("bodkin stopped"));
    Ok(())
}

#[allow(clippy::too_many_arguments)]
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
    let score = score_launch(
        &intel,
        &ScoreContext {
            deployer: dq,
            farm_twins: twins,
            ..Default::default()
        },
    );
    if score.total < min_score || (fire_only && score.verdict != bodkin::score::Verdict::Fire) {
        return None;
    }
    if json {
        println!(
            "{}",
            serde_json::json!({"t": chrono::Utc::now().to_rfc3339(), "token": format!("{:#x}", ev.token), "score": score.total, "verdict": score.verdict.as_str()})
        );
    } else {
        println!(
            "{}",
            launch_card(
                &intel,
                &score,
                &ViewCtx {
                    eth_usd: price,
                    deployer: dq,
                    activity: None
                },
                links
            )
        );
        println!(
            "   {}",
            muted(format!("read · {}", intel.errors.join("; ")))
        );
    }
    if let Ok(mut f) = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open("data/launches.jsonl")
    {
        use std::io::Write;
        let _ = writeln!(
            f,
            "{}",
            serde_json::json!({"t": bodkin::pons::clock::now_ms(), "token": format!("{:#x}", ev.token), "score": score.total})
        );
    }
    Some(intel)
}

async fn follow_launch(
    rpc: &Rpc,
    intel: bodkin::pons::enrich::LaunchIntel,
    dq: Option<(u32, u32)>,
    price: Option<f64>,
) {
    let launch_ts = intel
        .tx
        .as_ref()
        .map(|t| t.timestamp)
        .unwrap_or(bodkin::pons::clock::now_ms() / 1000);
    for at in [15u64, 60] {
        let wait = (launch_ts + at) * 1000;
        let now = bodkin::pons::clock::now_ms();
        if wait > now {
            tokio::time::sleep(std::time::Duration::from_millis(wait - now)).await;
        }
        if let (Ok(act), Ok(curve)) = (
            curve_activity_safe(rpc, intel.ev.curve, intel.ev.block_number).await,
            read_cs(rpc, intel.ev.curve, bodkin::chain::DEAD).await,
        ) {
            let mut fresh = intel.clone();
            fresh.curve = Some(curve);
            let score = score_launch(
                &fresh,
                &ScoreContext {
                    deployer: dq,
                    activity: Some(act.clone()),
                    age_sec: Some(at),
                    ..Default::default()
                },
            );
            println!(
                "{}",
                launch_update(
                    &fresh,
                    &score,
                    &act,
                    at,
                    &ViewCtx {
                        eth_usd: price,
                        deployer: dq,
                        activity: Some(act.clone())
                    }
                )
            );
        }
    }
}

async fn curve_activity_safe(
    rpc: &Rpc,
    curve: Address,
    from: u64,
) -> anyhow::Result<bodkin::pons::enrich::CurveActivity> {
    bodkin::pons::enrich::curve_activity(rpc, curve, from, None).await
}

async fn scan(rpc: Arc<Rpc>, token: Address) -> anyhow::Result<()> {
    let price = eth_usd().await;
    let ev = find_launch(&rpc, token)
        .await?
        .ok_or_else(|| anyhow::anyhow!("no pons v2 TokenLaunched event found"))?;
    let intel = enrich_launch(&rpc, ev.clone(), bodkin::chain::DEAD).await;
    let act = curve_activity_safe(&rpc, ev.curve, ev.block_number)
        .await
        .ok();
    let score = score_launch(
        &intel,
        &ScoreContext {
            activity: act.clone(),
            age_sec: intel
                .tx
                .as_ref()
                .map(|t| bodkin::pons::clock::now_ms() / 1000 - t.timestamp),
            ..Default::default()
        },
    );
    let links = Links::from_env();
    println!(
        "{}",
        launch_card(
            &intel,
            &score,
            &ViewCtx {
                eth_usd: price,
                deployer: None,
                activity: act
            },
            &links
        )
    );
    info(hr(72));
    info(format!(
        "{}   {}  block {}  tx {}",
        white("launched"),
        intel
            .tx
            .as_ref()
            .map(|t| iso(t.timestamp))
            .unwrap_or_else(|| "?".into()),
        ev.block_number,
        muted(format!("{:#x}", ev.tx_hash))
    ));
    info(format!("{}   {:#x}", white("explorer"), token));
    Ok(())
}

async fn watch(
    rpc: Arc<Rpc>,
    token: Address,
    every: u64,
    for_s: Option<u64>,
) -> anyhow::Result<()> {
    let ev = find_launch(&rpc, token)
        .await?
        .ok_or_else(|| anyhow::anyhow!("no TokenLaunched"))?;
    let intel = enrich_launch(&rpc, ev.clone(), bodkin::chain::DEAD).await;
    let sym = intel
        .meta
        .as_ref()
        .map(|m| format!("${}", clean_text(&m.symbol, 16)))
        .unwrap_or_else(|| short(&format!("{token:#x}"), 4));
    let name = clean_text(
        intel.meta.as_ref().map(|m| m.name.as_str()).unwrap_or("?"),
        40,
    );
    info(format!("{} {}  {:#x}", white(name), neon(&sym), token));
    let t0 = std::time::Instant::now();
    loop {
        if let Ok(rec) = rpc
            .eth_call(
                Lane::Hot,
                ADDR.pons_factory,
                factory::getLaunchedTokenCall { token },
                None,
            )
            .await
        {
            if rec.phase == 0 {
                if let Ok(c) = read_cs(&rpc, ev.curve, bodkin::chain::DEAD).await {
                    let p = bodkin::pons::curve::progress(&c);
                    info(format!(
                        "{}  curve {:.1}%  {}/{} ETH  tax {}",
                        muted(hhmmss(None)),
                        p * 100.0,
                        eth(c.real_quote_reserve),
                        eth(c.graduation_threshold),
                        bodkin::fmt::bps(c.opening_tax_bps.try_into().unwrap_or(0))
                    ));
                }
            } else if rec.phase == 2
                && let Ok((key, pair, _)) = pool_key_for(&rpc, token).await
                && pair == Address::ZERO
                && let Ok(out) = quote_v4(&rpc, &key, false, U256::from(10u128.pow(24))).await
            {
                info(format!(
                    "{}  pool  1M tokens → {} ETH",
                    muted(hhmmss(None)),
                    eth(out)
                ));
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
    let ev = find_launch(&rpc, token)
        .await?
        .ok_or_else(|| anyhow::anyhow!("no TokenLaunched"))?;
    let rec = rpc
        .eth_call(
            Lane::Background,
            ADDR.pons_factory,
            factory::getLaunchedTokenCall { token },
            None,
        )
        .await?;
    let fees = fee_forensics(&rpc, rec.creatorFeeRecipient, ev.curve, ev.block_number).await?;
    info(format!(
        "{}  recipient {:#x}  credited {} ETH  claimed {} ETH  pending {} ETH",
        white("fees"),
        fees.recipient,
        eth(fees.total_credited),
        eth(fees.total_claimed),
        eth(fees.pending)
    ));
    for c in &fees.claims {
        info(format!(
            "   {}  {}  {:#x}",
            if c.timestamp > 0 {
                iso(c.timestamp)
            } else {
                format!("block {}", c.block)
            },
            pad(&format!("{} ETH", eth(c.amount)), 14),
            c.tx
        ));
    }
    Ok(())
}

async fn dev(rpc: Arc<Rpc>, deployer: Address, blocks: u64) -> anyhow::Result<()> {
    let evs = recent_launches(&rpc, blocks, Some(deployer), None).await?;
    info(format!(
        "{:#x}  {} launches in {blocks} blocks",
        deployer,
        evs.len()
    ));
    info(hr(72));
    for ev in evs.iter().rev().take(25) {
        let meta = read_token_meta(&rpc, ev.token).await.ok();
        let symbol = meta
            .as_ref()
            .map(|m| format!("${}", clean_text(&m.symbol, 16)))
            .unwrap_or_else(|| "?".into());
        let name = clean_text(
            meta.as_ref().map(|m| m.name.as_str()).unwrap_or_default(),
            40,
        );
        info(format!(
            "   {}  {}  {:#x}",
            pad(&symbol, 12),
            pad(&name, 28),
            ev.token
        ));
    }
    Ok(())
}

async fn wallet_cmd(rpc: Arc<Rpc>) -> anyhow::Result<()> {
    let Some(w) = Wallet::from_env()? else {
        info(muted("no PRIVATE_KEY in .env"));
        return Ok(());
    };
    let bal = rpc.get_balance(Lane::Hot, w.address()).await?;
    let pending: U256 = rpc
        .eth_call(
            Lane::Background,
            ADDR.pons_escrow,
            escrow::balanceOfCall {
                recipient: w.address(),
            },
            None,
        )
        .await
        .unwrap_or(U256::ZERO);
    let price = eth_usd().await;
    info(format!("{}   {:#x}", white("address"), w.address()));
    info(format!(
        "{}   {} ETH {}",
        white("balance"),
        eth(bal),
        muted(usd(price.map(|p| wei_eth(bal) * p)))
    ));
    info(format!(
        "{}      {} ETH unclaimed",
        white("fees"),
        eth(pending)
    ));
    Ok(())
}

fn finish_command<T>(
    kit: Option<&ExecKit>,
    operation_id: Option<&str>,
    result: anyhow::Result<T>,
) -> anyhow::Result<T> {
    match result {
        Ok(value) => {
            if let (Some(kit), Some(operation_id)) = (kit, operation_id) {
                kit.complete(operation_id)?;
            }
            Ok(value)
        }
        Err(error) => {
            if let (Some(kit), Some(operation_id)) = (kit, operation_id)
                && !kit.fail_if_resolved(operation_id)?
            {
                anyhow::bail!(
                    "{error}; transaction state is unresolved and must be reconciled before retrying"
                );
            }
            Err(error)
        }
    }
}

async fn claim_cmd(rpc: Arc<Rpc>, cfg: &Config, live: bool) -> anyhow::Result<()> {
    let w = Wallet::require()?;
    let pending: U256 = rpc
        .eth_call(
            Lane::Hot,
            ADDR.pons_escrow,
            escrow::balanceOfCall {
                recipient: w.address(),
            },
            None,
        )
        .await?;
    if pending.is_zero() {
        info(muted(format!("nothing to claim for {:#x}", w.address())));
        return Ok(());
    }
    info(format!(
        "{} {} ETH for {:#x}",
        if live {
            "claiming"
        } else {
            "dry run: would claim"
        },
        eth(pending),
        w.address()
    ));
    if !live {
        return Ok(());
    }
    let kit = ExecKit::arm(&rpc, cfg).await?;
    let operation_id =
        kit.begin_event_command("claim", ADDR.pons_escrow, escrow::Claimed::SIGNATURE_HASH)?;
    let data = escrow::claimCall {}.abi_encode();
    // Send AND confirm: a hash is not a claim.
    let final_state = match kit
        .exec_for(&rpc, &operation_id)
        .send_and_wait(ADDR.pons_escrow, U256::ZERO, data.into(), None)
        .await
    {
        Ok(final_state) => final_state,
        Err(error) => {
            if !kit.fail_if_resolved(&operation_id)? {
                anyhow::bail!(
                    "{error}; claim transaction state is unresolved and must be reconciled before retrying"
                );
            }
            return Err(error);
        }
    };
    match final_state {
        TxFinal::Confirmed { hash, logs, .. } => {
            let claimed = logs
                .iter()
                .filter(|log| log.address() == ADDR.pons_escrow)
                .filter_map(|log| escrow::Claimed::decode_log(&log.clone().into()).ok())
                .filter(|event| event.recipient == w.address())
                .fold(U256::ZERO, |sum, event| sum.saturating_add(event.amount));
            anyhow::ensure!(
                !claimed.is_zero(),
                "claim {hash:#x} confirmed but emitted no nonzero Claimed event for the signer"
            );
            kit.complete(&operation_id)?;
            info(format!(
                "{} claimed {} ETH ({hash:#x})",
                neon("ok"),
                eth(claimed)
            ));
        }
        TxFinal::Reverted { hash, .. } => {
            kit.fail_if_resolved(&operation_id)?;
            anyhow::bail!("claim reverted on-chain ({hash:#x})")
        }
        TxFinal::Unresolved { hash } => {
            anyhow::bail!(
                "claim {hash:#x} accepted, no receipt yet — it may still land; restart to reconcile before retrying"
            )
        }
    }
    Ok(())
}

async fn buy_cmd(
    rpc: Arc<Rpc>,
    cfg: &Config,
    token: Address,
    amount: U256,
    live: bool,
    slip: u64,
) -> anyhow::Result<()> {
    let rec = rpc
        .eth_call(
            Lane::Enrich,
            ADDR.pons_factory,
            factory::getLaunchedTokenCall { token },
            None,
        )
        .await?;
    if !rec.exists {
        anyhow::bail!("not a pons v2 token");
    }
    anyhow::ensure!(
        matches!(rec.phase, 0 | 2),
        "phase {}: trading is halted between sweep and pool creation",
        rec.phase
    );
    let kit = if live {
        Some(ExecKit::arm(&rpc, cfg).await?)
    } else {
        None
    };
    let operation_id = kit
        .as_ref()
        .map(|kit| kit.begin_command(format!("buy:{token:#x}")))
        .transpose()?;
    let exec = match (&kit, &operation_id) {
        (Some(kit), Some(operation_id)) => Some(kit.exec_for(&rpc, operation_id)),
        _ => None,
    };
    let result = if rec.phase == 0 {
        buy_on_curve(&rpc, exec.as_ref(), rec.curve, amount, slip, !live, None).await
    } else if rec.phase == 2 {
        buy_on_pool(&rpc, exec.as_ref(), token, amount, slip, !live).await
    } else {
        anyhow::bail!(
            "phase {}: trading is halted between sweep and pool creation",
            rec.phase
        )
    };
    let res = finish_command(kit.as_ref(), operation_id.as_deref(), result)?;
    info(format!(
        "{} {} ETH → {} tokens on the {}",
        if live { "bought" } else { "dry run:" },
        eth(res.eth_in),
        bodkin::fmt::tokens(res.tokens_out.unwrap_or(res.tokens_quoted)),
        res.venue
    ));
    if let Some(h) = res.hash {
        info(format!("   tx {h:#x}"));
    }
    Ok(())
}

async fn sell_cmd(
    rpc: Arc<Rpc>,
    cfg: &Config,
    token: Address,
    pct: String,
    live: bool,
    slip: u64,
) -> anyhow::Result<()> {
    let w = Wallet::require()?;
    // Strict percent: garbage must not silently become 100.
    let pct: f64 = pct
        .parse()
        .map_err(|_| anyhow::anyhow!("invalid sell percent: {pct}"))?;
    anyhow::ensure!(
        pct.is_finite() && pct > 0.0 && pct <= 100.0,
        "sell percent must be in (0, 100], got {pct}"
    );
    let bal = token_balance(&rpc, token, w.address()).await?;
    let amount = bal * U256::from((pct * 100.0) as u64) / U256::from(10_000u64);
    if amount.is_zero() {
        anyhow::bail!("nothing to sell");
    }
    let kit = if live {
        Some(ExecKit::arm(&rpc, cfg).await?)
    } else {
        None
    };
    let operation_id = kit
        .as_ref()
        .map(|kit| kit.begin_command(format!("sell:{token:#x}")))
        .transpose()?;
    let exec = match (&kit, &operation_id) {
        (Some(kit), Some(operation_id)) => Some(kit.exec_for(&rpc, operation_id)),
        _ => None,
    };
    let result = sell_anywhere(&rpc, exec.as_ref(), token, amount, slip, !live).await;
    let res = finish_command(kit.as_ref(), operation_id.as_deref(), result)?;
    info(format!(
        "{} {} tokens → {} ETH on the {}",
        if live { "sold" } else { "dry run:" },
        bodkin::fmt::tokens(res.tokens_in),
        eth(res.eth_out.unwrap_or(res.eth_quoted)),
        res.venue
    ));
    if let Some(h) = res.hash {
        info(format!("   tx {h:#x}"));
    }
    Ok(())
}

async fn positions_cmd(rpc: Arc<Rpc>) -> anyhow::Result<()> {
    let list = PositionStore::open("data")?.load();
    if list.is_empty() {
        info(muted("no positions yet"));
        return Ok(());
    }
    let price = eth_usd().await;
    for p in list {
        let entry: U256 = p.entry_eth.parse().unwrap_or(U256::ZERO);
        let basis = p.basis();
        let mut mark: U256 = p.last_eth.parse().unwrap_or(basis);
        if p.status == "open" {
            if let Ok(Some(value)) = value_now(&rpc, p.token, p.held()).await {
                mark = value.eth;
            }
        } else {
            mark = p.realized_out();
        }
        let pnl = p
            .net_pnl_pct(if p.status == "open" {
                mark
            } else {
                p.realized_out()
            })
            .map(|value| format!("{value:+.1}%"))
            .unwrap_or_else(|| "n/a".into());
        info(format!(
            "{} ${} {}  in {} ETH  {} {} ETH  {}  {}",
            if p.status == "open" {
                neon("open  ")
            } else {
                muted("closed")
            },
            pad(&clean_text(&p.symbol, 16), 8),
            if p.dry_run {
                muted("dry")
            } else {
                white("live")
            },
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
    let bytecode = helper_bytecode()
        .context("compile contracts/ with `forge build`, or set BODKIN_HELPER_BYTECODE")?;
    info(format!("BodkinBuyOnce bytecode {} bytes", bytecode.len()));
    if !live {
        info(muted(
            "dry run: pass --live to broadcast. After deploy, set HELPER_ADDRESS.",
        ));
        return Ok(());
    }
    let kit = ExecKit::arm(&rpc, cfg).await?;
    let nonce = kit.wallet.peek_nonce();
    let deployed = kit.wallet.address().create(nonce);
    let runtime = helper_runtime_bytecode()?;
    let operation_id = kit.begin_runtime_deploy("helper_deploy", deployed, &runtime)?;
    let final_state = match kit
        .exec_for(&rpc, &operation_id)
        .deploy_and_wait(bytecode)
        .await
    {
        Ok(final_state) => final_state,
        Err(error) => {
            if !kit.fail_if_resolved(&operation_id)? {
                anyhow::bail!(
                    "{error}; deployment transaction state is unresolved and must be reconciled before retrying"
                );
            }
            return Err(error);
        }
    };
    match final_state {
        TxFinal::Confirmed {
            block_number,
            contract_address,
            ..
        } => {
            anyhow::ensure!(
                contract_address == Some(deployed),
                "deployment receipt address {:?} does not match predicted {deployed:#x}",
                contract_address
            );
            verify_helper_code(&rpc, deployed).await?;
            kit.complete(&operation_id)?;
            info(format!(
                "{} deployed at {deployed:#x} (block {block_number}) — set HELPER_ADDRESS={deployed:#x}",
                neon("ok")
            ));
        }
        TxFinal::Reverted { hash, .. } => {
            kit.fail_if_resolved(&operation_id)?;
            anyhow::bail!("deploy reverted on-chain ({hash:#x})")
        }
        TxFinal::Unresolved { hash } => {
            anyhow::bail!(
                "deploy {hash:#x} sent but unconfirmed — restart to reconcile before retrying"
            )
        }
    }
    Ok(())
}

async fn helper_check(rpc: Arc<Rpc>, cfg: &Config) -> anyhow::Result<()> {
    let Some(h) = cfg.helper else {
        anyhow::bail!("HELPER_ADDRESS unset");
    };
    let code = verify_helper_code(&rpc, h).await?;
    info(format!("{} {:#x}  {} bytes", neon("ok"), h, code.len()));
    Ok(())
}

const COMMITTED_HELPER_ARTIFACT: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/bytecode/BodkinBuyOnce.json"
));

async fn verify_helper_code(
    rpc: &Rpc,
    address: Address,
) -> anyhow::Result<alloy::primitives::Bytes> {
    let code = rpc.get_code(Lane::Hot, address).await?;
    anyhow::ensure!(!code.is_empty(), "no code at {address:#x}");
    let expected = helper_runtime_bytecode()?;
    anyhow::ensure!(
        code == expected,
        "runtime bytecode at {address:#x} does not match BodkinBuyOnce"
    );
    Ok(code)
}

fn helper_runtime_bytecode() -> anyhow::Result<alloy::primitives::Bytes> {
    if let Some(encoded) = bodkin::config::env_str("BODKIN_HELPER_RUNTIME_BYTECODE") {
        let raw = hex::decode(encoded.trim().trim_start_matches("0x"))?;
        anyhow::ensure!(!raw.is_empty(), "BODKIN_HELPER_RUNTIME_BYTECODE is empty");
        return Ok(raw.into());
    }
    let built = std::fs::read_to_string("contracts/out/BodkinBuyOnce.sol/BodkinBuyOnce.json").ok();
    for text in built
        .as_deref()
        .into_iter()
        .chain(std::iter::once(COMMITTED_HELPER_ARTIFACT))
    {
        let value: serde_json::Value = serde_json::from_str(text)?;
        if let Some(object) = value
            .get("deployedBytecode")
            .and_then(|bytecode| bytecode.get("object"))
            .and_then(|object| object.as_str())
        {
            let raw = hex::decode(object.trim_start_matches("0x"))?;
            if !raw.is_empty() {
                return Ok(raw.into());
            }
        }
    }
    anyhow::bail!(
        "no BodkinBuyOnce runtime bytecode (run `forge build` in contracts/, or set BODKIN_HELPER_RUNTIME_BYTECODE)"
    )
}

fn helper_bytecode() -> anyhow::Result<alloy::primitives::Bytes> {
    if let Some(hex) = bodkin::config::env_str("BODKIN_HELPER_BYTECODE") {
        let raw = hex::decode(hex.trim().trim_start_matches("0x"))?;
        anyhow::ensure!(!raw.is_empty(), "BODKIN_HELPER_BYTECODE is empty");
        return Ok(raw.into());
    }
    let built = std::fs::read_to_string("contracts/out/BodkinBuyOnce.sol/BodkinBuyOnce.json").ok();
    for text in built
        .as_deref()
        .into_iter()
        .chain(std::iter::once(COMMITTED_HELPER_ARTIFACT))
    {
        let value: serde_json::Value = serde_json::from_str(text)?;
        if let Some(object) = value
            .get("bytecode")
            .and_then(|bytecode| bytecode.get("object"))
            .and_then(|object| object.as_str())
            .or_else(|| value.get("bytecode").and_then(|bytecode| bytecode.as_str()))
        {
            let raw = hex::decode(object.trim_start_matches("0x"))?;
            if !raw.is_empty() {
                return Ok(raw.into());
            }
        }
    }
    anyhow::bail!("no BodkinBuyOnce bytecode (run `forge build` in contracts/)")
}

struct PendingCapture {
    due: tokio::time::Instant,
    event: LaunchEvent,
    launched_at: u64,
    insiders: Vec<Address>,
}

struct FollowCapture {
    canonical: CanonicalObservation,
    data: serde_json::Value,
}

async fn capture_cmd(
    rpc: Arc<Rpc>,
    cfg: Config,
    duration_seconds: u64,
    max_bytes: u64,
    output: PathBuf,
) -> anyhow::Result<()> {
    let _isolation = EngineOptions::research(ResearchProfile::default(), output.clone())?;
    let limits = CaptureLimits {
        duration_seconds,
        max_bytes,
    }
    .validate()?;
    let mut recorder = ResearchRecorder::create(output, limits)?;
    let (tx, mut rx) = tokio::sync::mpsc::channel(1_024);
    let health = Arc::new(parking_lot::Mutex::new(FeedHealth::default()));
    let watch = watch_launches(rpc.clone(), cfg, tx, health);
    let deadline = tokio::time::Instant::now()
        .checked_add(std::time::Duration::from_secs(duration_seconds))
        .ok_or_else(|| anyhow::anyhow!("capture deadline overflows monotonic time"))?;
    let follow_limit = Arc::new(tokio::sync::Semaphore::new(4));
    let mut pending = VecDeque::<PendingCapture>::new();
    let mut followers = tokio::task::JoinSet::new();
    let stop_reason = 'capture: loop {
        let next_due = pending.front().map(|item| item.due);
        tokio::select! {
            _ = tokio::signal::ctrl_c() => break CaptureStopReason::Requested,
            _ = tokio::time::sleep_until(deadline) => break CaptureStopReason::DurationLimit,
            _ = async {
                if let Some(due) = next_due {
                    tokio::time::sleep_until(due).await;
                } else {
                    std::future::pending::<()>().await;
                }
            } => {
                let Some(item) = pending.pop_front() else { continue };
                let rpc = rpc.clone();
                let follow_limit = follow_limit.clone();
                followers.spawn(async move {
                    let token = item.event.token;
                    let permit = follow_limit.acquire_owned().await;
                    let result = match permit {
                        Ok(_permit) => capture_follow(&rpc, item).await,
                        Err(error) => Err(anyhow::anyhow!(error)),
                    };
                    (token, result)
                });
            }
            Some(result) = followers.join_next(), if !followers.is_empty() => {
                match result {
                    Ok((_token, Ok(follow))) => {
                        if let CaptureRecordStatus::Stopped(manifest) = recorder.record(
                            ResearchEventKind::Flow,
                            Some(follow.canonical),
                            follow.data,
                        )? {
                            break 'capture manifest.stop_reason;
                        }
                    }
                    Ok((token, Err(error))) => {
                        if let CaptureRecordStatus::Stopped(manifest) = recorder.record(
                            ResearchEventKind::Missing,
                            None,
                            serde_json::json!({
                                "token":format!("{token:#x}"),
                                "stage":"follow_up",
                                "error":first_line(&error.to_string()),
                            }),
                        )? {
                            break 'capture manifest.stop_reason;
                        }
                    }
                    Err(error) => {
                        if let CaptureRecordStatus::Stopped(manifest) = recorder.record(
                            ResearchEventKind::Missing,
                            None,
                            serde_json::json!({
                                "stage":"follow_task",
                                "error":first_line(&error.to_string()),
                            }),
                        )? {
                            break 'capture manifest.stop_reason;
                        }
                    }
                }
            }
            event = rx.recv() => {
                let Some(event) = event else {
                    break CaptureStopReason::SourceClosed;
                };
                let canonical = canonical_observation(&rpc, event.block_number).await.ok();
                let intel = enrich_launch(&rpc, event.clone(), bodkin::chain::DEAD).await;
                let launched_at = intel
                    .curve
                    .as_ref()
                    .map(|curve| curve.launched_at)
                    .or_else(|| intel.tx.as_ref().map(|transaction| transaction.timestamp))
                    .unwrap_or(0);
                let insiders = capture_insiders(&intel);
                if let CaptureRecordStatus::Stopped(manifest) = recorder.record(
                    ResearchEventKind::Launch,
                    canonical,
                    capture_launch_data(&intel),
                )? {
                    break manifest.stop_reason;
                }
                let now_seconds = bodkin::pons::clock::now_ms() / 1_000;
                let wait_seconds = launched_at
                    .saturating_add(3_600)
                    .saturating_sub(now_seconds);
                pending.push_back(PendingCapture {
                    due: tokio::time::Instant::now()
                        + std::time::Duration::from_secs(wait_seconds),
                    event,
                    launched_at,
                    insiders,
                });
            }
        }
    };
    watch.shutdown().await;
    followers.abort_all();
    while followers.join_next().await.is_some() {}
    let manifest = recorder.finish(stop_reason)?;
    info(format!("capture {}", serde_json::to_string(&manifest)?));
    Ok(())
}

async fn canonical_observation(rpc: &Rpc, block: u64) -> anyhow::Result<CanonicalObservation> {
    let (block_hash, block_timestamp) = tokio::try_join!(
        rpc.block_hash(Lane::Background, block),
        rpc.block_timestamp(Lane::Background, block),
    )?;
    Ok(CanonicalObservation {
        block_number: block,
        block_hash,
        block_timestamp,
    })
}

fn capture_insiders(intel: &bodkin::pons::enrich::LaunchIntel) -> Vec<Address> {
    let mut insiders = intel
        .tx
        .as_ref()
        .map(|transaction| transaction.exemptions.clone())
        .unwrap_or_default();
    insiders.push(intel.ev.deployer);
    if let Some(transaction) = &intel.tx {
        insiders.push(transaction.recipient);
    }
    if let Some(record) = &intel.record {
        insiders.push(record.creator_fee_recipient);
    }
    insiders
}

fn capture_launch_data(intel: &bodkin::pons::enrich::LaunchIntel) -> serde_json::Value {
    serde_json::json!({
        "token":format!("{:#x}",intel.ev.token),
        "curve":format!("{:#x}",intel.ev.curve),
        "deployer":format!("{:#x}",intel.ev.deployer),
        "pair_token":format!("{:#x}",intel.ev.pair_token),
        "launch_config_id":intel.ev.launch_config_id.to_string(),
        "graduation_threshold":intel.ev.graduation_threshold.to_string(),
        "launch_block":intel.ev.block_number,
        "tx_hash":format!("{:#x}",intel.ev.tx_hash),
        "log_index":intel.ev.log_index,
        "detected_at_ms":intel.ev.detected_at_ms,
        "source":intel.ev.source,
        "meta":intel.meta.as_ref().map(|meta| serde_json::json!({
            "name":meta.name,
            "symbol":meta.symbol,
            "description":meta.description,
            "socials":{
                "twitter":meta.socials.twitter,
                "telegram":meta.socials.telegram,
                "website":meta.socials.website,
            },
        })),
        "record":intel.record.as_ref().map(|record| serde_json::json!({
            "creator_fee_recipient":format!("{:#x}",record.creator_fee_recipient),
            "creator_tax_bps":record.creator_tax_bps,
            "phase":record.phase,
        })),
        "transaction":intel.tx.as_ref().map(|transaction| serde_json::json!({
            "from":format!("{:#x}",transaction.from),
            "dev_buy_wei":transaction.dev_buy_wei.to_string(),
            "dev_tokens":transaction.dev_tokens.to_string(),
            "exemptions":transaction.exemptions.iter().map(|address| format!("{address:#x}")).collect::<Vec<_>>(),
            "recipient":format!("{:#x}",transaction.recipient),
            "timestamp":transaction.timestamp,
        })),
        "curve_state":intel.curve,
        "pair":{
            "address":format!("{:#x}",intel.pair.address),
            "symbol":intel.pair.symbol,
            "decimals":intel.pair.decimals,
            "usd_per_unit":intel.pair.usd_per_unit,
        },
        "fee_recipient_is_contract":intel.fee_recipient_is_contract,
        "fee_check_ms":intel.fee_check_ms,
        "errors":intel.errors,
    })
}

async fn capture_follow(rpc: &Rpc, item: PendingCapture) -> anyhow::Result<FollowCapture> {
    let head = rpc.block_number(Lane::Background).await?;
    anyhow::ensure!(
        head >= item.event.block_number,
        "follow-up head precedes launch block"
    );
    let canonical = canonical_observation(rpc, head).await?;
    let logs = bodkin::pons::stream::fetch_curve_logs_on(
        rpc,
        Lane::Background,
        item.event.curve,
        item.event.block_number,
        head,
    )
    .await?;
    let mut tracker = bodkin::pons::stream::FlowTracker::default();
    tracker.watch(
        item.event.curve,
        item.launched_at,
        item.event.block_number,
        item.insiders,
    );
    tracker.apply_range(
        item.event.curve,
        item.event.block_number,
        head,
        canonical.block_hash,
        &logs,
    )?;
    let graduation_filter = alloy::rpc::types::Filter::new()
        .address(ADDR.pons_factory)
        .event_signature(bodkin::abi::topics::pool_graduated())
        .topic1(alloy::primitives::B256::left_padding_from(
            item.event.token.as_slice(),
        ))
        .from_block(item.event.block_number)
        .to_block(head);
    let graduation_logs = rpc.get_logs(Lane::Background, graduation_filter).await?;
    let snapshot = tracker.snapshot(item.event.curve);
    Ok(FollowCapture {
        canonical,
        data: serde_json::json!({
            "token":format!("{:#x}",item.event.token),
            "curve":format!("{:#x}",item.event.curve),
            "from_block":item.event.block_number,
            "to_block":head,
            "launched_at":item.launched_at,
            "horizon_seconds":3_600,
            "records":tracker.records(item.event.curve),
            "snapshot":{
                "taxed_buyers_s1":snapshot.taxed_buyers_s1,
                "exempt_buys_s0":snapshot.exempt_buys_s0,
                "insider_sold":snapshot.insider_sold,
                "ready_to_graduate":snapshot.ready_to_graduate,
                "buys":snapshot.buys,
                "sells":snapshot.sells,
                "unique_buyers":snapshot.unique_buyers,
                "quote_in":snapshot.quote_in.to_string(),
                "quote_out":snapshot.quote_out.to_string(),
                "last_non_insider_buy_at":snapshot.last_non_insider_buy_at,
            },
            "graduated":!graduation_logs.is_empty(),
            "coverage_complete":true,
        }),
    })
}

async fn replay_cmd(
    rpc: Arc<Rpc>,
    hours: u64,
    sample: Option<u64>,
    entry_second: u64,
) -> anyhow::Result<()> {
    anyhow::ensure!(hours > 0, "--hours must be greater than zero");
    anyhow::ensure!(entry_second > 0, "--entry-second must be greater than zero");
    if let Some(sample) = sample {
        anyhow::ensure!(sample > 0, "--sample must be greater than zero");
    }
    let events = OutcomeLog::open("data").read_all()?;
    let rules = try_rules_from_env()?;
    let alt = bodkin::replay::ReplayAlt {
        entry_second,
        min_taxed_s1: rules.min_taxed_buyers_s1,
    };
    let manifest = bodkin::replay::experiment_manifest(&events, &rules, &alt);
    info(format!("experiment {}", serde_json::to_string(&manifest)?));
    let cutoff = bodkin::pons::clock::now_ms().saturating_sub(hours.saturating_mul(3_600_000));
    let mut launches = bodkin::replay::recorded_launches(&events, cutoff);
    anyhow::ensure!(
        !launches.is_empty(),
        "no launch-time replay snapshots in the last {hours} hours; run `bodkin snipe` or `bodkin board` to capture them"
    );
    if let Some(sample) = sample {
        let sample = usize::try_from(sample).unwrap_or(usize::MAX);
        if launches.len() > sample {
            launches.drain(..launches.len() - sample);
        }
    }
    bodkin::replay::observe_first_sixty_blocks(&rpc, &mut launches).await?;
    let report = bodkin::replay::replay(&launches, &rules, &alt);
    bodkin::replay::print_report(&report, &format!("entry second +{entry_second}"));
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn research_and_capture_cli_options_parse() {
        let capture = Cli::try_parse_from([
            "bodkin",
            "capture",
            "--for",
            "60",
            "--max-bytes",
            "8192",
            "--output",
            "research/capture",
        ])
        .unwrap();
        assert!(matches!(
            capture.cmd,
            Cmd::Capture {
                r#for: 60,
                max_bytes: 8192,
                output,
            } if output == std::path::Path::new("research/capture")
        ));
        let board = Cli::try_parse_from([
            "bodkin",
            "board",
            "--research",
            "--data-dir",
            "research/board",
        ])
        .unwrap();
        assert!(matches!(
            board.cmd,
            Cmd::Board {
                research: true,
                live: false,
                data_dir: Some(path),
                ..
            } if path == std::path::Path::new("research/board")
        ));
        let replay = Cli::try_parse_from([
            "bodkin",
            "replay",
            "--research",
            "--dataset",
            "research/capture",
        ])
        .unwrap();
        assert!(matches!(
            replay.cmd,
            Cmd::Replay {
                research: true,
                dataset: Some(path),
                ..
            } if path == std::path::Path::new("research/capture")
        ));
    }

    #[test]
    fn committed_helper_artifact_has_creation_and_runtime_bytecode() {
        let creation = helper_bytecode().unwrap();
        let runtime = helper_runtime_bytecode().unwrap();
        assert!(!creation.is_empty());
        assert!(!runtime.is_empty());
        assert!(creation.len() > runtime.len());
        let built_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../contracts/out/BodkinBuyOnce.sol/BodkinBuyOnce.json");
        if let Ok(built) = std::fs::read_to_string(built_path) {
            let built: serde_json::Value = serde_json::from_str(&built).unwrap();
            let committed: serde_json::Value =
                serde_json::from_str(COMMITTED_HELPER_ARTIFACT).unwrap();
            assert_eq!(built["bytecode"]["object"], committed["bytecode"]["object"]);
            assert_eq!(
                built["deployedBytecode"]["object"],
                committed["deployedBytecode"]["object"]
            );
        }
    }
}
