use crate::engine::SnipeRules;
use crate::pons::curve::{CurveState, quote_buy};
use crate::pons::stream::{FlowEvent, FlowRecord};
use crate::pons::tax::snipe_tax_bps;
use crate::research::{CaptureManifest, ResearchEventKind, visit_capture};
use alloy::primitives::{Address, B256, U256};
use alloy::rpc::types::Filter;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::path::Path;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayLaunch {
    pub token: String,
    #[serde(default)]
    pub curve: String,
    #[serde(default)]
    pub launch_block: u64,
    #[serde(default)]
    pub recorded_at: u64,
    pub launched_at: u64,
    pub start_bps: u64,
    pub window: u64,
    pub taxed_buyers_s1: u32,
    pub exempt_buys_s0: u32,
    #[serde(default)]
    pub gate_observed: bool,
    pub graduated: bool,
    #[serde(default)]
    pub outcome_observed: bool,
    #[serde(default)]
    pub provenance_complete: bool,
    #[serde(default)]
    pub run_id: Option<String>,
    #[serde(default)]
    pub launch_sequence: Option<u64>,
    #[serde(default)]
    pub curve_state: Option<CurveState>,
    #[serde(default)]
    pub flow: Vec<FlowRecord>,
    pub intel: Option<ReplayIntel>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayIntel {
    pub score_total: i32,
    pub fire: bool,
}

#[derive(Debug, Clone)]
pub struct ReplayAlt {
    pub entry_second: u64,
    pub min_taxed_s1: u32,
}

#[derive(Debug, Clone, Serialize)]
pub struct ExperimentManifest {
    pub schema: u32,
    pub dataset_hash: B256,
    pub event_count: usize,
    pub entry_second: u64,
    pub min_taxed_s1: u32,
    pub min_score: i32,
    pub max_opening_tax_bps: u64,
    pub entry_wei: U256,
}

pub fn experiment_manifest(
    events: &[serde_json::Value],
    rules: &SnipeRules,
    alt: &ReplayAlt,
) -> ExperimentManifest {
    ExperimentManifest {
        schema: 1,
        dataset_hash: alloy::primitives::keccak256(
            serde_json::to_vec(events).expect("JSON values always serialize"),
        ),
        event_count: events.len(),
        entry_second: alt.entry_second,
        min_taxed_s1: alt.min_taxed_s1,
        min_score: rules.min_score,
        max_opening_tax_bps: rules.max_opening_tax_bps,
        entry_wei: rules.eth_per_buy,
    }
}

#[derive(Debug, Default)]
pub struct ReplayReport {
    pub n: u64,
    pub would_fire: u64,
    pub would_skip_gate: u64,
    pub graduated_of_fires: u64,
    pub unmeasured: u64,
    pub measured_outcomes: u64,
    pub unmeasured_outcomes: u64,
    pub would_skip_rules: u64,
    pub measured_accounting: u64,
    pub unmeasured_accounting: u64,
    pub quoted_entry_wei: U256,
    pub quoted_tokens: U256,
}

/// Sampled replay: TokenLaunched + first-60-block curve path vs alternative rules / entry seconds.
pub fn replay(launches: &[ReplayLaunch], rules: &SnipeRules, alt: &ReplayAlt) -> ReplayReport {
    let mut r = ReplayReport::default();
    for l in launches {
        r.n += 1;
        let Some(intel) = l
            .intel
            .as_ref()
            .filter(|_| l.launched_at > 0 && l.start_bps <= 10_000 && l.window > 0)
        else {
            r.unmeasured += 1;
            continue;
        };
        if !intel.fire || intel.score_total < rules.min_score {
            r.would_skip_rules += 1;
            continue;
        }
        if !l.gate_observed {
            r.unmeasured += 1;
            continue;
        }
        let tax = snipe_tax_bps(l.start_bps, l.window, alt.entry_second);
        let gate_ok = alt.entry_second > 0
            && l.taxed_buyers_s1 >= alt.min_taxed_s1
            && l.exempt_buys_s0 <= rules.max_exempt_buys_s0
            && tax <= rules.max_opening_tax_bps;
        if !gate_ok {
            r.would_skip_gate += 1;
            continue;
        }
        r.would_fire += 1;
        if let Some(quote) = quoted_entry(l, rules, alt.entry_second) {
            r.measured_accounting += 1;
            r.quoted_entry_wei = r.quoted_entry_wei.saturating_add(quote.spent);
            r.quoted_tokens = r.quoted_tokens.saturating_add(quote.tokens_out);
        } else {
            r.unmeasured_accounting += 1;
        }
        if l.outcome_observed {
            r.measured_outcomes += 1;
            if l.graduated {
                r.graduated_of_fires += 1;
            }
        } else {
            r.unmeasured_outcomes += 1;
        }
    }
    r
}

fn event_timestamp(event: &FlowEvent) -> u64 {
    match event {
        FlowEvent::Buy { timestamp, .. }
        | FlowEvent::Sell { timestamp, .. }
        | FlowEvent::Tax { timestamp, .. }
        | FlowEvent::Completed { timestamp } => *timestamp,
    }
}

fn state_before(launch: &ReplayLaunch, entry_second: u64) -> Option<CurveState> {
    if !launch.provenance_complete {
        return None;
    }
    let mut state = launch.curve_state.clone()?;
    let target = launch.launched_at.checked_add(entry_second)?;
    if state.read_chain_ts >= target {
        return None;
    }
    let mut records = launch
        .flow
        .iter()
        .filter(|record| {
            record.block_number > state.read_block && event_timestamp(&record.event) < target
        })
        .collect::<Vec<_>>();
    records.sort_by_key(|record| {
        (
            record.block_number,
            record.transaction_index,
            record.log_index,
            record.transaction_hash,
        )
    });
    for record in records {
        match &record.event {
            FlowEvent::Buy {
                quote_in,
                tokens_out,
                fee,
                tax,
                ..
            } => {
                // CurveBuy.fee already carries the snipe tax; SnipeTaxCharged is
                // informational and must not be subtracted a second time.
                let net = quote_in.checked_sub(*fee)?.checked_sub(*tax)?;
                state.quote_reserve = state.quote_reserve.checked_add(net)?;
                state.real_quote_reserve = state.real_quote_reserve.checked_add(net)?;
                state.token_reserve = state.token_reserve.checked_sub(*tokens_out)?;
                state.sellable_tokens = state.sellable_tokens.checked_sub(*tokens_out)?;
            }
            FlowEvent::Sell {
                tokens_in,
                quote_out,
                fee,
                tax,
                ..
            } => {
                let gross = quote_out.checked_add(*fee)?.checked_add(*tax)?;
                state.quote_reserve = state.quote_reserve.checked_sub(gross)?;
                state.real_quote_reserve = state.real_quote_reserve.checked_sub(gross)?;
                state.token_reserve = state.token_reserve.checked_add(*tokens_in)?;
                state.sellable_tokens = state.sellable_tokens.checked_add(*tokens_in)?;
            }
            FlowEvent::Completed { .. } => state.ready_to_graduate = true,
            FlowEvent::Tax { .. } => {}
        }
    }
    Some(state)
}

pub fn quoted_entry(
    launch: &ReplayLaunch,
    rules: &SnipeRules,
    entry_second: u64,
) -> Option<crate::pons::curve::BuyQuote> {
    let mut state = state_before(launch, entry_second)?;
    state.opening_tax_bps =
        U256::from(snipe_tax_bps(launch.start_bps, launch.window, entry_second));
    Some(quote_buy(&state, rules.eth_per_buy))
}

pub fn print_report(r: &ReplayReport, label: &str) {
    let rate = if r.measured_outcomes == 0 {
        0.0
    } else {
        r.graduated_of_fires as f64 / r.measured_outcomes as f64 * 100.0
    };
    println!(
        "{label}: deterministic launch-time screen, ordered reserve transitions, and observed first-60-block logs; no invented fill or profitability estimate"
    );
    println!(
        "n={} qualified={} skip_rules={} skip_gate={} unmeasured_inputs={} measured_accounting={} unmeasured_accounting={} measured_outcomes={} unmeasured_outcomes={} observed_graduated={}/{} ({:.1}%)",
        r.n,
        r.would_fire,
        r.would_skip_rules,
        r.would_skip_gate,
        r.unmeasured,
        r.measured_accounting,
        r.unmeasured_accounting,
        r.measured_outcomes,
        r.unmeasured_outcomes,
        r.graduated_of_fires,
        r.measured_outcomes,
        rate
    );
    println!(
        "quoted qualified entries: {} wei -> {} token-wei",
        r.quoted_entry_wei, r.quoted_tokens
    );
}

pub fn recorded_launches(events: &[serde_json::Value], cutoff_ms: u64) -> Vec<ReplayLaunch> {
    let mut launches = HashMap::<String, ReplayLaunch>::new();
    for (index, event) in events.iter().enumerate() {
        if event.get("kind").and_then(|v| v.as_str()) != Some("launch_seen")
            || event.get("t").and_then(|v| v.as_u64()).unwrap_or(0) < cutoff_ms
        {
            continue;
        }
        let token = event
            .get("token")
            .and_then(|v| v.as_str())
            .map(str::to_owned)
            .unwrap_or_else(|| format!("unreadable-{index}"));
        let score = event
            .get("score")
            .and_then(|v| v.as_i64())
            .and_then(|v| i32::try_from(v).ok());
        let screen_fire = event.get("screen_fire").and_then(|v| v.as_bool());
        let intel = score
            .zip(screen_fire)
            .map(|(score_total, fire)| ReplayIntel { score_total, fire });
        launches.insert(
            token.clone(),
            ReplayLaunch {
                token,
                curve: event
                    .get("curve")
                    .and_then(|v| v.as_str())
                    .unwrap_or_default()
                    .to_string(),
                launch_block: event
                    .get("launch_block")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(0),
                recorded_at: event.get("t").and_then(|v| v.as_u64()).unwrap_or(0),
                launched_at: event
                    .get("launched_at")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(0),
                start_bps: event.get("start_bps").and_then(|v| v.as_u64()).unwrap_or(0),
                window: event.get("window").and_then(|v| v.as_u64()).unwrap_or(0),
                taxed_buyers_s1: 0,
                exempt_buys_s0: 0,
                gate_observed: false,
                graduated: false,
                outcome_observed: false,
                provenance_complete: false,
                run_id: event
                    .get("run_id")
                    .and_then(|value| value.as_str())
                    .map(str::to_owned),
                launch_sequence: event.get("sequence").and_then(|value| value.as_u64()),
                curve_state: event
                    .get("curve_state")
                    .cloned()
                    .and_then(|value| serde_json::from_value(value).ok()),
                flow: Vec::new(),
                intel,
            },
        );
    }
    for event in events {
        if event.get("kind").and_then(|v| v.as_str()) != Some("gate_observed") {
            continue;
        }
        let Some(token) = event.get("token").and_then(|v| v.as_str()) else {
            continue;
        };
        let Some(launch) = launches.get_mut(token) else {
            continue;
        };
        let Some(taxed) = event
            .get("taxed_buyers_s1")
            .and_then(|v| v.as_u64())
            .and_then(|v| u32::try_from(v).ok())
        else {
            continue;
        };
        let Some(exempt) = event
            .get("exempt_buys_s0")
            .and_then(|v| v.as_u64())
            .and_then(|v| u32::try_from(v).ok())
        else {
            continue;
        };
        let parsed_flow = event
            .get("flow")
            .cloned()
            .and_then(|value| serde_json::from_value::<Vec<FlowRecord>>(value).ok());
        let flow_complete = parsed_flow.is_some();
        let flow = parsed_flow.unwrap_or_default();
        let run_matches = event
            .get("run_id")
            .and_then(|value| value.as_str())
            .zip(launch.run_id.as_deref())
            .is_some_and(|(gate, launch)| gate == launch);
        let sequence_follows = event
            .get("sequence")
            .and_then(|value| value.as_u64())
            .zip(launch.launch_sequence)
            .is_some_and(|(gate, launch)| gate > launch);
        launch.taxed_buyers_s1 = taxed;
        launch.exempt_buys_s0 = exempt;
        launch.flow = flow;
        launch.gate_observed = true;
        launch.provenance_complete =
            run_matches && sequence_follows && launch.curve_state.is_some() && flow_complete;
    }
    let mut launches = launches.into_values().collect::<Vec<_>>();
    launches.sort_by_key(|launch| launch.recorded_at);
    launches
}

pub async fn observe_first_sixty_blocks(
    rpc: &crate::rpc::Rpc,
    launches: &mut [ReplayLaunch],
) -> anyhow::Result<()> {
    let head = rpc.block_number(crate::rpc::Lane::Background).await?;
    for launch in launches {
        if launch.intel.is_none() || launch.launch_block == 0 || launch.launched_at == 0 {
            continue;
        }
        let Ok(curve) = launch.curve.parse::<Address>() else {
            continue;
        };
        let Ok(token) = launch.token.parse::<Address>() else {
            continue;
        };
        let end = launch.launch_block.saturating_add(60);
        if head < end {
            continue;
        }
        if let Ok(logs) = crate::pons::stream::fetch_curve_logs_on(
            rpc,
            crate::rpc::Lane::Background,
            curve,
            launch.launch_block,
            end,
        )
        .await
        {
            let mut tracker = crate::pons::stream::FlowTracker::default();
            tracker.watch(
                curve,
                launch.launched_at,
                launch.launch_block,
                std::iter::empty(),
            );
            for (log, timestamp) in &logs {
                tracker.ingest(log, *timestamp)?;
            }
            let snapshot = tracker.snapshot(curve);
            launch.flow = tracker.records(curve);
            launch.taxed_buyers_s1 = snapshot.taxed_buyers_s1;
            launch.exempt_buys_s0 = snapshot.exempt_buys_s0;
            launch.gate_observed = true;
        }
        let filter = Filter::new()
            .address(crate::chain::ADDR.pons_factory)
            .event_signature(crate::abi::topics::pool_graduated())
            .topic1(B256::left_padding_from(token.as_slice()))
            .from_block(launch.launch_block)
            .to_block(end);
        if let Ok(logs) = rpc.get_logs(crate::rpc::Lane::Background, filter).await {
            launch.graduated = !logs.is_empty();
            launch.outcome_observed = true;
        }
    }
    Ok(())
}

#[derive(Debug, Clone, Serialize)]
pub struct ExperimentCount {
    pub parameter: u64,
    pub measured: u64,
    pub matched: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct ResearchDatasetReport {
    pub manifest: CaptureManifest,
    pub launches: u64,
    pub complete_followups: u64,
    pub censored_launches: u64,
    pub missing_records: u64,
    pub launches_with_reused_fee_recipient: u64,
    pub near_pattern_repeats_within_30m: u64,
    pub min_taxed_buyers_s1: Vec<ExperimentCount>,
    pub pre_entry_unique_buyers: Vec<ExperimentCount>,
    pub positive_pre_entry_imbalance: u64,
    pub pre_entry_insider_sells: u64,
    pub inactivity_seconds: Vec<ExperimentCount>,
    pub on_curve_hold_seconds: Vec<ExperimentCount>,
    pub graduated_within_horizon: u64,
    pub pool_paths_measured: u64,
    pub quote_in_wei: Option<String>,
    pub quote_out_wei: Option<String>,
    pub economic_outcomes_measured: u64,
    pub risk_result: &'static str,
    pub recommendation: &'static str,
}

#[derive(Default)]
struct ResearchLaunchFacts {
    launched_at: u64,
    launch_block: u64,
    launch_canonical: bool,
    fee_recipient: Option<String>,
    near_pattern: Option<String>,
    insiders: HashSet<Address>,
    flow: Option<Vec<FlowRecord>>,
    taxed_buyers_s1: Option<u64>,
    pre_entry_unique_buyers: Option<u64>,
    pre_entry_quote_in: Option<U256>,
    pre_entry_quote_out: Option<U256>,
    pre_entry_insider_sold: bool,
    quote_in: Option<U256>,
    quote_out: Option<U256>,
    coverage_complete: bool,
}

pub fn research_dataset_report(
    directory: impl AsRef<Path>,
) -> anyhow::Result<ResearchDatasetReport> {
    let mut launches = HashMap::<String, ResearchLaunchFacts>::new();
    let mut missing_records = 0u64;
    let manifest = visit_capture(directory, |envelope| {
        match envelope.kind {
            ResearchEventKind::Launch => {
                if let Some(token) = envelope.data.get("token").and_then(|value| value.as_str()) {
                    launches.entry(token.to_string()).or_insert_with(|| {
                        research_launch_facts(&envelope.data, envelope.canonical.is_some())
                    });
                }
            }
            ResearchEventKind::Flow => {
                if let Some(token) = envelope.data.get("token").and_then(|value| value.as_str())
                    && let Some(launch) = launches.get_mut(token)
                {
                    launch.flow = envelope
                        .data
                        .get("records")
                        .cloned()
                        .and_then(|value| serde_json::from_value(value).ok());
                    let range_complete = envelope.canonical.as_ref().is_some_and(|canonical| {
                        envelope
                            .data
                            .get("from_block")
                            .and_then(|value| value.as_u64())
                            == Some(launch.launch_block)
                            && envelope
                                .data
                                .get("to_block")
                                .and_then(|value| value.as_u64())
                                == Some(canonical.block_number)
                    });
                    launch.coverage_complete = launch.launch_canonical
                        && launch.launched_at > 0
                        && range_complete
                        && envelope
                            .data
                            .get("coverage_complete")
                            .and_then(|value| value.as_bool())
                            == Some(true)
                        && launch.flow.is_some();
                    launch.taxed_buyers_s1 = envelope
                        .data
                        .pointer("/snapshot/taxed_buyers_s1")
                        .and_then(|value| value.as_u64());
                    launch.quote_in = parse_u256_at(&envelope.data, "/snapshot/quote_in");
                    launch.quote_out = parse_u256_at(&envelope.data, "/snapshot/quote_out");
                    if let Some(records) = &launch.flow
                        && let Some((buyers, quote_in, quote_out, insider_sold)) =
                            pre_entry_flow(launch.launched_at, records, &launch.insiders)
                    {
                        launch.pre_entry_unique_buyers = Some(buyers);
                        launch.pre_entry_quote_in = Some(quote_in);
                        launch.pre_entry_quote_out = Some(quote_out);
                        launch.pre_entry_insider_sold = insider_sold;
                    }
                }
            }
            ResearchEventKind::Missing => missing_records = missing_records.saturating_add(1),
            ResearchEventKind::Refusal
            | ResearchEventKind::Outcome
            | ResearchEventKind::Coverage => {}
        }
        Ok(())
    })?;
    let complete = launches
        .values()
        .filter(|launch| launch.coverage_complete)
        .count();
    let mut fee_counts = HashMap::<&str, usize>::new();
    for fee in launches
        .values()
        .filter_map(|launch| launch.fee_recipient.as_deref())
    {
        *fee_counts.entry(fee).or_default() += 1;
    }
    let reused_fee = launches
        .values()
        .filter(|launch| {
            launch
                .fee_recipient
                .as_deref()
                .and_then(|fee| fee_counts.get(fee))
                .is_some_and(|count| *count > 1)
        })
        .count();
    let mut patterns = HashMap::<&str, Vec<u64>>::new();
    for launch in launches.values() {
        if let Some(pattern) = launch.near_pattern.as_deref() {
            patterns
                .entry(pattern)
                .or_default()
                .push(launch.launched_at);
        }
    }
    let mut near_repeats = 0u64;
    for times in patterns.values_mut() {
        times.sort_unstable();
        for pair in times.windows(2) {
            if pair[1].saturating_sub(pair[0]) <= 1_800 {
                near_repeats = near_repeats.saturating_add(1);
            }
        }
    }
    let min_taxed_buyers_s1 = [0u64, 1, 2]
        .into_iter()
        .map(|minimum| {
            let matched = launches
                .values()
                .filter(|launch| {
                    launch.coverage_complete
                        && launch
                            .taxed_buyers_s1
                            .is_some_and(|buyers| buyers >= minimum)
                })
                .count();
            ExperimentCount {
                parameter: minimum,
                measured: u64::try_from(complete).unwrap_or(u64::MAX),
                matched: u64::try_from(matched).unwrap_or(u64::MAX),
            }
        })
        .collect();
    let pre_entry_unique_buyers = [1u64, 2, 3]
        .into_iter()
        .map(|minimum| {
            let matched = launches
                .values()
                .filter(|launch| {
                    launch.coverage_complete
                        && launch
                            .pre_entry_unique_buyers
                            .is_some_and(|buyers| buyers >= minimum)
                })
                .count();
            ExperimentCount {
                parameter: minimum,
                measured: u64::try_from(complete).unwrap_or(u64::MAX),
                matched: u64::try_from(matched).unwrap_or(u64::MAX),
            }
        })
        .collect();
    let positive_pre_entry_imbalance = launches
        .values()
        .filter(|launch| {
            launch.coverage_complete
                && launch
                    .pre_entry_quote_in
                    .zip(launch.pre_entry_quote_out)
                    .is_some_and(|(quote_in, quote_out)| quote_in > quote_out)
        })
        .count();
    let pre_entry_insider_sells = launches
        .values()
        .filter(|launch| launch.coverage_complete && launch.pre_entry_insider_sold)
        .count();
    let inactivity_seconds = [60u64, 90, 180]
        .into_iter()
        .map(|window| {
            let matched = launches
                .values()
                .filter(|launch| launch.coverage_complete && has_inactivity(launch, window))
                .count();
            ExperimentCount {
                parameter: window,
                measured: u64::try_from(complete).unwrap_or(u64::MAX),
                matched: u64::try_from(matched).unwrap_or(u64::MAX),
            }
        })
        .collect();
    let on_curve_hold_seconds = [300u64, 900, 1_800]
        .into_iter()
        .map(|seconds| {
            let matched = launches
                .values()
                .filter(|launch| launch.coverage_complete && on_curve_at(launch, seconds))
                .count();
            ExperimentCount {
                parameter: seconds,
                measured: u64::try_from(complete).unwrap_or(u64::MAX),
                matched: u64::try_from(matched).unwrap_or(u64::MAX),
            }
        })
        .collect();
    let graduated = launches
        .values()
        .filter(|launch| {
            launch.coverage_complete
                && launch.flow.as_ref().is_some_and(|records| {
                    records
                        .iter()
                        .any(|record| matches!(record.event, FlowEvent::Completed { .. }))
                })
        })
        .count();
    let (quote_in_wei, quote_out_wei) = aggregate_flow(&launches);
    Ok(ResearchDatasetReport {
        manifest,
        launches: u64::try_from(launches.len()).unwrap_or(u64::MAX),
        complete_followups: u64::try_from(complete).unwrap_or(u64::MAX),
        censored_launches: u64::try_from(launches.len().saturating_sub(complete))
            .unwrap_or(u64::MAX),
        missing_records,
        launches_with_reused_fee_recipient: u64::try_from(reused_fee).unwrap_or(u64::MAX),
        near_pattern_repeats_within_30m: near_repeats,
        min_taxed_buyers_s1,
        pre_entry_unique_buyers,
        positive_pre_entry_imbalance: u64::try_from(positive_pre_entry_imbalance)
            .unwrap_or(u64::MAX),
        pre_entry_insider_sells: u64::try_from(pre_entry_insider_sells).unwrap_or(u64::MAX),
        inactivity_seconds,
        on_curve_hold_seconds,
        graduated_within_horizon: u64::try_from(graduated).unwrap_or(u64::MAX),
        pool_paths_measured: 0,
        quote_in_wei: quote_in_wei.map(|value| value.to_string()),
        quote_out_wei: quote_out_wei.map(|value| value.to_string()),
        economic_outcomes_measured: 0,
        risk_result: "unmeasured",
        recommendation: "inconclusive: capture has no complete entry-to-exit counterfactual pricing",
    })
}

fn research_launch_facts(data: &serde_json::Value, launch_canonical: bool) -> ResearchLaunchFacts {
    let transaction = data.get("transaction");
    let record = data.get("record");
    let meta = data.get("meta");
    let launched_at = data
        .pointer("/curve_state/launched_at")
        .and_then(|value| value.as_u64())
        .or_else(|| {
            transaction
                .and_then(|value| value.get("timestamp"))
                .and_then(|value| value.as_u64())
        })
        .unwrap_or(0);
    let mut insiders = HashSet::new();
    for pointer in [
        "/deployer",
        "/transaction/from",
        "/transaction/recipient",
        "/record/creator_fee_recipient",
    ] {
        if let Some(address) = data.pointer(pointer).and_then(|value| value.as_str())
            && let Ok(address) = address.parse()
        {
            insiders.insert(address);
        }
    }
    if let Some(exemptions) = data
        .pointer("/transaction/exemptions")
        .and_then(|value| value.as_array())
    {
        for address in exemptions {
            if let Some(address) = address.as_str()
                && let Ok(address) = address.parse()
            {
                insiders.insert(address);
            }
        }
    }
    let dev_buy = transaction
        .and_then(|value| value.get("dev_buy_wei"))
        .and_then(|value| value.as_str())
        .and_then(|value| value.parse::<U256>().ok());
    let creator_tax = record
        .and_then(|value| value.get("creator_tax_bps"))
        .and_then(|value| value.as_u64());
    let exemption_count = transaction
        .and_then(|value| value.get("exemptions"))
        .and_then(|value| value.as_array())
        .map(Vec::len);
    let social_bits = ["twitter", "website", "telegram"]
        .into_iter()
        .fold(0u8, |bits, key| {
            let present = meta
                .and_then(|value| value.pointer(&format!("/socials/{key}")))
                .and_then(|value| value.as_str())
                .is_some_and(|value| !value.trim().is_empty());
            (bits << 1) | u8::from(present)
        });
    let near_pattern = dev_buy.zip(creator_tax).zip(exemption_count).map(
        |((dev_buy, creator_tax), exemption_count)| {
            let dev_bucket = dev_buy / U256::from(100_000_000_000_000u64);
            let tax_bucket = creator_tax / 25;
            format!("{dev_bucket}:{tax_bucket}:{exemption_count}:{social_bits}")
        },
    );
    ResearchLaunchFacts {
        launched_at,
        launch_block: data
            .get("launch_block")
            .and_then(|value| value.as_u64())
            .unwrap_or(0),
        launch_canonical,
        fee_recipient: record
            .and_then(|value| value.get("creator_fee_recipient"))
            .and_then(|value| value.as_str())
            .map(str::to_owned),
        near_pattern,
        insiders,
        ..Default::default()
    }
}

fn parse_u256_at(value: &serde_json::Value, pointer: &str) -> Option<U256> {
    value
        .pointer(pointer)
        .and_then(|value| value.as_str())
        .and_then(|value| value.parse().ok())
}

fn pre_entry_flow(
    launched_at: u64,
    records: &[FlowRecord],
    insiders: &HashSet<Address>,
) -> Option<(u64, U256, U256, bool)> {
    let cutoff = launched_at.checked_add(2)?;
    let mut buyers = HashSet::new();
    let mut quote_in = U256::ZERO;
    let mut quote_out = U256::ZERO;
    let mut insider_sold = false;
    for record in records {
        match &record.event {
            FlowEvent::Buy {
                recipient,
                quote_in: amount,
                timestamp,
                ..
            } if *timestamp < cutoff => {
                if !insiders.contains(recipient) {
                    buyers.insert(*recipient);
                }
                quote_in = quote_in.checked_add(*amount)?;
            }
            FlowEvent::Sell {
                seller,
                recipient,
                quote_out: amount,
                timestamp,
                ..
            } if *timestamp < cutoff => {
                insider_sold |= insiders.contains(seller) || insiders.contains(recipient);
                quote_out = quote_out.checked_add(*amount)?;
            }
            _ => {}
        }
    }
    Some((
        u64::try_from(buyers.len()).unwrap_or(u64::MAX),
        quote_in,
        quote_out,
        insider_sold,
    ))
}

fn event_time(event: &FlowEvent) -> u64 {
    match event {
        FlowEvent::Buy { timestamp, .. }
        | FlowEvent::Sell { timestamp, .. }
        | FlowEvent::Tax { timestamp, .. }
        | FlowEvent::Completed { timestamp } => *timestamp,
    }
}

fn has_inactivity(launch: &ResearchLaunchFacts, window: u64) -> bool {
    let Some(records) = &launch.flow else {
        return false;
    };
    let horizon = records
        .iter()
        .filter_map(|record| match &record.event {
            FlowEvent::Completed { timestamp } => Some(*timestamp),
            _ => None,
        })
        .min()
        .unwrap_or_else(|| launch.launched_at.saturating_add(3_600));
    let mut last = launch.launched_at;
    for record in records {
        if event_time(&record.event) > horizon {
            break;
        }
        if let FlowEvent::Buy {
            recipient,
            quote_in,
            timestamp,
            ..
        } = &record.event
            && !quote_in.is_zero()
            && !launch.insiders.contains(recipient)
        {
            if timestamp.saturating_sub(last) >= window {
                return true;
            }
            last = *timestamp;
        }
    }
    horizon.saturating_sub(last) >= window
}

fn on_curve_at(launch: &ResearchLaunchFacts, seconds: u64) -> bool {
    let target = launch.launched_at.saturating_add(seconds);
    launch.flow.as_ref().is_some_and(|records| {
        !records.iter().any(|record| {
            matches!(record.event, FlowEvent::Completed { .. })
                && event_time(&record.event) <= target
        })
    })
}

fn aggregate_flow(launches: &HashMap<String, ResearchLaunchFacts>) -> (Option<U256>, Option<U256>) {
    launches
        .values()
        .filter(|launch| launch.coverage_complete)
        .fold(
            (Some(U256::ZERO), Some(U256::ZERO)),
            |(quote_in, quote_out), launch| {
                (
                    quote_in.and_then(|total| total.checked_add(launch.quote_in?)),
                    quote_out.and_then(|total| total.checked_add(launch.quote_out?)),
                )
            },
        )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn research_dataset_report_is_streamed_and_keeps_missing_economics_explicit() {
        let directory = tempfile::tempdir().unwrap();
        let output = directory.path().join("capture");
        let mut recorder = crate::research::ResearchRecorder::create(
            &output,
            crate::research::CaptureLimits {
                duration_seconds: 60,
                max_bytes: 100_000,
            },
        )
        .unwrap();
        let token = Address::from([1; 20]);
        let curve = Address::from([2; 20]);
        recorder
            .record(
                ResearchEventKind::Launch,
                Some(crate::research::CanonicalObservation {
                    block_number: 1,
                    block_hash: B256::from([1; 32]),
                    block_timestamp: 100,
                }),
                serde_json::json!({
                    "token":format!("{token:#x}"),
                    "curve":format!("{curve:#x}"),
                    "deployer":format!("{:#x}", Address::from([3; 20])),
                    "launch_block":1,
                    "curve_state":{"launched_at":100},
                    "record":{"creator_fee_recipient":format!("{:#x}", Address::from([4; 20])),"creator_tax_bps":100},
                    "transaction":{"from":format!("{:#x}", Address::from([3; 20])),"recipient":format!("{:#x}", Address::from([3; 20])),"timestamp":100,"dev_buy_wei":"1000000000000000","exemptions":[]},
                    "meta":{"socials":{"twitter":"x","website":"","telegram":""}},
                }),
            )
            .unwrap();
        let records = vec![
            FlowRecord {
                block_number: 2,
                block_hash: B256::from([2; 32]),
                transaction_index: 0,
                log_index: 0,
                transaction_hash: B256::from([5; 32]),
                event: FlowEvent::Buy {
                    recipient: Address::from([9; 20]),
                    quote_in: U256::from(10u64),
                    tokens_out: U256::from(10u64),
                    fee: U256::ZERO,
                    tax: U256::ZERO,
                    timestamp: 101,
                    transaction_hash: B256::from([5; 32]),
                },
            },
            FlowRecord {
                block_number: 3,
                block_hash: B256::from([3; 32]),
                transaction_index: 0,
                log_index: 0,
                transaction_hash: B256::from([6; 32]),
                event: FlowEvent::Completed { timestamp: 700 },
            },
        ];
        recorder
            .record(
                ResearchEventKind::Flow,
                Some(crate::research::CanonicalObservation {
                    block_number: 100,
                    block_hash: B256::from([9; 32]),
                    block_timestamp: 3_700,
                }),
                serde_json::json!({
                    "token":format!("{token:#x}"),
                    "from_block":1,
                    "to_block":100,
                    "records":records,
                    "coverage_complete":true,
                    "snapshot":{"taxed_buyers_s1":2,"quote_in":"10","quote_out":"0"},
                }),
            )
            .unwrap();
        recorder
            .finish(crate::research::CaptureStopReason::Requested)
            .unwrap();
        let report = research_dataset_report(&output).unwrap();
        assert_eq!(report.launches, 1);
        assert_eq!(report.complete_followups, 1);
        assert_eq!(report.min_taxed_buyers_s1[2].matched, 1);
        assert_eq!(report.pre_entry_unique_buyers[0].matched, 1);
        assert_eq!(report.pre_entry_unique_buyers[1].matched, 0);
        assert_eq!(report.positive_pre_entry_imbalance, 1);
        assert_eq!(report.pre_entry_insider_sells, 0);
        assert_eq!(report.inactivity_seconds[1].matched, 1);
        assert_eq!(report.on_curve_hold_seconds[0].matched, 1);
        assert_eq!(report.on_curve_hold_seconds[1].matched, 0);
        assert_eq!(report.economic_outcomes_measured, 0);
        assert_eq!(report.risk_result, "unmeasured");
    }

    #[test]
    fn experiment_manifest_pins_dataset_and_strategy() {
        let rules = SnipeRules::default();
        let alt = ReplayAlt {
            entry_second: 2,
            min_taxed_s1: 1,
        };
        let first = experiment_manifest(&[serde_json::json!({"kind":"launch_seen"})], &rules, &alt);
        let same = experiment_manifest(&[serde_json::json!({"kind":"launch_seen"})], &rules, &alt);
        let changed =
            experiment_manifest(&[serde_json::json!({"kind":"gate_observed"})], &rules, &alt);
        assert_eq!(first.dataset_hash, same.dataset_hash);
        assert_ne!(first.dataset_hash, changed.dataset_hash);
    }

    #[test]
    fn placeholder_launches_never_become_replay_fires() {
        let launch = ReplayLaunch {
            token: "test".into(),
            curve: String::new(),
            launch_block: 0,
            recorded_at: 0,
            launched_at: 0,
            start_bps: 9900,
            window: 3,
            taxed_buyers_s1: 0,
            exempt_buys_s0: 0,
            gate_observed: false,
            graduated: false,
            outcome_observed: false,
            provenance_complete: false,
            run_id: None,
            launch_sequence: None,
            curve_state: None,
            flow: vec![],
            intel: None,
        };
        let report = replay(
            &[launch],
            &SnipeRules::default(),
            &ReplayAlt {
                entry_second: 2,
                min_taxed_s1: 0,
            },
        );
        assert_eq!(report.n, 1);
        assert_eq!(report.unmeasured, 1);
        assert_eq!(report.would_fire, 0);
        assert_eq!(report.graduated_of_fires, 0);
    }

    #[test]
    fn snapshot_screen_respects_recorded_refusals() {
        let launch = ReplayLaunch {
            token: "test".into(),
            curve: String::new(),
            launch_block: 1,
            recorded_at: 1,
            launched_at: 1,
            start_bps: 9900,
            window: 3,
            taxed_buyers_s1: 10,
            exempt_buys_s0: 0,
            gate_observed: true,
            graduated: true,
            outcome_observed: true,
            provenance_complete: false,
            run_id: None,
            launch_sequence: None,
            curve_state: None,
            flow: vec![],
            intel: Some(ReplayIntel {
                score_total: 100,
                fire: false,
            }),
        };
        let report = replay(
            &[launch],
            &SnipeRules::default(),
            &ReplayAlt {
                entry_second: 2,
                min_taxed_s1: 0,
            },
        );
        assert_eq!(report.would_skip_rules, 1);
        assert_eq!(report.would_fire, 0);
    }

    #[test]
    fn missing_gate_is_unmeasured_not_a_skip() {
        let launch = ReplayLaunch {
            token: "test".into(),
            curve: String::new(),
            launch_block: 1,
            recorded_at: 1,
            launched_at: 1,
            start_bps: 9900,
            window: 3,
            taxed_buyers_s1: 0,
            exempt_buys_s0: 0,
            gate_observed: false,
            graduated: false,
            outcome_observed: false,
            provenance_complete: false,
            run_id: None,
            launch_sequence: None,
            curve_state: None,
            flow: vec![],
            intel: Some(ReplayIntel {
                score_total: 100,
                fire: true,
            }),
        };
        let report = replay(
            &[launch],
            &SnipeRules::default(),
            &ReplayAlt {
                entry_second: 2,
                min_taxed_s1: 0,
            },
        );
        assert_eq!(report.unmeasured, 1);
        assert_eq!(report.would_skip_gate, 0);
        assert_eq!(report.would_fire, 0);
    }

    #[test]
    fn replay_accounting_is_deterministic_and_ignores_future_flow() {
        let state = CurveState {
            quote_reserve: U256::from(1_000),
            token_reserve: U256::from(1_000),
            sellable_tokens: U256::from(1_000),
            real_quote_reserve: U256::from(100),
            graduation_threshold: U256::from(10_000),
            fee_bps: U256::ZERO,
            creator_tax_bps: U256::ZERO,
            ..Default::default()
        };
        let record = |index: u64, timestamp: u64, quote_in: u64, tokens_out: u64| FlowRecord {
            block_number: 1,
            block_hash: B256::from([1u8; 32]),
            transaction_index: index,
            log_index: index,
            transaction_hash: B256::from(U256::from(index + 1)),
            event: FlowEvent::Buy {
                recipient: Address::from([1u8; 20]),
                quote_in: U256::from(quote_in),
                tokens_out: U256::from(tokens_out),
                fee: U256::ZERO,
                tax: U256::ZERO,
                timestamp,
                transaction_hash: B256::from(U256::from(index + 1)),
            },
        };
        let launch = ReplayLaunch {
            token: format!("{:#x}", Address::from([2u8; 20])),
            curve: format!("{:#x}", Address::from([3u8; 20])),
            launch_block: 1,
            recorded_at: 1,
            launched_at: 100,
            start_bps: 9_900,
            window: 3,
            taxed_buyers_s1: 0,
            exempt_buys_s0: 0,
            gate_observed: true,
            graduated: false,
            outcome_observed: true,
            provenance_complete: true,
            run_id: Some("run".into()),
            launch_sequence: Some(0),
            curve_state: Some(state),
            flow: vec![
                record(0, 101, 100, 90),
                FlowRecord {
                    block_number: 2,
                    block_hash: B256::from([2u8; 32]),
                    transaction_index: 0,
                    log_index: 0,
                    transaction_hash: B256::from(U256::from(9)),
                    event: FlowEvent::Sell {
                        seller: Address::from([4u8; 20]),
                        recipient: Address::from([4u8; 20]),
                        tokens_in: U256::from(10),
                        quote_out: U256::from(10),
                        fee: U256::from(1),
                        tax: U256::from(1),
                        timestamp: 101,
                    },
                },
            ],
            intel: Some(ReplayIntel {
                score_total: 100,
                fire: true,
            }),
        };
        let rules = SnipeRules {
            eth_per_buy: U256::from(10),
            ..Default::default()
        };
        let before = quoted_entry(&launch, &rules, 2).unwrap();
        let mut with_future = launch.clone();
        with_future.flow.push(record(1, 103, 10_000, 900));
        let after = quoted_entry(&with_future, &rules, 2).unwrap();
        let mut shuffled = launch.clone();
        shuffled.flow.reverse();
        assert_eq!(before, quoted_entry(&shuffled, &rules, 2).unwrap());
        assert_eq!(before, after);
        assert!(!before.tokens_out.is_zero());
        let mut malformed = launch;
        if let FlowEvent::Sell { quote_out, .. } = &mut malformed.flow[1].event {
            *quote_out = U256::from(10_000);
        }
        assert!(quoted_entry(&malformed, &rules, 2).is_none());
    }

    #[test]
    fn snipe_tax_inside_buy_fee_is_not_subtracted_twice() {
        let state = CurveState {
            quote_reserve: U256::from(1_000),
            token_reserve: U256::from(10_000),
            sellable_tokens: U256::from(10_000),
            real_quote_reserve: U256::from(500),
            ..Default::default()
        };
        let tx = B256::from(U256::from(7));
        // The CurveBuy fee (30) already includes the snipe tax that the paired
        // SnipeTaxCharged event (20) only reports — subtracting it again would
        // leave the reserve 20 short.
        let launch = ReplayLaunch {
            token: format!("{:#x}", Address::from([2u8; 20])),
            curve: format!("{:#x}", Address::from([3u8; 20])),
            launch_block: 1,
            recorded_at: 1,
            launched_at: 100,
            start_bps: 9_900,
            window: 3,
            taxed_buyers_s1: 0,
            exempt_buys_s0: 0,
            gate_observed: true,
            graduated: false,
            outcome_observed: true,
            provenance_complete: true,
            run_id: Some("run".into()),
            launch_sequence: Some(0),
            curve_state: Some(state),
            flow: vec![
                FlowRecord {
                    block_number: 1,
                    block_hash: B256::from([1u8; 32]),
                    transaction_index: 0,
                    log_index: 0,
                    transaction_hash: tx,
                    event: FlowEvent::Buy {
                        recipient: Address::from([1u8; 20]),
                        quote_in: U256::from(100),
                        tokens_out: U256::from(50),
                        fee: U256::from(30),
                        tax: U256::from(5),
                        timestamp: 101,
                        transaction_hash: tx,
                    },
                },
                FlowRecord {
                    block_number: 1,
                    block_hash: B256::from([1u8; 32]),
                    transaction_index: 0,
                    log_index: 1,
                    transaction_hash: tx,
                    event: FlowEvent::Tax {
                        transaction_hash: tx,
                        recipient: Address::from([1u8; 20]),
                        amount: U256::from(20),
                        timestamp: 101,
                    },
                },
            ],
            intel: Some(ReplayIntel {
                score_total: 100,
                fire: true,
            }),
        };
        let state = state_before(&launch, 2).unwrap();
        assert_eq!(state.quote_reserve, U256::from(1_065));
        assert_eq!(state.real_quote_reserve, U256::from(565));
        assert_eq!(state.token_reserve, U256::from(9_950));
    }

    #[test]
    fn recorded_launches_never_promote_legacy_fire_fields() {
        let events = [
            serde_json::json!({"kind":"launch_seen","t":10,"token":"0x1","score":100,"fire":true}),
            serde_json::json!({
                "kind":"launch_seen","t":11,"token":"0x2","curve":"0x3","launch_block":1,
                "launched_at":1,"start_bps":9900,"window":3,"score":90,"screen_fire":false
            }),
            serde_json::json!({"kind":"gate_observed","t":12,"token":"0x2","taxed_buyers_s1":2,"exempt_buys_s0":1}),
        ];
        let launches = recorded_launches(&events, 0);
        assert_eq!(launches.len(), 2);
        assert!(
            launches
                .iter()
                .find(|launch| launch.token == "0x1")
                .unwrap()
                .intel
                .is_none()
        );
        let measured = launches
            .iter()
            .find(|launch| launch.token == "0x2")
            .unwrap();
        assert_eq!(measured.intel.as_ref().map(|intel| intel.fire), Some(false));
        assert!(measured.gate_observed);
    }
}
