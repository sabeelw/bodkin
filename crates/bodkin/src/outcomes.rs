use crate::pons::clock::now_ms;
use alloy::primitives::{B256, I256, U256};
use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutcomeKind {
    LaunchSeen,
    GateObserved,
    Submission,
    Attempt,
    Fire,
    Exit,
    ExitFailed,
}

impl OutcomeKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::LaunchSeen => "launch_seen",
            Self::GateObserved => "gate_observed",
            Self::Submission => "submission",
            Self::Attempt => "attempt",
            Self::Fire => "fire",
            Self::Exit => "exit",
            Self::ExitFailed => "exit_failed",
        }
    }
}

pub struct OutcomeLog {
    path: PathBuf,
    write_lock: Mutex<()>,
    run_id: String,
    sequence: AtomicU64,
}

impl OutcomeLog {
    pub fn open(dir: impl AsRef<Path>) -> Self {
        let dir = dir.as_ref();
        let _ = std::fs::create_dir_all(dir);
        Self {
            path: dir.join("outcomes.jsonl"),
            write_lock: Mutex::new(()),
            run_id: format!("{}-{}", now_ms(), std::process::id()),
            sequence: AtomicU64::new(0),
        }
    }

    pub fn write(&self, kind: OutcomeKind, fields: serde_json::Value) {
        let mut obj = fields.as_object().cloned().unwrap_or_default();
        obj.insert("schema".into(), serde_json::json!(2));
        obj.insert("kind".into(), serde_json::json!(kind.as_str()));
        obj.insert("t".into(), serde_json::json!(now_ms()));
        obj.insert("run_id".into(), serde_json::json!(&self.run_id));
        obj.insert(
            "sequence".into(),
            serde_json::json!(self.sequence.fetch_add(1, Ordering::Relaxed)),
        );
        obj.insert("chain_id".into(), serde_json::json!(crate::chain::CHAIN_ID));
        obj.insert(
            "version".into(),
            serde_json::json!(env!("CARGO_PKG_VERSION")),
        );
        let result = (|| -> anyhow::Result<()> {
            let _guard = self
                .write_lock
                .lock()
                .map_err(|_| anyhow::anyhow!("outcome log lock poisoned"))?;
            let mut file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(&self.path)?;
            writeln!(file, "{}", serde_json::Value::Object(obj))?;
            Ok(())
        })();
        if let Err(e) = result {
            tracing::warn!("outcome log write: {e}");
        }
    }

    pub fn read_all(&self) -> anyhow::Result<Vec<serde_json::Value>> {
        let file = match std::fs::File::open(&self.path) {
            Ok(file) => file,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(e.into()),
        };
        BufReader::new(file)
            .lines()
            .enumerate()
            .map(|(index, line)| {
                let line = line?;
                let value: serde_json::Value = serde_json::from_str(&line).map_err(|e| {
                    anyhow::anyhow!("{} line {}: {e}", self.path.display(), index + 1)
                })?;
                if let Some(schema) = value.get("schema") {
                    anyhow::ensure!(
                        schema.as_u64() == Some(2),
                        "{} line {} has unsupported schema",
                        self.path.display(),
                        index + 1
                    );
                    anyhow::ensure!(
                        value
                            .get("run_id")
                            .and_then(|field| field.as_str())
                            .is_some()
                            && value
                                .get("sequence")
                                .and_then(|field| field.as_u64())
                                .is_some()
                            && value.get("chain_id").and_then(|field| field.as_u64())
                                == Some(crate::chain::CHAIN_ID)
                            && value
                                .get("version")
                                .and_then(|field| field.as_str())
                                .is_some(),
                        "{} line {} has incomplete provenance",
                        self.path.display(),
                        index + 1
                    );
                }
                Ok(value)
            })
            .collect()
    }
}

#[derive(Debug, Default)]
pub struct OutcomeSummary {
    pub launches: u64,
    pub attempts: u64,
    pub fills: u64,
    /// Simulated dry-run entries — counted apart from real fills.
    pub simulated: u64,
    pub exits: u64,
    pub hit: u64,
    /// Realized PnL summed across exit legs, ETH.
    pub pnl_eth: f64,
    pub first_block_plus2: u64,
    pub by_rule: HashMap<String, (u64, u64)>,
    pub unknown_fires: u64,
    pub measured_exits: u64,
    pub simulated_exits: u64,
    pub unmeasured_exits: u64,
    pub simulated_pnl_eth: f64,
    pub pnl_complete: bool,
    pub simulated_pnl_complete: bool,
    pub failed_exits: u64,
    pub failed_exit_gas_eth: f64,
    pub failed_exit_gas_complete: bool,
}

pub fn summarize(events: &[serde_json::Value]) -> OutcomeSummary {
    let mut s = OutcomeSummary::default();
    let mut live_total = Some(I256::ZERO);
    let mut simulated_total = Some(I256::ZERO);
    let mut failed_gas = Some(U256::ZERO);
    for e in events {
        let flag = |key: &str| e.get(key).and_then(|v| v.as_bool());
        let tx = e
            .get("tx")
            .and_then(|v| v.as_str())
            .and_then(|v| v.parse::<B256>().ok());
        let simulated = flag("simulated") == Some(true)
            || flag("live") == Some(false)
            || flag("dryRun") == Some(true);
        match e.get("kind").and_then(|k| k.as_str()).unwrap_or("") {
            "launch_seen" => s.launches += 1,
            "attempt" => {
                s.attempts += 1;
                if !simulated
                    && flag("confirmed") == Some(true)
                    && flag("first_block") == Some(true)
                    && e.get("entry_second").and_then(|v| v.as_u64()) == Some(2)
                {
                    s.first_block_plus2 += 1;
                }
            }
            // "fire" is written only after reconciliation: fill:true means a
            // receipt confirmed the position. simulated:true is a dry run.
            "fire" => {
                if simulated {
                    s.simulated += 1;
                } else if flag("fill") == Some(true) && tx.is_some() {
                    s.fills += 1;
                } else {
                    // Legacy records had no fill/simulated markers.
                    s.unknown_fires += 1;
                }
            }
            "exit" => {
                s.exits += 1;
                // Realized wei per leg is signed; positive legs are "hits".
                let realized = e
                    .get("realized_wei")
                    .and_then(|v| v.as_str())
                    .and_then(|v| I256::from_dec_str(v).ok());
                let Some(realized) = realized else {
                    s.unmeasured_exits += 1;
                    continue;
                };
                if e.get("gas_wei").and_then(|value| value.as_str()).is_none() {
                    s.unmeasured_exits += 1;
                    continue;
                }
                if simulated {
                    s.simulated_exits += 1;
                    simulated_total = simulated_total.and_then(|v| v.checked_add(realized));
                } else if flag("live") == Some(true) && tx.is_some() {
                    s.measured_exits += 1;
                    let hit = realized > I256::ZERO;
                    s.hit += u64::from(hit);
                    live_total = live_total.and_then(|v| v.checked_add(realized));
                    if let Some(rule) = e
                        .get("rule")
                        .or_else(|| e.get("reason"))
                        .and_then(|v| v.as_str())
                    {
                        let entry = s.by_rule.entry(rule.to_string()).or_insert((0, 0));
                        entry.0 += 1;
                        entry.1 += u64::from(hit);
                    }
                } else {
                    s.unmeasured_exits += 1;
                }
            }
            "exit_failed" => {
                s.failed_exits += 1;
                if flag("pending") == Some(true) {
                    failed_gas = None;
                } else {
                    let gas = e
                        .get("gas_wei")
                        .and_then(|value| value.as_str())
                        .and_then(|value| value.parse::<U256>().ok());
                    failed_gas = failed_gas.and_then(|total| total.checked_add(gas?));
                }
            }
            _ => {}
        }
    }
    let to_eth = |value: I256| {
        let (sign, magnitude) = value.into_sign_and_abs();
        let amount = crate::fmt::wei_to_f64(magnitude) / 1e18;
        if sign.is_negative() { -amount } else { amount }
    };
    s.pnl_complete = live_total.is_some();
    s.simulated_pnl_complete = simulated_total.is_some();
    s.pnl_eth = live_total.map(to_eth).unwrap_or_default();
    s.simulated_pnl_eth = simulated_total.map(to_eth).unwrap_or_default();
    s.failed_exit_gas_complete = failed_gas.is_some();
    s.failed_exit_gas_eth = failed_gas
        .map(|value| crate::fmt::wei_to_f64(value) / 1e18)
        .unwrap_or_default();
    s
}

pub fn print_summary(s: &OutcomeSummary) {
    println!(
        "launches {}  attempts {}  confirmed fills {}  simulated {}  unverified fires {}  observed first-block-of-+2 {}",
        s.launches, s.attempts, s.fills, s.simulated, s.unknown_fires, s.first_block_plus2
    );
    let hit_rate = if s.measured_exits == 0 {
        "n/a".into()
    } else {
        format!("{:.0}%", s.hit as f64 / s.measured_exits as f64 * 100.0)
    };
    let realized = if s.pnl_complete && s.measured_exits > 0 {
        format!("{:.4} ETH", s.pnl_eth)
    } else {
        "n/a".into()
    };
    let simulated = if s.simulated_pnl_complete && s.simulated_exits > 0 {
        format!("{:.4} ETH", s.simulated_pnl_eth)
    } else {
        "n/a".into()
    };
    println!(
        "exit legs {}  measured live {}  simulated {}  unmeasured {}  live positive-leg rate {}",
        s.exits, s.measured_exits, s.simulated_exits, s.unmeasured_exits, hit_rate
    );
    println!(
        "recorded realized PnL (after gas when captured): live {realized}  simulated {simulated}"
    );
    if s.failed_exits > 0 {
        let failed_gas = if s.failed_exit_gas_complete {
            format!("{:.6} ETH", s.failed_exit_gas_eth)
        } else {
            "incomplete".into()
        };
        println!(
            "failed exit attempts {}  recorded gas {failed_gas}",
            s.failed_exits
        );
    }
    if !s.by_rule.is_empty() {
        println!("per-rule observed positive exit legs (not causal lift):");
        let mut rules: Vec<_> = s.by_rule.iter().collect();
        rules.sort_by_key(|(key, _)| *key);
        for (k, (n, hit)) in rules {
            println!("  {k}: {hit}/{n}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn typed_envelope_preserves_provenance_and_reserved_fields() {
        let dir = tempfile::tempdir().unwrap();
        let log = OutcomeLog::open(dir.path());
        log.write(
            OutcomeKind::Fire,
            json!({"kind":"forged","schema":999,"token":"0x1"}),
        );
        let events = log.read_all().unwrap();
        assert_eq!(events[0]["schema"], 2);
        assert_eq!(events[0]["kind"], "fire");
        assert_eq!(events[0]["chain_id"], crate::chain::CHAIN_ID);
        assert!(events[0]["run_id"].as_str().is_some());
        assert_eq!(events[0]["version"], env!("CARGO_PKG_VERSION"));
        assert_eq!(events[0]["sequence"], 0);
    }

    #[test]
    fn malformed_jsonl_is_an_error_not_a_dropped_record() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("outcomes.jsonl"),
            "{\"kind\":\"fire\"}\n{bad\n",
        )
        .unwrap();
        assert!(OutcomeLog::open(dir.path()).read_all().is_err());
    }

    #[test]
    fn legacy_and_unconfirmed_fires_are_not_fills() {
        let hash = format!("{:#x}", B256::ZERO);
        let s = summarize(&[
            json!({"kind":"fire"}),
            json!({"kind":"fire","fill":false,"tx":hash}),
            json!({"kind":"fire","fill":true}),
            json!({"kind":"fire","fill":true,"tx":hash}),
            json!({"kind":"fire","fill":true,"simulated":true,"tx":hash}),
            json!({"kind":"attempt","first_block":true}),
            json!({"kind":"attempt","confirmed":true,"first_block":true,"entry_second":2}),
        ]);
        assert_eq!(s.fills, 1);
        assert_eq!(s.simulated, 1);
        assert_eq!(s.unknown_fires, 3);
        assert_eq!(s.first_block_plus2, 1);
    }

    #[test]
    fn realized_metrics_separate_live_simulated_and_missing() {
        let hash = format!("{:#x}", B256::ZERO);
        let s = summarize(&[
            json!({"kind":"exit","live":true,"tx":hash,"realized_wei":"200000000000000000","gas_wei":"0","rule":"ladder"}),
            json!({"kind":"exit","live":true,"tx":hash,"realized_wei":"-100000000000000000","gas_wei":"0","reason":"ladder"}),
            json!({"kind":"exit","live":false,"realized_wei":"9000000000000000000","gas_wei":"0"}),
            json!({"kind":"exit","live":true,"tx":hash}),
            json!({"kind":"exit","realized_wei":"8000000000000000000"}),
            json!({"kind":"exit","live":true,"tx":hash,"realized_wei":"malformed"}),
            json!({"kind":"exit_failed","live":true,"gas_wei":"100","pending":false}),
        ]);
        assert_eq!(s.exits, 6);
        assert_eq!(s.measured_exits, 2);
        assert_eq!(s.simulated_exits, 1);
        assert_eq!(s.unmeasured_exits, 3);
        assert_eq!(s.hit, 1);
        assert!((s.pnl_eth - 0.1).abs() < 1e-12);
        assert!((s.simulated_pnl_eth - 9.0).abs() < 1e-12);
        assert_eq!(s.by_rule["ladder"], (2, 1));
        assert_eq!(s.failed_exits, 1);
        assert!(s.failed_exit_gas_complete);
        assert!((s.failed_exit_gas_eth - 1e-16).abs() < 1e-30);
    }

    #[test]
    fn overflowing_totals_are_explicitly_unavailable() {
        let hash = format!("{:#x}", B256::ZERO);
        let s = summarize(&[
            json!({"kind":"exit","live":true,"tx":hash,"realized_wei":I256::MAX.to_string(),"gas_wei":"0"}),
            json!({"kind":"exit","live":true,"tx":hash,"realized_wei":"1","gas_wei":"0"}),
        ]);
        assert!(!s.pnl_complete);
    }
}
