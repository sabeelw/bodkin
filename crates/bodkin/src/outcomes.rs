use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutcomeEvent {
    pub kind: String,
    pub t: u64,
    #[serde(flatten)]
    pub fields: serde_json::Value,
}

pub struct OutcomeLog {
    path: PathBuf,
}

impl OutcomeLog {
    pub fn open(dir: impl AsRef<Path>) -> Self {
        let dir = dir.as_ref();
        let _ = std::fs::create_dir_all(dir);
        Self { path: dir.join("outcomes.jsonl") }
    }

    pub fn write(&self, kind: &str, fields: serde_json::Value) {
        let ev = serde_json::json!({
            "kind": kind,
            "t": now_ms(),
        });
        let mut obj = ev.as_object().cloned().unwrap_or_default();
        if let Some(map) = fields.as_object() {
            for (k, v) in map {
                obj.insert(k.clone(), v.clone());
            }
        }
        if let Ok(mut f) = OpenOptions::new().create(true).append(true).open(&self.path) {
            let _ = writeln!(f, "{}", serde_json::Value::Object(obj));
        }
    }

    pub fn read_all(&self) -> Vec<serde_json::Value> {
        let Ok(f) = std::fs::File::open(&self.path) else { return vec![] };
        BufReader::new(f).lines().filter_map(|l| l.ok().and_then(|s| serde_json::from_str(&s).ok())).collect()
    }
}

#[derive(Debug, Default)]
pub struct OutcomeSummary {
    pub launches: u64,
    pub attempts: u64,
    pub fills: u64,
    pub exits: u64,
    pub hit: u64,
    pub pnl_eth: f64,
    pub first_block_plus2: u64,
    pub by_rule: HashMap<String, (u64, u64)>,
}

pub fn summarize(events: &[serde_json::Value]) -> OutcomeSummary {
    let mut s = OutcomeSummary::default();
    for e in events {
        match e.get("kind").and_then(|k| k.as_str()).unwrap_or("") {
            "launch_seen" => s.launches += 1,
            "attempt" | "fire" => {
                s.attempts += 1;
                if e.get("fill").and_then(|v| v.as_bool()).unwrap_or(false) || e.get("kind").and_then(|k| k.as_str()) == Some("fire") {
                    s.fills += 1;
                }
                if e.get("first_block").and_then(|v| v.as_bool()).unwrap_or(false) {
                    s.first_block_plus2 += 1;
                }
            }
            "exit" => {
                s.exits += 1;
                let pnl = e.get("pnl_pct").and_then(|v| v.as_f64()).unwrap_or(0.0);
                if pnl > 0.0 {
                    s.hit += 1;
                }
                if let Some(eth) = e.get("pnl_eth").and_then(|v| v.as_f64()) {
                    s.pnl_eth += eth;
                }
            }
            _ => {}
        }
        if let Some(rule) = e.get("rule").and_then(|v| v.as_str()) {
            let entry = s.by_rule.entry(rule.to_string()).or_insert((0, 0));
            entry.0 += 1;
            if e.get("hit").and_then(|v| v.as_bool()).unwrap_or(false) {
                entry.1 += 1;
            }
        }
    }
    s
}

pub fn print_summary(s: &OutcomeSummary) {
    println!("launches {}  attempts {}  fills {}  first-block-of-+2 {}  exits {}  hit rate {:.0}%  pnl {:.4} ETH",
        s.launches, s.attempts, s.fills, s.first_block_plus2, s.exits,
        if s.exits == 0 { 0.0 } else { s.hit as f64 / s.exits as f64 * 100.0 },
        s.pnl_eth
    );
    if !s.by_rule.is_empty() {
        println!("per-rule lift:");
        for (k, (n, hit)) in &s.by_rule {
            println!("  {k}: {hit}/{n}");
        }
    }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0)
}
