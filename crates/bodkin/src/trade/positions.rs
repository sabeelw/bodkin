use alloy::primitives::{Address, B256, U256};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Exit {
    pub at: u64,
    pub tokens: String,
    pub eth_out: String,
    pub reason: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tx: Option<String>,
    pub dry_run: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Position {
    pub id: String,
    pub token: Address,
    pub curve: Address,
    pub symbol: String,
    pub name: String,
    pub opened_at: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub entry_tx: Option<String>,
    pub dry_run: bool,
    pub entry_eth: String,
    pub tokens: String,
    pub peak_eth: String,
    pub last_eth: String,
    pub last_at: u64,
    pub status: String,
    pub exits: Vec<Exit>,
}

#[derive(Debug, Clone)]
pub struct ExitRules {
    pub take_profit_pct: f64,
    pub stop_loss_pct: f64,
    pub trailing_pct: f64,
    pub max_hold_min: f64,
}

#[derive(Debug, Clone)]
pub struct ExitLadder {
    pub rungs: Vec<(u32, i32)>,
}

#[derive(Debug, Clone)]
pub struct ExitAction {
    pub fraction_bps: u32,
    pub reason: String,
}

impl ExitLadder {
    /// Sell `frac`% of the *remaining* bag the first time gain crosses `at`%.
    pub fn hit(&self, pos: &Position, value_eth: U256) -> Option<ExitAction> {
        let entry = parse_u256(&pos.entry_eth)?;
        if entry.is_zero() {
            return None;
        }
        let gain = pct_gain(value_eth, entry);
        let already: Vec<String> = pos.exits.iter().map(|e| e.reason.clone()).collect();
        for (frac, at) in &self.rungs {
            let tag = format!("ladder +{at}%");
            if gain >= *at as f64 && !already.iter().any(|r| r.starts_with(&tag)) {
                return Some(ExitAction { fraction_bps: frac * 100, reason: tag });
            }
        }
        None
    }
}

fn parse_u256(s: &str) -> Option<U256> {
    s.parse().ok()
}

fn pct_gain(value: U256, entry: U256) -> f64 {
    let v = (value * U256::from(10_000u64) / entry).try_into().unwrap_or(0u64) as f64 / 100.0;
    v - 100.0
}

/// Why a position should close now, or None to keep holding. Post-graduation SL / trail / max-hold.
pub fn exit_reason(pos: &Position, value_eth: U256, rules: &ExitRules, now_sec: u64) -> Option<String> {
    let entry = parse_u256(&pos.entry_eth)?;
    if entry.is_zero() {
        return None;
    }
    let peak = parse_u256(&pos.peak_eth).unwrap_or(entry);
    let peak = if peak > value_eth { peak } else { value_eth };
    let pct = |a: U256, b: U256| (a * U256::from(10_000u64) / b).try_into().unwrap_or(0u64) as f64 / 100.0;
    let gain = pct(value_eth, entry) - 100.0;
    if gain >= rules.take_profit_pct {
        return Some(format!("take profit {gain:.1}% ≥ {}%", rules.take_profit_pct));
    }
    if gain <= -rules.stop_loss_pct {
        return Some(format!("stop loss {gain:.1}% ≤ -{}%", rules.stop_loss_pct));
    }
    if peak > entry {
        let from_peak = 100.0 - pct(value_eth, peak);
        if from_peak >= rules.trailing_pct {
            return Some(format!(
                "trailing stop, {from_peak:.1}% below peak ({:.1}% high)",
                pct(peak, entry) - 100.0
            ));
        }
    }
    if now_sec.saturating_sub(pos.opened_at) as f64 >= rules.max_hold_min * 60.0 {
        return Some(format!("max hold {} min reached at {gain:.1}%", rules.max_hold_min));
    }
    None
}

pub struct PositionStore {
    path: PathBuf,
    list: Mutex<Vec<Position>>,
    dirty: Mutex<Option<Instant>>,
}

impl PositionStore {
    pub fn open(dir: impl AsRef<Path>) -> Self {
        let path = dir.as_ref().join("positions.json");
        let list = if path.exists() {
            std::fs::read_to_string(&path).ok().and_then(|s| serde_json::from_str(&s).ok()).unwrap_or_default()
        } else {
            vec![]
        };
        Self { path, list: Mutex::new(list), dirty: Mutex::new(None) }
    }

    pub fn load(&self) -> Vec<Position> {
        self.list.lock().unwrap().clone()
    }

    pub fn open_positions(&self) -> Vec<Position> {
        self.list.lock().unwrap().iter().filter(|p| p.status == "open").cloned().collect()
    }

    pub fn open_position(&self, token: Address, curve: Address, symbol: String, name: String, opened_at: u64, entry_tx: Option<B256>, dry_run: bool, entry_eth: U256, tokens: U256) -> Position {
        let pos = Position {
            id: format!("{:#x}-{opened_at}", token),
            token,
            curve,
            symbol,
            name,
            opened_at,
            entry_tx: entry_tx.map(|h| format!("{h:#x}")),
            dry_run,
            entry_eth: entry_eth.to_string(),
            tokens: tokens.to_string(),
            peak_eth: entry_eth.to_string(),
            last_eth: entry_eth.to_string(),
            last_at: opened_at,
            status: "open".into(),
            exits: vec![],
        };
        self.list.lock().unwrap().push(pos.clone());
        self.mark_dirty();
        pos
    }

    pub fn update(&self, id: &str, f: impl FnOnce(&mut Position)) -> Option<Position> {
        let mut list = self.list.lock().unwrap();
        let pos = list.iter_mut().find(|p| p.id == id)?;
        f(pos);
        let out = pos.clone();
        drop(list);
        self.mark_dirty();
        Some(out)
    }

    fn mark_dirty(&self) {
        *self.dirty.lock().unwrap() = Some(Instant::now());
    }

    pub fn flush_if_due(&self) {
        let due = self.dirty.lock().unwrap().is_some_and(|t| t.elapsed() >= Duration::from_millis(250));
        if due {
            self.flush();
        }
    }

    pub fn flush(&self) {
        let list = self.list.lock().unwrap();
        if let Some(parent) = self.path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        if let Ok(s) = serde_json::to_string_pretty(&*list) {
            let _ = std::fs::write(&self.path, s);
        }
        *self.dirty.lock().unwrap() = None;
    }
}
