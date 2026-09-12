use crate::trade::state::StateDb;
use alloy::primitives::{Address, B256, U256};
use atomic_write_file::AtomicWriteFile;
use serde::{Deserialize, Serialize};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Exit {
    pub at: u64,
    pub tokens: String,
    pub eth_out: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gas_wei: Option<String>,
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
    /// Chain and wallet that own this record — a live engine must never manage
    /// a dry record or another wallet's inventory.
    #[serde(default)]
    pub chain_id: u64,
    #[serde(default)]
    pub wallet: String,
    /// Total ETH committed at entry (for display and realized-PnL math).
    pub entry_eth: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub entry_gas_wei: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub basis_gas_wei: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub overhead_gas_wei: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub overhead_operations: Vec<String>,
    /// REMAINING tokens held — decremented by every partial exit.
    pub tokens: String,
    /// Remaining cost basis; `None` on legacy records means `entry_eth`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub basis_eth: Option<String>,
    pub peak_eth: String,
    pub last_eth: String,
    pub last_at: u64,
    pub status: String,
    pub exits: Vec<Exit>,
}

/// Outcome of `Position::apply_exit` for reporting/emitting.
#[derive(Debug, Clone)]
pub struct AppliedExit {
    pub tokens_sold: U256,
    pub basis_sold: U256,
    pub entry_gas_sold: U256,
    pub exit_gas: U256,
    pub remaining: U256,
    pub closed: bool,
    /// `eth_out − basis_sold`, signed. This exit leg's realized PnL before gas.
    pub realized: String,
    pub net_realized: String,
}

impl Position {
    /// Remaining cost basis: `basis_eth` when present, else the full entry for
    /// legacy records that predate basis tracking.
    pub fn basis(&self) -> U256 {
        self.basis_eth
            .as_deref()
            .and_then(|s| s.parse().ok())
            .or_else(|| self.entry_eth.parse().ok())
            .unwrap_or(U256::ZERO)
    }

    pub fn gas_basis(&self) -> U256 {
        self.basis_gas_wei
            .as_deref()
            .and_then(|value| value.parse().ok())
            .or_else(|| {
                if self.exits.is_empty() {
                    self.entry_gas_wei.as_deref()?.parse().ok()
                } else {
                    None
                }
            })
            .unwrap_or(U256::ZERO)
    }

    pub fn held(&self) -> U256 {
        self.tokens.parse().unwrap_or(U256::ZERO)
    }

    /// Total ETH already taken out by previous exits.
    pub fn realized_out(&self) -> U256 {
        self.exits
            .iter()
            .filter_map(|e| e.eth_out.parse::<U256>().ok())
            .fold(U256::ZERO, U256::saturating_add)
    }

    pub fn entry_gas(&self) -> U256 {
        self.entry_gas_wei
            .as_deref()
            .and_then(|value| value.parse().ok())
            .unwrap_or(U256::ZERO)
    }

    pub fn exit_gas(&self) -> U256 {
        self.exits
            .iter()
            .filter_map(|exit| exit.gas_wei.as_deref()?.parse::<U256>().ok())
            .fold(
                self.overhead_gas_wei
                    .as_deref()
                    .and_then(|value| value.parse().ok())
                    .unwrap_or(U256::ZERO),
                U256::saturating_add,
            )
    }

    pub fn charge_overhead_gas(&mut self, operation_id: &str, gas_wei: U256) -> bool {
        if self
            .overhead_operations
            .iter()
            .any(|existing| existing == operation_id)
        {
            return false;
        }
        self.overhead_operations.push(operation_id.to_string());
        self.overhead_gas_wei = Some(
            self.overhead_gas_wei
                .as_deref()
                .and_then(|value| value.parse::<U256>().ok())
                .unwrap_or(U256::ZERO)
                .saturating_add(gas_wei)
                .to_string(),
        );
        true
    }

    pub fn gas_known(&self) -> bool {
        self.dry_run
            || (self.entry_gas_wei.is_some()
                && self.overhead_gas_wei.is_some()
                && self
                    .exits
                    .iter()
                    .all(|exit| exit.dry_run || exit.gas_wei.is_some()))
    }

    pub fn net_pnl_pct(&self, mark: U256) -> Option<f64> {
        self.gas_known().then(|| self.pnl_pct(mark))
    }

    fn pnl_pct(&self, mark: U256) -> f64 {
        let cost = if self.status == "closed" {
            self.entry_eth
                .parse::<U256>()
                .unwrap_or(U256::ZERO)
                .saturating_add(self.entry_gas())
                .saturating_add(self.exit_gas())
        } else {
            self.basis().saturating_add(self.gas_basis())
        };
        pnl_pct(mark, cost)
    }

    pub fn realized_pnl_wei(&self) -> Option<String> {
        let entry: U256 = self.entry_eth.parse().ok()?;
        let remaining = if self.status == "closed" {
            U256::ZERO
        } else if let Some(basis) = &self.basis_eth {
            basis.parse::<U256>().ok()?
        } else if self.exits.is_empty() {
            entry
        } else {
            return None;
        };
        let released = entry.checked_sub(remaining)?;
        let out = self.exits.iter().try_fold(U256::ZERO, |sum, e| {
            sum.checked_add(e.eth_out.parse::<U256>().ok()?)
        })?;
        Some(signed_wei(out, released))
    }

    pub fn net_realized_pnl_wei(&self) -> Option<String> {
        if !self.gas_known() {
            return None;
        }
        let entry_gas = parse_optional_wei(self.entry_gas_wei.as_deref())?;
        let remaining_gas = if self.status == "closed" {
            U256::ZERO
        } else {
            match self.basis_gas_wei.as_deref() {
                Some(value) => value.parse().ok()?,
                None => entry_gas,
            }
        };
        let released_gas = entry_gas.checked_sub(remaining_gas)?;
        let exit_gas = self.exit_gas();
        let before_gas: alloy::primitives::I256 = self.realized_pnl_wei()?.parse().ok()?;
        let gas = released_gas.checked_add(exit_gas)?;
        Some((before_gas - alloy::primitives::I256::try_from(gas).ok()?).to_string())
    }

    /// Record a sell of `tokens_sold` for `eth_out`. Decrements inventory and
    /// releases basis pro-rata; the position closes when nothing remains.
    #[allow(clippy::too_many_arguments)]
    pub fn apply_exit(
        &mut self,
        tokens_sold: U256,
        eth_out: U256,
        exit_gas: U256,
        reason: String,
        tx: Option<B256>,
        dry_run: bool,
        at: u64,
    ) -> AppliedExit {
        let held = self.held();
        if held.is_zero() || tokens_sold.is_zero() {
            return AppliedExit {
                tokens_sold: U256::ZERO,
                basis_sold: U256::ZERO,
                entry_gas_sold: U256::ZERO,
                exit_gas,
                remaining: held,
                closed: self.status == "closed",
                realized: "0".into(),
                net_realized: signed_wei(U256::ZERO, exit_gas),
            };
        }
        let sold = tokens_sold.min(held);
        let basis = self.basis();
        // Final sell releases all remaining basis; partial sells release pro-rata.
        let basis_sold = if sold >= held {
            basis
        } else {
            scale_held(basis, sold, held)
        };
        let gas_basis = self.gas_basis();
        let entry_gas_sold = if sold >= held {
            gas_basis
        } else {
            scale_held(gas_basis, sold, held)
        };
        let remaining = held - sold;
        self.peak_eth =
            scale_held(self.peak_eth.parse().unwrap_or(U256::ZERO), remaining, held).to_string();
        self.last_eth =
            scale_held(self.last_eth.parse().unwrap_or(U256::ZERO), remaining, held).to_string();
        self.tokens = remaining.to_string();
        self.basis_eth = Some((basis - basis_sold).to_string());
        self.basis_gas_wei = Some((gas_basis - entry_gas_sold).to_string());
        if remaining.is_zero() {
            self.status = "closed".into();
        }
        self.exits.push(Exit {
            at,
            tokens: sold.to_string(),
            eth_out: eth_out.to_string(),
            gas_wei: Some(exit_gas.to_string()),
            reason,
            tx: tx.map(|h| format!("{h:#x}")),
            dry_run,
        });
        let realized = signed_wei(eth_out, basis_sold);
        let net_realized = signed_wei(
            eth_out,
            basis_sold
                .saturating_add(entry_gas_sold)
                .saturating_add(exit_gas),
        );
        AppliedExit {
            tokens_sold: sold,
            basis_sold,
            entry_gas_sold,
            exit_gas,
            remaining,
            closed: remaining.is_zero(),
            realized,
            net_realized,
        }
    }
}

fn parse_optional_wei(value: Option<&str>) -> Option<U256> {
    match value {
        Some(value) => value.parse().ok(),
        None => Some(U256::ZERO),
    }
}

fn scale_held(value: U256, remaining: U256, held: U256) -> U256 {
    (value.widening_mul(remaining) / alloy::primitives::U512::from(held)).to::<U256>()
}

pub fn signed_wei(value: U256, basis: U256) -> String {
    if value >= basis {
        (value - basis).to_string()
    } else {
        format!("-{}", basis - value)
    }
}

/// `(value − basis) / basis × 100` — display PnL. Zero basis → 0.
pub fn pnl_pct(value: U256, basis: U256) -> f64 {
    if basis.is_zero() {
        return 0.0;
    }
    (crate::fmt::wei_to_f64(value) - crate::fmt::wei_to_f64(basis)) / crate::fmt::wei_to_f64(basis)
        * 100.0
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
    /// Gain is measured against the remaining basis so partial exits don't
    /// distort later rungs.
    pub fn hit(&self, pos: &Position, value_eth: U256) -> Option<ExitAction> {
        let basis = pos.basis();
        if basis.is_zero() {
            return None;
        }
        let gain = pct_gain(value_eth, basis);
        let already: Vec<String> = pos.exits.iter().map(|e| e.reason.clone()).collect();
        for (frac, at) in &self.rungs {
            let tag = format!("ladder +{at}%");
            if gain >= *at as f64 && !already.iter().any(|r| r.starts_with(&tag)) {
                return Some(ExitAction {
                    fraction_bps: frac * 100,
                    reason: tag,
                });
            }
        }
        None
    }
}

fn parse_u256(s: &str) -> Option<U256> {
    s.parse().ok()
}

fn pct_gain(value: U256, entry: U256) -> f64 {
    pnl_pct(value, entry)
}

/// Why a position should close now, or None to keep holding. Post-graduation SL / trail / max-hold.
pub fn exit_reason(
    pos: &Position,
    value_eth: U256,
    rules: &ExitRules,
    now_sec: u64,
) -> Option<String> {
    let entry = pos.basis();
    if entry.is_zero() {
        return None;
    }
    let peak = parse_u256(&pos.peak_eth).unwrap_or(entry);
    let peak = if peak > value_eth { peak } else { value_eth };
    let pct = |a: U256, b: U256| 100.0 + pnl_pct(a, b);
    let gain = pct(value_eth, entry) - 100.0;
    if gain >= rules.take_profit_pct {
        return Some(format!(
            "take profit {gain:.1}% ≥ {}%",
            rules.take_profit_pct
        ));
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
        return Some(format!(
            "max hold {} min reached at {gain:.1}%",
            rules.max_hold_min
        ));
    }
    None
}

pub struct PositionStore {
    state: Arc<StateDb>,
    export_path: PathBuf,
    list: Mutex<Vec<Position>>,
    dirty: Mutex<Option<Instant>>,
}

impl PositionStore {
    /// Redb is authoritative. A legacy positions.json is imported only when
    /// the database has no position snapshot, and is never removed or renamed.
    pub fn open(dir: impl AsRef<Path>) -> anyhow::Result<Self> {
        Self::from_state(StateDb::open(dir)?)
    }

    pub fn from_state(state: Arc<StateDb>) -> anyhow::Result<Self> {
        let export_path = state.dir().join("positions.json");
        let list = match state.load_positions()? {
            Some(list) => list,
            None if export_path.exists() => {
                let encoded = std::fs::read_to_string(&export_path)
                    .map_err(|e| anyhow::anyhow!("read {}: {e}", export_path.display()))?;
                if encoded.trim().is_empty() {
                    anyhow::bail!(
                        "{} is empty — file left in place; fix or remove it to import",
                        export_path.display()
                    );
                }
                let list: Vec<Position> = serde_json::from_str(&encoded).map_err(|e| {
                    anyhow::anyhow!(
                        "parse {}: {e} — file left in place; fix or remove it to import",
                        export_path.display()
                    )
                })?;
                state.save_positions(&list)?;
                list
            }
            None => Vec::new(),
        };
        Ok(Self {
            state,
            export_path,
            list: Mutex::new(list),
            dirty: Mutex::new(None),
        })
    }

    pub fn load(&self) -> Vec<Position> {
        self.list.lock().unwrap().clone()
    }

    pub fn open_positions(&self) -> Vec<Position> {
        self.list
            .lock()
            .unwrap()
            .iter()
            .filter(|p| p.status == "open")
            .cloned()
            .collect()
    }

    /// Open positions this engine instance actually owns: matching dry/live
    /// mode and, when the record names a wallet, this wallet. Legacy records
    /// with no wallet are managed only by the mode that matches.
    pub fn open_positions_for(&self, dry_run: bool, wallet: Option<Address>) -> Vec<Position> {
        let w = wallet.map(|a| format!("{a:#x}"));
        self.open_positions()
            .into_iter()
            .filter(|p| {
                if p.dry_run != dry_run {
                    return false;
                }
                if dry_run {
                    return p.chain_id == 0 || p.chain_id == crate::chain::CHAIN_ID;
                }
                p.chain_id == crate::chain::CHAIN_ID
                    && w.as_ref()
                        .is_some_and(|w| !p.wallet.is_empty() && p.wallet.eq_ignore_ascii_case(w))
            })
            .collect()
    }

    #[allow(clippy::too_many_arguments)]
    pub fn open_position(
        &self,
        token: Address,
        curve: Address,
        symbol: String,
        name: String,
        opened_at: u64,
        entry_tx: Option<B256>,
        dry_run: bool,
        entry_eth: U256,
        entry_gas_wei: U256,
        tokens: U256,
        chain_id: u64,
        wallet: Option<Address>,
    ) -> Position {
        let pos = Position {
            id: format!("{:#x}-{opened_at}", token),
            token,
            curve,
            symbol,
            name,
            opened_at,
            entry_tx: entry_tx.map(|h| format!("{h:#x}")),
            dry_run,
            chain_id,
            wallet: wallet.map(|a| format!("{a:#x}")).unwrap_or_default(),
            entry_eth: entry_eth.to_string(),
            entry_gas_wei: Some(entry_gas_wei.to_string()),
            basis_gas_wei: Some(entry_gas_wei.to_string()),
            overhead_gas_wei: Some(U256::ZERO.to_string()),
            overhead_operations: Vec::new(),
            tokens: tokens.to_string(),
            basis_eth: Some(entry_eth.to_string()),
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
        self.update_with(id, |p| (f(p), ())).map(|(p, _)| p)
    }

    /// Update a position and return the closure's value alongside the clone.
    pub fn update_with<R>(
        &self,
        id: &str,
        f: impl FnOnce(&mut Position) -> R,
    ) -> Option<(Position, R)> {
        let mut list = self.list.lock().unwrap();
        let pos = list.iter_mut().find(|p| p.id == id)?;
        let r = f(pos);
        let out = pos.clone();
        drop(list);
        self.mark_dirty();
        Some((out, r))
    }

    /// Deadline is from the *first* unflushed mutation, so a hot mutation
    /// stream can't starve the write.
    fn mark_dirty(&self) {
        self.dirty.lock().unwrap().get_or_insert_with(Instant::now);
    }

    pub fn flush_if_due(&self) {
        let due = self
            .dirty
            .lock()
            .unwrap()
            .is_some_and(|t| t.elapsed() >= Duration::from_millis(250));
        if due && let Err(e) = self.flush() {
            tracing::warn!("positions flush: {e}");
        }
    }

    /// Commit the authoritative redb snapshot, then refresh the JSON export.
    pub fn flush(&self) -> std::io::Result<()> {
        let list = self.list.lock().unwrap();
        self.state
            .save_positions(&*list)
            .map_err(std::io::Error::other)?;
        *self.dirty.lock().unwrap() = None;
        let export = (|| -> std::io::Result<()> {
            let encoded = serde_json::to_vec_pretty(&*list).map_err(std::io::Error::other)?;
            let mut file = AtomicWriteFile::open(&self.export_path)?;
            file.write_all(&encoded)?;
            file.commit()
        })();
        if let Err(error) = export {
            tracing::warn!("positions JSON export: {error}");
        }
        drop(list);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::primitives::B256;
    use proptest::prelude::*;

    fn pos(tokens: u64, entry: u64) -> Position {
        Position {
            id: "t".into(),
            token: Address::ZERO,
            curve: Address::ZERO,
            symbol: "X".into(),
            name: "x".into(),
            opened_at: 0,
            entry_tx: None,
            dry_run: true,
            chain_id: 0,
            wallet: String::new(),
            entry_eth: U256::from(entry).to_string(),
            entry_gas_wei: None,
            basis_gas_wei: None,
            overhead_gas_wei: None,
            overhead_operations: Vec::new(),
            tokens: U256::from(tokens).to_string(),
            basis_eth: None,
            peak_eth: "0".into(),
            last_eth: "0".into(),
            last_at: 0,
            status: "open".into(),
            exits: vec![],
        }
    }

    #[test]
    fn partial_exit_conserves_inventory_and_basis() {
        let mut p = pos(1_000, 1_000);
        // Sell 40% for 0.6× — releases 40% of basis.
        let a = p.apply_exit(
            U256::from(400),
            U256::from(600),
            U256::ZERO,
            "ladder".into(),
            None,
            true,
            1,
        );
        assert_eq!(a.tokens_sold, U256::from(400));
        assert_eq!(a.basis_sold, U256::from(400));
        assert!(!a.closed);
        assert_eq!(p.held(), U256::from(600));
        assert_eq!(p.basis(), U256::from(600));
        assert_eq!(a.realized, "200");
        // Final sell releases ALL remaining basis, then closes.
        let b = p.apply_exit(
            U256::from(600),
            U256::from(900),
            U256::ZERO,
            "ladder".into(),
            None,
            true,
            2,
        );
        assert_eq!(b.basis_sold, U256::from(600));
        assert!(b.closed);
        assert_eq!(p.held(), U256::ZERO);
        assert_eq!(p.basis(), U256::ZERO);
        assert_eq!(p.status, "closed");
        assert_eq!(b.realized, "300");
        // Never oversell: asking for more than held sells only what remains.
        let mut p2 = pos(10, 10);
        let c = p2.apply_exit(
            U256::from(999),
            U256::from(5),
            U256::ZERO,
            "x".into(),
            Some(B256::ZERO),
            true,
            3,
        );
        assert_eq!(c.tokens_sold, U256::from(10));
        assert!(c.closed);
    }

    #[test]
    fn legacy_records_fall_back_to_full_entry_basis() {
        let p = pos(100, 42);
        assert_eq!(p.basis(), U256::from(42));
        let mut live = p;
        live.dry_run = false;
        assert!(live.net_pnl_pct(U256::from(42)).is_none());
        assert!(live.net_realized_pnl_wei().is_none());
    }

    #[test]
    fn failed_exit_gas_is_idempotent_by_operation() {
        let mut position = pos(1_000, 1_000);
        position.dry_run = false;
        position.entry_gas_wei = Some("0".into());
        position.basis_gas_wei = Some("0".into());
        position.overhead_gas_wei = Some("0".into());
        assert!(position.charge_overhead_gas("op-1", U256::from(10)));
        assert!(!position.charge_overhead_gas("op-1", U256::from(10)));
        assert_eq!(position.exit_gas(), U256::from(10));
        assert_eq!(position.net_realized_pnl_wei().as_deref(), Some("-10"));
        assert!(position.gas_known());
    }

    #[test]
    fn partial_exits_allocate_entry_gas_and_charge_exit_gas() {
        let mut position = pos(1_000, 1_000);
        position.entry_gas_wei = Some("100".into());
        position.basis_gas_wei = Some("100".into());
        let first = position.apply_exit(
            U256::from(400),
            U256::from(600),
            U256::from(10),
            "ladder".into(),
            None,
            false,
            1,
        );
        assert_eq!(first.entry_gas_sold, U256::from(40));
        assert_eq!(first.net_realized, "150");
        assert_eq!(position.gas_basis(), U256::from(60));
        let final_exit = position.apply_exit(
            U256::from(600),
            U256::from(900),
            U256::from(20),
            "final".into(),
            None,
            false,
            2,
        );
        assert_eq!(final_exit.net_realized, "220");
        assert_eq!(position.net_realized_pnl_wei().as_deref(), Some("370"));
    }

    #[test]
    fn apply_exit_scales_peak_and_tracks_realized_wei() {
        let mut p = pos(1_000, 1_000);
        p.peak_eth = "2000".into();
        p.last_eth = "1500".into();
        let a = p.apply_exit(
            U256::from(400),
            U256::from(600),
            U256::ZERO,
            "ladder".into(),
            None,
            true,
            1,
        );
        assert_eq!(a.basis_sold, U256::from(400));
        assert_eq!(p.basis(), U256::from(600));
        assert_eq!(p.peak_eth, "1200");
        assert_eq!(p.last_eth, "900");
        assert_eq!(p.realized_pnl_wei().as_deref(), Some("200"));
        let b = p.apply_exit(
            U256::from(600),
            U256::from(900),
            U256::ZERO,
            "ladder".into(),
            None,
            true,
            2,
        );
        assert!(b.closed);
        assert_eq!(p.realized_pnl_wei().as_deref(), Some("500"));
        let exits = p.exits.len();
        let z = p.apply_exit(
            U256::ZERO,
            U256::from(5),
            U256::ZERO,
            "x".into(),
            None,
            true,
            3,
        );
        assert_eq!(z.realized, "0");
        assert_eq!(p.exits.len(), exits);
    }

    #[test]
    fn signed_wei_handles_full_width_and_negatives() {
        assert_eq!(signed_wei(U256::MAX, U256::ZERO), U256::MAX.to_string());
        assert_eq!(signed_wei(U256::ZERO, U256::MAX), format!("-{}", U256::MAX));
        assert_eq!(signed_wei(U256::from(5), U256::from(5)), "0");
    }

    #[test]
    fn exit_reason_measures_gain_against_remaining_basis() {
        let mut p = pos(1_000, 1_000);
        p.apply_exit(
            U256::from(500),
            U256::from(500),
            U256::ZERO,
            "ladder".into(),
            None,
            true,
            1,
        );
        let rules = ExitRules {
            take_profit_pct: 80.0,
            stop_loss_pct: 35.0,
            trailing_pct: 25.0,
            max_hold_min: 45.0,
        };
        assert_eq!(exit_reason(&p, U256::from(500), &rules, 100), None);
    }

    proptest! {
        #[test]
        fn arbitrary_partial_exit_conserves_inventory_basis_and_entry_gas(
            held in 1u64..1_000_000,
            sold in 1u64..1_000_000,
            basis in 1u64..1_000_000,
            entry_gas in 0u64..100_000,
            exit_gas in 0u64..100_000,
            proceeds in 0u64..1_000_000,
        ) {
            let mut position = pos(held, basis);
            position.entry_gas_wei = Some(entry_gas.to_string());
            position.basis_gas_wei = Some(entry_gas.to_string());
            let applied = position.apply_exit(
                U256::from(sold),
                U256::from(proceeds),
                U256::from(exit_gas),
                "property".into(),
                None,
                false,
                1,
            );
            prop_assert_eq!(applied.tokens_sold + applied.remaining, U256::from(held));
            prop_assert_eq!(applied.basis_sold + position.basis(), U256::from(basis));
            prop_assert_eq!(applied.entry_gas_sold + position.gas_basis(), U256::from(entry_gas));
        }
    }

    #[test]
    fn open_positions_for_scopes_by_mode_chain_and_wallet() {
        let wallet = Address::from_word(B256::from(U256::from(1)));
        let other = Address::from_word(B256::from(U256::from(2)));
        let chain = crate::chain::CHAIN_ID;
        let named = |id: &str, dry: bool, chain_id: u64, w: String| {
            let mut p = pos(10, 10);
            p.id = id.into();
            p.dry_run = dry;
            p.chain_id = chain_id;
            p.wallet = w;
            p
        };
        let dir = tempfile::tempdir().unwrap();
        let state = StateDb::open(dir.path()).unwrap();
        let store = PositionStore {
            state,
            export_path: dir.path().join("positions.json"),
            list: Mutex::new(vec![
                named("legacy-dry", true, 0, String::new()),
                named("live-ok", false, chain, format!("{wallet:#x}")),
                named("live-no-wallet", false, chain, String::new()),
                named("live-wrong-chain", false, chain + 1, format!("{wallet:#x}")),
                named("live-wrong-wallet", false, chain, format!("{other:#x}")),
            ]),
            dirty: Mutex::new(None),
        };
        let dry = store
            .open_positions_for(true, None)
            .into_iter()
            .map(|p| p.id)
            .collect::<Vec<_>>();
        assert_eq!(dry, ["legacy-dry"]);
        let live = store
            .open_positions_for(false, Some(wallet))
            .into_iter()
            .map(|p| p.id)
            .collect::<Vec<_>>();
        assert_eq!(live, ["live-ok"]);
        drop(store);
    }

    #[test]
    fn legacy_json_is_imported_once_then_redb_wins() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("positions.json"),
            serde_json::to_vec(&vec![pos(10, 10)]).unwrap(),
        )
        .unwrap();
        let store = PositionStore::open(dir.path()).unwrap();
        assert_eq!(store.load().len(), 1);
        drop(store);
        std::fs::write(dir.path().join("positions.json"), "[]").unwrap();
        let reopened = PositionStore::open(dir.path()).unwrap();
        assert_eq!(reopened.load().len(), 1);
        drop(reopened);
    }

    #[test]
    fn open_rejects_an_empty_file_and_leaves_it() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("positions.json"), "  \n").unwrap();
        assert!(PositionStore::open(dir.path()).is_err());
        assert_eq!(
            std::fs::read_to_string(dir.path().join("positions.json")).unwrap(),
            "  \n"
        );
    }

    #[test]
    fn flush_writes_the_list_and_clears_dirty() {
        let dir = tempfile::tempdir().unwrap();
        let state = StateDb::open(dir.path()).unwrap();
        let store = PositionStore {
            state,
            export_path: dir.path().join("positions.json"),
            list: Mutex::new(vec![pos(10, 10)]),
            dirty: Mutex::new(Some(Instant::now())),
        };
        store.flush().unwrap();
        assert!(store.dirty.lock().unwrap().is_none());
        let back: Vec<Position> = serde_json::from_str(
            &std::fs::read_to_string(dir.path().join("positions.json")).unwrap(),
        )
        .unwrap();
        assert_eq!(back.len(), 1);
        assert_eq!(back[0].id, "t");
        assert_eq!(
            store
                .state
                .load_positions::<Vec<Position>>()
                .unwrap()
                .unwrap()[0]
                .id,
            "t"
        );
        drop(store);
    }
}
