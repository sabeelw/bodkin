use crate::abi::factory;
use crate::chain::CHAIN_ID;
use crate::pons::clock::now_ms;
use crate::rpc::{Lane, Rpc};
use crate::trade::curve::{parse_curve_buy_tokens, parse_curve_refund, parse_curve_sell_fill};
use crate::trade::pool::{net_pool_quote, parse_pool_sell_fill};
use crate::trade::positions::{PositionStore, signed_wei};
use crate::trade::state::StateDb;
use crate::trade::v4::pons_pool_key;
use alloy::primitives::{Address, B256, U256};
use atomic_write_file::AtomicWriteFile;
use serde::{Deserialize, Serialize};
use std::io::Write;
use std::path::Path;
use std::sync::{Arc, Mutex};

const CANONICALITY_DEPTH: u64 = 64;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CommandEffect {
    #[default]
    Receipt,
    RequiredLog {
        address: Address,
        topic: B256,
    },
    RuntimeCode {
        address: Address,
        code_hash: B256,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum OperationSpec {
    Entry {
        token: Address,
        curve: Address,
        symbol: String,
        name: String,
        value: String,
    },
    Exit {
        position_id: String,
        token: Address,
        curve: Address,
        reason: String,
    },
    Command {
        label: String,
        #[serde(default)]
        effect: CommandEffect,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum OperationState {
    Pending,
    Applied,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum TxState {
    Prepared,
    Submitted,
    Confirmed,
    Reverted,
    Rejected,
    Unresolved,
    Dropped,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TxRecord {
    step: String,
    nonce: u64,
    hash: String,
    state: TxState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    block_number: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    block_hash: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    gas_wei: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Operation {
    id: String,
    chain_id: u64,
    wallet: String,
    created_at: u64,
    updated_at: u64,
    state: OperationState,
    spec: OperationSpec,
    txs: Vec<TxRecord>,
}

pub enum JournalEvent {
    Stage {
        step: String,
        nonce: u64,
        hash: B256,
    },
    StageMany {
        step: String,
        txs: Vec<(u64, B256)>,
    },
    Submitted {
        hash: B256,
    },
    Confirmed {
        hash: B256,
        block_number: u64,
        block_hash: B256,
        gas_wei: U256,
    },
    Reverted {
        hash: B256,
        block_number: u64,
        block_hash: B256,
        gas_wei: U256,
    },
    Rejected {
        hash: B256,
    },
    Unresolved {
        hash: B256,
    },
    AllRejected,
    Applied,
    Failed,
}

#[derive(Debug, Default)]
pub struct RecoveryReport {
    pub recovered_entries: u64,
    pub recovered_exits: u64,
    pub failed: u64,
    pub blocked: Vec<String>,
}

pub struct TxJournal {
    state: Arc<StateDb>,
    export_path: std::path::PathBuf,
    operations: Mutex<Vec<Operation>>,
}

impl TxJournal {
    pub fn open(dir: impl AsRef<Path>) -> anyhow::Result<Self> {
        Self::from_state(StateDb::open(dir)?)
    }

    pub fn from_state(state: Arc<StateDb>) -> anyhow::Result<Self> {
        let export_path = state.dir().join("transactions.json");
        let mut operations: Vec<Operation> = state.load_operations()?;
        if operations.is_empty() && export_path.exists() {
            let encoded = std::fs::read_to_string(&export_path)
                .map_err(|e| anyhow::anyhow!("read {}: {e}", export_path.display()))?;
            if encoded.trim().is_empty() {
                anyhow::bail!(
                    "{} is empty — file left in place; fix or remove it to import",
                    export_path.display()
                );
            }
            operations = serde_json::from_str(&encoded).map_err(|e| {
                anyhow::anyhow!(
                    "parse {}: {e} — file left in place; fix or remove it to import",
                    export_path.display()
                )
            })?;
            state.save_operations(
                operations
                    .iter()
                    .map(|operation| (operation.id.clone(), operation)),
            )?;
        }
        Ok(Self {
            state,
            export_path,
            operations: Mutex::new(operations),
        })
    }

    pub fn begin(&self, wallet: Address, spec: OperationSpec) -> anyhow::Result<String> {
        let mut operations = self
            .operations
            .lock()
            .map_err(|_| anyhow::anyhow!("transaction journal lock poisoned"))?;
        if let Some(existing) = operations.iter().find(|op| {
            op.state == OperationState::Pending
                && op.wallet.eq_ignore_ascii_case(&format!("{wallet:#x}"))
                && same_subject(&op.spec, &spec)
        }) {
            anyhow::bail!("operation {} is still pending recovery", existing.id);
        }
        let now = now_ms();
        let id = format!("{now}-{}", operations.len());
        operations.push(Operation {
            id: id.clone(),
            chain_id: CHAIN_ID,
            wallet: format!("{wallet:#x}"),
            created_at: now,
            updated_at: now,
            state: OperationState::Pending,
            spec,
            txs: Vec::new(),
        });
        self.persist(operations.last().expect("just inserted"), &operations)?;
        Ok(id)
    }

    pub fn record(&self, operation_id: &str, event: JournalEvent) -> anyhow::Result<()> {
        let mut operations = self
            .operations
            .lock()
            .map_err(|_| anyhow::anyhow!("transaction journal lock poisoned"))?;
        let operation = operations
            .iter_mut()
            .find(|op| op.id == operation_id)
            .ok_or_else(|| anyhow::anyhow!("transaction operation {operation_id} not found"))?;
        apply_event(operation, event)?;
        operation.updated_at = now_ms();
        let changed = operation.clone();
        self.persist(&changed, &operations)?;
        Ok(())
    }

    pub fn requires_recovery(&self, operation_id: &str) -> bool {
        self.operations
            .lock()
            .ok()
            .and_then(|ops| ops.iter().find(|op| op.id == operation_id).cloned())
            .is_some_and(|op| operation_requires_recovery(&op))
    }

    pub fn recorded_gas(&self, operation_id: &str) -> anyhow::Result<U256> {
        operation_gas(&self.operation(operation_id)?)
    }

    pub async fn canonical_issues(
        &self,
        rpc: &Rpc,
        wallet: Address,
    ) -> anyhow::Result<Vec<String>> {
        let head = rpc.block_number(Lane::Hot).await?;
        let wallet = format!("{wallet:#x}");
        let operations = self
            .operations
            .lock()
            .map_err(|_| anyhow::anyhow!("transaction journal lock poisoned"))?
            .iter()
            .filter(|operation| {
                operation.state == OperationState::Applied
                    && operation.chain_id == CHAIN_ID
                    && operation.wallet.eq_ignore_ascii_case(&wallet)
            })
            .cloned()
            .collect::<Vec<_>>();
        let mut issues = Vec::new();
        for operation in operations {
            for transaction in operation.txs.iter().filter(|transaction| {
                matches!(transaction.state, TxState::Confirmed | TxState::Reverted)
                    && transaction
                        .block_number
                        .is_none_or(|block| head <= block.saturating_add(CANONICALITY_DEPTH))
            }) {
                let hash = parse_hash(&transaction.hash)?;
                let Ok(receipt) = rpc.get_transaction_receipt(Lane::Hot, hash).await else {
                    issues.push(format!("{} receipt {hash:#x} disappeared", operation.id));
                    continue;
                };
                let expected_status = transaction.state == TxState::Confirmed;
                let canonical = receipt.tx_hash == Some(hash)
                    && receipt.status == Some(expected_status)
                    && receipt.block_number == transaction.block_number
                    && transaction.block_hash.as_ref().is_some_and(|expected| {
                        receipt.block_hash.is_some_and(|actual| {
                            expected.eq_ignore_ascii_case(&format!("{actual:#x}"))
                        })
                    })
                    && match (receipt.block_number, receipt.block_hash) {
                        (Some(block), Some(hash)) => {
                            rpc.block_hash(Lane::Hot, block).await.ok() == Some(hash)
                        }
                        _ => false,
                    };
                if !canonical {
                    issues.push(format!(
                        "{} receipt {} is no longer canonical",
                        operation.id, transaction.hash
                    ));
                }
            }
        }
        Ok(issues)
    }

    pub async fn recover(
        &self,
        rpc: &Rpc,
        positions: &PositionStore,
        wallet: Address,
    ) -> anyhow::Result<RecoveryReport> {
        let wallet_text = format!("{wallet:#x}");
        let head = rpc.block_number(Lane::Hot).await?;
        let operations = self
            .operations
            .lock()
            .map_err(|_| anyhow::anyhow!("transaction journal lock poisoned"))?
            .iter()
            .filter(|operation| {
                operation.chain_id == CHAIN_ID
                    && operation.wallet.eq_ignore_ascii_case(&wallet_text)
                    && (operation.state == OperationState::Pending
                        || (operation.state == OperationState::Applied
                            && operation.txs.iter().any(|tx| {
                                matches!(tx.state, TxState::Confirmed | TxState::Reverted)
                                    && tx.block_number.is_none_or(|block| {
                                        head <= block.saturating_add(CANONICALITY_DEPTH)
                                    })
                            })))
            })
            .cloned()
            .collect::<Vec<_>>();
        if operations.is_empty() {
            return Ok(RecoveryReport::default());
        }
        let latest_nonce = rpc.get_transaction_count(Lane::Hot, wallet, false).await?;
        let pending_nonce = rpc.get_transaction_count(Lane::Hot, wallet, true).await?;
        let mut report = RecoveryReport::default();
        for operation in operations {
            self.refresh_receipts(rpc, &operation, latest_nonce, pending_nonce)
                .await?;
            let current = self.operation(&operation.id)?;
            if operation.state == OperationState::Applied {
                if resolved_receipts_changed(&operation, &current) {
                    report.blocked.push(format!(
                        "operation {} changed canonical receipt state",
                        operation.id
                    ));
                }
                continue;
            }
            match &current.spec {
                OperationSpec::Entry { .. } => {
                    self.recover_entry(rpc, positions, wallet, &current, &mut report)
                        .await?
                }
                OperationSpec::Exit { .. } => {
                    self.recover_exit(rpc, positions, wallet, &current, &mut report)
                        .await?
                }
                OperationSpec::Command { .. } => {
                    self.recover_command(rpc, &current, &mut report).await?
                }
            }
        }
        Ok(report)
    }

    async fn refresh_receipts(
        &self,
        rpc: &Rpc,
        operation: &Operation,
        latest_nonce: u64,
        pending_nonce: u64,
    ) -> anyhow::Result<()> {
        for tx in &operation.txs {
            if matches!(tx.state, TxState::Rejected | TxState::Dropped) {
                continue;
            }
            let hash = parse_hash(&tx.hash)?;
            match rpc.get_transaction_receipt(Lane::Hot, hash).await {
                Ok(receipt) if receipt.tx_hash == Some(hash) => {
                    let canonical = match (receipt.block_number, receipt.block_hash) {
                        (Some(number), Some(block_hash)) => {
                            rpc.block_hash(Lane::Hot, number).await.ok() == Some(block_hash)
                        }
                        _ => false,
                    };
                    if !canonical {
                        self.record(&operation.id, JournalEvent::Unresolved { hash })?;
                        continue;
                    }
                    let block_number = receipt.block_number.expect("canonical receipt has a block");
                    let block_hash = receipt
                        .block_hash
                        .expect("canonical receipt has a block hash");
                    let gas_wei = receipt
                        .gas_used
                        .expect("strict mined receipt has gasUsed")
                        .saturating_mul(
                            receipt
                                .effective_gas_price
                                .expect("strict mined receipt has effectiveGasPrice"),
                        );
                    self.record(
                        &operation.id,
                        match receipt.status {
                            Some(true) => JournalEvent::Confirmed {
                                hash,
                                block_number,
                                block_hash,
                                gas_wei,
                            },
                            Some(false) => JournalEvent::Reverted {
                                hash,
                                block_number,
                                block_hash,
                                gas_wei,
                            },
                            None => JournalEvent::Unresolved { hash },
                        },
                    )?;
                }
                Ok(_) => self.record(&operation.id, JournalEvent::Unresolved { hash })?,
                Err(_)
                    if now_ms().saturating_sub(operation.created_at) >= 60_000
                        && latest_nonce <= tx.nonce
                        && pending_nonce <= tx.nonce =>
                {
                    self.set_tx_state(&operation.id, hash, TxState::Dropped, None)?;
                }
                Err(_) => self.record(&operation.id, JournalEvent::Unresolved { hash })?,
            }
        }
        Ok(())
    }

    async fn recover_entry(
        &self,
        rpc: &Rpc,
        positions: &PositionStore,
        wallet: Address,
        operation: &Operation,
        report: &mut RecoveryReport,
    ) -> anyhow::Result<()> {
        let OperationSpec::Entry {
            token,
            curve,
            symbol,
            name,
            value,
        } = &operation.spec
        else {
            unreachable!();
        };
        if positions.load().iter().any(|p| {
            p.entry_tx.as_deref().is_some_and(|hash| {
                operation
                    .txs
                    .iter()
                    .any(|tx| tx.hash.eq_ignore_ascii_case(hash))
            })
        }) {
            if operation_has_ambiguous(operation) {
                report.blocked.push(format!(
                    "{} recovered entry still has unresolved burst attempts",
                    operation.id
                ));
            } else {
                self.record(&operation.id, JournalEvent::Applied)?;
            }
            return Ok(());
        }
        let mut fills = Vec::new();
        for tx in operation
            .txs
            .iter()
            .filter(|tx| tx.state == TxState::Confirmed && tx.step == "entry")
        {
            let hash = parse_hash(&tx.hash)?;
            let receipt = rpc.get_transaction_receipt(Lane::Hot, hash).await?;
            let tokens = parse_curve_buy_tokens(&receipt.logs, *curve, wallet);
            if !tokens.is_zero() {
                let sent: U256 = value.parse()?;
                let actual = sent.saturating_sub(parse_curve_refund(&receipt.logs, *curve, wallet));
                if actual.is_zero() {
                    report.blocked.push(format!(
                        "{} confirmed entry {hash:#x} has zero net ETH",
                        operation.id
                    ));
                    return Ok(());
                }
                fills.push((hash, tokens, actual));
            }
        }
        if !fills.is_empty() {
            let hash = fills[0].0;
            let tokens = fills
                .iter()
                .fold(U256::ZERO, |sum, fill| sum.saturating_add(fill.1));
            let actual = fills
                .iter()
                .fold(U256::ZERO, |sum, fill| sum.saturating_add(fill.2));
            positions.open_position(
                *token,
                *curve,
                symbol.clone(),
                name.clone(),
                operation.created_at / 1000,
                Some(hash),
                false,
                actual,
                operation_gas(operation)?,
                tokens,
                CHAIN_ID,
                Some(wallet),
            );
            positions.flush()?;
            report.recovered_entries += 1;
            if operation_has_ambiguous(operation) {
                report.blocked.push(format!(
                    "{} recovered entry still has unresolved burst attempts",
                    operation.id
                ));
            } else {
                self.record(&operation.id, JournalEvent::Applied)?;
            }
        } else if operation_has_ambiguous(operation) {
            report.blocked.push(format!(
                "{} entry still has unresolved transaction state",
                operation.id
            ));
        } else {
            self.record(&operation.id, JournalEvent::Failed)?;
            report.failed += 1;
        }
        Ok(())
    }

    async fn recover_exit(
        &self,
        rpc: &Rpc,
        positions: &PositionStore,
        wallet: Address,
        operation: &Operation,
        report: &mut RecoveryReport,
    ) -> anyhow::Result<()> {
        let OperationSpec::Exit {
            position_id,
            token,
            curve,
            reason,
        } = &operation.spec
        else {
            unreachable!();
        };
        if positions.load().iter().any(|p| {
            p.id == *position_id
                && p.exits.iter().any(|exit| {
                    exit.tx.as_deref().is_some_and(|hash| {
                        operation
                            .txs
                            .iter()
                            .any(|tx| tx.hash.eq_ignore_ascii_case(hash))
                    })
                })
        }) {
            self.record(&operation.id, JournalEvent::Applied)?;
            return Ok(());
        }
        let sales = operation
            .txs
            .iter()
            .filter(|tx| {
                tx.state == TxState::Confirmed
                    && matches!(tx.step.as_str(), "curve_sell" | "pool_sell")
            })
            .collect::<Vec<_>>();
        if sales.len() > 1 {
            report.blocked.push(format!(
                "{} has multiple confirmed exit fills",
                operation.id
            ));
            return Ok(());
        }
        if let Some(tx) = sales.first() {
            let hash = parse_hash(&tx.hash)?;
            let receipt = rpc.get_transaction_receipt(Lane::Hot, hash).await?;
            let fill = if tx.step == "curve_sell" {
                let (tokens, out) = parse_curve_sell_fill(&receipt.logs, *curve, wallet);
                (!tokens.is_zero() && !out.is_zero()).then_some((tokens, out))
            } else {
                let record = rpc
                    .eth_call(
                        Lane::Hot,
                        crate::chain::ADDR.pons_factory,
                        factory::getLaunchedTokenCall { token: *token },
                        None,
                    )
                    .await?;
                let key = pons_pool_key(
                    *token,
                    record.pairToken,
                    i32::try_from(record.tickSpacing).unwrap_or(0),
                );
                let (tokens, gross) = match parse_pool_sell_fill(&receipt.logs, &key) {
                    Some(fill) => fill,
                    None => {
                        report.blocked.push(format!(
                            "{} confirmed pool exit has no matching Swap",
                            operation.id
                        ));
                        return Ok(());
                    }
                };
                Some((
                    tokens,
                    net_pool_quote(rpc, *curve, record.creatorTaxBps, gross).await?,
                ))
            };
            let Some((tokens, out)) = fill else {
                report.blocked.push(format!(
                    "{} confirmed exit has no nonzero fill",
                    operation.id
                ));
                return Ok(());
            };
            if tokens.is_zero() || out.is_zero() {
                report.blocked.push(format!(
                    "{} confirmed exit has zero token or ETH movement",
                    operation.id
                ));
                return Ok(());
            }
            let gas_wei = operation_gas(operation)?;
            let Some((updated, applied)) = positions.update_with(position_id, |position| {
                position.apply_exit(
                    tokens,
                    out,
                    gas_wei,
                    reason.clone(),
                    Some(hash),
                    false,
                    now_ms() / 1000,
                )
            }) else {
                report.blocked.push(format!(
                    "{} confirmed exit references missing position {position_id}",
                    operation.id
                ));
                return Ok(());
            };
            if applied.tokens_sold.is_zero() {
                report.blocked.push(format!(
                    "{} confirmed exit could not be applied to position {position_id}",
                    operation.id
                ));
                return Ok(());
            }
            positions.flush()?;
            self.record(&operation.id, JournalEvent::Applied)?;
            report.recovered_exits += 1;
            tracing::warn!(
                "recovered exit {}: {} wei realized, total {}",
                position_id,
                applied.realized,
                updated
                    .realized_pnl_wei()
                    .unwrap_or_else(|| signed_wei(U256::ZERO, U256::ZERO))
            );
        } else if operation_requires_recovery(operation) {
            report.blocked.push(format!(
                "{} exit still has unresolved transaction state",
                operation.id
            ));
        } else {
            let gas_wei = operation_gas(operation)?;
            if !gas_wei.is_zero() {
                let Some(_) = positions.update(position_id, |position| {
                    position.charge_overhead_gas(&operation.id, gas_wei);
                }) else {
                    report.blocked.push(format!(
                        "{} failed exit gas references missing position {position_id}",
                        operation.id
                    ));
                    return Ok(());
                };
                positions.flush()?;
            }
            self.record(&operation.id, JournalEvent::Failed)?;
            report.failed += 1;
        }
        Ok(())
    }

    async fn recover_command(
        &self,
        rpc: &Rpc,
        operation: &Operation,
        report: &mut RecoveryReport,
    ) -> anyhow::Result<()> {
        if operation_has_ambiguous(operation) {
            report.blocked.push(format!(
                "{} command still has unresolved transaction state",
                operation.id
            ));
        } else if operation
            .txs
            .iter()
            .any(|tx| tx.state == TxState::Confirmed && tx_has_effect(tx))
        {
            if let OperationSpec::Command { effect, .. } = &operation.spec {
                match effect {
                    CommandEffect::Receipt => {}
                    CommandEffect::RequiredLog { address, topic } => {
                        let hash = operation
                            .txs
                            .iter()
                            .find(|tx| tx.state == TxState::Confirmed && tx_has_effect(tx))
                            .map(|tx| parse_hash(&tx.hash))
                            .transpose()?
                            .ok_or_else(|| anyhow::anyhow!("confirmed command hash missing"))?;
                        let receipt = rpc.get_transaction_receipt(Lane::Hot, hash).await?;
                        if !receipt.logs.iter().any(|log| {
                            log.address() == *address
                                && log.topics().first().copied() == Some(*topic)
                        }) {
                            report.blocked.push(format!(
                                "{} confirmed command is missing required log {topic:#x}",
                                operation.id
                            ));
                            return Ok(());
                        }
                    }
                    CommandEffect::RuntimeCode { address, code_hash } => {
                        let code = rpc.get_code(Lane::Hot, *address).await?;
                        if code.is_empty() || alloy::primitives::keccak256(&code) != *code_hash {
                            report.blocked.push(format!(
                                "{} helper runtime verification failed at {address:#x}",
                                operation.id
                            ));
                            return Ok(());
                        }
                    }
                }
            }
            self.record(&operation.id, JournalEvent::Applied)?;
        } else {
            self.record(&operation.id, JournalEvent::Failed)?;
            report.failed += 1;
        }
        Ok(())
    }

    fn operation(&self, operation_id: &str) -> anyhow::Result<Operation> {
        self.operations
            .lock()
            .map_err(|_| anyhow::anyhow!("transaction journal lock poisoned"))?
            .iter()
            .find(|op| op.id == operation_id)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("transaction operation {operation_id} not found"))
    }

    fn set_tx_state(
        &self,
        operation_id: &str,
        hash: B256,
        state: TxState,
        block_number: Option<u64>,
    ) -> anyhow::Result<()> {
        let mut operations = self
            .operations
            .lock()
            .map_err(|_| anyhow::anyhow!("transaction journal lock poisoned"))?;
        let operation = operations
            .iter_mut()
            .find(|op| op.id == operation_id)
            .ok_or_else(|| anyhow::anyhow!("transaction operation {operation_id} not found"))?;
        let tx = operation
            .txs
            .iter_mut()
            .find(|tx| tx.hash.eq_ignore_ascii_case(&format!("{hash:#x}")))
            .ok_or_else(|| anyhow::anyhow!("transaction {hash:#x} not staged"))?;
        tx.state = state;
        tx.block_number = block_number;
        operation.updated_at = now_ms();
        let changed = operation.clone();
        self.persist(&changed, &operations)?;
        Ok(())
    }

    fn persist(&self, operation: &Operation, operations: &[Operation]) -> anyhow::Result<()> {
        self.state.save_operation(&operation.id, operation)?;
        if operation.state == OperationState::Pending
            && !operation
                .txs
                .iter()
                .any(|tx| tx.state == TxState::Unresolved)
        {
            return Ok(());
        }
        let export = (|| -> anyhow::Result<()> {
            let encoded = serde_json::to_vec_pretty(operations)?;
            let mut file = AtomicWriteFile::open(&self.export_path)?;
            file.write_all(&encoded)?;
            file.commit()?;
            Ok(())
        })();
        if let Err(error) = export {
            tracing::warn!("transaction JSON export: {error}");
        }
        Ok(())
    }
}

fn apply_event(operation: &mut Operation, event: JournalEvent) -> anyhow::Result<()> {
    match event {
        JournalEvent::Stage { step, nonce, hash } => {
            let hash = format!("{hash:#x}");
            if !operation
                .txs
                .iter()
                .any(|tx| tx.hash.eq_ignore_ascii_case(&hash))
            {
                operation.txs.push(TxRecord {
                    step,
                    nonce,
                    hash,
                    state: TxState::Prepared,
                    block_number: None,
                    block_hash: None,
                    gas_wei: None,
                });
            }
        }
        JournalEvent::StageMany { step, txs } => {
            for (nonce, hash) in txs {
                let hash = format!("{hash:#x}");
                if !operation
                    .txs
                    .iter()
                    .any(|tx| tx.hash.eq_ignore_ascii_case(&hash))
                {
                    operation.txs.push(TxRecord {
                        step: step.clone(),
                        nonce,
                        hash,
                        state: TxState::Prepared,
                        block_number: None,
                        block_hash: None,
                        gas_wei: None,
                    });
                }
            }
        }
        JournalEvent::Submitted { hash } => {
            set_record(operation, hash, TxState::Submitted, None, None, None)?
        }
        JournalEvent::Confirmed {
            hash,
            block_number,
            block_hash,
            gas_wei,
        } => set_record(
            operation,
            hash,
            TxState::Confirmed,
            Some(block_number),
            Some(block_hash),
            Some(gas_wei),
        )?,
        JournalEvent::Reverted {
            hash,
            block_number,
            block_hash,
            gas_wei,
        } => set_record(
            operation,
            hash,
            TxState::Reverted,
            Some(block_number),
            Some(block_hash),
            Some(gas_wei),
        )?,
        JournalEvent::Rejected { hash } => {
            set_record(operation, hash, TxState::Rejected, None, None, None)?
        }
        JournalEvent::Unresolved { hash } => {
            set_record(operation, hash, TxState::Unresolved, None, None, None)?
        }
        JournalEvent::AllRejected => {
            for tx in &mut operation.txs {
                if matches!(
                    tx.state,
                    TxState::Prepared | TxState::Submitted | TxState::Unresolved
                ) {
                    tx.state = TxState::Rejected;
                }
            }
        }
        JournalEvent::Applied => operation.state = OperationState::Applied,
        JournalEvent::Failed => operation.state = OperationState::Failed,
    }
    Ok(())
}

fn set_record(
    operation: &mut Operation,
    hash: B256,
    state: TxState,
    block_number: Option<u64>,
    block_hash: Option<B256>,
    gas_wei: Option<U256>,
) -> anyhow::Result<()> {
    let hash = format!("{hash:#x}");
    let tx = operation
        .txs
        .iter_mut()
        .find(|tx| tx.hash.eq_ignore_ascii_case(&hash))
        .ok_or_else(|| anyhow::anyhow!("transaction {hash} not staged"))?;
    tx.state = state;
    tx.block_number = block_number;
    tx.block_hash = block_hash.map(|hash| format!("{hash:#x}"));
    tx.gas_wei = gas_wei.map(|gas| gas.to_string());
    Ok(())
}

fn same_subject(left: &OperationSpec, right: &OperationSpec) -> bool {
    match (left, right) {
        (OperationSpec::Entry { token: a, .. }, OperationSpec::Entry { token: b, .. }) => a == b,
        (
            OperationSpec::Exit { position_id: a, .. },
            OperationSpec::Exit { position_id: b, .. },
        ) => a == b,
        (OperationSpec::Command { label: a, .. }, OperationSpec::Command { label: b, .. }) => {
            a == b
        }
        _ => false,
    }
}

fn operation_has_ambiguous(operation: &Operation) -> bool {
    operation.txs.iter().any(|tx| {
        matches!(
            tx.state,
            TxState::Prepared | TxState::Submitted | TxState::Unresolved
        )
    })
}

fn resolved_receipts_changed(previous: &Operation, current: &Operation) -> bool {
    previous
        .txs
        .iter()
        .filter(|tx| matches!(tx.state, TxState::Confirmed | TxState::Reverted))
        .any(|before| {
            current
                .txs
                .iter()
                .find(|after| after.hash.eq_ignore_ascii_case(&before.hash))
                .is_none_or(|after| {
                    after.state != before.state
                        || after.block_number != before.block_number
                        || after.block_hash.is_none()
                        || before.block_hash.as_ref().is_some_and(|hash| {
                            after
                                .block_hash
                                .as_ref()
                                .is_none_or(|current| !current.eq_ignore_ascii_case(hash))
                        })
                })
        })
}

fn operation_gas(operation: &Operation) -> anyhow::Result<U256> {
    operation
        .txs
        .iter()
        .filter_map(|tx| tx.gas_wei.as_deref())
        .try_fold(U256::ZERO, |total, gas| {
            Ok(total.saturating_add(gas.parse::<U256>()?))
        })
}

fn tx_has_effect(tx: &TxRecord) -> bool {
    !matches!(tx.step.as_str(), "token_approval" | "permit2_approval")
}

fn operation_requires_recovery(operation: &Operation) -> bool {
    operation.state == OperationState::Pending
        && (operation_has_ambiguous(operation)
            || operation
                .txs
                .iter()
                .any(|tx| tx.state == TxState::Confirmed && tx_has_effect(tx)))
}

fn parse_hash(value: &str) -> anyhow::Result<B256> {
    value
        .parse()
        .map_err(|_| anyhow::anyhow!("malformed transaction hash in journal: {value}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::abi::curve;
    use alloy::sol_types::SolEvent;
    use axum::extract::State;
    use axum::routing::post;
    use axum::{Json, Router};
    use serde_json::{Value, json};
    use std::collections::HashMap;
    use std::sync::Arc;

    fn dir(_: &str) -> tempfile::TempDir {
        tempfile::tempdir().unwrap()
    }

    fn entry() -> OperationSpec {
        OperationSpec::Entry {
            token: Address::from([1u8; 20]),
            curve: Address::from([2u8; 20]),
            symbol: "X".into(),
            name: "x".into(),
            value: "10".into(),
        }
    }

    #[derive(Clone)]
    struct MockRpc {
        receipts: Arc<HashMap<String, Value>>,
        nonce: u64,
        code: Option<String>,
    }

    async fn rpc_handler(State(state): State<MockRpc>, Json(request): Json<Value>) -> Json<Value> {
        let method = request
            .get("method")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let result = match method {
            "eth_blockNumber" => json!("0x7"),
            "eth_getTransactionCount" => json!(format!("0x{:x}", state.nonce)),
            "eth_getTransactionReceipt" => request
                .get("params")
                .and_then(|params| params.get(0))
                .and_then(Value::as_str)
                .and_then(|hash| state.receipts.get(hash))
                .cloned()
                .unwrap_or(Value::Null),
            "eth_getCode" => json!(state.code.as_deref().unwrap_or("0x")),
            "eth_getBlockByNumber" => json!({
                "number": request["params"][0],
                "hash": format!("{:#x}", B256::from([9u8; 32])),
                "timestamp": "0xa"
            }),
            _ => Value::Null,
        };
        Json(
            json!({"jsonrpc":"2.0","id":request.get("id").cloned().unwrap_or(json!(1)),"result":result}),
        )
    }

    async fn mock_rpc(
        receipts: HashMap<String, Value>,
        nonce: u64,
    ) -> (Arc<Rpc>, tokio::task::JoinHandle<()>) {
        mock_rpc_with_code(receipts, nonce, None).await
    }

    async fn mock_rpc_with_code(
        receipts: HashMap<String, Value>,
        nonce: u64,
        code: Option<String>,
    ) -> (Arc<Rpc>, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let state = MockRpc {
            receipts: Arc::new(receipts),
            nonce,
            code,
        };
        let task = tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new()
                    .route("/", post(rpc_handler))
                    .with_state(state),
            )
            .await
            .unwrap();
        });
        let config = crate::Config {
            rpc_http: vec![crate::config::EndpointCfg {
                url: format!("http://{address}"),
                logs: true,
                label: "mock".into(),
            }],
            rpc_ws: Vec::new(),
            sequencer_url: format!("http://{address}"),
            helper: None,
            poll_ms: 1,
            rpc_in_flight: 3,
            rpc_spacing_ms: 0,
            rpc_logs_spacing_ms: 0,
            board_port: 0,
        };
        (Arc::new(Rpc::new(&config).unwrap()), task)
    }

    fn event_log(address: Address, data: alloy::primitives::LogData, hash: B256) -> Value {
        serde_json::to_value(alloy::rpc::types::Log {
            inner: alloy::primitives::Log { address, data },
            block_hash: Some(B256::from([9u8; 32])),
            block_number: Some(7),
            block_timestamp: Some(10),
            transaction_hash: Some(hash),
            transaction_index: Some(0),
            log_index: Some(0),
            removed: false,
        })
        .unwrap()
    }

    fn receipt(hash: B256, logs: Vec<Value>) -> Value {
        json!({
            "status":"0x1","blockNumber":"0x7","blockHash":format!("{:#x}", B256::from([9u8; 32])),
            "gasUsed":"0x5208","effectiveGasPrice":"0x1",
            "transactionHash":format!("{hash:#x}"),"logs":logs
        })
    }

    #[test]
    fn write_ahead_state_round_trips() {
        let dir = dir("round-trip");
        let journal = TxJournal::open(&dir).unwrap();
        let id = journal.begin(Address::from([3u8; 20]), entry()).unwrap();
        let hash = B256::from([4u8; 32]);
        journal
            .record(
                &id,
                JournalEvent::Stage {
                    step: "entry".into(),
                    nonce: 7,
                    hash,
                },
            )
            .unwrap();
        assert!(journal.requires_recovery(&id));
        journal
            .record(
                &id,
                JournalEvent::Reverted {
                    hash,
                    block_number: 1,
                    block_hash: B256::from([9u8; 32]),
                    gas_wei: U256::from(21_000),
                },
            )
            .unwrap();
        assert!(!journal.requires_recovery(&id));
        journal.record(&id, JournalEvent::Failed).unwrap();
        drop(journal);
        let reopened = TxJournal::open(&dir).unwrap();
        assert!(!reopened.requires_recovery(&id));
        drop(reopened);
    }

    #[test]
    fn pending_subject_cannot_be_started_twice() {
        let dir = dir("duplicate");
        let journal = TxJournal::open(&dir).unwrap();
        let wallet = Address::from([3u8; 20]);
        journal.begin(wallet, entry()).unwrap();
        assert!(journal.begin(wallet, entry()).is_err());
        drop(journal);
    }

    #[test]
    fn confirmed_approval_is_retryable_but_confirmed_sale_requires_recovery() {
        let dir = dir("effects");
        let journal = TxJournal::open(&dir).unwrap();
        let wallet = Address::from([3u8; 20]);
        let id = journal
            .begin(
                wallet,
                OperationSpec::Exit {
                    position_id: "p".into(),
                    token: Address::from([1u8; 20]),
                    curve: Address::from([2u8; 20]),
                    reason: "manual".into(),
                },
            )
            .unwrap();
        let approval = B256::from([4u8; 32]);
        journal
            .record(
                &id,
                JournalEvent::Stage {
                    step: "token_approval".into(),
                    nonce: 1,
                    hash: approval,
                },
            )
            .unwrap();
        journal
            .record(
                &id,
                JournalEvent::Confirmed {
                    hash: approval,
                    block_number: 2,
                    block_hash: B256::from([9u8; 32]),
                    gas_wei: U256::from(21_000),
                },
            )
            .unwrap();
        assert!(!journal.requires_recovery(&id));
        let sale = B256::from([5u8; 32]);
        journal
            .record(
                &id,
                JournalEvent::Stage {
                    step: "pool_sell".into(),
                    nonce: 2,
                    hash: sale,
                },
            )
            .unwrap();
        journal
            .record(
                &id,
                JournalEvent::Confirmed {
                    hash: sale,
                    block_number: 3,
                    block_hash: B256::from([9u8; 32]),
                    gas_wei: U256::from(21_000),
                },
            )
            .unwrap();
        assert!(journal.requires_recovery(&id));
        drop(journal);
    }

    #[tokio::test]
    async fn confirmed_command_is_completed_during_recovery() {
        let dir = dir("command");
        let journal = TxJournal::open(&dir).unwrap();
        let wallet = Address::from([3u8; 20]);
        let id = journal
            .begin(
                wallet,
                OperationSpec::Command {
                    label: "claim".into(),
                    effect: CommandEffect::Receipt,
                },
            )
            .unwrap();
        let hash = B256::from([8u8; 32]);
        journal
            .record(
                &id,
                JournalEvent::Stage {
                    step: "transaction".into(),
                    nonce: 0,
                    hash,
                },
            )
            .unwrap();
        journal
            .record(
                &id,
                JournalEvent::Confirmed {
                    hash,
                    block_number: 1,
                    block_hash: B256::from([9u8; 32]),
                    gas_wei: U256::from(21_000),
                },
            )
            .unwrap();
        let operation = journal.operation(&id).unwrap();
        let mut report = RecoveryReport::default();
        let (rpc, server) = mock_rpc(HashMap::new(), 1).await;
        journal
            .recover_command(&rpc, &operation, &mut report)
            .await
            .unwrap();
        assert!(!journal.requires_recovery(&id));
        server.abort();
        drop(journal);
    }

    #[tokio::test]
    async fn claim_recovery_requires_the_claimed_event() {
        let dir = dir("claim-event");
        let journal = TxJournal::open(&dir).unwrap();
        let wallet = Address::from([3u8; 20]);
        let hash = B256::from([7u8; 32]);
        let id = journal
            .begin(
                wallet,
                OperationSpec::Command {
                    label: "claim".into(),
                    effect: CommandEffect::RequiredLog {
                        address: crate::chain::ADDR.pons_escrow,
                        topic: crate::abi::escrow::Claimed::SIGNATURE_HASH,
                    },
                },
            )
            .unwrap();
        journal
            .record(
                &id,
                JournalEvent::Stage {
                    step: "transaction".into(),
                    nonce: 0,
                    hash,
                },
            )
            .unwrap();
        journal
            .record(
                &id,
                JournalEvent::Confirmed {
                    hash,
                    block_number: 7,
                    block_hash: B256::from([9u8; 32]),
                    gas_wei: U256::from(21_000),
                },
            )
            .unwrap();
        let claimed = crate::abi::escrow::Claimed {
            recipient: wallet,
            amount: U256::from(5),
        };
        let mut receipts = HashMap::new();
        receipts.insert(
            format!("{hash:#x}"),
            receipt(
                hash,
                vec![event_log(
                    crate::chain::ADDR.pons_escrow,
                    claimed.encode_log_data(),
                    hash,
                )],
            ),
        );
        let (rpc, server) = mock_rpc(receipts, 1).await;
        let operation = journal.operation(&id).unwrap();
        let mut report = RecoveryReport::default();
        journal
            .recover_command(&rpc, &operation, &mut report)
            .await
            .unwrap();
        assert!(report.blocked.is_empty());
        assert_eq!(
            journal.operation(&id).unwrap().state,
            OperationState::Applied
        );
        server.abort();
        drop(journal);
    }

    #[tokio::test]
    async fn helper_deploy_recovery_requires_exact_runtime_code() {
        let dir = dir("helper-runtime");
        let journal = TxJournal::open(&dir).unwrap();
        let wallet = Address::from([3u8; 20]);
        let address = Address::from([4u8; 20]);
        let runtime = [1u8, 2, 3];
        let id = journal
            .begin(
                wallet,
                OperationSpec::Command {
                    label: "helper_deploy".into(),
                    effect: CommandEffect::RuntimeCode {
                        address,
                        code_hash: alloy::primitives::keccak256(runtime),
                    },
                },
            )
            .unwrap();
        let hash = B256::from([8u8; 32]);
        journal
            .record(
                &id,
                JournalEvent::Stage {
                    step: "deploy".into(),
                    nonce: 0,
                    hash,
                },
            )
            .unwrap();
        journal
            .record(
                &id,
                JournalEvent::Confirmed {
                    hash,
                    block_number: 1,
                    block_hash: B256::from([9u8; 32]),
                    gas_wei: U256::from(21_000),
                },
            )
            .unwrap();
        let operation = journal.operation(&id).unwrap();
        let (rpc, server) = mock_rpc_with_code(
            HashMap::new(),
            1,
            Some(format!("0x{}", hex::encode(runtime))),
        )
        .await;
        let mut report = RecoveryReport::default();
        journal
            .recover_command(&rpc, &operation, &mut report)
            .await
            .unwrap();
        assert!(report.blocked.is_empty());
        assert_eq!(
            journal.operation(&id).unwrap().state,
            OperationState::Applied
        );
        assert!(!journal.requires_recovery(&id));
        server.abort();
        drop(journal);
    }

    #[tokio::test]
    async fn restart_recovers_a_confirmed_entry_once() {
        let dir = dir("recover-entry");
        let state = StateDb::open(&dir).unwrap();
        let journal = TxJournal::from_state(state.clone()).unwrap();
        let positions = PositionStore::from_state(state).unwrap();
        let wallet = Address::from([3u8; 20]);
        let token = Address::from([1u8; 20]);
        let curve_address = Address::from([2u8; 20]);
        let hash = B256::from([4u8; 32]);
        let id = journal
            .begin(
                wallet,
                OperationSpec::Entry {
                    token,
                    curve: curve_address,
                    symbol: "X".into(),
                    name: "x".into(),
                    value: "1000".into(),
                },
            )
            .unwrap();
        journal
            .record(
                &id,
                JournalEvent::Stage {
                    step: "entry".into(),
                    nonce: 0,
                    hash,
                },
            )
            .unwrap();
        journal
            .record(&id, JournalEvent::Submitted { hash })
            .unwrap();
        let buy = curve::CurveBuy {
            buyer: wallet,
            recipient: wallet,
            quoteIn: U256::from(1000),
            tokensOut: U256::from(100),
            fee: U256::ZERO,
            tax: U256::ZERO,
        };
        let mut receipts = HashMap::new();
        receipts.insert(
            format!("{hash:#x}"),
            receipt(
                hash,
                vec![event_log(curve_address, buy.encode_log_data(), hash)],
            ),
        );
        let (rpc, server) = mock_rpc(receipts, 1).await;
        let recovered = journal.recover(&rpc, &positions, wallet).await.unwrap();
        assert_eq!(recovered.recovered_entries, 1);
        assert!(recovered.blocked.is_empty());
        let stored = positions.load();
        assert_eq!(stored.len(), 1);
        assert_eq!(stored[0].held(), U256::from(100));
        assert_eq!(stored[0].entry_eth, "1000");
        assert_eq!(stored[0].entry_gas_wei.as_deref(), Some("21000"));
        assert!(!journal.requires_recovery(&id));
        let again = journal.recover(&rpc, &positions, wallet).await.unwrap();
        assert_eq!(again.recovered_entries, 0);
        assert_eq!(positions.load().len(), 1);
        server.abort();
        drop(journal);
        drop(positions);
    }

    #[tokio::test]
    async fn restart_applies_a_confirmed_curve_exit_once() {
        let dir = dir("recover-exit");
        let state = StateDb::open(&dir).unwrap();
        let journal = TxJournal::from_state(state.clone()).unwrap();
        let positions = PositionStore::from_state(state).unwrap();
        let wallet = Address::from([3u8; 20]);
        let token = Address::from([1u8; 20]);
        let curve_address = Address::from([2u8; 20]);
        let position = positions.open_position(
            token,
            curve_address,
            "X".into(),
            "x".into(),
            1,
            Some(B256::from([1u8; 32])),
            false,
            U256::from(1000),
            U256::ZERO,
            U256::from(100),
            CHAIN_ID,
            Some(wallet),
        );
        positions.flush().unwrap();
        let hash = B256::from([5u8; 32]);
        let id = journal
            .begin(
                wallet,
                OperationSpec::Exit {
                    position_id: position.id.clone(),
                    token,
                    curve: curve_address,
                    reason: "manual".into(),
                },
            )
            .unwrap();
        journal
            .record(
                &id,
                JournalEvent::Stage {
                    step: "curve_sell".into(),
                    nonce: 1,
                    hash,
                },
            )
            .unwrap();
        journal
            .record(&id, JournalEvent::Submitted { hash })
            .unwrap();
        let sell = curve::CurveSell {
            seller: wallet,
            recipient: wallet,
            tokensIn: U256::from(100),
            quoteOut: U256::from(1500),
            fee: U256::ZERO,
            tax: U256::ZERO,
        };
        let mut receipts = HashMap::new();
        receipts.insert(
            format!("{hash:#x}"),
            receipt(
                hash,
                vec![event_log(curve_address, sell.encode_log_data(), hash)],
            ),
        );
        let (rpc, server) = mock_rpc(receipts, 2).await;
        let recovered = journal.recover(&rpc, &positions, wallet).await.unwrap();
        assert_eq!(recovered.recovered_exits, 1);
        assert!(recovered.blocked.is_empty());
        let stored = positions.load();
        assert_eq!(stored[0].status, "closed");
        assert_eq!(stored[0].realized_pnl_wei().as_deref(), Some("500"));
        assert_eq!(stored[0].net_realized_pnl_wei().as_deref(), Some("-20500"));
        let again = journal.recover(&rpc, &positions, wallet).await.unwrap();
        assert_eq!(again.recovered_exits, 0);
        assert_eq!(positions.load()[0].exits.len(), 1);
        server.abort();
        drop(journal);
        drop(positions);
    }

    #[tokio::test]
    async fn restart_drops_an_old_never_submitted_prepared_transaction() {
        let dir = dir("recover-dropped");
        let state = StateDb::open(&dir).unwrap();
        let journal = TxJournal::from_state(state.clone()).unwrap();
        let positions = PositionStore::from_state(state).unwrap();
        let wallet = Address::from([3u8; 20]);
        let hash = B256::from([6u8; 32]);
        let id = journal.begin(wallet, entry()).unwrap();
        journal
            .record(
                &id,
                JournalEvent::Stage {
                    step: "entry".into(),
                    nonce: 0,
                    hash,
                },
            )
            .unwrap();
        journal
            .operations
            .lock()
            .unwrap()
            .iter_mut()
            .find(|operation| operation.id == id)
            .unwrap()
            .created_at = 0;
        let (rpc, server) = mock_rpc(HashMap::new(), 0).await;
        let recovered = journal.recover(&rpc, &positions, wallet).await.unwrap();
        assert_eq!(recovered.failed, 1);
        assert!(recovered.blocked.is_empty());
        assert!(!journal.requires_recovery(&id));
        server.abort();
        drop(journal);
        drop(positions);
    }

    #[tokio::test]
    async fn restart_keeps_a_consumed_nonce_without_a_receipt_blocked() {
        let dir = dir("recover-blocked");
        let state = StateDb::open(&dir).unwrap();
        let journal = TxJournal::from_state(state.clone()).unwrap();
        let positions = PositionStore::from_state(state).unwrap();
        let wallet = Address::from([3u8; 20]);
        let hash = B256::from([7u8; 32]);
        let id = journal.begin(wallet, entry()).unwrap();
        journal
            .record(
                &id,
                JournalEvent::Stage {
                    step: "entry".into(),
                    nonce: 0,
                    hash,
                },
            )
            .unwrap();
        journal
            .record(&id, JournalEvent::Submitted { hash })
            .unwrap();
        journal
            .operations
            .lock()
            .unwrap()
            .iter_mut()
            .find(|operation| operation.id == id)
            .unwrap()
            .created_at = 0;
        let (rpc, server) = mock_rpc(HashMap::new(), 1).await;
        let recovered = journal.recover(&rpc, &positions, wallet).await.unwrap();
        assert_eq!(recovered.blocked.len(), 1);
        assert!(journal.requires_recovery(&id));
        server.abort();
        drop(journal);
        drop(positions);
    }

    #[tokio::test]
    async fn restart_downgrades_a_disappeared_confirmed_receipt_to_blocked() {
        let dir = dir("recover-reorg");
        let state = StateDb::open(&dir).unwrap();
        let journal = TxJournal::from_state(state.clone()).unwrap();
        let positions = PositionStore::from_state(state).unwrap();
        let wallet = Address::from([3u8; 20]);
        let hash = B256::from([8u8; 32]);
        let id = journal.begin(wallet, entry()).unwrap();
        journal
            .record(
                &id,
                JournalEvent::Stage {
                    step: "entry".into(),
                    nonce: 0,
                    hash,
                },
            )
            .unwrap();
        journal
            .record(
                &id,
                JournalEvent::Confirmed {
                    hash,
                    block_number: 1,
                    block_hash: B256::from([9u8; 32]),
                    gas_wei: U256::from(21_000),
                },
            )
            .unwrap();
        journal
            .operations
            .lock()
            .unwrap()
            .iter_mut()
            .find(|operation| operation.id == id)
            .unwrap()
            .created_at = 0;
        let (rpc, server) = mock_rpc(HashMap::new(), 1).await;
        let recovered = journal.recover(&rpc, &positions, wallet).await.unwrap();
        assert_eq!(recovered.blocked.len(), 1);
        assert!(journal.requires_recovery(&id));
        server.abort();
        drop(journal);
        drop(positions);
    }

    #[tokio::test]
    async fn applied_operation_blocks_when_its_receipt_leaves_the_canonical_chain() {
        let dir = dir("applied-reorg");
        let state = StateDb::open(&dir).unwrap();
        let journal = TxJournal::from_state(state.clone()).unwrap();
        let positions = PositionStore::from_state(state).unwrap();
        let wallet = Address::from([3u8; 20]);
        let hash = B256::from([8u8; 32]);
        let id = journal.begin(wallet, entry()).unwrap();
        journal
            .record(
                &id,
                JournalEvent::Stage {
                    step: "entry".into(),
                    nonce: 0,
                    hash,
                },
            )
            .unwrap();
        journal
            .record(
                &id,
                JournalEvent::Confirmed {
                    hash,
                    block_number: 1,
                    block_hash: B256::from([9u8; 32]),
                    gas_wei: U256::from(21_000),
                },
            )
            .unwrap();
        journal.record(&id, JournalEvent::Applied).unwrap();
        let (rpc, server) = mock_rpc(HashMap::new(), 1).await;
        assert_eq!(
            journal.canonical_issues(&rpc, wallet).await.unwrap().len(),
            1
        );
        let recovered = journal.recover(&rpc, &positions, wallet).await.unwrap();
        assert_eq!(recovered.blocked.len(), 1);
        server.abort();
        drop(journal);
        drop(positions);
    }

    #[test]
    fn journal_write_failure_prevents_an_operation_from_starting() {
        let file = tempfile::NamedTempFile::new().unwrap();
        assert!(TxJournal::open(file.path()).is_err());
    }

    #[test]
    fn legacy_json_is_imported_once_then_redb_wins() {
        let dir = dir("legacy-import");
        let operation = Operation {
            id: "legacy".into(),
            chain_id: CHAIN_ID,
            wallet: format!("{:#x}", Address::from([3u8; 20])),
            created_at: 1,
            updated_at: 1,
            state: OperationState::Pending,
            spec: entry(),
            txs: Vec::new(),
        };
        std::fs::write(
            dir.path().join("transactions.json"),
            serde_json::to_vec(&vec![operation]).unwrap(),
        )
        .unwrap();
        let journal = TxJournal::open(&dir).unwrap();
        assert_eq!(journal.operations.lock().unwrap().len(), 1);
        drop(journal);
        std::fs::write(dir.path().join("transactions.json"), "[]").unwrap();
        let reopened = TxJournal::open(&dir).unwrap();
        assert_eq!(reopened.operations.lock().unwrap().len(), 1);
        drop(reopened);
    }

    #[test]
    fn empty_journal_refuses_without_overwriting() {
        let dir = dir("empty");
        std::fs::write(dir.path().join("transactions.json"), " \n").unwrap();
        assert!(TxJournal::open(&dir).is_err());
        assert_eq!(
            std::fs::read_to_string(dir.path().join("transactions.json")).unwrap(),
            " \n"
        );
    }
}
