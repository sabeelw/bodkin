use super::journal::{CommandEffect, JournalEvent, OperationSpec, TxJournal};
use super::submitter::{SendOutcome, Submitter};
use super::wallet::Wallet;
use crate::pons::clock::ChainClock;
use crate::rpc::{Lane, Rpc};
use alloy::primitives::{Address, B256, U256};
use alloy::rpc::types::Log;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// How a submitted transaction actually resolved on-chain. `Submitted` is a
/// pre-receipt state — a hash means the sequencer accepted the bytes, not that
/// the call succeeded.
#[derive(Debug)]
pub enum TxFinal {
    /// Receipt exists and `status == 0x1`.
    Confirmed {
        hash: B256,
        block_number: u64,
        block_hash: B256,
        contract_address: Option<Address>,
        gas_used: U256,
        effective_gas_price: U256,
        logs: Vec<Log>,
    },
    /// Receipt exists and `status == 0x0`.
    Reverted {
        hash: B256,
        block_number: u64,
        block_hash: B256,
        gas_used: U256,
        effective_gas_price: U256,
    },
    /// Sent, but no receipt within the window. The tx may still land — callers
    /// must keep any reserved budget/slot, not retry blindly.
    Unresolved { hash: B256 },
}

impl TxFinal {
    pub fn gas_cost(&self) -> U256 {
        match self {
            Self::Confirmed {
                gas_used,
                effective_gas_price,
                ..
            }
            | Self::Reverted {
                gas_used,
                effective_gas_price,
                ..
            } => gas_used.saturating_mul(*effective_gas_price),
            Self::Unresolved { .. } => U256::ZERO,
        }
    }
}

/// Wallet + sequencer + clock bundled for the non-burst execution paths
/// (manual buy/sell, approve, claim, pool swap, engine exits). Every send goes
/// through `send_and_wait`, which returns only once a receipt says what
/// happened — quote-only fills are impossible by construction here.
pub struct LiveExec<'a> {
    pub rpc: &'a Rpc,
    pub sub: &'a Submitter,
    pub wallet: &'a Wallet,
    pub clock: &'a ChainClock,
    journal: Option<Arc<TxJournal>>,
    operation_id: Option<String>,
}

const CONFIRM_MS: u64 = 12_000;
const CONFIRM_POLL_MS: u64 = 150;

/// Owned wallet+submitter+clock bundle for CLI commands. `LiveExec` borrows,
/// so one-off commands hold this and borrow an exec from it per send.
pub struct ExecKit {
    pub wallet: Wallet,
    pub sub: Submitter,
    pub clock: ChainClock,
    journal: Arc<TxJournal>,
}

impl ExecKit {
    /// Require key + sequencer, pin the sequencer IP, seed the clock from the
    /// latest header, and seed the nonce allocator once.
    pub async fn arm(rpc: &Rpc, cfg: &crate::Config) -> anyhow::Result<Self> {
        let wallet = Wallet::require()?;
        let state = crate::trade::state::StateDb::open("data")?;
        let positions = crate::trade::positions::PositionStore::from_state(state.clone())?;
        let journal = Arc::new(TxJournal::from_state(state)?);
        let recovered = journal.recover(rpc, &positions, wallet.address()).await?;
        anyhow::ensure!(
            recovered.blocked.is_empty(),
            "unresolved transaction recovery blocked this command: {}",
            recovered.blocked.join("; ")
        );
        let sub = Submitter::new(cfg)?;
        let ips = sub.resolve_and_pin().await?;
        anyhow::ensure!(!ips.is_empty(), "sequencer DNS returned no addresses");
        let clock = ChainClock::default();
        let h = rpc.latest_header(Lane::Hot).await?;
        clock.note_header(
            h.timestamp,
            crate::pons::clock::now_ms(),
            h.base_fee,
            h.number,
        );
        let n = rpc
            .get_transaction_count(Lane::Hot, wallet.address(), true)
            .await?;
        wallet.set_nonce(n);
        Ok(Self {
            wallet,
            sub,
            clock,
            journal,
        })
    }

    pub fn begin_command(&self, label: impl Into<String>) -> anyhow::Result<String> {
        self.journal.begin(
            self.wallet.address(),
            OperationSpec::Command {
                label: label.into(),
                effect: CommandEffect::Receipt,
            },
        )
    }

    pub fn begin_event_command(
        &self,
        label: impl Into<String>,
        address: Address,
        topic: B256,
    ) -> anyhow::Result<String> {
        self.journal.begin(
            self.wallet.address(),
            OperationSpec::Command {
                label: label.into(),
                effect: CommandEffect::RequiredLog { address, topic },
            },
        )
    }

    pub fn begin_runtime_deploy(
        &self,
        label: impl Into<String>,
        address: Address,
        runtime_code: &[u8],
    ) -> anyhow::Result<String> {
        self.journal.begin(
            self.wallet.address(),
            OperationSpec::Command {
                label: label.into(),
                effect: CommandEffect::RuntimeCode {
                    address,
                    code_hash: alloy::primitives::keccak256(runtime_code),
                },
            },
        )
    }

    pub fn exec<'a>(&'a self, rpc: &'a Rpc) -> LiveExec<'a> {
        LiveExec::new(rpc, &self.sub, &self.wallet, &self.clock)
    }

    pub fn exec_for<'a>(&'a self, rpc: &'a Rpc, operation_id: &str) -> LiveExec<'a> {
        self.exec(rpc)
            .with_operation(self.journal.clone(), operation_id.to_string())
    }

    pub fn complete(&self, operation_id: &str) -> anyhow::Result<()> {
        self.journal.record(operation_id, JournalEvent::Applied)
    }

    pub fn fail_if_resolved(&self, operation_id: &str) -> anyhow::Result<bool> {
        if self.journal.requires_recovery(operation_id) {
            Ok(false)
        } else {
            self.journal.record(operation_id, JournalEvent::Failed)?;
            Ok(true)
        }
    }
}

impl<'a> LiveExec<'a> {
    pub fn new(
        rpc: &'a Rpc,
        sub: &'a Submitter,
        wallet: &'a Wallet,
        clock: &'a ChainClock,
    ) -> Self {
        Self {
            rpc,
            sub,
            wallet,
            clock,
            journal: None,
            operation_id: None,
        }
    }

    pub fn with_operation(mut self, journal: Arc<TxJournal>, operation_id: String) -> Self {
        self.journal = Some(journal);
        self.operation_id = Some(operation_id);
        self
    }

    fn record(&self, event: JournalEvent) -> anyhow::Result<()> {
        match (&self.journal, &self.operation_id) {
            (Some(journal), Some(operation_id)) => journal.record(operation_id, event),
            _ => Ok(()),
        }
    }

    /// Sign → send → wait for receipt. Nonce comes from the wallet's shared
    /// allocator (never re-read per send inside a running engine).
    pub async fn send_and_wait(
        &self,
        to: Address,
        value: U256,
        data: alloy::primitives::Bytes,
        gas: Option<u64>,
    ) -> anyhow::Result<TxFinal> {
        self.send_and_wait_tagged("transaction", to, value, data, gas)
            .await
    }

    pub async fn send_and_wait_tagged(
        &self,
        step: &str,
        to: Address,
        value: U256,
        data: alloy::primitives::Bytes,
        gas: Option<u64>,
    ) -> anyhow::Result<TxFinal> {
        let nonce = self.wallet.next_nonce();
        let signed = match gas {
            Some(g) => self
                .wallet
                .sign_eip1559_gas(to, value, data, nonce, self.clock, g),
            None => self.wallet.sign_eip1559(to, value, data, nonce, self.clock),
        };
        let (hash, raw) = match signed {
            Ok(signed) => signed,
            Err(e) => {
                anyhow::ensure!(
                    self.wallet.rewind_nonces(nonce, 1),
                    "signing failed and a later nonce was already reserved; restart before sending again: {e}"
                );
                return Err(e);
            }
        };
        self.submit_signed(step, nonce, hash, raw).await
    }

    pub async fn deploy_and_wait(
        &self,
        bytecode: alloy::primitives::Bytes,
    ) -> anyhow::Result<TxFinal> {
        let nonce = self.wallet.next_nonce();
        let (hash, raw) = match self.wallet.sign_create(bytecode, nonce, self.clock) {
            Ok(signed) => signed,
            Err(e) => {
                anyhow::ensure!(
                    self.wallet.rewind_nonces(nonce, 1),
                    "deployment signing failed and a later nonce was already reserved; restart before sending again: {e}"
                );
                return Err(e);
            }
        };
        self.submit_signed("deploy", nonce, hash, raw).await
    }

    async fn submit_signed(
        &self,
        step: &str,
        nonce: u64,
        hash: B256,
        raw: alloy::primitives::Bytes,
    ) -> anyhow::Result<TxFinal> {
        if let Err(e) = self.record(JournalEvent::Stage {
            step: step.into(),
            nonce,
            hash,
        }) {
            anyhow::ensure!(
                self.wallet.rewind_nonces(nonce, 1),
                "journal staging failed and a later nonce was already reserved; restart before sending again: {e}"
            );
            return Err(e);
        }
        match self.sub.send_raw(&raw).await {
            SendOutcome::Hash(returned) if returned != hash => {
                self.record(JournalEvent::Unresolved { hash })?;
                Ok(TxFinal::Unresolved { hash })
            }
            SendOutcome::Hash(_) | SendOutcome::Known => {
                self.record(JournalEvent::Submitted { hash })?;
                let final_state = confirm(self.rpc, hash, CONFIRM_MS).await?;
                match &final_state {
                    TxFinal::Confirmed {
                        block_number,
                        block_hash,
                        ..
                    } => self.record(JournalEvent::Confirmed {
                        hash,
                        block_number: *block_number,
                        block_hash: *block_hash,
                        gas_wei: final_state.gas_cost(),
                    })?,
                    TxFinal::Reverted {
                        block_number,
                        block_hash,
                        ..
                    } => self.record(JournalEvent::Reverted {
                        hash,
                        block_number: *block_number,
                        block_hash: *block_hash,
                        gas_wei: final_state.gas_cost(),
                    })?,
                    TxFinal::Unresolved { .. } => self.record(JournalEvent::Unresolved { hash })?,
                }
                Ok(final_state)
            }
            SendOutcome::Revert { message } => {
                let rewound = self.wallet.rewind_nonces(nonce, 1);
                self.record(if rewound {
                    JournalEvent::Rejected { hash }
                } else {
                    JournalEvent::Unresolved { hash }
                })?;
                anyhow::ensure!(
                    rewound,
                    "send reverted before inclusion but a later nonce was already reserved; restart before sending again: {message}"
                );
                anyhow::bail!("send reverted at sequencer: {message}")
            }
            SendOutcome::Reject { code, message } => {
                let rewound = self.wallet.rewind_nonces(nonce, 1);
                self.record(if rewound {
                    JournalEvent::Rejected { hash }
                } else {
                    JournalEvent::Unresolved { hash }
                })?;
                anyhow::ensure!(
                    rewound,
                    "send was rejected but a later nonce was already reserved; restart before sending again: {message}"
                );
                anyhow::bail!("send rejected ({code}): {message}")
            }
            SendOutcome::Error(_) => {
                self.record(JournalEvent::Unresolved { hash })?;
                Ok(TxFinal::Unresolved { hash })
            }
        }
    }
}

/// Poll `eth_getTransactionReceipt` until the tx has a status or the window
/// closes. A missing receipt is retried — it is "pending", not failure.
pub async fn confirm(rpc: &Rpc, hash: B256, timeout_ms: u64) -> anyhow::Result<TxFinal> {
    let t0 = Instant::now();
    loop {
        if let Ok(receipt) = rpc.get_transaction_receipt(Lane::Hot, hash).await
            && receipt.tx_hash == Some(hash)
            && let (Some(block_number), Some(block_hash)) =
                (receipt.block_number, receipt.block_hash)
            && rpc.block_hash(Lane::Hot, block_number).await.ok() == Some(block_hash)
        {
            let gas_used = receipt.gas_used.expect("strict mined receipt has gasUsed");
            let effective_gas_price = receipt
                .effective_gas_price
                .expect("strict mined receipt has effectiveGasPrice");
            match receipt.status {
                Some(true) => {
                    return Ok(TxFinal::Confirmed {
                        hash,
                        block_number,
                        block_hash,
                        contract_address: receipt.contract_address,
                        gas_used,
                        effective_gas_price,
                        logs: receipt.logs,
                    });
                }
                Some(false) => {
                    return Ok(TxFinal::Reverted {
                        hash,
                        block_number,
                        block_hash,
                        gas_used,
                        effective_gas_price,
                    });
                }
                None => {}
            }
        }
        if t0.elapsed() > Duration::from_millis(timeout_ms) {
            return Ok(TxFinal::Unresolved { hash });
        }
        tokio::time::sleep(Duration::from_millis(CONFIRM_POLL_MS)).await;
    }
}
