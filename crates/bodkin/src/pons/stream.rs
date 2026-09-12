use crate::abi::{curve, topics, TOPIC_SNIPE_TAX_CHARGED};
use crate::rpc::{Lane, Rpc};
use alloy::primitives::{Address, B256, U256};
use alloy::rpc::types::{Filter, Log};
use alloy::sol_types::SolEvent;
use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone, Default)]
pub struct FlowSnapshot {
    pub taxed_buyers_s1: u32,
    pub exempt_buys_s0: u32,
    pub insider_sold: bool,
    pub ready_to_graduate: bool,
    pub buys: u32,
    pub sells: u32,
    pub unique_buyers: u32,
    pub quote_in: U256,
    pub quote_out: U256,
}

#[derive(Debug, Clone)]
struct Buy {
    recipient: Address,
    #[allow(dead_code)]
    buyer: Address,
    quote_in: U256,
    #[allow(dead_code)]
    tokens_out: U256,
    ts: u64,
    taxed: bool,
}

/// In-memory curve flow from CurveBuy / CurveSell / SnipeTax topics.
pub struct FlowTracker {
    launched_at: HashMap<Address, u64>,
    insiders: HashMap<Address, HashSet<Address>>,
    buys: HashMap<Address, Vec<Buy>>,
    sells: HashMap<Address, u32>,
    taxed: HashMap<Address, HashSet<B256>>,
    ready: HashSet<Address>,
}

impl Default for FlowTracker {
    fn default() -> Self {
        Self {
            launched_at: HashMap::new(),
            insiders: HashMap::new(),
            buys: HashMap::new(),
            sells: HashMap::new(),
            taxed: HashMap::new(),
            ready: HashSet::new(),
        }
    }
}

impl FlowTracker {
    pub fn watch(&mut self, curve: Address, launched_at: u64, insiders: impl IntoIterator<Item = Address>) {
        self.launched_at.insert(curve, launched_at);
        self.insiders.insert(curve, insiders.into_iter().collect());
    }

    pub fn ingest(&mut self, log: &Log, block_ts: u64) {
        let addr = log.address();
        let topic0 = log.topics().first().copied().unwrap_or_default();
        if topic0 == topics::curve_buy() {
            if let Ok(b) = curve::CurveBuy::decode_log(&log.clone().into()) {
                let taxed = self.taxed.get(&addr).and_then(|s| log.transaction_hash.map(|h| s.contains(&h))).unwrap_or(false);
                self.buys.entry(addr).or_default().push(Buy {
                    recipient: b.recipient,
                    buyer: b.buyer,
                    quote_in: b.quoteIn,
                    tokens_out: b.tokensOut,
                    ts: block_ts,
                    taxed,
                });
            }
        } else if topic0 == topics::curve_sell() {
            if let Ok(s) = curve::CurveSell::decode_log(&log.clone().into()) {
                *self.sells.entry(addr).or_default() += 1;
                if self.insiders.get(&addr).is_some_and(|set| set.contains(&s.seller) || set.contains(&s.recipient)) {
                    // marked in snapshot
                    self.buys.entry(addr).or_default(); // ensure key
                    self.insiders.entry(addr).or_default().insert(Address::repeat_byte(0xff)); // sentinel: sold
                }
            }
        } else if topic0 == TOPIC_SNIPE_TAX_CHARGED {
            if let Some(h) = log.transaction_hash {
                self.taxed.entry(addr).or_default().insert(h);
            }
        } else if topic0 == curve::CurveCompleted::SIGNATURE_HASH {
            self.ready.insert(addr);
        }
    }

    pub fn mark_insider_sold(&mut self, curve: Address) {
        self.insiders.entry(curve).or_default().insert(Address::repeat_byte(0xff));
    }

    pub fn snapshot(&self, curve: Address) -> FlowSnapshot {
        let t0 = self.launched_at.get(&curve).copied().unwrap_or(0);
        let buys = self.buys.get(&curve).cloned().unwrap_or_default();
        let mut buyers = HashSet::new();
        let mut taxed_s1 = HashSet::new();
        let mut exempt_s0 = 0u32;
        let mut quote_in = U256::ZERO;
        for b in &buys {
            buyers.insert(b.recipient);
            quote_in += b.quote_in;
            let elapsed = b.ts.saturating_sub(t0);
            if elapsed == 1 && b.taxed {
                taxed_s1.insert(b.recipient);
            }
            if elapsed == 0 && !b.taxed {
                exempt_s0 += 1;
            }
        }
        let insider_sold = self.insiders.get(&curve).is_some_and(|s| s.contains(&Address::repeat_byte(0xff)));
        FlowSnapshot {
            taxed_buyers_s1: taxed_s1.len() as u32,
            exempt_buys_s0: exempt_s0,
            insider_sold,
            ready_to_graduate: self.ready.contains(&curve),
            buys: buys.len() as u32,
            sells: self.sells.get(&curve).copied().unwrap_or(0),
            unique_buyers: buyers.len() as u32,
            quote_in,
            quote_out: U256::ZERO,
        }
    }

    pub async fn pull_curve_logs(&mut self, rpc: &Rpc, curve: Address, from: u64, to: u64, ts_of: impl Fn(u64) -> u64) -> anyhow::Result<()> {
        let logs = rpc
            .get_logs(Lane::Hot, Filter::new().address(curve).from_block(from).to_block(to))
            .await?;
        for log in &logs {
            let ts = log.block_timestamp.unwrap_or_else(|| ts_of(log.block_number.unwrap_or(0)));
            self.ingest(log, ts);
        }
        Ok(())
    }
}
