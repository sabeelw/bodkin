use crate::abi::{TOPIC_SNIPE_TAX_CHARGED, curve, topics};
use crate::rpc::{Lane, Rpc};
use alloy::primitives::{Address, B256, U256};
use alloy::rpc::types::{Filter, Log};
use alloy::sol_types::{SolEvent, SolValue};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet};

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

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct EventKey {
    block_number: u64,
    transaction_index: u64,
    log_index: u64,
    transaction_hash: B256,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum FlowEvent {
    Buy {
        recipient: Address,
        quote_in: U256,
        tokens_out: U256,
        fee: U256,
        tax: U256,
        timestamp: u64,
        transaction_hash: B256,
    },
    Sell {
        seller: Address,
        recipient: Address,
        tokens_in: U256,
        quote_out: U256,
        fee: U256,
        tax: U256,
        timestamp: u64,
    },
    Tax {
        transaction_hash: B256,
        tax_bps: U256,
        tax_paid: U256,
        timestamp: u64,
    },
    Completed {
        timestamp: u64,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlowRecord {
    pub block_number: u64,
    pub block_hash: B256,
    pub transaction_index: u64,
    pub log_index: u64,
    pub transaction_hash: B256,
    pub event: FlowEvent,
}

#[derive(Debug, Clone)]
struct StoredEvent {
    block_hash: B256,
    event: FlowEvent,
}

#[derive(Default)]
struct CurveFlow {
    launched_at: u64,
    origin_block: u64,
    next_block: u64,
    anchor: Option<(u64, B256)>,
    insiders: HashSet<Address>,
    events: BTreeMap<EventKey, StoredEvent>,
    identities: HashMap<(B256, u64), EventKey>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FlowSync {
    pub origin_block: u64,
    pub from_block: u64,
    pub anchor: Option<(u64, B256)>,
}

/// Ordered, reversible curve-flow reducer. Effects are folded from canonical
/// event records, so duplicate/out-of-order delivery and removed logs cannot
/// increment counters twice or leave stale insider/tax state behind.
#[derive(Default)]
pub struct FlowTracker {
    curves: HashMap<Address, CurveFlow>,
}

impl FlowTracker {
    pub fn watch(
        &mut self,
        curve: Address,
        launched_at: u64,
        origin_block: u64,
        insiders: impl IntoIterator<Item = Address>,
    ) {
        let flow = self.curves.entry(curve).or_default();
        flow.launched_at = launched_at;
        flow.origin_block = origin_block;
        if flow.next_block == 0 {
            flow.next_block = origin_block;
        }
        flow.insiders.extend(
            insiders
                .into_iter()
                .filter(|address| *address != Address::ZERO),
        );
    }

    pub fn sync(&self, curve: Address) -> Option<FlowSync> {
        let flow = self.curves.get(&curve)?;
        Some(FlowSync {
            origin_block: flow.origin_block,
            from_block: flow.next_block,
            anchor: flow.anchor,
        })
    }

    pub fn rewind(&mut self, curve: Address, from_block: u64) {
        let Some(flow) = self.curves.get_mut(&curve) else {
            return;
        };
        flow.events.retain(|key, _| key.block_number < from_block);
        flow.identities
            .retain(|_, key| key.block_number < from_block);
        flow.next_block = from_block.max(flow.origin_block);
        flow.anchor = None;
    }

    pub fn apply_range(
        &mut self,
        curve: Address,
        from_block: u64,
        to_block: u64,
        to_hash: B256,
        logs: &[(Log, u64)],
    ) -> anyhow::Result<()> {
        let expected = self
            .curves
            .get(&curve)
            .ok_or_else(|| anyhow::anyhow!("curve {curve:#x} is not watched"))?
            .next_block;
        anyhow::ensure!(from_block <= to_block, "flow range is reversed");
        anyhow::ensure!(
            to_hash != B256::ZERO,
            "flow range has no canonical block hash"
        );
        anyhow::ensure!(
            from_block <= expected,
            "flow gap for {curve:#x}: expected block {expected}, received {from_block}"
        );
        for (log, timestamp) in logs {
            self.ingest(log, *timestamp)?;
        }
        let flow = self.curves.get_mut(&curve).expect("checked watched curve");
        flow.next_block = flow.next_block.max(to_block.saturating_add(1));
        flow.anchor = Some((to_block, to_hash));
        Ok(())
    }

    pub fn ingest(&mut self, log: &Log, block_ts: u64) -> anyhow::Result<()> {
        let curve = log.address();
        let flow = self
            .curves
            .get_mut(&curve)
            .ok_or_else(|| anyhow::anyhow!("curve {curve:#x} is not watched"))?;
        let transaction_hash = log
            .transaction_hash
            .ok_or_else(|| anyhow::anyhow!("curve log transaction hash missing"))?;
        let log_index = log
            .log_index
            .ok_or_else(|| anyhow::anyhow!("curve log index missing"))?;
        let identity = (transaction_hash, log_index);
        if log.removed {
            if let Some(key) = flow.identities.remove(&identity) {
                flow.events.remove(&key);
            }
            return Ok(());
        }
        let key = EventKey {
            block_number: log
                .block_number
                .ok_or_else(|| anyhow::anyhow!("curve log block number missing"))?,
            transaction_index: log
                .transaction_index
                .ok_or_else(|| anyhow::anyhow!("curve log transaction index missing"))?,
            log_index,
            transaction_hash,
        };
        let block_hash = log
            .block_hash
            .ok_or_else(|| anyhow::anyhow!("curve log block hash missing"))?;
        let topic = log.topics().first().copied().unwrap_or_default();
        let event = if topic == topics::curve_buy() {
            let buy = curve::CurveBuy::decode_log(&log.clone().into())?;
            FlowEvent::Buy {
                recipient: buy.recipient,
                quote_in: buy.quoteIn,
                tokens_out: buy.tokensOut,
                fee: buy.fee,
                tax: buy.tax,
                timestamp: block_ts,
                transaction_hash,
            }
        } else if topic == topics::curve_sell() {
            let sell = curve::CurveSell::decode_log(&log.clone().into())?;
            FlowEvent::Sell {
                seller: sell.seller,
                recipient: sell.recipient,
                tokens_in: sell.tokensIn,
                quote_out: sell.quoteOut,
                fee: sell.fee,
                tax: sell.tax,
                timestamp: block_ts,
            }
        } else if topic == TOPIC_SNIPE_TAX_CHARGED {
            let (tax_bps, tax_paid) = <(U256, U256)>::abi_decode(log.data().data.as_ref())?;
            FlowEvent::Tax {
                transaction_hash,
                tax_bps,
                tax_paid,
                timestamp: block_ts,
            }
        } else if topic == curve::CurveCompleted::SIGNATURE_HASH {
            FlowEvent::Completed {
                timestamp: block_ts,
            }
        } else {
            return Ok(());
        };
        if let Some(previous) = flow.identities.insert(identity, key) {
            flow.events.remove(&previous);
        }
        flow.events.insert(key, StoredEvent { block_hash, event });
        Ok(())
    }

    pub fn unwatch(&mut self, curve: Address) {
        self.curves.remove(&curve);
    }

    pub fn records(&self, curve: Address) -> Vec<FlowRecord> {
        self.curves
            .get(&curve)
            .into_iter()
            .flat_map(|flow| flow.events.iter())
            .map(|(key, stored)| FlowRecord {
                block_number: key.block_number,
                block_hash: stored.block_hash,
                transaction_index: key.transaction_index,
                log_index: key.log_index,
                transaction_hash: key.transaction_hash,
                event: stored.event.clone(),
            })
            .collect()
    }

    pub fn snapshot(&self, curve: Address) -> FlowSnapshot {
        let Some(flow) = self.curves.get(&curve) else {
            return FlowSnapshot::default();
        };
        let taxed = flow
            .events
            .values()
            .filter_map(|stored| match &stored.event {
                FlowEvent::Tax {
                    transaction_hash, ..
                } => Some(transaction_hash),
                _ => None,
            })
            .copied()
            .collect::<HashSet<_>>();
        let mut snapshot = FlowSnapshot::default();
        let mut buyers = HashSet::new();
        let mut taxed_s1 = HashSet::new();
        for stored in flow.events.values() {
            match &stored.event {
                FlowEvent::Buy {
                    recipient,
                    quote_in,
                    timestamp,
                    transaction_hash,
                    ..
                } => {
                    snapshot.buys = snapshot.buys.saturating_add(1);
                    snapshot.quote_in = snapshot.quote_in.saturating_add(*quote_in);
                    buyers.insert(*recipient);
                    let was_taxed = taxed.contains(transaction_hash);
                    let elapsed = timestamp.saturating_sub(flow.launched_at);
                    if elapsed == 1 && was_taxed {
                        taxed_s1.insert(*recipient);
                    }
                    if elapsed == 0 && !was_taxed {
                        snapshot.exempt_buys_s0 = snapshot.exempt_buys_s0.saturating_add(1);
                    }
                }
                FlowEvent::Sell {
                    seller,
                    recipient,
                    quote_out,
                    ..
                } => {
                    snapshot.sells = snapshot.sells.saturating_add(1);
                    snapshot.quote_out = snapshot.quote_out.saturating_add(*quote_out);
                    snapshot.insider_sold |=
                        flow.insiders.contains(seller) || flow.insiders.contains(recipient);
                }
                FlowEvent::Completed { .. } => snapshot.ready_to_graduate = true,
                FlowEvent::Tax { .. } => {}
            }
        }
        snapshot.taxed_buyers_s1 = taxed_s1.len() as u32;
        snapshot.unique_buyers = buyers.len() as u32;
        snapshot
    }
}

/// Fetch curve logs and resolve a block timestamp for each (from the log when
/// the node provides it, else one `eth_getBlockByNumber` per distinct block).
/// Returns `(log, ts)` pairs; the caller locks the tracker and ingests.
pub async fn fetch_curve_logs(
    rpc: &Rpc,
    curve: Address,
    from: u64,
    to: u64,
) -> anyhow::Result<Vec<(Log, u64)>> {
    fetch_curve_logs_on(rpc, Lane::Hot, curve, from, to).await
}

pub async fn fetch_curve_logs_on(
    rpc: &Rpc,
    lane: Lane,
    curve: Address,
    from: u64,
    to: u64,
) -> anyhow::Result<Vec<(Log, u64)>> {
    let logs = rpc
        .get_logs(
            lane,
            Filter::new().address(curve).from_block(from).to_block(to),
        )
        .await?;
    let mut ts_cache: HashMap<u64, u64> = HashMap::new();
    for log in &logs {
        if log.block_timestamp.is_none() {
            let block = log.block_number.ok_or_else(|| {
                anyhow::anyhow!("curve log is missing both block timestamp and block number")
            })?;
            if let std::collections::hash_map::Entry::Vacant(entry) = ts_cache.entry(block) {
                entry.insert(rpc.block_timestamp(lane, block).await?);
            }
        }
    }
    logs.into_iter()
        .map(|log| {
            let timestamp = log
                .block_timestamp
                .or_else(|| {
                    log.block_number
                        .and_then(|block| ts_cache.get(&block).copied())
                })
                .ok_or_else(|| anyhow::anyhow!("curve log timestamp unavailable"))?;
            Ok((log, timestamp))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::sol_types::{SolEvent, SolValue};

    fn word(a: Address) -> B256 {
        B256::left_padding_from(a.as_slice())
    }

    /// sig + two indexed addresses, abi-encoded tail — mirrors the curve events.
    fn mk_log(
        curve: Address,
        sig: B256,
        i0: Address,
        i1: Address,
        data: Vec<u8>,
        tx: B256,
        ix: u64,
    ) -> Log {
        Log {
            inner: alloy::primitives::Log::new(curve, vec![sig, word(i0), word(i1)], data.into())
                .expect("topics"),
            block_hash: Some(B256::from([1u8; 32])),
            block_number: Some(1),
            block_timestamp: None,
            transaction_hash: Some(tx),
            transaction_index: Some(0),
            log_index: Some(ix),
            removed: false,
        }
    }

    fn buy_log(
        curve: Address,
        buyer: Address,
        recipient: Address,
        quote_in: U256,
        tx: B256,
        ix: u64,
    ) -> Log {
        let data = (quote_in, U256::from(1), U256::ZERO, U256::ZERO).abi_encode();
        mk_log(
            curve,
            curve::CurveBuy::SIGNATURE_HASH,
            buyer,
            recipient,
            data,
            tx,
            ix,
        )
    }

    fn tax_log(curve: Address, buyer: Address, recipient: Address, tx: B256, ix: u64) -> Log {
        let data = (U256::from(100), U256::from(1)).abi_encode();
        mk_log(
            curve,
            TOPIC_SNIPE_TAX_CHARGED,
            buyer,
            recipient,
            data,
            tx,
            ix,
        )
    }

    fn sell_log(
        curve: Address,
        seller: Address,
        recipient: Address,
        quote_out: U256,
        tx: B256,
        ix: u64,
    ) -> Log {
        let data = (U256::from(1), quote_out, U256::ZERO, U256::ZERO).abi_encode();
        mk_log(
            curve,
            curve::CurveSell::SIGNATURE_HASH,
            seller,
            recipient,
            data,
            tx,
            ix,
        )
    }

    fn a(n: u64) -> Address {
        Address::from_word(B256::from(U256::from(n)))
    }

    #[test]
    fn tax_after_buy_still_counts() {
        let curve = a(9);
        let mut f = FlowTracker::default();
        f.watch(curve, 1_000, 1, vec![a(99)]);
        let tx = B256::from(U256::from(42));
        // SnipeTaxCharged delivered BEFORE the CurveBuy of the same tx — the
        // association must resolve at snapshot time regardless of order.
        f.ingest(&tax_log(curve, a(1), a(1), tx, 0), 1_001).unwrap();
        f.ingest(&buy_log(curve, a(1), a(1), U256::from(10), tx, 1), 1_001)
            .unwrap();
        let s = f.snapshot(curve);
        assert_eq!(
            s.taxed_buyers_s1, 1,
            "taxed log arrived first but must still associate by tx hash"
        );
        assert_eq!(s.buys, 1);
    }

    #[test]
    fn dedup_by_tx_and_index() {
        let curve = a(9);
        let mut f = FlowTracker::default();
        f.watch(curve, 1_000, 1, vec![]);
        let tx = B256::from(U256::from(7));
        let l = buy_log(curve, a(2), a(2), U256::from(10), tx, 3);
        f.ingest(&l, 1_000).unwrap();
        f.ingest(&l, 1_000).unwrap(); // replay / reconnect re-deliver
        assert_eq!(f.snapshot(curve).buys, 1);
    }

    #[test]
    fn removed_and_replaced_logs_reverse_previous_effects() {
        let curve = a(9);
        let mut tracker = FlowTracker::default();
        tracker.watch(curve, 1_000, 1, []);
        let tx = B256::from(U256::from(8));
        let original = buy_log(curve, a(2), a(2), U256::from(10), tx, 0);
        tracker.ingest(&original, 1_000).unwrap();
        assert_eq!(tracker.snapshot(curve).quote_in, U256::from(10));

        let mut removed = original.clone();
        removed.removed = true;
        tracker.ingest(&removed, 1_000).unwrap();
        assert_eq!(tracker.snapshot(curve).buys, 0);

        let mut replacement = buy_log(curve, a(2), a(2), U256::from(20), tx, 0);
        replacement.block_number = Some(2);
        replacement.block_hash = Some(B256::from([2u8; 32]));
        tracker.ingest(&replacement, 1_001).unwrap();
        assert_eq!(tracker.snapshot(curve).quote_in, U256::from(20));
    }

    #[test]
    fn range_application_refuses_gaps_and_rewinds_reorged_blocks() {
        let curve = a(9);
        let mut tracker = FlowTracker::default();
        tracker.watch(curve, 1_000, 10, []);
        assert!(
            tracker
                .apply_range(curve, 11, 11, B256::from([1u8; 32]), &[])
                .is_err()
        );
        tracker
            .apply_range(curve, 10, 11, B256::from([1u8; 32]), &[])
            .unwrap();
        assert_eq!(tracker.sync(curve).unwrap().from_block, 12);
        tracker.rewind(curve, 11);
        assert_eq!(tracker.sync(curve).unwrap().from_block, 11);
        assert!(tracker.sync(curve).unwrap().anchor.is_none());
    }

    #[test]
    fn insider_sell_flags_and_quote_out_sums() {
        let curve = a(9);
        let insider = a(50);
        let mut f = FlowTracker::default();
        f.watch(curve, 1_000, 1, vec![insider]);
        f.ingest(
            &sell_log(
                curve,
                a(60),
                a(61),
                U256::from(5),
                B256::from(U256::from(1)),
                0,
            ),
            1_001,
        )
        .unwrap();
        assert!(
            !f.snapshot(curve).insider_sold,
            "non-insider sell must not flag"
        );
        f.ingest(
            &sell_log(
                curve,
                insider,
                a(61),
                U256::from(7),
                B256::from(U256::from(2)),
                1,
            ),
            1_002,
        )
        .unwrap();
        let s = f.snapshot(curve);
        assert!(s.insider_sold);
        assert_eq!(s.sells, 2);
        assert_eq!(s.quote_out, U256::from(12));
    }
}
