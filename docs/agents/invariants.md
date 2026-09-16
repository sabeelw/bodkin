# Invariants (do not regress)

Measured on Robinhood Chain mainnet. Factory params are owner-mutable (tax window max 60 s). Each curve **snapshots** start/window in `initialize`. If live `doctor` disagrees with a number below, the chain won.

Full score/exit tables: [docs/STRATEGY.md](../STRATEGY.md). Send path: [docs/ARCHITECTURE.md](../ARCHITECTURE.md).

## Chain

| Fact | Value |
|---|---|
| Chain id | 4663 |
| Stack | Arbitrum / Nitro, ETH gas |
| Block time | ~100 ms; ~10 blocks share one Unix second |
| Ordering | FCFS arrival. No mempool, no Timeboost, no gas auction |
| Priority fee | 0. It does not reorder |
| Sequencer | `https://sequencer.mainnet.chain.robinhood.com` — write-only, AWS us-east-2, 3 EC2 IPs, Istio/Envoy, no Cloudflare |
| Raw sequencer feed | unsupported; detection uses canonical RPC logs |
| Explorer | `https://robinhoodchain.blockscout.com` |
| Config 0 curve | 1B supply, 1% fee, 1.68 ETH phantom, 4.2 ETH graduation |

Addresses: `crates/bodkin/src/chain.rs` (`ADDR`). `bodkin doctor` checks the factory still points at the hook / escrow / pool manager we know.

Public RPC pair (default when `RPC_URL` is empty):

- publicnode — state reads, refuses `eth_getLogs` (`#nologs`)
- official Robinhood RPC — logs, 429s bursts

One gate, no JSON-RPC batches. Lanes: hot > enrich > background. Queue depth and pacing are cancellation-safe, permits cover body reads, launch and control queues are bounded, and HDR p50/p95/p99 latency is observable. Deadline cancellation must not reserve an unused future request slot.

## Opening tax

```
tax = startBps >> floor(14 * elapsed / window)
elapsed = block.timestamp - launchedAt
```

Live snapshot: start **9900**, window **3** → **9900 / 618 / 19 / 0** at elapsed 0 / 1 / 2 / ≥3.

- Tax comes off ETH-in **before** pricing. `fee + snipeTax` goes to the base-fee bucket. The live event is `SnipeTaxCharged(address indexed recipient, uint256 amount)`—one indexed address and one data word.
- Some `eth_getLogs` responses include `blockTimestamp: 0x0`; zero is unavailable, not genesis time. Resolve that log's canonical block timestamp before second-based flow classification.
- Exempt: deployer, fee recipient, router-buy recipient, plus ≤32 declared. Event `SnipeTaxExempted` exists on the deployed curve.
- No max buy, cooldown, or launch-block lock.
- Default ceiling `SNIPE_MAX_TAX_BPS=300`. Any ceiling in **[19, 617] ≡ enter first block of second +2**.
- +1 is 6.18% and crowded. +3 only saves 19 bps and loses if size already arrived at +2.
- Some wallets buy ~0.0025 ETH at +1 and sell at +3. That is not our entry.

`ENTRY_SECOND` default is **2**. Clock-scheduled burst; do not poll `currentSnipeTaxBps` every 150 ms as the old Node loop did. Dry entries require a canonical reserve snapshot from +1 or +2 and model the immutable +2 tax; a later snapshot is an unfilled attempt, not a paper position.

## Graduation

Reserved supply is 28.57%. **Do not say the pool gets that.**

| Piece | Share |
|---|---|
| v4 pool | **20.41% of supply + 4.2 ETH** |
| permanently locked | **8.16%** |
| spot at graduation | ≈ 12.25× launch |

`sell_anywhere` refuses during the swept gap between curve close and pool. Constants: `GRAD_POOL_SUPPLY_BPS`, `GRAD_LOCKED_SUPPLY_BPS`, `GRAD_POOL_QUOTE_WEI`, `PHANTOM_QUOTE_WEI` in `chain.rs`.

## Score

Starts at 50, clamps 0–100. Verdicts: FIRE ≥ 75, WATCH ≥ 45, else SKIP. Engine `min_score` default is **60** (board can raise it).

Socials: X **+8**, website **+8**, telegram **+8**. Do not also hard-refuse socials in `decide` beyond `require_socials`. Do not score telegram as +3.

Fee recipient is a contract: **−8**, only when `eth_getCode` was actually looked up (`ScoreContext.fee_recipient_is_contract`). `None` means “not looked up”, not “EOA”.

Deployer window: **~2 days (~1.73M blocks)**, not 11 hours. Launch and graduation scans share one canonical range; readiness is fail-closed. The checkpoint is reusable only after its chain/factory/range anchor is reverified.

Fingerprint: same dev-buy wei, creator tax, links, **exemption count** from another wallet inside 30 minutes.

## Decide / live gate

`decide` is a pure filter. Unreadable launch (no meta, no record, no curve) is a pass reason, not a fire. **Missing launch tx is a refuse.**

On top of score: ETH pairs only (unless `--allow-pairs`), dev share ≤ 8%, creator tax ≤ 3%, socials required, exempt wallets ≤ 2, farm twins ≤ 1, max open 3, session budget 0.05 ETH. Live admission reserves entry value plus worst-case gas for every configured burst attempt.

Live gate at the +2 boundary minus `BURST_LEAD_MS`:

| Env | Default | Meaning |
|---|---|---|
| `MIN_TAXED_BUYERS_S1` | 0 (off) | require taxed flow in second 1 |
| `MAX_EXEMPT_BUYS_S0` | 32 | abort if too many exempt buys in second 0 |
| `ABORT_IF_INSIDER_SOLD` | true | abort if an insider already sold |

## Exemptions

Declared `snipeTaxExemptions` come from successfully decoded `launchAndBuy` calldata. **Keep zeros in the vector.** Count is declared length. Missing/mismatched calldata refuses; event logs cannot reconstruct zero entries and are not a fallback.

## Exits

On-curve stop-loss was removed. On-curve: ladder, stale, insider sell.

| Rule | Default |
|---|---|
| ladder | 34% @ +100%, 33% @ +300% (`EXIT_LADDER`) |
| stale | <2% curve progress after 90 s |
| insider sold | full exit |
| TP / SL / trail / max-hold | **after graduation only** (80 / 35 / 25 / 45 min) |

Marks are a real sell quote for the **whole** position (curve `quoteSell` or `V4Quoter`). A fresh 0.01 ETH entry marking about −10% is the round trip (1% fee + creator tax + impact), not a loss yet.

## Sequencer / burst

- Accepts `eth_sendRawTransaction`, `eth_sendRawTransactionSync`, `eth_sendRawTransactionConditional`. Rejects reads.
- `eth_sendRawTransaction` **blocks until the block is built** (~100 ms). Queue 1024, `queue-timeout` 12 s. Client HTTP timeout **> 12 s** (we use 15 s).
- Sync timeout is a **`0x`-hex quantity** (e.g. `0x1f4`), not a JSON integer. Sync is confirmation **after** a fill, not the fire path.
- Conditional `blockNumberMin/Max` are **L1**. `timestampMin` is L2 Unix seconds. Default TxPreChecker compares to the **last sealed** header, so `timestampMin = launchedAt+2` is late by one ~100 ms block. Conditional is reject-not-wait. Park on it only if `doctor --probe` shows immediate `-32003`.
- Helper reverts **pay gas** (Nitro `max-revert-gas-reject` default 0). Clock lead stays tight.
- Build and warm one persistent `ClientBuilder::resolve` client per sequencer IP so SNI/`Host` stay official. The sequencer is write-only, so a well-formed JSON-RPC error to the harmless `eth_chainId` warm-up still proves the pinned HTTP path. Spray **nonce 0** across the pinned set, reuse those clients for later attempts, and re-resolve every 5 minutes.
- Pre-sign `BURST_MAX` consecutive nonces. Fire in parallel. Default lead 150 ms. Preparation that misses the dispatch cutoff is skipped; only scheduler wake-up has a 25 ms tolerance. Receipt hash/status/block hash/logs/gas fields are strict; the canonical block hash is checked immediately, for 64 blocks on startup, and every ten seconds while live. Lead adaptation uses the actual unique filling attempt and does nothing when first-block classification is unknown.
- One execution scheduler owns nonce-affecting work. Manual, insider and research-risk liquidation are emergency class; deadline entry follows; fresh routine exits may be deferred for at most 100 ms. Already submitted work is never preempted before reconciliation and durable application.

## Research isolation

`capture` is read-only, constructs no wallet/submitter, and is capped at 24 hours / 1 GiB including its manifest. `board --research` requires an explicit path outside/unaliasing normal `data/` and is incompatible with `--live`. The default persistent research ledger is 0.05 ETH equity, 2% entry commitment including modeled entry-attempt gas, three open positions, realized-proceeds reuse, and a 10% peak drawdown liquidation latch. Missing liquidation value halts admission; it is never zero-filled.

## Feed

The sequencer feed is intentionally unsupported. The removed stub checked a claimed signer address but did not cryptographically verify `signatureV2`; do not reintroduce it without full signature validation, sequence continuity, reconnect recovery, and measured latency benefit over `newHeads`.

## Helper (`BodkinBuyOnce`)

`buyOnce(curve, token, recipient, maxTaxBps, minTokensOut, maxRealQuote) payable`

Reverts: tax too high, `balanceOf(recipient) != 0`, reserve cap. Refunds leftover ETH to `msg.sender`.

`minTokensOut` is sized as if this buy is last in the entry block.

## What the strategy does not do

No first-block race. No bundle. No priority fee. No sell into the graduation sweep. No ponsfamily HTTP. No custody.
