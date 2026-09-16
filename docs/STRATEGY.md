# Strategy: what the rules mean and where the numbers come from

Everything below was measured on Robinhood Chain on 2026-09-03. Change the rules to fit your own reading of the chain;
the defaults are a starting point, not advice.

## The opening tax is the whole game

Each curve snapshots `snipeTaxStartBps` / `snipeTaxSeconds` in `initialize` (factory params are owner-mutable, max 60 s).

```
tax = startBps >> floor(14 · elapsed / window)
elapsed = block.timestamp − launchedAt
```

Live snapshot: start 9900, window 3 → **9900 / 618 / 19 / 0** at e = 0 / 1 / 2 / ≥3 (~10 blocks share a Unix second). The tax is taken
off ETH-in before pricing; `fee + snipeTax` goes to the base-fee bucket. Exempt: deployer, fee recipient, router-buy recipient, plus ≤32
declared. No max buy, cooldown, or launch-block lock.

Default ceiling `SNIPE_MAX_TAX_BPS=300`. Any ceiling in [19, 617] is the same rule: enter in the **first block of second +2**.
+1 is 6.18 % and crowded. +3 only saves 19 bps and loses if 0.1 ETH already arrived at +2. Some wallets buy 0.0025 ETH at +1 and sell at +3.

## What the score rewards and punishes

| Signal | Points | Why |
|---|---|---|
| dev buy 1–6 % of supply | +15 | skin in the game without a bag that can flatten the curve; the builder launches that graduated this week sat at 3 % |
| dev buy over 10 % | −25 | a 12.5 % dev buy killed a Grok Build clone at $5.9K while a 2.5 % one reached $140K |
| no dev buy | −10 | nothing at stake |
| creator tax ≤ 2 % | +10 | the creator earns on volume and has a reason to keep posting |
| creator tax > 5 % | −25 | traders pay 6 %+ per side; flow dies |
| fees routed to a third party | +5 and a flag | the builder/KOL deal pattern: the wallet that launched is not the wallet that gets paid |
| X link / website / telegram | +8 / +8 / +8 | telegram scores the same as a website; do not also hard-refuse socials twice |
| fee recipient is a contract | −8 | cached `eth_getCode`; only when looked up |
| no socials | −15 | |
| exempt wallets 1–3 / 4+ | −5 / −20 | addresses declared exempt from the opening tax at launch are the declared bundle |
| fresh deployer | +5 | |
| deployer graduated ≥ 30 % of recent launches | +15 | |
| serial deployer, ≥ 5 launches, none graduated | −25 | the feed shows deployers with 185 and 297 launches in 11 hours and zero graduations |
| one earlier launch with the same fingerprint in 30 min | −8 | same dev-buy wei, creator tax, links and exemption count from another wallet |
| two or more earlier twins in 30 min | −25 | a launch farm: one operator, fresh wallets, identical calldata |
| ≥ 10 distinct buyers in the first minute | +10 | organic flow (follow-ups only) |
| every early buy paid the opening tax | −10 | bots only |

Verdicts: FIRE ≥ 75, WATCH ≥ 45, SKIP below.

## The sniper's rules (on top of the score)

| Rule | Default | Flag |
|---|---|---|
| minimum score | 60 | `--min-score` |
| ETH-paired launches only | yes | `--allow-pairs` (stock-token and USDG pairs exist and are common) |
| dev share | ≤ 8 % | edit `rulesFromEnv` |
| creator tax | ≤ 3 % | |
| socials required | yes | |
| exempt wallets | ≤ 2 | |
| keyword on name/symbol/description | none | `--keyword` |
| deployer allow-list | none | `--deployer` |
| max open positions | 3 | `--max-open` |
| launch-farm twins | ≤ 1 | `maxFarmTwins` in `rulesFromEnv` |
| ETH per shot | `SNIPE_ETH` = 0.01 | `--eth` |
| entry second | 2 | `ENTRY_SECOND` |
| taxed buyers in second 1 | 0 (off) | `MIN_TAXED_BUYERS_S1` |
| exempt buys in second 0 | 32 | `MAX_EXEMPT_BUYS_S0` |
| abort if insider sold | yes | `ABORT_IF_INSIDER_SOLD` |

Five of these (min score, max open, tax ceiling, dev share, exempt wallets) can be changed while the engine runs, from the board.

A launch with four wallets exempt from the opening tax is refused by the default rules even when everything else looks good.
That is the point of showing reasons: you decide which rule to relax, on purpose.

The opt-in research profile does not change those live defaults. It starts from 0.05 modeled ETH, caps one token's entry value plus modeled attempt gas at 2% of current known equity, permits three positions, reserves modeled exit gas, reuses settled sale proceeds, and latches full liquidation at 10% peak drawdown. Its predefined offline comparisons are second-1 buyer minimums 0/1/2, inactivity 60/90/180 seconds, on-curve holds 5/15/30 minutes, and pre-graduation versus hold-through-graduation. Missing pool/counterfactual pricing is unmeasured, not a win or loss.

## Exits

On the curve, stop-loss is replaced by **stale** and **insider sell**. Ladder takes partials. SL / trail / max-hold still fire after graduation.

| Rule | Default | Env |
|---|---|---|
| ladder | 34 % at +100 %, 33 % at +300 % | `EXIT_LADDER` |
| stale | <2 % progress after 90 s | `STALE_SEC`, `STALE_MIN_PROGRESS` |
| insider sold | full exit | continuously refreshed, reorg-reversible FlowTracker |
| take profit (post-grad) | +80 % | `TAKE_PROFIT_PCT` |
| stop loss (post-grad) | −35 % | `STOP_LOSS_PCT` |
| trailing stop (post-grad) | 25 % below the peak | `TRAILING_PCT` |
| max hold (post-grad) | 45 min | `MAX_HOLD_MIN` |

Marks come from a real quote (curve `quoteSell` or `V4Quoter`), so a mark includes the 1 % fee, creator tax, price impact, and the remaining allocated entry gas. Prospective exit gas is unknown until a receipt lands; closed-position PnL includes actual approval/sell gas plus any resolved failed-exit gas charged idempotently to that position. A fresh 0.01 ETH entry can therefore mark around −10 % immediately; that is a quote, not a realized loss.

## Graduation

The curve closes when 4.2 ETH of real quote is in (config 0). Reserved supply is 28.57 %, but the graduated Uniswap v4 pool gets
**20.41 % + 4.2 ETH**; 8.16 % is permanently locked. Spot at graduation is about 12.25× launch. Between sweep and pool there is a gap
when nothing can trade; `sell_anywhere` refuses during that gap. On 2026-09-02/03: 24 462 launches, 559 graduations, so about one
launch in forty-four graduates.

## Known blind spots

- **Launch farms that vary their numbers.** The fingerprint rule catches the common farm: brand-new wallets, identical dev-buy wei,
  identical tax and links, minutes apart (the feed showed runs scoring 86–97 each before the rule existed). A farm that randomizes the dev buy
  slips through; `--keyword` and `--deployer` narrow the feed further.
- **Fee recipient identity.** `third party` tells you the fees leave the deployer; it cannot tell you who receives them.
- **Stock-token pairs.** FDV in NVDA or MSFT units is shown without a USD figure; the board and the feed do not price stock tokens.
- **One execution owner per `data/` directory.** redb exclusively locks `data/bodkin.redb`; a concurrent engine or live command refuses rather than sharing nonces or portfolio state.

## Things the strategy does not do

It does not chase the first block. It does not bundle. It does not add priority fees (they do not reorder anything here).
It does not sell into the graduation sweep. It does not promise anything.
