use super::clock::now_ms;
use super::curve::CurveState;
use super::launches::LaunchEvent;
use crate::chain::{ADDR, BPS, DEAD, MULTICALL3, SUPPLY, ZERO};
use crate::rpc::{Lane, Rpc};
use alloy::primitives::{Address, B256, Bytes, U256};
use alloy::sol_types::{SolCall, SolEvent, SolValue};
use anyhow::Context;
use std::collections::{HashMap, HashSet};
use std::sync::Mutex;

#[derive(Debug, Clone, Default)]
pub struct Socials {
    pub twitter: String,
    pub telegram: String,
    pub website: String,
}

#[derive(Debug, Clone)]
pub struct TokenMeta {
    pub name: String,
    pub symbol: String,
    pub description: String,
    pub socials: Socials,
}

#[derive(Debug, Clone)]
pub struct LaunchRecord {
    pub creator_fee_recipient: Address,
    pub creator_tax_bps: u16,
    pub phase: u8,
}

#[derive(Debug, Clone)]
pub struct LaunchTx {
    pub from: Address,
    pub dev_buy_wei: U256,
    pub dev_tokens: U256,
    pub exemptions: Vec<Address>,
    pub recipient: Address,
    pub timestamp: u64,
}

#[derive(Debug, Clone)]
pub struct PairInfo {
    pub address: Address,
    pub symbol: String,
    pub decimals: u8,
    pub usd_per_unit: Option<f64>,
}

impl PairInfo {
    pub fn eth() -> Self {
        Self {
            address: ZERO,
            symbol: "ETH".into(),
            decimals: 18,
            usd_per_unit: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct LaunchIntel {
    pub ev: LaunchEvent,
    pub meta: Option<TokenMeta>,
    pub record: Option<LaunchRecord>,
    pub tx: Option<LaunchTx>,
    pub curve: Option<CurveState>,
    pub pair: PairInfo,
    pub errors: Vec<String>,
    /// Cached `eth_getCode` of the fee recipient. None = not looked up.
    pub fee_recipient_is_contract: Option<bool>,
}

#[derive(Debug, Clone, Default)]
pub struct CurveActivity {
    pub buys: u32,
    pub sells: u32,
    pub unique_buyers: u32,
    pub taxed_buys: u32,
    pub quote_in: U256,
    pub quote_out: U256,
}

pub fn dev_share_pct(tx: Option<&LaunchTx>) -> f64 {
    match tx {
        Some(t) => (crate::fmt::wei_to_f64(t.dev_tokens) / SUPPLY as f64) * 100.0,
        None => 0.0,
    }
}

pub fn has_socials(meta: Option<&TokenMeta>) -> SocialFlags {
    let s = meta.map(|m| &m.socials);
    let twitter = s.is_some_and(|x| !x.twitter.trim().is_empty());
    let website = s.is_some_and(|x| !x.website.trim().is_empty());
    let telegram = s.is_some_and(|x| !x.telegram.trim().is_empty());
    SocialFlags {
        twitter,
        website,
        telegram,
        any: twitter || website || telegram,
    }
}

#[derive(Debug, Clone, Copy)]
pub struct SocialFlags {
    pub twitter: bool,
    pub website: bool,
    pub telegram: bool,
    pub any: bool,
}

static PAIR_CACHE: Mutex<Option<HashMap<Address, PairInfo>>> = Mutex::new(None);
const STABLES: &[&str] = &["USDG", "USDC", "USDT", "USDC.E", "DAI"];

pub async fn read_pair_info(rpc: &Rpc, pair_token: Address) -> PairInfo {
    if pair_token == ZERO {
        return PairInfo::eth();
    }
    {
        let mut g = PAIR_CACHE.lock().unwrap();
        let cache = g.get_or_insert_with(HashMap::new);
        if let Some(hit) = cache.get(&pair_token) {
            return hit.clone();
        }
    }
    let symbol = rpc
        .eth_call(
            Lane::Enrich,
            pair_token,
            crate::abi::token::symbolCall {},
            None,
        )
        .await
        .unwrap_or_else(|_| "?".into());
    let decimals: u8 = rpc
        .eth_call(
            Lane::Enrich,
            pair_token,
            crate::abi::token::decimalsCall {},
            None,
        )
        .await
        .unwrap_or(18);
    let info = PairInfo {
        address: pair_token,
        symbol: symbol.clone(),
        decimals,
        usd_per_unit: if STABLES.iter().any(|s| symbol.eq_ignore_ascii_case(s)) {
            Some(1.0)
        } else {
            None
        },
    };
    PAIR_CACHE
        .lock()
        .unwrap()
        .as_mut()
        .unwrap()
        .insert(pair_token, info.clone());
    info
}

pub async fn enrich_launch(rpc: &Rpc, ev: LaunchEvent, recipient: Address) -> LaunchIntel {
    let mut errors = Vec::new();
    let recip = if recipient == ZERO { DEAD } else { recipient };
    let (bundle, tx, pair) = tokio::join!(
        read_launch_bundle(rpc, &ev, recip),
        read_launch_tx(rpc, &ev),
        read_pair_info(rpc, ev.pair_token),
    );
    let (meta, record, curve, mut berr) = match bundle {
        Ok(b) => b,
        Err(e) => {
            errors.push(format!("bundle: {}", first_line(&e.to_string())));
            (None, None, None, vec![])
        }
    };
    errors.append(&mut berr);
    let tx = match tx {
        Ok(t) => Some(t),
        Err(e) => {
            errors.push(format!("tx: {}", first_line(&e.to_string())));
            None
        }
    };
    LaunchIntel {
        ev,
        meta,
        record,
        tx,
        curve,
        pair,
        errors,
        fee_recipient_is_contract: None,
    }
}

type Bundle = (
    Option<TokenMeta>,
    Option<LaunchRecord>,
    Option<CurveState>,
    Vec<String>,
);

pub async fn read_launch_bundle(
    rpc: &Rpc,
    ev: &LaunchEvent,
    recipient: Address,
) -> anyhow::Result<Bundle> {
    use crate::abi::{curve, factory, token};
    let mut calls = Vec::new();
    let t = ev.token;
    let c = ev.curve;
    calls.push((t, token::nameCall {}.abi_encode()));
    calls.push((t, token::symbolCall {}.abi_encode()));
    calls.push((t, token::getTokenInfoCall {}.abi_encode()));
    calls.push((
        ADDR.pons_factory,
        factory::getLaunchedTokenCall { token: t }.abi_encode(),
    ));
    calls.push((c, curve::getReservesCall {}.abi_encode()));
    calls.push((c, curve::realQuoteReserveCall {}.abi_encode()));
    calls.push((c, curve::sellableTokensCall {}.abi_encode()));
    calls.push((c, curve::reservedTokensCall {}.abi_encode()));
    calls.push((c, curve::graduationThresholdCall {}.abi_encode()));
    calls.push((c, curve::feeBpsCall {}.abi_encode()));
    calls.push((c, curve::creatorTaxBpsCall {}.abi_encode()));
    calls.push((c, curve::currentSnipeTaxBpsCall { recipient }.abi_encode()));
    calls.push((c, curve::graduatedCall {}.abi_encode()));
    calls.push((c, curve::readyToGraduateCall {}.abi_encode()));
    calls.push((c, curve::launchedAtCall {}.abi_encode()));
    calls.push((c, curve::snipeTaxStartBpsCall {}.abi_encode()));
    calls.push((c, curve::snipeTaxSecondsCall {}.abi_encode()));

    let read_header = rpc.latest_header(Lane::Enrich).await?;
    let results = rpc
        .multicall3_at(
            Lane::Enrich,
            &calls,
            serde_json::json!({
                "blockHash": format!("{:#x}", read_header.hash),
                "requireCanonical": true
            }),
        )
        .await
        .context("multicall3")?;
    let mut errors = Vec::new();
    let ok_bytes = |i: usize| results.get(i).and_then(|r| r.clone());

    let mut meta = None;
    if let (Some(nb), Some(sb), Some(ib)) = (ok_bytes(0), ok_bytes(1), ok_bytes(2))
        && let (Ok(name), Ok(symbol), Ok(info)) = (
            token::nameCall::abi_decode_returns(&nb),
            token::symbolCall::abi_decode_returns(&sb),
            token::getTokenInfoCall::abi_decode_returns(&ib),
        )
    {
        meta = Some(TokenMeta {
            name,
            symbol,
            description: info.tokenDescription,
            socials: Socials {
                twitter: info.tokenSocials.twitter,
                telegram: info.tokenSocials.telegram,
                website: info.tokenSocials.website,
            },
        });
    }
    if meta.is_none() {
        errors.push("token metadata unreadable".into());
    }

    let mut record = None;
    if let Some(rb) = ok_bytes(3)
        && let Ok(rec) = factory::getLaunchedTokenCall::abi_decode_returns(&rb)
        && rec.exists
    {
        record = Some(LaunchRecord {
            creator_fee_recipient: rec.creatorFeeRecipient,
            creator_tax_bps: rec.creatorTaxBps,
            phase: rec.phase,
        });
    }
    if record.is_none() {
        errors.push("factory record unreadable".into());
    }

    // Every curve subcall is load-bearing: tax start/window, launch time, phase
    // and reserves all feed entry validation, so a failed or undecodable call
    // marks the whole curve unreadable rather than defaulting to zero/false.
    let cs: Vec<Option<Vec<u8>>> = (4usize..=16).map(ok_bytes).collect();
    let dec_u =
        |i: usize| -> Option<U256> { cs[i - 4].as_ref().and_then(|b| U256::abi_decode(b).ok()) };
    let dec_bool =
        |i: usize| -> Option<bool> { cs[i - 4].as_ref().and_then(|b| bool::abi_decode(b).ok()) };
    let mut curve = None;
    if let (Some(resb), Some(sellb), Some(feeb)) = (&cs[0], &cs[2], &cs[5])
        && let (Ok(reserves), Ok(sellable), Ok(fee_bps)) = (
            curve::getReservesCall::abi_decode_returns(resb),
            curve::sellableTokensCall::abi_decode_returns(sellb),
            curve::feeBpsCall::abi_decode_returns(feeb),
        )
    {
        let parts = (
            dec_u(5),
            dec_u(7),
            dec_u(8),
            dec_u(10),
            dec_u(11),
            dec_bool(12),
            dec_bool(13),
            dec_u(14),
            dec_u(15),
            dec_u(16),
        );
        if let (
            Some(real_q),
            Some(reserved),
            Some(grad_thr),
            Some(creator_tax),
            Some(opening_tax),
            Some(graduated),
            Some(ready),
            Some(launched_at),
            Some(tax_start),
            Some(tax_secs),
        ) = parts
        {
            let launched_at: u64 = launched_at.try_into().unwrap_or(0);
            if launched_at == 0 {
                errors.push("curve launchedAt is zero".into());
            } else {
                curve = Some(CurveState {
                    quote_reserve: reserves.quoteReserve,
                    token_reserve: reserves.tokenReserve,
                    real_quote_reserve: real_q,
                    sellable_tokens: sellable,
                    reserved_tokens: reserved,
                    graduation_threshold: {
                        // The curve call is authoritative; the launch event
                        // value is the launch-time snapshot, only a fallback
                        // for a degenerate zero.
                        if grad_thr.is_zero() {
                            ev.graduation_threshold
                        } else {
                            grad_thr
                        }
                    },
                    fee_bps,
                    creator_tax_bps: creator_tax,
                    opening_tax_bps: opening_tax,
                    graduated,
                    ready_to_graduate: ready,
                    launched_at,
                    read_at_ms: now_ms(),
                    read_block: read_header.number,
                    read_chain_ts: read_header.timestamp,
                    snipe_tax_start_bps: tax_start,
                    snipe_tax_seconds: tax_secs,
                });
            }
        }
    }
    if curve.is_none() {
        errors.push("curve state unreadable".into());
    }
    let _ = (BPS, MULTICALL3);
    Ok((meta, record, curve, errors))
}

pub async fn read_launch_tx(rpc: &Rpc, ev: &LaunchEvent) -> anyhow::Result<LaunchTx> {
    use crate::abi::{curve, router};
    let tx = rpc.get_transaction(Lane::Enrich, ev.tx_hash).await?;
    let receipt = rpc
        .get_transaction_receipt(Lane::Enrich, ev.tx_hash)
        .await?;
    anyhow::ensure!(
        receipt.tx_hash == Some(ev.tx_hash),
        "launch receipt hash does not match requested transaction"
    );
    match receipt.status {
        Some(true) => {}
        Some(false) => anyhow::bail!("launch tx reverted"),
        None => anyhow::bail!("launch tx receipt has no status"),
    }
    anyhow::ensure!(
        tx.from == ev.deployer,
        "launch tx sender does not match TokenLaunched deployer"
    );
    anyhow::ensure!(
        tx.to == Some(ADDR.pons_router),
        "launch tx target is not the pons v2 router"
    );
    anyhow::ensure!(
        tx.input
            .as_ref()
            .starts_with(&crate::abi::selectors::launch_and_buy()),
        "launch tx calldata is not launchAndBuy"
    );
    let decoded =
        router::launchAndBuyCall::abi_decode(&tx.input).context("decode launchAndBuy calldata")?;
    anyhow::ensure!(
        decoded.pairToken == ev.pair_token,
        "launch tx pair does not match TokenLaunched"
    );
    anyhow::ensure!(
        decoded.launchConfigId == ev.launch_config_id,
        "launch config does not match TokenLaunched"
    );
    let mut dev_tokens = U256::ZERO;
    for log in &receipt.logs {
        if log.address() != ev.curve {
            continue;
        }
        if let Ok(buy) = curve::CurveBuy::decode_log(&log.clone().into())
            && buy.recipient == decoded.recipient
        {
            dev_tokens = dev_tokens.saturating_add(buy.tokensOut);
        }
    }
    let block_number = receipt
        .block_number
        .expect("strict mined receipt has a block");
    let timestamp = match receipt.block_timestamp {
        Some(timestamp) => timestamp,
        None => rpc.block_timestamp(Lane::Enrich, block_number).await?,
    };
    Ok(LaunchTx {
        from: tx.from,
        dev_buy_wei: decoded.quoteIn,
        dev_tokens,
        exemptions: decoded.snipeTaxExemptions,
        recipient: decoded.recipient,
        timestamp,
    })
}

pub async fn is_contract(rpc: &Rpc, addr: Address) -> anyhow::Result<bool> {
    let code = rpc.get_code(Lane::Background, addr).await?;
    Ok(!code.is_empty())
}

pub async fn read_token_meta(rpc: &Rpc, token: Address) -> anyhow::Result<TokenMeta> {
    let name: String = rpc
        .eth_call(
            Lane::Background,
            token,
            crate::abi::token::nameCall {},
            None,
        )
        .await?;
    let symbol: String = rpc
        .eth_call(
            Lane::Background,
            token,
            crate::abi::token::symbolCall {},
            None,
        )
        .await?;
    let info = rpc
        .eth_call(
            Lane::Background,
            token,
            crate::abi::token::getTokenInfoCall {},
            None,
        )
        .await?;
    Ok(TokenMeta {
        name,
        symbol,
        description: info.tokenDescription,
        socials: Socials {
            twitter: info.tokenSocials.twitter,
            telegram: info.tokenSocials.telegram,
            website: info.tokenSocials.website,
        },
    })
}

pub async fn curve_activity(
    rpc: &Rpc,
    curve: Address,
    from_block: u64,
    to_block: Option<u64>,
) -> anyhow::Result<CurveActivity> {
    use crate::abi::{TOPIC_SNIPE_TAX_CHARGED, curve as cabi};
    use alloy::rpc::types::Filter;
    use alloy::sol_types::SolEvent;
    let to = match to_block {
        Some(t) => t,
        None => rpc.block_number(Lane::Background).await?,
    };
    let logs = rpc
        .get_logs(
            Lane::Background,
            Filter::new()
                .address(curve)
                .from_block(from_block)
                .to_block(to),
        )
        .await?;
    let mut out = CurveActivity::default();
    let mut buyers = HashSet::new();
    for l in &logs {
        if let Ok(b) = cabi::CurveBuy::decode_log(&l.clone().into()) {
            out.buys += 1;
            buyers.insert(b.recipient);
            out.quote_in += b.quoteIn;
        } else if let Ok(s) = cabi::CurveSell::decode_log(&l.clone().into()) {
            out.sells += 1;
            out.quote_out += s.quoteOut;
        }
        if l.topics().first().copied() == Some(TOPIC_SNIPE_TAX_CHARGED) {
            out.taxed_buys += 1;
        }
    }
    out.unique_buyers = buyers.len() as u32;
    Ok(out)
}

fn first_line(s: &str) -> String {
    s.lines().next().unwrap_or(s).to_string()
}

pub struct TxView {
    pub from: Address,
    pub to: Option<Address>,
    pub value: U256,
    pub input: Bytes,
}

pub struct ReceiptView {
    pub logs: Vec<alloy::rpc::types::Log>,
    pub block_timestamp: Option<u64>,
    /// `Some(true)` success, `Some(false)` reverted, `None` field absent.
    pub status: Option<bool>,
    pub block_number: Option<u64>,
    pub block_hash: Option<B256>,
    pub tx_hash: Option<B256>,
    pub contract_address: Option<Address>,
    pub gas_used: Option<U256>,
    pub effective_gas_price: Option<U256>,
}
