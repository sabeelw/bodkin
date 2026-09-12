use super::curve::CurveState;
use super::launches::LaunchEvent;
use crate::chain::{ADDR, BPS, DEAD, MULTICALL3, SUPPLY, ZERO};
use crate::rpc::{Lane, Rpc};
use alloy::primitives::{Address, Bytes, B256, U256};
use alloy::sol_types::{SolCall, SolEvent, SolValue};
use anyhow::Context;
use std::collections::{HashMap, HashSet};
use std::sync::Mutex;

#[derive(Debug, Clone)]
pub struct Socials {
    pub twitter: String,
    pub telegram: String,
    pub discord: String,
    pub website: String,
    pub farcaster: String,
}

impl Default for Socials {
    fn default() -> Self {
        Self {
            twitter: String::new(),
            telegram: String::new(),
            discord: String::new(),
            website: String::new(),
            farcaster: String::new(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct TokenMeta {
    pub name: String,
    pub symbol: String,
    pub logo: String,
    pub description: String,
    pub socials: Socials,
}

#[derive(Debug, Clone)]
pub struct LaunchRecord {
    pub creator_fee_recipient: Address,
    pub creator_tax_bps: u16,
    pub phase: u8,
    pub pool_fee: u32,
    pub tick_spacing: i32,
    pub buyback_enabled: bool,
}

#[derive(Debug, Clone)]
pub struct LaunchTx {
    pub from: Address,
    pub to: Address,
    pub value_wei: U256,
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
        Self { address: ZERO, symbol: "ETH".into(), decimals: 18, usd_per_unit: None }
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
    pub last_block: u64,
}

pub fn dev_share_pct(tx: Option<&LaunchTx>) -> f64 {
    match tx {
        Some(t) => (u256_f64(t.dev_tokens) / SUPPLY as f64) * 100.0,
        None => 0.0,
    }
}

pub fn has_socials(meta: Option<&TokenMeta>) -> SocialFlags {
    let s = meta.map(|m| &m.socials);
    let twitter = s.is_some_and(|x| !x.twitter.trim().is_empty());
    let website = s.is_some_and(|x| !x.website.trim().is_empty());
    let telegram = s.is_some_and(|x| !x.telegram.trim().is_empty());
    SocialFlags { twitter, website, telegram, any: twitter || website || telegram }
}

#[derive(Debug, Clone, Copy)]
pub struct SocialFlags {
    pub twitter: bool,
    pub website: bool,
    pub telegram: bool,
    pub any: bool,
}

pub fn logo_url(logo: &str) -> String {
    if logo.is_empty() {
        return String::new();
    }
    if let Some(rest) = logo.strip_prefix("ipfs://") {
        return format!("https://www.ponsfamily.com/api/ipfs/content/{rest}?variant=card");
    }
    logo.to_string()
}

fn u256_f64(v: U256) -> f64 {
    let mut acc = 0.0f64;
    let mut base = 1.0f64;
    for limb in v.as_limbs() {
        acc += (*limb as f64) * base;
        base *= 2.0f64.powi(64);
    }
    acc
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
        .eth_call(Lane::Enrich, pair_token, crate::abi::token::symbolCall {}, None)
        .await
        .unwrap_or_else(|_| "?".into());
    let decimals: u8 = rpc.eth_call(Lane::Enrich, pair_token, crate::abi::token::decimalsCall {}, None).await.unwrap_or(18);
    let info = PairInfo {
        address: pair_token,
        symbol: symbol.clone(),
        decimals,
        usd_per_unit: if STABLES.iter().any(|s| symbol.eq_ignore_ascii_case(s)) { Some(1.0) } else { None },
    };
    PAIR_CACHE.lock().unwrap().as_mut().unwrap().insert(pair_token, info.clone());
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

type Bundle = (Option<TokenMeta>, Option<LaunchRecord>, Option<CurveState>, Vec<String>);

pub async fn read_launch_bundle(rpc: &Rpc, ev: &LaunchEvent, recipient: Address) -> anyhow::Result<Bundle> {
    use crate::abi::{curve, factory, token};
    let mut calls = Vec::new();
    let t = ev.token;
    let c = ev.curve;
    calls.push((t, token::nameCall {}.abi_encode()));
    calls.push((t, token::symbolCall {}.abi_encode()));
    calls.push((t, token::getTokenInfoCall {}.abi_encode()));
    calls.push((ADDR.pons_factory, factory::getLaunchedTokenCall { token: t }.abi_encode()));
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

    let results = rpc.multicall3(Lane::Enrich, &calls).await.context("multicall3")?;
    let mut errors = Vec::new();
    let ok_bytes = |i: usize| results.get(i).and_then(|r| r.clone());

    let mut meta = None;
    if let (Some(nb), Some(sb), Some(ib)) = (ok_bytes(0), ok_bytes(1), ok_bytes(2)) {
        if let (Ok(name), Ok(symbol), Ok(info)) = (
            token::nameCall::abi_decode_returns(&nb),
            token::symbolCall::abi_decode_returns(&sb),
            token::getTokenInfoCall::abi_decode_returns(&ib),
        ) {
            meta = Some(TokenMeta {
                name,
                symbol,
                logo: info.tokenLogo,
                description: info.tokenDescription,
                socials: Socials {
                    twitter: info.tokenSocials.twitter,
                    telegram: info.tokenSocials.telegram,
                    discord: info.tokenSocials.discord,
                    website: info.tokenSocials.website,
                    farcaster: info.tokenSocials.farcaster,
                },
            });
        }
    }
    if meta.is_none() {
        errors.push("token metadata unreadable".into());
    }

    let mut record = None;
    if let Some(rb) = ok_bytes(3) {
        if let Ok(rec) = factory::getLaunchedTokenCall::abi_decode_returns(&rb) {
            if rec.exists {
                record = Some(LaunchRecord {
                    creator_fee_recipient: rec.creatorFeeRecipient,
                    creator_tax_bps: rec.creatorTaxBps,
                    phase: rec.phase,
                    pool_fee: u32::try_from(rec.poolFee).unwrap_or(0),
                    tick_spacing: i32::try_from(rec.tickSpacing).unwrap_or(0),
                    buyback_enabled: rec.buybackEnabled,
                });
            }
        }
    }
    if record.is_none() {
        errors.push("factory record unreadable".into());
    }

    let mut curve = None;
    if let (Some(resb), Some(sellb), Some(feeb)) = (ok_bytes(4), ok_bytes(6), ok_bytes(9)) {
        if let (Ok(reserves), Ok(sellable), Ok(fee_bps)) = (
            curve::getReservesCall::abi_decode_returns(&resb),
            curve::sellableTokensCall::abi_decode_returns(&sellb),
            curve::feeBpsCall::abi_decode_returns(&feeb),
        ) {
            let dec_u = |i: usize| ok_bytes(i).and_then(|b| U256::abi_decode(&b).ok()).unwrap_or(U256::ZERO);
            let dec_bool = |i: usize| ok_bytes(i).and_then(|b| bool::abi_decode(&b).ok()).unwrap_or(false);
            curve = Some(CurveState {
                quote_reserve: reserves.quoteReserve,
                token_reserve: reserves.tokenReserve,
                real_quote_reserve: dec_u(5),
                sellable_tokens: sellable,
                reserved_tokens: dec_u(7),
                graduation_threshold: {
                    let g = dec_u(8);
                    if g.is_zero() { ev.graduation_threshold } else { g }
                },
                fee_bps,
                creator_tax_bps: dec_u(10),
                opening_tax_bps: dec_u(11),
                graduated: dec_bool(12),
                ready_to_graduate: dec_bool(13),
                launched_at: dec_u(14).try_into().unwrap_or(0u64),
                read_at_ms: now_ms(),
                snipe_tax_start_bps: {
                    let v = dec_u(15);
                    if v.is_zero() { U256::from(9900u64) } else { v }
                },
                snipe_tax_seconds: {
                    let v = dec_u(16);
                    if v.is_zero() { U256::from(3u64) } else { v }
                },
            });
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
    let receipt = rpc.get_transaction_receipt(Lane::Enrich, ev.tx_hash).await?;
    let mut dev_buy_wei = U256::ZERO;
    let mut exemptions = Vec::new();
    let mut recipient = tx.from;
    if tx.input.as_ref().starts_with(&crate::abi::launch_and_buy_selector()) {
        if let Ok(decoded) = router::launchAndBuyCall::abi_decode(&tx.input) {
            dev_buy_wei = decoded.quoteIn;
            recipient = decoded.recipient;
            exemptions = decoded.snipeTaxExemptions;
        }
    }
    let mut dev_tokens = U256::ZERO;
    let mut spent_in_tx = U256::ZERO;
    let mut exempt_from_events = Vec::new();
    for log in &receipt.logs {
        if log.address() != ev.curve {
            continue;
        }
        if let Ok(b) = curve::CurveBuy::decode_log(&log.clone().into()) {
            dev_tokens += b.tokensOut;
            spent_in_tx += b.quoteIn;
        }
        if let Ok(e) = curve::SnipeTaxExempted::decode_log(&log.clone().into()) {
            exempt_from_events.push(e.account);
        }
    }
    if exemptions.is_empty() && !exempt_from_events.is_empty() {
        exemptions = exempt_from_events;
    }
    if dev_buy_wei.is_zero() {
        dev_buy_wei = spent_in_tx;
    }
    Ok(LaunchTx {
        from: tx.from,
        to: tx.to.unwrap_or(ZERO),
        value_wei: tx.value,
        dev_buy_wei,
        dev_tokens,
        exemptions,
        recipient,
        timestamp: receipt.block_timestamp.unwrap_or(0),
    })
}

pub async fn is_contract(rpc: &Rpc, addr: Address) -> anyhow::Result<bool> {
    let code = rpc.get_code(Lane::Background, addr).await?;
    Ok(!code.is_empty())
}

pub async fn read_token_meta(rpc: &Rpc, token: Address) -> anyhow::Result<TokenMeta> {
    let name: String = rpc.eth_call(Lane::Background, token, crate::abi::token::nameCall {}, None).await?;
    let symbol: String = rpc.eth_call(Lane::Background, token, crate::abi::token::symbolCall {}, None).await?;
    let info = rpc.eth_call(Lane::Background, token, crate::abi::token::getTokenInfoCall {}, None).await?;
    Ok(TokenMeta {
        name,
        symbol,
        logo: info.tokenLogo,
        description: info.tokenDescription,
        socials: Socials {
            twitter: info.tokenSocials.twitter,
            telegram: info.tokenSocials.telegram,
            discord: info.tokenSocials.discord,
            website: info.tokenSocials.website,
            farcaster: info.tokenSocials.farcaster,
        },
    })
}

pub async fn curve_activity(rpc: &Rpc, curve: Address, from_block: u64, to_block: Option<u64>) -> anyhow::Result<CurveActivity> {
    use crate::abi::{curve as cabi, TOPIC_SNIPE_TAX_CHARGED};
    use alloy::rpc::types::Filter;
    use alloy::sol_types::SolEvent;
    let to = match to_block {
        Some(t) => t,
        None => rpc.block_number(Lane::Background).await?,
    };
    let logs = rpc.get_logs(Lane::Background, Filter::new().address(curve).from_block(from_block).to_block(to)).await?;
    let mut out = CurveActivity { last_block: from_block, ..Default::default() };
    let mut buyers = HashSet::new();
    for l in &logs {
        if let Some(bn) = l.block_number {
            if bn > out.last_block {
                out.last_block = bn;
            }
        }
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

pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
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
}

// silence unused import in types-only builds
#[allow(dead_code)]
fn _b256(_: B256) {}
