use alloy::primitives::{address, Address};

/// Robinhood Chain mainnet. ETH gas, Arbitrum stack, ~100 ms blocks.
pub const CHAIN_ID: u64 = 4663;
pub const BLOCK_TIME_MS: u64 = 100;
/// ~10 blocks share one Unix timestamp on this chain.
pub const BLOCKS_PER_SECOND: u64 = 10;

pub const EXPLORER: &str = "https://robinhoodchain.blockscout.com";
pub const USER_AGENT: &str = "bodkin/0.1 (+https://github.com/Phosphenq/bodkin)";

pub const DEFAULT_HTTP_PUBLICNODE: &str = "https://robinhood-rpc.publicnode.com";
pub const DEFAULT_HTTP_OFFICIAL: &str = "https://rpc.mainnet.chain.robinhood.com";
pub const DEFAULT_WS_PUBLICNODE: &str = "wss://robinhood-rpc.publicnode.com";
pub const DEFAULT_SEQUENCER: &str = "https://sequencer.mainnet.chain.robinhood.com";
pub const DEFAULT_FEED: &str = "wss://feed.mainnet.chain.robinhood.com";

/// Sequencer-feed `signatureV2` signer (Robinhood Chain).
pub const FEED_SIGNER: Address = address!("0xDaa526086787d9DEbE1D7F3FFdb1fE50cf8687F4");

pub const ZERO: Address = Address::ZERO;
pub const DEAD: Address = address!("0x000000000000000000000000000000000000dEaD");
pub const MULTICALL3: Address = address!("0xcA11bde05977b3631167028862bE2a173976CA11");

/// Addresses read from the live factory / official pages on 2026-09-03. `bodkin doctor` re-checks them.
pub struct Addresses {
    pub pons_factory: Address,
    pub pons_router: Address,
    pub pons_deployer: Address,
    pub pons_escrow: Address,
    pub pons_hook: Address,
    pub pons_locker: Address,
    pub weth: Address,
    pub permit2: Address,
    pub v4_pool_manager: Address,
    pub v4_quoter: Address,
    pub v4_state_view: Address,
    pub universal_router: Address,
}

pub const ADDR: Addresses = Addresses {
    pons_factory: address!("0x7eD598BcEf8bd9Edd8C97A195C6d13f40801EC7e"),
    pons_router: address!("0xe33E9E479dF8802cb0866d5d05258bEc4cF62948"),
    pons_deployer: address!("0x3711ceA4feaDE896C913C68F01Eda97Cb06D1A42"),
    pons_escrow: address!("0xd3AFEB2a57f70eF218Aa82451c51B2fb0416Ac9e"),
    pons_hook: address!("0xE5e702641Ea86F4ae6cC3cDaeD2B886f976Be044"),
    pons_locker: address!("0x267444D099b10fB5Ed7c3Cc7B7c767AdcA574952"),
    weth: address!("0x0Bd7D308f8E1639FAb988df18A8011f41EAcAD73"),
    permit2: address!("0x000000000022D473030F116dDEE9F6B43aC78BA3"),
    v4_pool_manager: address!("0x8366a39cc670b4001a1121b8f6a443a643e40951"),
    v4_quoter: address!("0x8dc178efb8111bb0973dd9d722ebeff267c98f94"),
    v4_state_view: address!("0xf3334192d15450cdd385c8b70e03f9a6bd9e673b"),
    universal_router: address!("0x8876789976decbfcbbbe364623c63652db8c0904"),
};

/// Reserved supply is 28.57%. The graduated Uniswap v4 pool gets 20.41% + 4.2 ETH.
/// 8.16% is permanently locked. Do not treat the pool as holding the full reserved share.
pub const GRAD_POOL_SUPPLY_BPS: u64 = 2041;
pub const GRAD_LOCKED_SUPPLY_BPS: u64 = 816;
pub const GRAD_POOL_QUOTE_WEI: u128 = 4_200_000_000_000_000_000;
pub const PHANTOM_QUOTE_WEI: u128 = 1_680_000_000_000_000_000;

pub const BPS: u64 = 10_000;
pub const SUPPLY: u128 = 1_000_000_000 * 10u128.pow(18);

pub fn explorer_tx(h: impl AsRef<str>) -> String {
    format!("{EXPLORER}/tx/{}", h.as_ref())
}
pub fn explorer_address(a: impl AsRef<str>) -> String {
    format!("{EXPLORER}/address/{}", a.as_ref())
}
pub fn explorer_token(a: impl AsRef<str>) -> String {
    format!("{EXPLORER}/token/{}", a.as_ref())
}
