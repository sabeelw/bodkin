//! pons v2 + Uniswap v4 ABIs. Sources: live factory / verified Blockscout + contractsV2 repo.
#![allow(non_snake_case, clippy::all)]

use alloy::primitives::{b256, B256};
use alloy::sol;

sol! {
    #[sol(rpc)]
    contract factory {
        struct LaunchedToken {
            address token;
            address curve;
            address deployer;
            address creatorFeeRecipient;
            address pairToken;
            uint256 graduationThreshold;
            uint24 poolFee;
            int24 tickSpacing;
            uint16 creatorTaxBps;
            bool buybackEnabled;
            uint8 phase;
            uint256 sweptQuote;
            uint256 sweptTokens;
            uint256 sweptAt;
            bool exists;
        }
        struct LaunchConfig {
            uint256 supply;
            uint256 curveFeeBps;
            uint256 phantomQuote;
            uint256 graduationThreshold;
            uint24 poolFee;
            int24 tickSpacing;
            bool enabled;
        }
        function getLaunchedToken(address token) view returns (LaunchedToken);
        function getLaunchConfig(uint256 id) view returns (LaunchConfig);
        function snipeTaxStartBps() view returns (uint256);
        function snipeTaxSeconds() view returns (uint256);
        function launchFee() view returns (uint256);
        function maxCreatorTaxBps() view returns (uint256);
        function launchEnabled() view returns (bool);
        function feeEscrow() view returns (address);
        function memeHook() view returns (address);
        function poolManager() view returns (address);
        function launchDeployer() view returns (address);
        event TokenLaunched(address indexed token, address indexed curve, address indexed deployer, address pairToken, uint256 launchConfigId, uint256 graduationThreshold);
        event LaunchSwept(address indexed token, uint256 quoteOut, uint256 tokenOut);
        event PoolGraduated(address indexed token, uint256 positionId, uint256 tokenAmount, uint256 pairTokenAmount);
        event SnipeTaxStartBpsUpdated(uint256 startBps);
        event SnipeTaxSecondsUpdated(uint256 seconds_);
    }

    #[sol(rpc)]
    contract curve {
        function buy(uint256 quoteIn, uint256 minTokensOut, address recipient) payable returns (uint256 tokensOut);
        function sell(uint256 tokensIn, uint256 minQuoteOut, address recipient) returns (uint256 quoteOut);
        function getReserves() view returns (uint256 quoteReserve, uint256 tokenReserve);
        function realQuoteReserve() view returns (uint256);
        function sellableTokens() view returns (uint256);
        function reservedTokens() view returns (uint256);
        function graduationThreshold() view returns (uint256);
        function readyToGraduate() view returns (bool);
        function graduated() view returns (bool);
        function feeBps() view returns (uint256);
        function creatorTaxBps() view returns (uint256);
        function isNativeQuote() view returns (bool);
        function pairToken() view returns (address);
        function launchedAt() view returns (uint256);
        function snipeTaxExempt(address account) view returns (bool);
        function currentSnipeTaxBps(address recipient) view returns (uint256);
        function snipeTaxStartBps() view returns (uint256);
        function snipeTaxSeconds() view returns (uint256);
        event CurveBuy(address indexed buyer, address indexed recipient, uint256 quoteIn, uint256 tokensOut, uint256 fee, uint256 tax);
        event CurveSell(address indexed seller, address indexed recipient, uint256 tokensIn, uint256 quoteOut, uint256 fee, uint256 tax);
        event CurveBuyRefunded(address indexed recipient, uint256 refundAmount);
        event CurveCompleted();
        event SnipeTaxExempted(address indexed account);
        event SnipeTaxCharged(address indexed buyer, address indexed recipient, uint256 taxBps, uint256 taxPaid);
    }

    #[sol(rpc)]
    contract token {
        struct Socials {
            string twitter;
            string telegram;
            string discord;
            string website;
            string farcaster;
        }
        function getTokenInfo() view returns (address tokenDeployer, string tokenLogo, string tokenDescription, Socials tokenSocials);
        function name() view returns (string);
        function symbol() view returns (string);
        function decimals() view returns (uint8);
        function totalSupply() view returns (uint256);
        function balanceOf(address owner) view returns (uint256);
        function allowance(address owner, address spender) view returns (uint256);
        function approve(address spender, uint256 amount) returns (bool);
        event Transfer(address indexed from, address indexed to, uint256 value);
    }

    #[sol(rpc)]
    contract escrow {
        function balanceOf(address recipient) view returns (uint256);
        function claim() returns (uint256 amount);
        event Credited(address indexed recipient, address indexed depositor, uint256 amount);
        event Claimed(address indexed recipient, uint256 amount);
    }

    #[sol(rpc)]
    contract router {
        struct Socials {
            string twitter;
            string telegram;
            string discord;
            string website;
            string farcaster;
        }
        struct TokenParams {
            string name;
            string symbol;
            string logo;
            string description;
            Socials socials;
            address creatorFeeRecipient;
            uint16 creatorTaxBps;
            bool buybackEnabled;
            bytes32 expectedEconomics;
            bytes32 salt;
        }
        function launchAndBuy(TokenParams params, uint256 launchConfigId, address pairToken, uint256 quoteIn, uint256 minTokensOut, address recipient, address[] snipeTaxExemptions) payable returns (address token, address curve, uint256 tokensOut);
    }

    #[sol(rpc)]
    contract deployer {
        function predictLaunchAddresses(bytes32 salt, address pairToken, uint256 launchConfigId) view returns (address token, address curve);
    }

    #[sol(rpc)]
    contract helper {
        function buyOnce(address curve, address token, address recipient, uint256 maxTaxBps, uint256 minTokensOut, uint256 maxRealQuote) payable returns (uint256 tokensOut);
    }

    #[sol(rpc)]
    contract multicall3 {
        struct Call3 {
            address target;
            bool allowFailure;
            bytes callData;
        }
        struct Result {
            bool success;
            bytes returnData;
        }
        function aggregate3(Call3[] calldata calls) payable returns (Result[] memory returnData);
    }

    #[sol(rpc)]
    contract v4Quoter {
        struct PoolKey {
            address currency0;
            address currency1;
            uint24 fee;
            int24 tickSpacing;
            address hooks;
        }
        struct QuoteExactSingleParams {
            PoolKey poolKey;
            bool zeroForOne;
            uint128 exactAmount;
            bytes hookData;
        }
        function quoteExactInputSingle(QuoteExactSingleParams params) returns (uint256 amountOut, uint256 gasEstimate);
    }

    #[sol(rpc)]
    contract stateView {
        function getSlot0(bytes32 poolId) view returns (uint160 sqrtPriceX96, int24 tick, uint24 protocolFee, uint24 lpFee);
        function getLiquidity(bytes32 poolId) view returns (uint128 liquidity);
    }

    #[sol(rpc)]
    contract universalRouter {
        function execute(bytes commands, bytes[] inputs, uint256 deadline) payable;
    }

    #[sol(rpc)]
    contract permit2 {
        function approve(address token, address spender, uint160 amount, uint48 expiration);
        function allowance(address user, address token, address spender) view returns (uint160 amount, uint48 expiration, uint48 nonce);
    }
}

/// keccak of `SnipeTaxCharged(address,address,uint256,uint256)` as seen on the live curve.
pub const TOPIC_SNIPE_TAX_CHARGED: B256 =
    b256!("0x3bc39a5562b28f5fe8f36cecabfbaa12bb969acf05717994709225fc412a9934");

pub const PHASE_NAME: [&str; 4] = ["curve", "swept", "pool", "rescued"];

pub mod topics {
    use super::*;
    use alloy::sol_types::SolEvent;
    pub fn token_launched() -> B256 {
        factory::TokenLaunched::SIGNATURE_HASH
    }
    pub fn curve_buy() -> B256 {
        curve::CurveBuy::SIGNATURE_HASH
    }
    pub fn curve_sell() -> B256 {
        curve::CurveSell::SIGNATURE_HASH
    }
    pub fn pool_graduated() -> B256 {
        factory::PoolGraduated::SIGNATURE_HASH
    }
    pub fn snipe_tax_exempted() -> B256 {
        curve::SnipeTaxExempted::SIGNATURE_HASH
    }
}

pub mod selectors {
    use super::*;
    use alloy::sol_types::SolCall;
    pub fn launch_and_buy() -> [u8; 4] {
        router::launchAndBuyCall::SELECTOR
    }
}

pub fn launch_and_buy_selector() -> [u8; 4] {
    selectors::launch_and_buy()
}
