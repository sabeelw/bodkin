// SPDX-License-Identifier: MIT
pragma solidity ^0.8.24;

interface IPonsCurve {
    function buy(uint256 quoteIn, uint256 minTokensOut, address recipient) external payable returns (uint256 tokensOut);
    function currentSnipeTaxBps(address recipient) external view returns (uint256);
    function realQuoteReserve() external view returns (uint256);
}

interface IERC20 {
    function balanceOf(address account) external view returns (uint256);
}

/// One-shot curve buy. Reverts unless the tax is legal and the recipient has not already bought.
/// Leftover ETH (curve refund on a clamp) is returned to `msg.sender`.
contract BodkinBuyOnce {
    error TaxTooHigh(uint256 tax, uint256 maxTaxBps);
    error AlreadyBought(address recipient);
    error ReserveCap(uint256 real, uint256 maxRealQuote);
    error RefundFailed();

    function buyOnce(
        address curve,
        address token,
        address recipient,
        uint256 maxTaxBps,
        uint256 minTokensOut,
        uint256 maxRealQuote
    ) external payable returns (uint256 tokensOut) {
        uint256 tax = IPonsCurve(curve).currentSnipeTaxBps(recipient);
        if (tax > maxTaxBps) revert TaxTooHigh(tax, maxTaxBps);
        if (IERC20(token).balanceOf(recipient) != 0) revert AlreadyBought(recipient);
        if (maxRealQuote != 0) {
            uint256 real = IPonsCurve(curve).realQuoteReserve();
            if (real > maxRealQuote) revert ReserveCap(real, maxRealQuote);
        }
        tokensOut = IPonsCurve(curve).buy{value: msg.value}(msg.value, minTokensOut, recipient);
        uint256 left = address(this).balance;
        if (left > 0) {
            (bool ok,) = payable(msg.sender).call{value: left}("");
            if (!ok) revert RefundFailed();
        }
    }

    receive() external payable {}
}
