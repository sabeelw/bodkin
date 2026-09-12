// SPDX-License-Identifier: MIT
pragma solidity ^0.8.24;

import {Test} from "forge-std/Test.sol";
import {BodkinBuyOnce} from "../src/BodkinBuyOnce.sol";

contract MockToken {
    mapping(address => uint256) public balanceOf;
    function mint(address to, uint256 amt) external {
        balanceOf[to] = amt;
    }
}

contract MockCurve {
    uint256 public taxBps;
    uint256 public realQuote;
    uint256 public lastQuoteIn;
    address public lastRecipient;
    bool public refundDust;

    function set(uint256 tax, uint256 real, bool dust) external {
        taxBps = tax;
        realQuote = real;
        refundDust = dust;
    }

    function currentSnipeTaxBps(address) external view returns (uint256) {
        return taxBps;
    }

    function realQuoteReserve() external view returns (uint256) {
        return realQuote;
    }

    function buy(uint256 quoteIn, uint256, address recipient) external payable returns (uint256) {
        lastQuoteIn = quoteIn;
        lastRecipient = recipient;
        if (refundDust && msg.value > 1 wei) {
            payable(msg.sender).transfer(1 wei);
        }
        return 1e18;
    }
}

contract BodkinBuyOnceTest is Test {
    BodkinBuyOnce helper;
    MockCurve curve;
    MockToken token;
    address recip = address(0xB0B);

    function setUp() public {
        helper = new BodkinBuyOnce();
        curve = new MockCurve();
        token = new MockToken();
        vm.deal(address(this), 10 ether);
    }

    function test_reverts_when_tax_too_high() public {
        curve.set(618, 0, false);
        vm.expectRevert(abi.encodeWithSelector(BodkinBuyOnce.TaxTooHigh.selector, 618, 19));
        helper.buyOnce{value: 0.01 ether}(address(curve), address(token), recip, 19, 1, 0);
    }

    function test_reverts_when_already_bought() public {
        curve.set(19, 0, false);
        token.mint(recip, 1);
        vm.expectRevert(abi.encodeWithSelector(BodkinBuyOnce.AlreadyBought.selector, recip));
        helper.buyOnce{value: 0.01 ether}(address(curve), address(token), recip, 300, 1, 0);
    }

    function test_happy_path_and_refund() public {
        curve.set(19, 0, true);
        uint256 before = address(this).balance;
        uint256 out = helper.buyOnce{value: 0.01 ether}(address(curve), address(token), recip, 300, 1, 0);
        assertEq(out, 1e18);
        assertEq(curve.lastRecipient(), recip);
        assertEq(curve.lastQuoteIn(), 0.01 ether);
        assertEq(address(this).balance, before - 0.01 ether + 1 wei);
    }

    function test_reserve_cap() public {
        curve.set(19, 5 ether, false);
        vm.expectRevert(abi.encodeWithSelector(BodkinBuyOnce.ReserveCap.selector, 5 ether, 4.2 ether));
        helper.buyOnce{value: 0.01 ether}(address(curve), address(token), recip, 300, 1, 4.2 ether);
    }

    receive() external payable {}
}
