#include <iostream>
#include <cassert>
#include "../src/core/lob.h"

void test_limit_order_matching() {
    LimitOrderBook lob;
    
    // Add SELL order
    Order sell_order{1, 10000, 5, Side::SELL, OrderType::LIMIT};
    auto trades1 = lob.process_order(sell_order);
    assert(trades1.empty()); // Should rest on book

    // Add BUY order crossing the spread
    Order buy_order{2, 10000, 3, Side::BUY, OrderType::LIMIT};
    auto trades2 = lob.process_order(buy_order);
    
    assert(trades2.size() == 1);
    assert(trades2[0].maker_order_id == 1);
    assert(trades2[0].taker_order_id == 2);
    assert(trades2[0].quantity == 3);
    assert(trades2[0].price == 10000);

    std::cout << "[PASS] test_limit_order_matching" << std::endl;
}

void test_market_order() {
    LimitOrderBook lob;
    
    Order sell_order{1, 10000, 10, Side::SELL, OrderType::LIMIT};
    lob.process_order(sell_order);

    Order market_buy{2, 0, 5, Side::BUY, OrderType::MARKET};
    auto trades = lob.process_order(market_buy);
    
    assert(trades.size() == 1);
    assert(trades[0].quantity == 5);
    assert(trades[0].price == 10000);

    std::cout << "[PASS] test_market_order" << std::endl;
}

void test_cancel_order() {
    LimitOrderBook lob;
    
    Order buy_order{1, 9000, 10, Side::BUY, OrderType::LIMIT};
    lob.process_order(buy_order);

    Order cancel_order{1, 0, 0, Side::BUY, OrderType::CANCEL};
    auto trades = lob.process_order(cancel_order);
    assert(trades.empty()); // Canceled successfully

    Order market_sell{2, 0, 10, Side::SELL, OrderType::MARKET};
    auto trades2 = lob.process_order(market_sell);
    assert(trades2.empty()); // Nothing to match against

    std::cout << "[PASS] test_cancel_order" << std::endl;
}

int main() {
    std::cout << "Running LOB Unit Tests..." << std::endl;
    test_limit_order_matching();
    test_market_order();
    test_cancel_order();
    std::cout << "All tests passed successfully!" << std::endl;
    return 0;
}
