#pragma once

#include "types.h"
#include <map>
#include <deque>
#include <vector>

class LimitOrderBook {
public:
    LimitOrderBook() = default;

    // Process a new order and return generated trades
    std::vector<Trade> process_order(const Order& order);

private:
    // Bids (Buy): Highest price first
    std::map<uint64_t, std::deque<Order>, std::greater<uint64_t>> bids;
    
    // Asks (Sell): Lowest price first
    std::map<uint64_t, std::deque<Order>> asks;

    // Track resting orders: order_id -> {price, side}
    std::map<uint64_t, std::pair<uint64_t, Side>> order_map;

    void match_bid(Order& order, std::vector<Trade>& trades);
    void match_ask(Order& order, std::vector<Trade>& trades);
};
