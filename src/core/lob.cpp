#include "lob.h"
#include <algorithm>

std::vector<Trade> LimitOrderBook::process_order(const Order& order) {
    std::vector<Trade> trades;
    Order remaining_order = order;

    if (remaining_order.type == OrderType::CANCEL) {
        auto it = order_map.find(remaining_order.order_id);
        if (it != order_map.end()) {
            uint64_t price = it->second.first;
            Side side = it->second.second;
            
            auto& book = (side == Side::BUY) ? bids[price] : asks[price];
            auto order_it = std::find_if(book.begin(), book.end(), [&](const Order& o) {
                return o.order_id == remaining_order.order_id;
            });
            
            if (order_it != book.end()) {
                book.erase(order_it);
                if (book.empty()) {
                    if (side == Side::BUY) bids.erase(price);
                    else asks.erase(price);
                }
            }
            order_map.erase(it);
        }
        return trades;
    }

    if (remaining_order.side == Side::BUY) {
        match_bid(remaining_order, trades);
        if (remaining_order.quantity > 0 && remaining_order.type == OrderType::LIMIT) {
            bids[remaining_order.price].push_back(remaining_order);
            order_map[remaining_order.order_id] = {remaining_order.price, Side::BUY};
        }
    } else {
        match_ask(remaining_order, trades);
        if (remaining_order.quantity > 0 && remaining_order.type == OrderType::LIMIT) {
            asks[remaining_order.price].push_back(remaining_order);
            order_map[remaining_order.order_id] = {remaining_order.price, Side::SELL};
        }
    }

    return trades;
}

void LimitOrderBook::match_bid(Order& order, std::vector<Trade>& trades) {
    auto it = asks.begin();
    while (it != asks.end() && order.quantity > 0 && 
           (order.type == OrderType::MARKET || it->first <= order.price)) {
        
        auto& price_level = it->second;
        
        while (!price_level.empty() && order.quantity > 0) {
            Order& maker = price_level.front();
            uint32_t traded_qty = std::min(order.quantity, maker.quantity);

            trades.push_back({
                maker.order_id,
                order.order_id,
                it->first, // executed at maker price
                traded_qty,
                maker.ingestion_ts,
                order.ingestion_ts
            });

            maker.quantity -= traded_qty;
            order.quantity -= traded_qty;

            if (maker.quantity == 0) {
                order_map.erase(maker.order_id);
                price_level.pop_front();
            }
        }
        
        if (price_level.empty()) {
            it = asks.erase(it);
        } else {
            ++it;
        }
    }
}

void LimitOrderBook::match_ask(Order& order, std::vector<Trade>& trades) {
    auto it = bids.begin();
    while (it != bids.end() && order.quantity > 0 && 
           (order.type == OrderType::MARKET || it->first >= order.price)) {
        
        auto& price_level = it->second;
        
        while (!price_level.empty() && order.quantity > 0) {
            Order& maker = price_level.front();
            uint32_t traded_qty = std::min(order.quantity, maker.quantity);

            trades.push_back({
                maker.order_id,
                order.order_id,
                it->first, // executed at maker price
                traded_qty,
                maker.ingestion_ts,
                order.ingestion_ts
            });

            maker.quantity -= traded_qty;
            order.quantity -= traded_qty;

            if (maker.quantity == 0) {
                order_map.erase(maker.order_id);
                price_level.pop_front();
            }
        }
        
        if (price_level.empty()) {
            it = bids.erase(it);
        } else {
            ++it;
        }
    }
}
