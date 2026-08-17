#pragma once

#include <cstdint>

// Fixed-size binary structs for zero-parse IPC.
// Packed to avoid padding issues between Node.js and C++.
#pragma pack(push, 1)

enum class Side : uint8_t {
    BUY = 1,
    SELL = 2
};

enum class OrderType : uint8_t {
    LIMIT = 1,
    MARKET = 2,
    CANCEL = 3
};

struct Order {
    uint64_t order_id; // Unique ID (hash of UUID)
    uint64_t price;    // Price in ticks (e.g., cents)
    uint32_t quantity; // Quantity of asset
    Side side;         // BUY or SELL
    OrderType type;    // LIMIT, MARKET, CANCEL
    uint64_t ingestion_ts; // Microsecond timestamp when TCP gateway read this
};

struct Trade {
    uint64_t maker_order_id; // The order that rested on the book
    uint64_t taker_order_id; // The order that crossed the spread
    uint64_t price;          // Price in ticks
    uint32_t quantity;       // Executed quantity
    uint64_t maker_ingestion_ts;
    uint64_t taker_ingestion_ts;
};

#pragma pack(pop)
