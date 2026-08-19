#pragma once

#include <atomic>
#include <vector>
#include <cstdint>

template<typename T, size_t Size>
class RingBuffer {
public:
    RingBuffer() : head(0), tail(0) {}

    bool push(const T& item) {
        size_t current_tail = tail.load(std::memory_order_relaxed);
        size_t next_tail = (current_tail + 1) % Size;
        
        if (next_tail == head.load(std::memory_order_acquire)) {
            return false; // Full
        }
        
        buffer[current_tail] = item;
        tail.store(next_tail, std::memory_order_release);
        return true;
    }

    bool pop(T& item) {
        size_t current_head = head.load(std::memory_order_relaxed);
        
        if (current_head == tail.load(std::memory_order_acquire)) {
            return false; // Empty
        }
        
        item = buffer[current_head];
        head.store((current_head + 1) % Size, std::memory_order_release);
        return true;
    }

private:
    T buffer[Size];
    std::atomic<size_t> head;
    std::atomic<size_t> tail;
};
