#pragma once

#include <atomic>
#include <vector>
#include <cstddef>
#include <new>

// Determine cache line size for padding
#if defined(__cpp_lib_hardware_interference_size)
    using std::hardware_destructive_interference_size;
#else
    constexpr std::size_t hardware_destructive_interference_size = 64;
#endif

template<typename T>
class SpscQueue {
public:
    explicit SpscQueue(size_t capacity) 
        : buffer_(capacity + 1) // +1 to distinguish empty from full
    {
    }

    // Producer only
    bool try_enqueue(const T& item) {
        const size_t current_tail = tail_.load(std::memory_order_relaxed);
        const size_t next_tail = (current_tail + 1) % buffer_.size();
        
        if (next_tail == head_.load(std::memory_order_acquire)) {
            return false; // Full
        }

        buffer_[current_tail] = item;
        tail_.store(next_tail, std::memory_order_release);
        return true;
    }

    // Consumer only
    bool try_dequeue(T& item) {
        const size_t current_head = head_.load(std::memory_order_relaxed);
        
        if (current_head == tail_.load(std::memory_order_acquire)) {
            return false; // Empty
        }

        item = buffer_[current_head];
        head_.store((current_head + 1) % buffer_.size(), std::memory_order_release);
        return true;
    }

    // Thread-safe size approximation
    size_t size() const {
        size_t head = head_.load(std::memory_order_acquire);
        size_t tail = tail_.load(std::memory_order_acquire);
        if (tail >= head) return tail - head;
        return buffer_.size() - (head - tail);
    }

private:
    alignas(hardware_destructive_interference_size) std::atomic<size_t> head_{0};
    alignas(hardware_destructive_interference_size) std::atomic<size_t> tail_{0};
    
    // Padding to separate tail from buffer start to avoid false sharing with tail
    char pad_[hardware_destructive_interference_size];
    
    std::vector<T> buffer_;
};
