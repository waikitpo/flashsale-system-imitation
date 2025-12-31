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

    // Producer only - Batch Enqueue
    bool try_enqueue_batch(const T* items, size_t count) {
        const size_t current_tail = tail_.load(std::memory_order_relaxed);
        const size_t current_head = head_.load(std::memory_order_acquire);
        
        size_t free_space;
        if (current_head > current_tail) {
            free_space = current_head - current_tail - 1;
        } else {
            free_space = buffer_.size() - (current_tail - current_head) - 1;
        }

        if (count > free_space) {
            return false; // Not enough space
        }

        // Copy items
        size_t first_chunk = std::min(count, buffer_.size() - current_tail);
        for (size_t i = 0; i < first_chunk; ++i) {
            buffer_[current_tail + i] = items[i];
        }
        
        if (first_chunk < count) {
            size_t second_chunk = count - first_chunk;
            for (size_t i = 0; i < second_chunk; ++i) {
                buffer_[i] = items[first_chunk + i];
            }
        }

        tail_.store((current_tail + count) % buffer_.size(), std::memory_order_release);
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

    // Consumer only - Batch Dequeue
    size_t try_dequeue_batch(T* items, size_t max_count) {
        const size_t current_head = head_.load(std::memory_order_relaxed);
        const size_t current_tail = tail_.load(std::memory_order_acquire);
        
        if (current_head == current_tail) {
            return 0; // Empty
        }

        size_t available;
        if (current_tail >= current_head) {
            available = current_tail - current_head;
        } else {
            available = buffer_.size() - current_head + current_tail;
        }

        size_t count = std::min(available, max_count);
        
        // Copy items
        size_t first_chunk = std::min(count, buffer_.size() - current_head);
        
        for (size_t i = 0; i < first_chunk; ++i) {
            items[i] = buffer_[current_head + i];
        }
        
        if (first_chunk < count) {
            // Wrap around
            size_t second_chunk = count - first_chunk;
            for (size_t i = 0; i < second_chunk; ++i) {
                items[first_chunk + i] = buffer_[i];
            }
        }

        head_.store((current_head + count) % buffer_.size(), std::memory_order_release);
        return count;
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
