#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <new>
#include <type_traits>
#include <stdexcept>
#include <utility>

// Vyukov's MPMC bounded queue.
// Implementation reference:
//   https://github.com/rigtorp/MPMCQueue
//
// Properties:
// - Multi-Producer Multi-Consumer
// - Bounded (capacity must be power of 2)
// - Lock-free
// - Element T must be default constructible and copyable/movable

template <typename T>
class MpmcQueue {
public:
    explicit MpmcQueue(std::size_t capacity)
        : buffer_(new Cell[capacity]),
          mask_(capacity - 1) {
        if (capacity < 2 || (capacity & (capacity - 1)) != 0) {
            throw std::invalid_argument("MpmcQueue: capacity must be power of 2 and >= 2");
        }

        for (std::size_t i = 0; i < capacity; ++i) {
            buffer_[i].sequence.store(i, std::memory_order_relaxed);
        }

        enqueue_pos_.store(0, std::memory_order_relaxed);
        dequeue_pos_.store(0, std::memory_order_relaxed);
    }

    ~MpmcQueue() {
        delete[] buffer_;
    }

    // Try to enqueue an item. Returns true on success, false if queue is full.
    bool try_enqueue(const T& data) {
        Cell* cell;
        std::size_t pos = enqueue_pos_.load(std::memory_order_relaxed);

        for (;;) {
            cell = &buffer_[pos & mask_];
            std::size_t seq = cell->sequence.load(std::memory_order_acquire);
            intptr_t dif = (intptr_t)seq - (intptr_t)pos;

            if (dif == 0) {
                if (enqueue_pos_.compare_exchange_weak(pos, pos + 1, std::memory_order_relaxed)) {
                    break;
                }
            } else if (dif < 0) {
                // Queue is full
                return false;
            } else {
                pos = enqueue_pos_.load(std::memory_order_relaxed);
            }
        }

        cell->data = data;
        cell->sequence.store(pos + 1, std::memory_order_release);
        return true;
    }

    // Try to dequeue an item. Returns true on success, false if queue is empty.
    bool try_dequeue(T& data) {
        Cell* cell;
        std::size_t pos = dequeue_pos_.load(std::memory_order_relaxed);

        for (;;) {
            cell = &buffer_[pos & mask_];
            std::size_t seq = cell->sequence.load(std::memory_order_acquire);
            intptr_t dif = (intptr_t)seq - (intptr_t)(pos + 1);

            if (dif == 0) {
                if (dequeue_pos_.compare_exchange_weak(pos, pos + 1, std::memory_order_relaxed)) {
                    break;
                }
            } else if (dif < 0) {
                // Queue is empty
                return false;
            } else {
                pos = dequeue_pos_.load(std::memory_order_relaxed);
            }
        }

        data = cell->data;
        cell->sequence.store(pos + mask_ + 1, std::memory_order_release);
        return true;
    }

    // Approximate size
    std::size_t size() const {
        std::size_t head = dequeue_pos_.load(std::memory_order_acquire);
        std::size_t tail = enqueue_pos_.load(std::memory_order_acquire);
        if (tail < head) return 0; // Should not happen unless overflow wrap-around handling is tricky, but here using size_t
        return tail - head;
    }

private:
    struct Cell {
        std::atomic<std::size_t> sequence;
        T data;
    };

    // Use cache line padding to prevent false sharing
    static constexpr size_t kCacheLineSize = 64;
    
    using Pad = char[kCacheLineSize];

    Pad pad0_;
    Cell* const buffer_;
    const std::size_t mask_;
    Pad pad1_;
    std::atomic<std::size_t> enqueue_pos_;
    Pad pad2_;
    std::atomic<std::size_t> dequeue_pos_;
    Pad pad3_;

    MpmcQueue(const MpmcQueue&) = delete;
    void operator=(const MpmcQueue&) = delete;
};
