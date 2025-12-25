#pragma once

#include <cstring>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <new>
#include <optional>
#include <stdexcept>
#include <type_traits>
#include <utility>

// Single-producer single-consumer bounded ring buffer.
// Capacity must be power of two and >= 2.
template <class T>
class SpscQueue {
public:
    using value_type = T;

    explicit SpscQueue(std::size_t capacity_pow2)
        : capacity_(capacity_pow2),
          mask_(capacity_pow2 - 1),
          buffer_(std::make_unique<Cell[]>(capacity_pow2)) {
        if (capacity_pow2 < 2 || (capacity_pow2 & (capacity_pow2 - 1)) != 0) {
            throw std::invalid_argument("SpscQueue: capacity must be power-of-two and >= 2");
        }
        head_.store(0, std::memory_order_relaxed);
        tail_.store(0, std::memory_order_relaxed);
    }

    SpscQueue(const SpscQueue&) = delete;
    SpscQueue& operator=(const SpscQueue&) = delete;

    ~SpscQueue() {
        drain_discard();
    }

    [[nodiscard]] std::size_t capacity() const noexcept { return capacity_; }

    [[nodiscard]] std::size_t size() const noexcept {
        const std::size_t head = head_.load(std::memory_order_relaxed);
        const std::size_t tail = tail_.load(std::memory_order_relaxed);
        if (tail < head) return 0; // Prevent underflow due to race reading
        return tail - head;
    }

    template <class... Args>
        requires(std::is_nothrow_constructible_v<T, Args...>)
    [[nodiscard]] bool try_emplace(Args&&... args) noexcept {
        const std::size_t tail = tail_.load(std::memory_order_relaxed);
        // Optimization: check cached head first
        if (tail - head_cache_ == capacity_) {
            // Only load real head if we think we are full
            head_cache_ = head_.load(std::memory_order_acquire);
            if (tail - head_cache_ == capacity_) {
                return false;
            }
        }

        Cell* cell = &buffer_[tail & mask_];
        ::new (cell->ptr()) T(std::forward<Args>(args)...);
        tail_.store(tail + 1, std::memory_order_release);
        return true;
    }

    [[nodiscard]] bool try_enqueue(const T& v)
        noexcept(std::is_nothrow_copy_constructible_v<T>) {
        return try_emplace(v);
    }

    [[nodiscard]] bool try_enqueue(T&& v)
        noexcept(std::is_nothrow_move_constructible_v<T>) {
        return try_emplace(std::move(v));
    }

    // Optimized bulk enqueue for raw pointers (User suggested optimization)
    // NOTE: This implementation has "All or Nothing" semantics for the requested count
    [[nodiscard]] std::size_t enqueue_bulk(const T* src, std::size_t count) noexcept {
        if (count == 0) return 0;

        const std::size_t tail = tail_.load(std::memory_order_relaxed);
        std::size_t used = tail - head_cache_;

        if (used + count > capacity_) {
            head_cache_ = head_.load(std::memory_order_acquire);
            used = tail - head_cache_;
            if (used + count > capacity_) {
                return 0;
            }
        }

        const std::size_t n = count; // All or nothing
        const std::size_t pos = tail & mask_;
        const std::size_t first = std::min(n, capacity_ - pos);

        if constexpr (std::is_trivially_copyable_v<T>) {
            std::memcpy(buffer_[pos].ptr(), src, first * sizeof(T));
            if (first < n) {
                std::memcpy(buffer_[0].ptr(), src + first, (n - first) * sizeof(T));
            }
        } else {
             for (std::size_t i = 0; i < n; ++i) {
                Cell* cell = &buffer_[(tail + i) & mask_];
                ::new (cell->ptr()) T(src[i]);
            }
        }

        tail_.store(tail + n, std::memory_order_release);
        return n;
    }

    // Batch enqueue for high throughput
    // Returns number of items enqueued (up to count)
    template <typename InputIt>
    [[nodiscard]] std::size_t enqueue_bulk(InputIt first, std::size_t count) noexcept
        requires(std::is_nothrow_copy_constructible_v<T>) {
        if (count == 0) return 0;

        const std::size_t tail = tail_.load(std::memory_order_relaxed);

        // Refresh head_cache_ if needed
        if (tail - head_cache_ == capacity_) {
            head_cache_ = head_.load(std::memory_order_acquire);
            if (tail - head_cache_ == capacity_) {
                return 0;
            }
        }

        const std::size_t available = capacity_ - (tail - head_cache_);
        const std::size_t n = std::min(available, count);

        if constexpr (std::is_trivially_copyable_v<T> && std::is_pointer_v<InputIt>) {
            // Memcpy optimization for trivially copyable types with pointer input
            const std::size_t write_idx = tail & mask_;
            const std::size_t to_end = capacity_ - write_idx;

            if (n <= to_end) {
                std::memcpy(&buffer_[write_idx], first, n * sizeof(T));
            } else {
                std::memcpy(&buffer_[write_idx], first, to_end * sizeof(T));
                std::memcpy(&buffer_[0], first + to_end, (n - to_end) * sizeof(T));
            }
        } else {
            // Fallback loop
            for (std::size_t i = 0; i < n; ++i) {
                Cell* cell = &buffer_[(tail + i) & mask_];
                ::new (cell->ptr()) T(*first++);
            }
        }

        // Commit all writes with a single atomic store
        tail_.store(tail + n, std::memory_order_release);
        return n;
    }

    // Batch dequeue for high throughput
    // Returns number of items dequeued (up to max_count)
    template <typename OutputIt>
    [[nodiscard]] std::size_t dequeue_bulk(OutputIt out_it, std::size_t max_count) noexcept
        requires(std::is_nothrow_move_assignable_v<T> && std::is_nothrow_destructible_v<T>) {
        if (max_count == 0) return 0;

        const std::size_t head = head_.load(std::memory_order_relaxed);
        
        // Refresh tail_cache_ if needed
        if (tail_cache_ == head) {
            tail_cache_ = tail_.load(std::memory_order_acquire);
            if (tail_cache_ == head) {
                return 0;
            }
        }

        const std::size_t available = tail_cache_ - head;
        const std::size_t count = std::min(available, max_count);

        if constexpr (std::is_trivially_copyable_v<T> && std::is_pointer_v<OutputIt>) {
             // Memcpy optimization
             const std::size_t read_idx = head & mask_;
             const std::size_t to_end = capacity_ - read_idx;
 
             if (count <= to_end) {
                 std::memcpy(out_it, &buffer_[read_idx], count * sizeof(T));
             } else {
                 std::memcpy(out_it, &buffer_[read_idx], to_end * sizeof(T));
                 std::memcpy(out_it + to_end, &buffer_[0], (count - to_end) * sizeof(T));
             }
        } else {
             for (std::size_t i = 0; i < count; ++i) {
                 Cell* cell = &buffer_[(head + i) & mask_];
                 T* p = cell->as_ptr();
                 *out_it++ = std::move(*p);
                 p->~T();
             }
        }

        // Commit all reads with a single atomic store
        head_.store(head + count, std::memory_order_release);
        return count;
    }

    [[nodiscard]] bool try_dequeue(T& out) noexcept
        requires(std::is_nothrow_move_assignable_v<T> && std::is_nothrow_destructible_v<T>) {
        const std::size_t head = head_.load(std::memory_order_relaxed);
        // Optimization: check cached tail first
        if (tail_cache_ == head) {
            // Only load real tail if we think we are empty
            tail_cache_ = tail_.load(std::memory_order_acquire);
            if (tail_cache_ == head) {
                return false;
            }
        }

        Cell* cell = &buffer_[head & mask_];
        T* p = cell->as_ptr();
        out = std::move(*p);
        p->~T();
        head_.store(head + 1, std::memory_order_release);
        return true;
    }

    [[nodiscard]] std::optional<T> try_dequeue() noexcept
        requires(std::is_nothrow_move_constructible_v<T> && std::is_nothrow_destructible_v<T>) {
        const std::size_t head = head_.load(std::memory_order_relaxed);
        // Optimization: check cached tail first
        if (tail_cache_ == head) {
            tail_cache_ = tail_.load(std::memory_order_acquire);
            if (tail_cache_ == head) {
                return std::nullopt;
            }
        }

        Cell* cell = &buffer_[head & mask_];
        T* p = cell->as_ptr();
        std::optional<T> out{std::in_place, std::move(*p)};
        p->~T();
        head_.store(head + 1, std::memory_order_release);
        return out;
    }

    [[nodiscard]] bool try_dequeue_discard() noexcept
        requires(std::is_nothrow_destructible_v<T>) {
        const std::size_t head = head_.load(std::memory_order_relaxed);
        if (tail_cache_ == head) {
            tail_cache_ = tail_.load(std::memory_order_acquire);
            if (tail_cache_ == head) {
                return false;
            }
        }

        Cell* cell = &buffer_[head & mask_];
        if constexpr (!std::is_trivially_destructible_v<T>) {
            cell->as_ptr()->~T();
        }
        head_.store(head + 1, std::memory_order_release);
        return true;
    }

    void drain_discard() noexcept {
        while (try_dequeue_discard()) { }
    }

private:
    static constexpr std::size_t kCacheLine =
#if defined(__cpp_lib_hardware_interference_size)
        std::hardware_destructive_interference_size;
#else
        64;
#endif

    struct Cell {
        alignas(T) std::byte storage[sizeof(T)];
        void* ptr() noexcept { return static_cast<void*>(storage); }
        T* as_ptr() noexcept { return std::launder(reinterpret_cast<T*>(storage)); }
    };

    const std::size_t capacity_;
    const std::size_t mask_;
    std::unique_ptr<Cell[]> buffer_;

    // Producer part
    alignas(kCacheLine) std::atomic<std::size_t> tail_{0};
    std::size_t head_cache_{0}; // Producer's view of head

    // Padding to ensure Consumer part is on a different cache line
    char pad1_[kCacheLine - sizeof(std::atomic<std::size_t>) - sizeof(std::size_t)];

    // Consumer part
    alignas(kCacheLine) std::atomic<std::size_t> head_{0};
    std::size_t tail_cache_{0}; // Consumer's view of tail
};
