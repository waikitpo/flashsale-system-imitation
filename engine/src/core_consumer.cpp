#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <memory>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#if defined(_MSC_VER) && (defined(_M_IX86) || defined(_M_X64))
#include <immintrin.h>
#elif defined(__i386__) || defined(__x86_64__)
#include <immintrin.h>
#endif

#include "spsc_queue.hpp"

// Minimal order intent payload.
struct OrderIntent {
    uint64_t request_id;
    uint64_t user_id;
    uint32_t sku_id;
    uint32_t qty;
    uint64_t ts_ns;
};

// Simple backoff for empty queues.
class Backoff {
public:
    void pause() noexcept {
        if (spins_ < kSpinLimit) {
            ++spins_;
#if defined(_MSC_VER) && (defined(_M_IX86) || defined(_M_X64))
            _mm_pause();
#elif defined(__i386__) || defined(__x86_64__)
            _mm_pause();
#endif
        } else {
            std::this_thread::sleep_for(std::chrono::microseconds(50));
        }
    }

    void reset() noexcept { spins_ = 0; }

private:
    static constexpr int kSpinLimit = 64;
    int spins_{0};
};

// NATS JetStream pull client stub. Replace with real NATS client integration.
class NatsPullClient {
public:
    struct Msg {
        std::string_view data;
        void (*ack_fn)(void* ctx) = nullptr;
        void (*term_fn)(void* ctx) = nullptr;
        void* ctx = nullptr;

        void ack() const noexcept { if (ack_fn) ack_fn(ctx); }
        void term() const noexcept { if (term_fn) term_fn(ctx); }
    };

    bool fetch_batch(std::vector<Msg>& out, std::size_t max_msgs,
                     std::chrono::milliseconds timeout) {
        (void)out; (void)max_msgs; (void)timeout;
        return false; // TODO: wire to nats.c / jetstream pull
    }
};

// Mock NATS pull client for local testing.
class MockPullClient {
public:
    using Msg = NatsPullClient::Msg;

    bool fetch_batch(std::vector<Msg>& out, std::size_t max_msgs,
                     std::chrono::milliseconds timeout) {
        (void)timeout;
        payloads_.clear();
        payloads_.reserve(max_msgs);
        out.clear();
        out.reserve(max_msgs);

        for (std::size_t i = 0; i < max_msgs; ++i) {
            const uint64_t request_id = seq_;
            const uint64_t user_id = request_id % 1024;
            const uint32_t sku_id = static_cast<uint32_t>(request_id % 50) + 1;
            const uint32_t qty = 1;
            const uint64_t ts = request_id;

            payloads_.push_back(std::to_string(request_id) + "," +
                                std::to_string(user_id) + "," +
                                std::to_string(sku_id) + "," +
                                std::to_string(qty) + "," +
                                std::to_string(ts));

            Msg msg;
            msg.data = std::string_view(payloads_.back());
            out.push_back(msg);
            ++seq_;
        }

        return true;
    }

private:
    uint64_t seq_{0};
    std::vector<std::string> payloads_;
};

static bool use_mock_mode(int argc, char** argv) {
    bool use_mock = true;
    if (const char* env = std::getenv("CORE_NATS_MODE")) {
        if (std::strcmp(env, "real") == 0) {
            use_mock = false;
        } else if (std::strcmp(env, "mock") == 0) {
            use_mock = true;
        }
    }
    for (int i = 1; i < argc; ++i) {
        if (std::strcmp(argv[i], "--real") == 0) {
            use_mock = false;
        } else if (std::strcmp(argv[i], "--mock") == 0) {
            use_mock = true;
        }
    }
    return use_mock;
}

static inline bool parse_u64(std::string_view s, uint64_t& out) {
    if (s.empty()) return false;
    uint64_t val = 0;
    for (char c : s) {
        if (c < '0' || c > '9') return false;
        val = val * 10 + static_cast<uint64_t>(c - '0');
    }
    out = val;
    return true;
}

static inline bool parse_u32(std::string_view s, uint32_t& out) {
    uint64_t tmp = 0;
    if (!parse_u64(s, tmp)) return false;
    if (tmp > 0xFFFFFFFFu) return false;
    out = static_cast<uint32_t>(tmp);
    return true;
}

// Simple CSV: request_id,user_id,sku_id,qty,ts(optional)
static inline bool parse_intent(std::string_view payload, OrderIntent& out) {
    std::string_view parts[5]{};
    std::size_t count = 0;
    std::size_t start = 0;
    for (std::size_t i = 0; i <= payload.size(); ++i) {
        if (i == payload.size() || payload[i] == ',') {
            if (count < 5) {
                parts[count++] = payload.substr(start, i - start);
            }
            start = i + 1;
        }
    }

    if (count < 4) return false;
    if (!parse_u64(parts[0], out.request_id)) return false;
    if (!parse_u64(parts[1], out.user_id)) return false;
    if (!parse_u32(parts[2], out.sku_id)) return false;
    if (!parse_u32(parts[3], out.qty)) return false;
    out.ts_ns = 0;
    if (count >= 5 && !parts[4].empty()) {
        if (!parse_u64(parts[4], out.ts_ns)) return false;
    }
    return true;
}

static inline std::size_t route_index(const OrderIntent& in, std::size_t workers) {
    const uint64_t key = (in.user_id != 0) ? in.user_id : in.request_id;
    return static_cast<std::size_t>(key % workers);
}

static inline void process_order(const OrderIntent& in) {
    (void)in;
    // TODO: idempotency, Redis, MySQL write, etc.
}

int main(int argc, char** argv) {
    constexpr std::size_t kWorkers = 4;
    constexpr std::size_t kQueueCap = 1u << 14; // power of two
    constexpr std::size_t kMaxBatch = 256;

    const bool use_mock = use_mock_mode(argc, argv);
    if (use_mock) {
        std::cout << "Mode: mock NATS (set CORE_NATS_MODE=real or --real for real client)\n";
    } else {
        std::cout << "Mode: real NATS client (stub)\n";
    }
    NatsPullClient nats_real;
    MockPullClient nats_mock;

    std::vector<std::unique_ptr<SpscQueue<OrderIntent>>> queues;
    queues.reserve(kWorkers);
    for (std::size_t i = 0; i < kWorkers; ++i) {
        queues.push_back(std::make_unique<SpscQueue<OrderIntent>>(kQueueCap));
    }

    std::atomic<bool> running{true};

    std::vector<std::thread> workers;
    workers.reserve(kWorkers);
    for (std::size_t i = 0; i < kWorkers; ++i) {
        workers.emplace_back([&, i] {
            Backoff backoff;
            while (true) {
                OrderIntent in{};
                if (queues[i]->try_dequeue(in)) {
                    backoff.reset();
                    process_order(in);
                    continue;
                }
                if (!running.load(std::memory_order_acquire)) {
                    break;
                }
                backoff.pause();
            }

            OrderIntent in{};
            while (queues[i]->try_dequeue(in)) {
                process_order(in);
            }
        });
    }

    std::vector<NatsPullClient::Msg> batch;
    batch.reserve(kMaxBatch);

    while (running.load(std::memory_order_acquire)) {
        batch.clear();
        const bool ok = use_mock
            ? nats_mock.fetch_batch(batch, kMaxBatch, std::chrono::milliseconds(10))
            : nats_real.fetch_batch(batch, kMaxBatch, std::chrono::milliseconds(10));
        if (!ok) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
            continue;
        }

        for (const auto& msg : batch) {
            OrderIntent in{};
            if (!parse_intent(msg.data, in)) {
                msg.term();
                continue;
            }

            const std::size_t idx = route_index(in, kWorkers);
            if (queues[idx]->try_enqueue(in)) {
                msg.ack();
            } else {
                // Reject policy: drop when full.
                msg.term();
            }
        }
    }

    running.store(false, std::memory_order_release);
    for (auto& t : workers) t.join();

    return 0;
}
