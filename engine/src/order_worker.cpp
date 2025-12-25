// order_worker.cpp (C++23)
// 目标：展示 mpmc_queue + backoff + tracing 三者如何在“订单 worker”里协作
// - 多 producer: 模拟网关/HTTP 线程产生 OrderIntent
// - 多 consumer(worker): 从队列取单 -> 幂等 -> “写 MySQL(模拟)”
// - tracing 输出 Chrome trace JSON：trace.json
//
// 你需要：把你自己实现的三个头文件/实现包含进来：
//   - mpmc_queue.hpp (或 .cpp 编译进来)
//   - backoff.hpp/.cpp
//   - tracing.hpp/.cpp
//
// 下面我假设你有类似命名空间：
//   seckill::core::Backoff
//   seckill::core::tracing::{set_config, flush_start, flush_thread, flush_end, TRACE_* 宏}
// 如果你的命名略不同，你只需要改 include 和 namespace 即可。

#include <atomic>
#include <cassert>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <random>
#include <thread>
#include <vector>
#include <unordered_set>
#include <mutex>

// ==== 你自己的模块 ====
#include "mpmc_queue.hpp"   // VyukovMPMCQueue<T> 或你自己的队列
#include "backoff.hpp"      // seckill::core::Backoff
#include "tracing.hpp"      // seckill::core::tracing + TRACE_* 宏

using namespace std::chrono_literals;

// -----------------------------
// 业务 payload：最小订单意图
// -----------------------------
struct OrderIntent {
    uint64_t request_id;   // 幂等主键
    uint64_t user_id;
    uint32_t sku_id;
    uint32_t qty;          // 秒杀通常 = 1
    uint64_t enqueue_ts_ns;
};

// -----------------------------
// 幂等存储（测试用）：用一个并发安全的 set 模拟
// 真实系统：MySQL unique key / Redis setnx / 幂等表
// -----------------------------
class IdempotencyTable {
public:
    bool try_mark(uint64_t request_id) {
        std::lock_guard<std::mutex> lk(mu_);
        auto [it, inserted] = seen_.insert(request_id);
        return inserted;
    }
private:
    std::mutex mu_;
    std::unordered_set<uint64_t> seen_;
};

// -----------------------------
// “写 MySQL”的模拟：用 sleep 代表 IO
// 真实系统里这里会是：
//  - UPDATE stock ... WHERE available>=1
//  - INSERT order ...
//  - COMMIT
// -----------------------------
static inline void simulate_mysql_write(uint32_t sku_id, uint64_t request_id) {
    // 模拟一些抖动：0.3ms ~ 1.2ms
    // 秒杀真实链路里，DB/Redis 抖动就是 p99 的来源之一
    (void)sku_id; (void)request_id;
    std::this_thread::sleep_for(300us);
}

// -----------------------------
// 简单的 request_id 生成（测试）
// 真实可用雪花/uuid64
// -----------------------------
static inline uint64_t make_request_id(int producer_id, uint64_t seq) {
    return (uint64_t(uint32_t(producer_id)) << 32) | (seq & 0xFFFFFFFFull);
}

// =============================
// 测试用例：
//  - P producers * N intents
//  - C workers 消费
//  - 队列 bounded：满了 producer 直接拒绝（秒杀风格）
//  - 混入重复 request_id（幂等验证）
// =============================
int main() {
    // ---- 配置（你可以改） ----
    constexpr std::size_t QUEUE_CAP = 1u << 14; // 16384
    constexpr int P = 8;                       // producer 数
    constexpr int C = 4;                       // worker 数
    constexpr uint64_t PER_PRODUCER = 200000;  // 每个 producer 产生多少请求
    constexpr double DUP_RATE = 0.05;          // 5% 重复 request_id

    // 1) 启动 tracing（输出 trace.json）
    seckill::core::tracing::set_config({ .enabled = true, .output_path = "trace.json" });
    if (!seckill::core::tracing::flush_start()) {
        std::cerr << "Failed to open trace.json for writing\n";
        return 1;
    }

    // 2) 初始化队列与幂等表
    VyukovMPMCQueue<OrderIntent> q(QUEUE_CAP);
    IdempotencyTable idem;

    // 3) 统计指标
    std::atomic<uint64_t> produced_total{0};
    std::atomic<uint64_t> accepted_total{0};
    std::atomic<uint64_t> rejected_full{0};
    std::atomic<uint64_t> consumed_total{0};
    std::atomic<uint64_t> idempotent_dups{0};

    std::atomic<bool> producers_done{false};

    // 4) Workers（消费者）
    std::vector<std::thread> workers;
    workers.reserve(C);

    for (int wid = 0; wid < C; ++wid) {
        workers.emplace_back([&, wid] {
            seckill::core::Backoff bk; // 队列空时退避

            while (true) {
                OrderIntent in{};
                if (!q.try_dequeue(in)) {
                    // 如果生产者都结束了，且队列空，就退出
                    if (producers_done.load(std::memory_order_acquire)) {
                        OrderIntent tmp{};
                        if (!q.try_dequeue(tmp)) break;
                        in = tmp; // drain 最后一条
                    } else {
                        bk.pause();
                        continue;
                    }
                }
                bk.reset();

                consumed_total.fetch_add(1, std::memory_order_relaxed);

                TRACE_INSTANT("dequeue_ok", in.request_id);

                // ---- 幂等 ----
                {
                    TRACE_SCOPE("idempotency", in.request_id);
                    if (!idem.try_mark(in.request_id)) {
                        idempotent_dups.fetch_add(1, std::memory_order_relaxed);
                        TRACE_INSTANT("dup_drop", in.request_id);
                        continue;
                    }
                }

                // ---- 业务处理（模拟写 MySQL）----
                {
                    TRACE_SCOPE("process_order", in.request_id);

                    // 排队时间观测：dequeue - enqueue
                    // 这里只打一个 instant，方便在 trace 里定位，也可以写入 metrics
                    TRACE_INSTANT("enter_mysql", in.request_id);

                    simulate_mysql_write(in.sku_id, in.request_id);

                    TRACE_INSTANT("mysql_done", in.request_id);
                }
            }

            // 每个线程退出前把自己的 tracing buffer flush 一次（最稳）
            seckill::core::tracing::flush_thread();
            (void)wid;
        });
    }

    // 5) Producers（生产者）：模拟网关/HTTP 接入层
    std::vector<std::thread> producers;
    producers.reserve(P);

    for (int pid = 0; pid < P; ++pid) {
        producers.emplace_back([&, pid] {
            std::mt19937_64 rng(uint64_t(pid) * 1315423911ull + 7ull);
            std::uniform_real_distribution<double> uni(0.0, 1.0);
            std::uniform_int_distribution<uint32_t> sku_pick(1, 50);

            for (uint64_t i = 0; i < PER_PRODUCER; ++i) {
                produced_total.fetch_add(1, std::memory_order_relaxed);

                // 混入重复 request_id：用前面某个 seq 的 id（仅测试幂等）
                uint64_t base_seq = i;
                if (i > 100 && uni(rng) < DUP_RATE) {
                    base_seq = i - 100; // 制造一个重复
                }

                OrderIntent intent{};
                intent.request_id = make_request_id(pid, base_seq);
                intent.user_id = (uint64_t(pid) << 20) ^ i;
                intent.sku_id = sku_pick(rng);
                intent.qty = 1;
                intent.enqueue_ts_ns = seckill::core::tracing::now_ns(); // 复用 tracing 的 now

                TRACE_INSTANT("produce", intent.request_id);

                // 秒杀风格：队列满则直接拒绝（不在 producer 处 backoff 自旋）
                if (q.try_enqueue(intent)) {
                    accepted_total.fetch_add(1, std::memory_order_relaxed);
                    TRACE_INSTANT("enqueue_ok", intent.request_id);
                } else {
                    rejected_full.fetch_add(1, std::memory_order_relaxed);
                    TRACE_INSTANT("enqueue_full", intent.request_id);
                }
            }

            // producer 线程也 flush 一次（可选）
            seckill::core::tracing::flush_thread();
        });
    }

    // 6) 等 producers 结束
    for (auto& t : producers) t.join();
    producers_done.store(true, std::memory_order_release);

    // 7) 等 workers 结束
    for (auto& t : workers) t.join();

    // 8) 结束 tracing
    seckill::core::tracing::flush_end();

    // 9) 输出测试结果 & 基本一致性断言
    const uint64_t produced = produced_total.load();
    const uint64_t accepted = accepted_total.load();
    const uint64_t rejected = rejected_full.load();
    const uint64_t consumed = consumed_total.load();
    const uint64_t dups = idempotent_dups.load();

    std::cout << "=== Order Worker Test ===\n";
    std::cout << "Queue cap: " << QUEUE_CAP << "\n";
    std::cout << "Producers: " << P << "  Workers: " << C << "\n";
    std::cout << "Per producer: " << PER_PRODUCER << "  Total produced: " << produced << "\n";
    std::cout << "Accepted (enqueued): " << accepted << "\n";
    std::cout << "Rejected (queue full): " << rejected << "\n";
    std::cout << "Consumed (dequeued): " << consumed << "\n";
    std::cout << "Idempotent duplicates dropped: " << dups << "\n";
    std::cout << "Trace output: trace.json (open in chrome://tracing)\n";

    // 合理性检查：
    // - producer 产生的要么 accepted 要么 rejected
    assert(produced == accepted + rejected);

    // - consumed 应该 >= accepted 的下限？（注意：accepted 的每条都会被 dequeue，
    //   但 worker 会丢弃 duplicates；丢弃仍然算 consumed）
    assert(consumed == accepted);

    return 0;
}
