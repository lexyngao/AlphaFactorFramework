//
// Created by lexyn on 25-7-14.
//
#ifndef ALPHAFACTORFRAMEWORK_CALCULATION_ENGINE_H
#define ALPHAFACTORFRAMEWORK_CALCULATION_ENGINE_H

#include <unordered_map>
#include <vector>
#include <queue>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <functional>
#include <future>
#include "data_structures.h"
#include "config.h"
#include <map>
#include <memory>

// Indicator计算接口（需由具体指标类实现）
class IndicatorCalculator {
public:
    virtual ~IndicatorCalculator() = default;
    // 计算Indicator（输入同步行情，输出结果写入holder）
    virtual void Calculate(const SyncTickData& tick_data, BarSeriesHolder* holder) = 0;
};

// 计算引擎（管理Indicator和Factor计算，PDF 3节）
class myCalculationEngine {
private:
    // 线程池（用于并发计算）
    std::vector<std::thread> workers;
    std::queue<std::function<void()>> tasks;
    std::mutex queue_mutex;
    std::condition_variable condition;
    bool stop = false;

    // Indicator存储：股票代码->BarSeriesHolder（PDF 1.3节）
    std::unordered_map<std::string, std::unique_ptr<BarSeriesHolder>> indicator_storage;

    // Factor存储：时间bar索引->factor_name->GSeries（PDF 1.3节）
    std::map<int, std::map<std::string, GSeries>> factor_storage;

    // 配置信息
    GlobalConfig config;

    // 因子实例（id->BaseFactor*）
    std::unordered_map<std::string, std::unique_ptr<BaseFactor>> factor_instances;

    // Indicator计算器实例（id->IndicatorCalculator*）
    std::unordered_map<std::string, std::unique_ptr<IndicatorCalculator>> indicator_calculators;

public:
    explicit myCalculationEngine(const GlobalConfig& cfg) : config(cfg) {
        // 初始化线程池（线程数=CPU核心数，避免过度创建）
        size_t thread_count = std::thread::hardware_concurrency();
        if (thread_count == 0) thread_count = 4;  // 保底4线程
        for (size_t i = 0; i < thread_count; ++i) {
            workers.emplace_back([this] {
                while (true) {
                    std::function<void()> task;
                    {
                        std::unique_lock<std::mutex> lock(this->queue_mutex);
                        this->condition.wait(lock, [this] { return this->stop || !this->tasks.empty(); });
                        if (this->stop && this->tasks.empty()) return;
                        task = std::move(this->tasks.front());
                        this->tasks.pop();
                    }
                    task();  // 执行任务
                }
            });
        }
        spdlog::info("CalculationEngine initialized with {} threads", thread_count);
    }

    ~myCalculationEngine() {
        {
            std::unique_lock<std::mutex> lock(queue_mutex);
            stop = true;
        }
        condition.notify_all();
        for (auto& worker : workers) {
            if (worker.joinable()) worker.join();
        }
    }

    // 注册Indicator计算器
    void register_indicator(const std::string& id, std::unique_ptr<IndicatorCalculator> calculator) {
        indicator_calculators[id] = std::move(calculator);
    }

    // 注册Factor实例
    void register_factor(const std::string& id, std::unique_ptr<BaseFactor> factor) {
        factor_instances[id] = std::move(factor);
    }

    // 初始化股票的Indicator存储（含历史数据加载）
    void init_indicator_storage(const std::vector<std::string>& stock_list) {
        for (const auto& stock : stock_list) {
            indicator_storage[stock] = std::make_unique<BarSeriesHolder>(stock);
        }
        spdlog::info("Indicator storage initialized for {} stocks", stock_list.size());
    }

    // 回调：处理Tick数据（触发Indicator计算，PDF 3.3节）
    void onTick(const SyncTickData& tick_data) {
        // 检查股票是否在当前池
        if (!indicator_storage.count(tick_data.symbol)) {
            spdlog::info("{}: not in current universe, skip onTick", tick_data.symbol);
            return;
        }
        // 按股票分配线程
        auto* holder = indicator_storage[tick_data.symbol].get();
        auto symbol = tick_data.symbol;

        // 关键：添加任务时加锁，确保worker线程能看到新任务
        {
            std::unique_lock<std::mutex> lock(queue_mutex);  // 与worker线程共用同一把锁
            tasks.emplace([this, tick_data, holder, symbol] {
                spdlog::info("[线程池任务] 开始处理股票: {} 的计算", symbol);
                for (const auto& [id, calculator] : indicator_calculators) {
                    spdlog::info("[线程池任务] 调用indicator ID: {}", id);
                    calculator->Calculate(tick_data, holder);
                }
            });
        }  // 锁在这里释放

        // 唤醒一个worker线程处理新任务（关键：通知线程有新任务）
        condition.notify_one();
    }

    // 回调：时间触发（触发Factor计算，PDF 3.4节）
    void onTime(int ti, const std::vector<std::string>& sorted_stock_list) {
        // 等待所有Indicator任务完成（确保先算Indicator，PDF 3节）
        {
            std::unique_lock<std::mutex> lock(queue_mutex);
            while (!tasks.empty()) {
                spdlog::debug("Waiting for Indicator tasks to finish before Factor (ti={})", ti);
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
            }
        }

        // 按Factor分配线程（PDF 3节）
        for (const auto& [factor_id, factor] : factor_instances) {
            // 提交因子计算任务
            tasks.emplace([this, factor = factor.get(), ti, sorted_stock_list, fid = factor_id] {
                // 转换indicator_storage为BarSeriesHolder*（符合definition参数要求）
                std::unordered_map<std::string, BarSeriesHolder*> bar_runners;
                for (const auto& [stock, holder] : indicator_storage) {
                    bar_runners[stock] = holder.get();
                }
                // 计算因子
                GSeries result = factor->definition(bar_runners, sorted_stock_list, ti);
                // 写入Factor存储（线程安全：加锁）
                std::lock_guard<std::mutex> lock(queue_mutex);
                factor_storage[ti][fid] = result;
                spdlog::debug("Factor {} calculated for ti={} (valid={}/{})",
                              fid, ti, result.get_valid_num(), result.get_size());
            });
        }
    }

    // 获取Indicator存储（用于结果存储）
    const std::unordered_map<std::string, std::unique_ptr<BarSeriesHolder>>& get_indicator_storage() const {
        return indicator_storage;
    }

    // 获取Factor存储（用于结果存储）
    const std::map<int, std::map<std::string, GSeries>>& get_factor_storage() const {
        return factor_storage;
    }
};

#endif //ALPHAFACTORFRAMEWORK_CALCULATION_ENGINE_H