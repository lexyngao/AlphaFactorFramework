#ifndef ALPHAFACTORFRAMEWORK_CAL_ENGINE_H
#define ALPHAFACTORFRAMEWORK_CAL_ENGINE_H

#include <vector>
#include <string>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <queue>
#include <functional>
#include <atomic>
#include <unordered_map>
#include <memory>
#include <spdlog/spdlog.h>
#include "data_structures.h"

// 工具类：用于在作用域结束时执行指定动作（模拟finally）
class FinalAction {
private:
    std::function<void()> action_;
public:
    explicit FinalAction(std::function<void()> action) : action_(std::move(action)) {}
    ~FinalAction() { action_(); }  // 析构时执行动作（无论是否异常）
};

// 辅助函数前置声明
std::vector<std::string> get_history_dates(const std::string& current_date, int days);
GSeries load_gseries_from_file(const std::string& file_path);

class CalculationEngine {
private:
    // 全局临时存储（按股票拆分，避免混合存储导致的锁竞争）
    std::unordered_map<std::string, std::vector<OrderData>> stock_orders_;  // key: 股票代码
    std::unordered_map<std::string, std::vector<TradeData>> stock_trades_;  // key: 股票代码
    std::mutex data_mutex_;  // 保护临时数据的互斥锁

    // 指标和因子容器
    std::unordered_map<std::string, std::shared_ptr<Indicator>> indicators_;  // key: 指标名
    mutable std::mutex indicators_mutex_;  // 保护indicators_的锁
    std::vector<std::shared_ptr<Factor>> factors_;

    // 存储股票列表
    std::vector<std::string> stock_list_;

    // 线程池（合并为一个通用线程池，处理所有并行任务）
    std::vector<std::thread> worker_threads_;  // 通用工作线程
    std::queue<std::function<void()>> task_queue_;  // 任务队列
    mutable std::mutex queue_mutex_;
    std::condition_variable task_cond_;
    std::atomic<bool> is_running_{true};

    // 时间触发线程（因子计算触发）
    std::thread timer_thread_;
    std::atomic<bool> timer_running_{true};
    uint64_t time_interval_ms_;  // 因子计算触发间隔（毫秒）

    std::atomic<size_t> total_tasks_{0};    // 已提交的总任务数
    std::atomic<size_t> completed_tasks_{0};  // 已完成的任务数

    std::atomic<bool> is_paused_{false};  // 新增：线程池暂停标志

    // 辅助函数：加载单只股票的历史指标数据
    void load_historical_data(const std::string& stock_code, BaseSeriesHolder& holder) {
        spdlog::debug("暂未实现历史数据加载 for {}", stock_code);
    }

    // 线程工作函数（通用，处理所有任务队列中的任务）
    void worker() {
        while (is_running_) {
            std::function<void()> task;
            {
                std::unique_lock<std::mutex> lock(queue_mutex_);
                // 等待任务或退出信号
                task_cond_.wait(lock, [this]() {
                    return !is_running_ || (is_paused_ == false && !task_queue_.empty());
                });
                if (!is_running_ && task_queue_.empty()) break;
                if (is_paused_) continue;  // 若暂停，继续等待

                task = std::move(task_queue_.front());
                task_queue_.pop();
            }
            // 执行任务（捕获异常，避免单个任务崩溃导致线程退出）
            try {
                task();
            } catch (const std::exception& e) {
                spdlog::error("任务执行失败: {}", e.what());
            }
        }
    }

    // 时间触发线程工作函数
    void timer_worker() {
        while (timer_running_) {
            std::this_thread::sleep_for(std::chrono::milliseconds(time_interval_ms_));
            onTime();  // 触发因子计算
        }
    }

    // 新增：独立的任务创建函数（处理单只股票的指标计算）
    void submit_indicator_calculation(const SyncTickData& sync_tick) {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        total_tasks_++;
        task_queue_.push([this, sync_tick]() {
            FinalAction on_exit([this]() { completed_tasks_++; });

            try {
                // 调用指标的try_calculate，内部判断是否需要计算
                for (auto& [ind_name, indicator] : indicators_) {
                    indicator->try_calculate(sync_tick); // 核心修改：通过try_calculate触发
                }
                spdlog::info("[指标计算] 股票{}处理完成，订单: {}条，成交: {}条",
                             sync_tick.symbol, sync_tick.orders.size(), sync_tick.trans.size());
            } catch (const std::exception& e) {
                spdlog::error("[指标计算] 股票{}失败: {}", sync_tick.symbol, e.what());
            }
        });
        task_cond_.notify_one();
    }

public:
    const GlobalConfig& config_;

    size_t get_total_tasks() const { return total_tasks_; }
    size_t get_completed_tasks() const { return completed_tasks_; }

    // 新增：暂停线程池（等待所有当前任务完成）
    void pause() {
        spdlog::info("[开始暂停]");

        // 等待队列清空且所有任务完成（复用已有计数器）
        while (!is_task_queue_empty() || get_completed_tasks() != get_total_tasks()) {
            // 每1秒输出一次状态（避免日志刷屏）
            static auto last_log_time = std::chrono::steady_clock::now();
            auto now = std::chrono::steady_clock::now();
            if (now - last_log_time > std::chrono::seconds(1)) {
                spdlog::info("[暂停中] 任务队列是否为空: {}, 总任务数: {}, 已完成: {}",
                             is_task_queue_empty() ? "是" : "否",
                             get_total_tasks(),
                             get_completed_tasks());
                last_log_time = now;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        is_paused_ = true;
        spdlog::info("[暂停完成] 所有任务已处理，线程池进入暂停状态");
    }

    // 新增：恢复线程池
    void resume() {
        is_paused_ = false;
        task_cond_.notify_all();  // 唤醒所有等待的工作线程
    }

    std::shared_ptr<Indicator> get_indicator(const std::string& name) const {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        auto it = indicators_.find(name);
        if (it != indicators_.end()) {
            return it->second;
        }
        return nullptr;
    }

    // 构造函数：初始化线程池
    explicit CalculationEngine(const GlobalConfig& cfg)
            : config_(cfg), time_interval_ms_(cfg.factor_frequency) {

        // 线程数：优先使用配置，否则用CPU核心数
        size_t thread_count = cfg.worker_thread_count;
        if (thread_count == 0) {
            thread_count = std::thread::hardware_concurrency();
            if (thread_count == 0) thread_count = 4;  // 保底4线程
        }

        // 启动工作线程（合并指标和因子线程池）
        for (size_t i = 0; i < thread_count; ++i) {
            worker_threads_.emplace_back(&CalculationEngine::worker, this);
        }

        // 启动时间触发线程
//        timer_thread_ = std::thread(&CalculationEngine::timer_worker, this);

        spdlog::info("CalculationEngine初始化完成: 工作线程数={}, 因子触发间隔={}ms",
                     thread_count, time_interval_ms_);
    }

    // 析构函数：停止线程
    ~CalculationEngine() {
        // 停止工作线程
        is_running_ = false;
        task_cond_.notify_all();
        for (auto& t : worker_threads_) {
            if (t.joinable()) t.join();
        }

        // 停止时间线程
        timer_running_ = false;
        if (timer_thread_.joinable()) timer_thread_.join();
    }

    // 初始化所有指标的存储（调用每个Indicator自己的init_storage）
    void init_indicator_storage(const std::vector<std::string>& stock_list) {
        std::lock_guard<std::mutex> lock(indicators_mutex_);  // 保护indicators_
        stock_list_ = stock_list;  // 保留股票列表供其他逻辑使用

        // 遍历所有注册的指标，初始化它们各自的存储
        for (const auto& [ind_name, indicator] : indicators_) {
            indicator->init_storage(stock_list);  // 调用Indicator的初始化方法
        }

        spdlog::info("所有指标已完成{}只股票的存储初始化", stock_list.size());
    }

    // 添加指标和因子
    void add_indicator(const std::string& name, std::shared_ptr<Indicator> ind) {
        std::lock_guard<std::mutex> lock(queue_mutex_);  // 避免并发修改
        indicators_[name] = ind;
    }

    void add_factor(std::shared_ptr<Factor> factor) {
        std::lock_guard<std::mutex> lock(queue_mutex_);  // 避免并发修改
        factors_.push_back(factor);
    }


    // 获取股票列表
    const std::vector<std::string>& get_stock_list() const {
        return stock_list_;
    }

    // 获取因子存储
    const std::vector<std::shared_ptr<Factor>>& get_factor_storage() const {
        return factors_;
    }

    // 处理订单（按股票缓存）
    void onOrder(const OrderData& order) {
        std::lock_guard<std::mutex> lock(data_mutex_);
        stock_orders_[order.symbol].push_back(order);
        spdlog::debug("[订单] {} 累计: {}条", order.symbol, stock_orders_[order.symbol].size());
    }

    // 处理成交（按股票缓存）
    void onTrade(const TradeData& trade) {
        std::lock_guard<std::mutex> lock(data_mutex_);
        stock_trades_[trade.symbol].push_back(trade);
        spdlog::debug("[成交] {} 累计: {}条", trade.symbol, stock_trades_[trade.symbol].size());
    }

// 处理Tick数据（仅负责数据组装和任务提交）
    void onTick(const TickData& tick) {
        // 1. 提取缓存的订单和成交数据
        std::vector<OrderData> orders;
        std::vector<TradeData> trades;
        {
            std::lock_guard<std::mutex> lock(data_mutex_);
            if (stock_orders_.count(tick.symbol)) {
                orders = std::move(stock_orders_[tick.symbol]);
                stock_orders_[tick.symbol].clear();
            }
            if (stock_trades_.count(tick.symbol)) {
                trades = std::move(stock_trades_[tick.symbol]);
                stock_trades_[tick.symbol].clear();
            }
        }

        // 2. 生成SyncTickData（包含本地时间戳）
        SyncTickData sync_tick;
        sync_tick.symbol = tick.symbol;
        sync_tick.tick_data = tick;
        sync_tick.orders = std::move(orders);
        sync_tick.trans = std::move(trades);
        // 确保local_time_stamp已正确设置（纳秒级）
        // 例如：
         sync_tick.local_time_stamp = tick.real_time;

        // 3. 提交计算任务（彻底解耦）
        submit_indicator_calculation(sync_tick);
    }

    // 时间触发因子计算
    void onTime() {
        std::vector<const Indicator*> ready_indicators;
        {
            std::lock_guard<std::mutex> lock(queue_mutex_);  // 避免并发修改indicators_
            for (const auto& [name, ind] : indicators_) {
                ready_indicators.push_back(ind.get());
            }
        }

        // 提交因子计算任务
        {
            std::lock_guard<std::mutex> lock(queue_mutex_);
            for (auto& factor : factors_) {
                task_queue_.push([factor, ready_indicators]() {
                    factor->Calculate(ready_indicators);
                });
            }
        }
        task_cond_.notify_all();  // 唤醒所有线程处理因子计算
    }

    // 统一更新入口（单线程调用，确保时序）
    void update(const MarketAllField& field) {
        // 单线程处理所有行情数据，保证时间顺序
        switch (field.type) {
            case MarketBufferType::Order:
                onOrder(field.get_order());
                break;
            case MarketBufferType::Trade:
                onTrade(field.get_trade());
                break;
            case MarketBufferType::Tick:
                onTick(field.get_tick());
                break;
            default:
                spdlog::warn("未知数据类型");
        }
    }

    // 检查任务队列是否为空
    bool is_task_queue_empty() const {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        return task_queue_.empty();
    }
};

#endif // ALPHAFACTORFRAMEWORK_CAL_ENGINE_H
