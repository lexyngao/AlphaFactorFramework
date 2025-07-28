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
    // 重构：为每只股票维护一个 SyncTickData，直接管理数据
    std::unordered_map<std::string, SyncTickData> stock_sync_data_;
    mutable std::mutex sync_data_mutex_;  // 保护 stock_sync_data_

    // 指标和因子容器
    std::unordered_map<std::string, std::shared_ptr<Indicator>> indicators_;  // key: 指标名
    std::unordered_map<std::string, std::shared_ptr<Factor>> factors_;  // key: 因子名

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
                    return !is_running_ || !task_queue_.empty();
                });
                if (!is_running_ && task_queue_.empty()) break;

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

    // 时间触发线程工作函数（已废弃，现在通过Time事件触发）
    void timer_worker() {
        while (timer_running_) {
            std::this_thread::sleep_for(std::chrono::milliseconds(time_interval_ms_));
            // 现在通过Time事件触发，不再使用定时器
            // onTime();  // 触发因子计算
        }
    }

    // 新增：独立的任务创建函数（处理单只股票的指标计算）
    void submit_indicator_calculation(const SyncTickData& sync_tick) {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        task_queue_.push([this, sync_tick]() {
            FinalAction on_exit([this]() {  });
            try {
                for (auto& [ind_name, indicator] : indicators_) {
                    // 检查计算状态，避免重复计算
                    if (!indicator->is_calculated()) {
                        indicator->try_calculate(sync_tick);
                    } else {
                        spdlog::debug("指标[{}]已计算，跳过", ind_name);
                    }
                }
            } catch (const std::exception& e) {
                spdlog::error("[指标计算] 股票{}失败: {}", sync_tick.symbol, e.what());
            }
        });
        task_cond_.notify_one();
    }

public:
    const GlobalConfig& config_;

    std::shared_ptr<Indicator> get_indicator(const std::string& name) const {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        auto it = indicators_.find(name);
        if (it != indicators_.end()) {
            return it->second;
        }
        return nullptr;
    }

    std::shared_ptr<Factor> get_factor(const std::string& name) const {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        auto it = factors_.find(name);
        if (it != factors_.end()) {
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
        factors_[factor->get_name()] = factor;
    }

    // 获取股票列表
    const std::vector<std::string>& get_stock_list() const {
        return stock_list_;
    }

    // 获取因子存储
    const std::unordered_map<std::string, std::shared_ptr<Factor>>& get_factor_storage() const {
        return factors_;
    }
    
    // 新增：重置所有指标的计算状态（用于强制重新计算）
    void reset_all_indicator_status() {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        for (auto& [ind_name, indicator] : indicators_) {
            indicator->reset_calculation_status();
            spdlog::info("重置指标[{}]的计算状态", ind_name);
        }
    }
    
    // 新增：重置指定指标的计算状态
    void reset_indicator_status(const std::string& indicator_name) {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        auto it = indicators_.find(indicator_name);
        if (it != indicators_.end()) {
            it->second->reset_calculation_status();
            spdlog::info("重置指标[{}]的计算状态", indicator_name);
        } else {
            spdlog::warn("未找到指标[{}]", indicator_name);
        }
    }

    // 重构：处理订单（直接添加到对应股票的 SyncTickData）
    void onOrder(const OrderData& order) {
        std::lock_guard<std::mutex> lock(sync_data_mutex_);
        stock_sync_data_[order.symbol].orders.push_back(order);
        spdlog::debug("[订单] {} 累计: {}条", order.symbol, stock_sync_data_[order.symbol].orders.size());
    }

    // 重构：处理成交（直接添加到对应股票的 SyncTickData）
    void onTrade(const TradeData& trade) {
        std::lock_guard<std::mutex> lock(sync_data_mutex_);
        stock_sync_data_[trade.symbol].trans.push_back(trade);
        spdlog::debug("[成交] {} 累计: {}条", trade.symbol, stock_sync_data_[trade.symbol].trans.size());
    }

    // 重构：处理Tick数据（更新 SyncTickData 并触发计算）
    void onTick(const TickData& tick) {
        SyncTickData sync_tick;
        
        {
            std::lock_guard<std::mutex> lock(sync_data_mutex_);
            auto& sync_data = stock_sync_data_[tick.symbol];
            
            // 复制当前状态
            sync_tick = sync_data;
            
            // 更新 tick_data
            sync_tick.tick_data = tick;
            sync_tick.symbol = tick.symbol;
            sync_tick.local_time_stamp = tick.real_time;
            
            // 清理数据，准备下一个周期
            sync_data.orders.clear();
            sync_data.trans.clear();
        }
        
        // 提交计算任务
        submit_indicator_calculation(sync_tick);
    }

    // 时间触发因子计算
    void onTime(uint64_t timestamp) {
        // 获取所有Indicator的存储数据
        std::unordered_map<std::string, BarSeriesHolder*> bar_runners;
        {
            std::lock_guard<std::mutex> lock(queue_mutex_);
            for (const auto& [name, indicator] : indicators_) {
                const auto& storage = indicator->get_storage();
                for (const auto& [stock, holder] : storage) {
                    bar_runners[stock] = holder.get();
                }
            }
        }

        // 根据时间戳计算5分钟时间桶索引（Factor固定为5分钟频率）
        int ti = calculate_time_bucket(timestamp, Frequency::F5MIN);
        
        if (ti < 0) {
            spdlog::warn("无法计算时间桶索引，跳过因子计算，timestamp: {}", timestamp);
            return;
        }

        spdlog::debug("Time事件触发因子计算，timestamp: {}, ti: {}", timestamp, ti);

        // 提交因子计算任务
        {
            std::lock_guard<std::mutex> lock(queue_mutex_);
            for (auto& [factor_name, factor] : factors_) {
                auto factor_ptr = factor;  // 创建局部副本避免捕获冲突
                auto bar_runners_copy = bar_runners;  // 创建bar_runners的副本
                task_queue_.push([this, factor_ptr, bar_runners_copy, ti]() {
                    try {
                        // 调用因子的definition函数
                        GSeries result = factor_ptr->definition(bar_runners_copy, stock_list_, ti);
                        
                        // 将结果存储到factor的存储结构中
                        factor_ptr->set_factor_result(ti, result);
                        
                        spdlog::debug("因子[{}]计算完成，时间桶: {}", factor_ptr->get_name(), ti);
                    } catch (const std::exception& e) {
                        spdlog::error("因子[{}]计算失败: {}", factor_ptr->get_name(), e.what());
                    }
                });
            }
        }
        task_cond_.notify_all();  // 唤醒所有线程处理因子计算
    }

    // 计算时间桶索引（通用函数，支持不同频率）
    int calculate_time_bucket(uint64_t timestamp, Frequency frequency) {
        if (timestamp == 0) return -1;

        // 转换为北京时间
        int64_t utc_sec = timestamp / 1000000000;
        int64_t beijing_sec = utc_sec + 8 * 3600;  // UTC + 8小时 = 北京时间
        int64_t beijing_seconds_in_day = beijing_sec % 86400;
        int hour = static_cast<int>(beijing_seconds_in_day / 3600);
        int minute = static_cast<int>((beijing_seconds_in_day % 3600) / 60);
        int second = static_cast<int>(beijing_seconds_in_day % 60);
        int total_minutes = hour * 60 + minute;

        // 交易时间
        const int morning_start = 9 * 60 + 30;   // 9:30
        const int morning_end = 11 * 60 + 30;    // 11:30
        const int afternoon_start = 13 * 60;     // 13:00
        const int afternoon_end = 15 * 60;       // 15:00

        bool is_morning = (total_minutes >= morning_start && total_minutes < morning_end);
        bool is_afternoon = (total_minutes >= afternoon_start && total_minutes < afternoon_end);
        if (!is_morning && !is_afternoon) return -1;

        int seconds_since_open = 0;
        if (is_morning) {
            seconds_since_open = (total_minutes - morning_start) * 60 + second;
        } else {
            seconds_since_open = (morning_end - morning_start) * 60 // 上午总秒数
                                + (total_minutes - afternoon_start) * 60 + second;
        }

        // 根据频率计算时间桶长度和最大桶数
        int bucket_len = 15; // 默认15s
        int max_buckets = 960; // 默认15s的最大桶数
        
        switch (frequency) {
            case Frequency::F15S: 
                bucket_len = 15; 
                max_buckets = 960;
                break;
            case Frequency::F1MIN: 
                bucket_len = 60; 
                max_buckets = 240;
                break;
            case Frequency::F5MIN: 
                bucket_len = 300; 
                max_buckets = 48;
                break;
            case Frequency::F30MIN: 
                bucket_len = 1800; 
                max_buckets = 8;
                break;
        }
        
        int ti = seconds_since_open / bucket_len;
        if (ti < 0 || ti >= max_buckets) return -1;
        return ti;
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
            case MarketBufferType::Time:
                onTime(field.get_time_trigger());
                break;
            default:
                spdlog::warn("未知数据类型: {}", static_cast<int>(field.type));
        }
    }
};

#endif // ALPHAFACTORFRAMEWORK_CAL_ENGINE_H
