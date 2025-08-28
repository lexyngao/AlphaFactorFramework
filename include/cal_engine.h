#ifndef ALPHAFACTORFRAMEWORK_CAL_ENGINE_H
#define ALPHAFACTORFRAMEWORK_CAL_ENGINE_H

#include "data_structures.h"
#include "config.h"
#include "my_indicator.h"  // 添加这行以支持VolumeIndicator和AmountIndicator
#include "diff_indicator.h"  // 添加这行以支持DiffIndicator
#include <unordered_map>
#include <vector>
#include <queue>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <functional>
#include <future>
#include <memory>
#include <atomic>
#include <chrono>
#include <ctime>
#include <limits>
#include <functional>  // 新增：支持std::enable_shared_from_this
#include "spdlog/spdlog.h"
#include "spdlog/fmt/bundled/format.h"

// 工具类：用于在作用域结束时执行指定动作（模拟finally）
class FinalAction {
private:
    std::function<void()> action_;
public:
    explicit FinalAction(std::function<void()> action) : action_(std::move(action)) {}
    ~FinalAction() { action_(); }  // 析构时执行动作（无论是否异常）
};

// TickDataManager类：管理单只股票的tick数据
class TickDataManager {
private:
    std::string stock_code_;                    // 股票代码
    SyncTickData current_sync_tick_data_;       // 当前的同步tick数据
    std::vector<SyncTickData> tick_data_list_; // 历史tick数据列表
    // 去掉 data_mutex_ - 每个实例只被一个线程访问，无并发竞争
    
    // 可选的预处理函数指针
    std::function<void(SyncTickData&)> preprocess_func_;
    
public:
    explicit TickDataManager(const std::string& stock_code) 
        : stock_code_(stock_code) {
        // 初始化current_sync_tick_data_
        current_sync_tick_data_.symbol = stock_code;
        current_sync_tick_data_.local_time_stamp = 0;
    }
    
    // 设置预处理函数
    void set_preprocess_function(std::function<void(SyncTickData&)> func) {
        preprocess_func_ = std::move(func);
    }
    
    // 核心更新方法
    void update(const SyncTickData& sync_tick_data) {
        // 复制数据
        current_sync_tick_data_ = sync_tick_data;
        
        // 如果需要预处理，调用预处理函数
        if (preprocess_func_) {
            preprocess_func_(current_sync_tick_data_);
        }
        
        // 添加到历史列表
        tick_data_list_.push_back(current_sync_tick_data_);
        
        spdlog::debug("[TickDataManager] {} 更新完成，历史数据量: {}", 
                     stock_code_, tick_data_list_.size());
    }
    
    // 获取当前的同步tick数据
    SyncTickData get_current_sync_tick_data() const {
        return current_sync_tick_data_;
    }
    
    // 获取历史数据列表
    std::vector<SyncTickData> get_tick_data_list() const {
        return tick_data_list_;
    }
    
    // 获取股票代码
    const std::string& get_stock_code() const {
        return stock_code_;
    }
    
    // 清空历史数据（用于每天开始时重置）
    void clear_history() {
        tick_data_list_.clear();
        spdlog::debug("[TickDataManager] {} 历史数据已清空", stock_code_);
    }
    
    // 获取历史数据数量
    size_t get_history_count() const {
        return tick_data_list_.size();
    }
    
    // 检查是否有数据
    bool has_data() const {
        return !tick_data_list_.empty();
    }
};

// 使用data_structures.h中现有的BarSeriesHolder类，不再重复定义

// 辅助函数前置声明
std::vector<std::string> get_history_dates(const std::string& current_date, int days);
GSeries load_gseries_from_file(const std::string& file_path);

class CalculationEngine : public std::enable_shared_from_this<CalculationEngine> {
private:
    // 重构：为每只股票维护一个 SyncTickData，直接管理数据
    // 每只股票的数据访问是独立的，不存在并发竞争，可以去掉锁保护
    std::unordered_map<std::string, SyncTickData> stock_sync_data_;
    // 去掉 sync_data_mutex_ - 每只股票独立访问，无并发竞争

    // 新增：TickDataManager映射，管理每只股票的tick数据
    // 这些映射在初始化后基本不变，可以去掉锁保护
    std::unordered_map<std::string, std::shared_ptr<TickDataManager>> stock_tick_managers_;
    // 去掉 tick_managers_mutex_ - 初始化后只读访问

    // 新增：BarSeriesHolder映射，集中管理所有频率的存储
    // 这些映射在初始化后基本不变，可以去掉锁保护
    std::unordered_map<std::string, std::shared_ptr<BarSeriesHolder>> stock_bar_holders_;
    // 去掉 bar_holders_mutex_ - 初始化后只读访问

    // 新增：Factor存储映射，集中管理所有Factor的数据存储
    // 结构：{factor_name -> {ti -> {stock_code -> value}}}
    // 每个Factor写入不同的数据分支，不存在并发竞争，可以去掉锁保护
    std::unordered_map<std::string, std::map<int, std::unordered_map<std::string, double>>> factor_storage_;
    // 去掉 factor_storage_mutex_ - 每个Factor独立写入，无竞争条件

    // 指标和因子容器 - 在初始化后基本不变，可以去掉锁保护
    std::unordered_map<std::string, std::shared_ptr<Indicator>> indicators_;  // key: 指标名
    std::unordered_map<std::string, std::shared_ptr<Factor>> factors_;  // key: 因子名

    // 存储股票列表 - 在初始化后不变，不需要锁保护
    std::vector<std::string> stock_list_;

    // 线程池（合并为一个通用线程池，处理所有并行任务）
    std::vector<std::thread> worker_threads_;  // 通用工作线程
    std::queue<std::function<void()>> task_queue_;  // 任务队列
    mutable std::mutex queue_mutex_;  // 保护任务队列 - 必须保留
    std::condition_variable task_cond_;
    std::atomic<bool> is_running_{true};

    // 时间触发线程（因子计算触发）
    std::thread timer_thread_;
    std::atomic<bool> timer_running_{true};
    uint64_t time_interval_ms_;  // 因子计算触发间隔（毫秒）

    // 性能统计相关
    struct PerformanceStats {
        std::atomic<uint64_t> total_orders{0};
        std::atomic<uint64_t> total_trades{0};
        std::atomic<uint64_t> total_ticks{0};
        std::atomic<uint64_t> total_indicators{0};
        
        std::atomic<uint64_t> total_order_time_us{0};
        std::atomic<uint64_t> total_trade_time_us{0};
        std::atomic<uint64_t> total_tick_time_us{0};
        std::atomic<uint64_t> total_indicator_time_us{0};
        
        std::atomic<uint64_t> max_order_time_us{0};
        std::atomic<uint64_t> max_trade_time_us{0};
        std::atomic<uint64_t> max_tick_time_us{0};
        std::atomic<uint64_t> max_indicator_time_us{0};
        
        void reset() {
            total_orders = 0;
            total_trades = 0;
            total_ticks = 0;
            total_indicators = 0;
            total_order_time_us = 0;
            total_trade_time_us = 0;
            total_tick_time_us = 0;
            total_indicator_time_us = 0;
            max_order_time_us = 0;
            max_trade_time_us = 0;
            max_tick_time_us = 0;
            max_indicator_time_us = 0;
        }
        
        void print_summary() const {
            if (total_orders > 0) {
                spdlog::info("[性能统计] Orders: 总数={}, 平均耗时={:.2f}μs, 最大耗时={}μs", 
                    total_orders.load(), 
                    static_cast<double>(total_order_time_us.load()) / total_orders.load(),
                    max_order_time_us.load());
            }
            if (total_trades > 0) {
                spdlog::info("[性能统计] Trades: 总数={}, 平均耗时={:.2f}μs, 最大耗时={}μs", 
                    total_trades.load(), 
                    static_cast<double>(total_trade_time_us.load()) / total_trades.load(),
                    max_trade_time_us.load());
            }
            if (total_ticks > 0) {
                spdlog::info("[性能统计] Ticks: 总数={}, 平均耗时={:.2f}μs, 最大耗时={}μs", 
                    total_ticks.load(), 
                    static_cast<double>(total_tick_time_us.load()) / total_ticks.load(),
                    max_tick_time_us.load());
            }
            if (total_indicators > 0) {
                spdlog::info("[性能统计] Indicators: 总数={}, 平均耗时={:.2f}μs, 最大耗时={}μs", 
                    total_indicators.load(), 
                    static_cast<double>(total_indicator_time_us.load()) / total_indicators.load(),
                    max_indicator_time_us.load());
            }
        }
    };
    
    PerformanceStats perf_stats_;
    std::chrono::steady_clock::time_point last_stats_time_;
    std::chrono::milliseconds stats_interval_{10000}; // 10秒输出一次统计

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
    // void submit_indicator_calculation(const SyncTickData& sync_tick) {
    //     std::lock_guard<std::mutex> lock(queue_mutex_);
    //     task_queue_.push([this, sync_tick]() {
    //         FinalAction on_exit([this]() {  });
    //         try {
    //             for (auto& [ind_name, indicator] : indicators_) {
    //                 // 检查计算状态，避免重复计算
    //                 if (!indicator->is_calculated()) {
    //                     indicator->try_calculate(sync_tick);
    //                 } else {
    //                     spdlog::debug("指标[{}]已计算，跳过", ind_name);
    //                 }
    //             }
    //         } catch (const std::exception& e) {
    //             spdlog::error("[指标计算] 股票{}失败: {}", sync_tick.symbol, e.what());
    //         }
    //     });
    //     task_cond_.notify_one();
    // }

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

    // 构造函数
    CalculationEngine(const GlobalConfig& config) 
        : config_(config), 
          time_interval_ms_(config.factor_frequency),
          last_stats_time_(std::chrono::steady_clock::now()) {
        
        spdlog::info("CalculationEngine初始化完成: 工作线程数={}, 因子触发间隔={}ms", 
                     config_.worker_thread_count, time_interval_ms_);
        
        // 线程数：优先使用配置，否则用CPU核心数
        size_t thread_count = config_.worker_thread_count;
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
    }

    // 析构函数：停止线程
    ~CalculationEngine() {
        // 停止工作线程
        is_running_ = false;
        task_cond_.notify_all();
        for (auto& t : worker_threads_) {
            if (t.joinable()) t.join();
        }

        // 停止时间线程（如果已启动）
        timer_running_ = false;
        // 注意：timer_thread_没有被启动，所以不需要join
        // if (timer_thread_.joinable()) timer_thread_.join();
    }

    // 初始化所有指标的存储（现在只需要初始化TickDataManager和BarSeriesHolder）
    void init_indicator_storage(const std::vector<std::string>& stock_list) {
        stock_list_ = stock_list;  // 保留股票列表供其他逻辑使用

        // 初始化TickDataManager和BarSeriesHolder
        init_tick_data_managers(stock_list);
        init_bar_series_holders(stock_list);

        spdlog::info("所有指标已完成{}只股票的存储初始化", stock_list.size());
    }

    // 新增：初始化TickDataManager
    void init_tick_data_managers(const std::vector<std::string>& stock_list) {
        // 清空现有的managers
        stock_tick_managers_.clear();
        
        // 为每只股票创建TickDataManager
        for (const auto& stock_code : stock_list) {
            stock_tick_managers_[stock_code] = std::make_shared<TickDataManager>(stock_code);
            
            // 可以在这里设置预处理函数（如果需要的话）
            // stock_tick_managers_[stock_code]->set_preprocess_function([](SyncTickData& data) {
            //     // 预处理逻辑
            // });
        }
        
        spdlog::info("已初始化{}只股票的TickDataManager", stock_list.size());
    }

    // 新增：初始化BarSeriesHolder
    void init_bar_series_holders(const std::vector<std::string>& stock_list) {
        // 清空现有的holders
        stock_bar_holders_.clear();
        
        // 为每只股票创建BarSeriesHolder
        for (const auto& stock_code : stock_list) {
            stock_bar_holders_[stock_code] = std::make_shared<BarSeriesHolder>(stock_code);
        }
        
        spdlog::info("已初始化{}只股票的BarSeriesHolder", stock_list.size());
    }

    // 添加指标和因子 - 通常在初始化阶段调用，不需要锁保护
    void add_indicator(const std::string& name, std::shared_ptr<Indicator> ind) {
        indicators_[name] = ind;
        spdlog::info("添加指标到engine: {}", name);
    }

    void add_factor(std::shared_ptr<Factor> factor) {
        factors_[factor->get_name()] = factor;
    }

    // 获取股票列表 - 只读访问，不需要锁
    const std::vector<std::string>& get_stock_list() const {
        return stock_list_;
    }

    // 获取因子存储 - 只读访问，不需要锁
    const std::unordered_map<std::string, std::shared_ptr<Factor>>& get_factor_storage() const {
        return factors_;
    }
    
    // 新增：获取指定股票的TickDataManager
    std::shared_ptr<TickDataManager> get_tick_data_manager(const std::string& stock_code) const {
        auto it = stock_tick_managers_.find(stock_code);
        if (it != stock_tick_managers_.end()) {
            return it->second;
        }
        return nullptr;
    }
    
    // 新增：获取所有TickDataManager
    const std::unordered_map<std::string, std::shared_ptr<TickDataManager>>& get_all_tick_data_managers() const {
        return stock_tick_managers_;
    }
    
    // 新增：获取指定股票的BarSeriesHolder
    std::shared_ptr<BarSeriesHolder> get_bar_series_holder(const std::string& stock_code) const {
        auto it = stock_bar_holders_.find(stock_code);
        if (it != stock_bar_holders_.end()) {
            return it->second;
        }
        return nullptr;
    }
    
    // 新增：获取所有BarSeriesHolder
    const std::unordered_map<std::string, std::shared_ptr<BarSeriesHolder>>& get_all_bar_series_holders() const {
        return stock_bar_holders_;
    }
    
    // 新增：获取指定股票的BarSeriesHolder（线程安全版本）
    BarSeriesHolder* get_stock_bar_holder(const std::string& stock_code) const {
        auto it = stock_bar_holders_.find(stock_code);
        return (it != stock_bar_holders_.end()) ? it->second.get() : nullptr;
    }
    
    // 新增：重置所有指标的计算状态（用于强制重新计算）
    void reset_all_indicator_status() {
        for (auto& [ind_name, indicator] : indicators_) {
            indicator->reset_calculation_status();
            spdlog::info("重置指标[{}]的计算状态", ind_name);
        }
    }
    
    // 新增：重置指定指标的计算状态
    void reset_indicator_status(const std::string& indicator_name) {
        auto it = indicators_.find(indicator_name);
        if (it != indicators_.end()) {
            it->second->reset_calculation_status();
            spdlog::info("重置指标[{}]的计算状态", indicator_name);
        } else {
            spdlog::warn("未找到指标[{}]", indicator_name);
        }
    }
    
    // 新增：重置差分存储（用于每天开始时重置）
    void reset_diff_storage() {
        for (auto& [ind_name, indicator] : indicators_) {
            // 尝试转换为VolumeIndicator、AmountIndicator或DiffIndicator并重置差分存储
            if (auto volume_ind = std::dynamic_pointer_cast<VolumeIndicator>(indicator)) {
                volume_ind->reset_diff_storage();
            } else if (auto amount_ind = std::dynamic_pointer_cast<AmountIndicator>(indicator)) {
                amount_ind->reset_diff_storage();
            } else if (auto diff_ind = std::dynamic_pointer_cast<DiffIndicator>(indicator)) {
                diff_ind->reset_diff_storage();
            }
        }
        spdlog::info("重置所有指标的差分存储");
        
        // 新增：同时重置所有TickDataManager、BarSeriesHolder（但不重置Factor存储）
        reset_tick_data_managers();
        reset_bar_series_holders();
        // 注意：不重置Factor存储，因为Factor数据需要在完整计算周期后保存
    }
    
    // 新增：更新TickDataManager
    void update_tick_data_manager(const SyncTickData& sync_tick) {
        auto it = stock_tick_managers_.find(sync_tick.symbol);
        if (it != stock_tick_managers_.end()) {
            it->second->update(sync_tick);
        } else {
            spdlog::warn("未找到股票{}的TickDataManager", sync_tick.symbol);
        }
    }
    
    // 新增：重置所有TickDataManager
    void reset_tick_data_managers() {
        for (auto& [stock_code, manager] : stock_tick_managers_) {
            manager->clear_history();
        }
        spdlog::info("已重置所有TickDataManager的历史数据");
    }
    
    // 重构：更新指定股票的BarSeriesHolder的时间
    void update_bar_series_holder_time(const std::string& stock_symbol, uint64_t real_time) {
        auto it = stock_bar_holders_.find(stock_symbol);
        if (it != stock_bar_holders_.end()) {
            it->second->update_time(real_time);
        }
    }

    // 保持向后兼容的重载函数（更新所有股票）
    void update_bar_series_holder_time(uint64_t real_time) {
        for (auto& [stock_code, holder] : stock_bar_holders_) {
            holder->update_time(real_time);
        }
    }
    
    // 新增：重置所有BarSeriesHolder
    void reset_bar_series_holders() {
        for (auto& [stock_code, holder] : stock_bar_holders_) {
            holder->reset_indices();
            holder->clear_daily_data();
        }
        spdlog::info("已重置所有BarSeriesHolder");
    }
    
    // 新增：设置Factor结果到CalculationEngine的存储中
    void set_factor_result(const std::string& factor_name, int ti, const std::string& stock_code, double value) {
        factor_storage_[factor_name][ti][stock_code] = value;
        spdlog::debug("设置Factor[{}]结果: ti={}, stock={}, value={}", factor_name, ti, stock_code, value);
    }
    
    // 新增：批量设置Factor结果（从GSeries）
    void set_factor_result_batch(const std::string& factor_name, int ti, const std::vector<std::string>& stock_list, const GSeries& series) {
        spdlog::info("开始设置Factor[{}]结果: ti={}, 股票数量={}, GSeries大小={}", 
                     factor_name, ti, stock_list.size(), series.get_size());
        
        int valid_count = 0;
        for (int i = 0; i < series.get_size() && i < stock_list.size(); ++i) {
            double value = series.get(i);
            if (!std::isnan(value)) {
                factor_storage_[factor_name][ti][stock_list[i]] = value;
                valid_count++;
            }
        }
        
        spdlog::info("Factor[{}]结果设置完成: ti={}, 有效数据: {}/{}, 存储后factor_storage_大小: {}", 
                     factor_name, ti, valid_count, series.get_size(), factor_storage_.size());
    }
    
    // 新增：获取Factor结果
    double get_factor_result(const std::string& factor_name, int ti, const std::string& stock_code) const {
        auto factor_it = factor_storage_.find(factor_name);
        if (factor_it != factor_storage_.end()) {
            auto ti_it = factor_it->second.find(ti);
            if (ti_it != factor_it->second.end()) {
                auto stock_it = ti_it->second.find(stock_code);
                if (stock_it != ti_it->second.end()) {
                    return stock_it->second;
                }
            }
        }
        return NAN;
    }
    
    // 新增：获取Factor的所有时间桶数据
    std::map<int, std::unordered_map<std::string, double>> get_factor_data(const std::string& factor_name) const {
        spdlog::debug("尝试获取Factor[{}]数据，当前factor_storage_大小: {}", factor_name, factor_storage_.size());
        
        auto it = factor_storage_.find(factor_name);
        if (it != factor_storage_.end()) {
            spdlog::debug("找到Factor[{}]数据，时间桶数量: {}", factor_name, it->second.size());
            return it->second;
        } else {
            spdlog::warn("未找到Factor[{}]数据", factor_name);
            // 打印所有可用的Factor名称
            std::string available_factors;
            for (const auto& [name, _] : factor_storage_) {
                available_factors += name + ", ";
            }
            if (!available_factors.empty()) {
                available_factors = available_factors.substr(0, available_factors.length() - 2);
                spdlog::debug("可用的Factor: [{}]", available_factors);
            }
        }
        return {};
    }
    
    // 新增：重置所有Factor存储
    void reset_factor_storage() {
        factor_storage_.clear();
        spdlog::info("已重置所有Factor存储");
    }

    // 重构：处理订单（直接添加到对应股票的 SyncTickData）
    void onOrder(const OrderData& order) {
        auto start_time = std::chrono::high_resolution_clock::now();
        
        stock_sync_data_[order.symbol].orders.push_back(order);
        
        auto end_time = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);
        
        spdlog::debug("[onOrder] {} 累计: {}条, 处理耗时:{}μs", order.symbol, stock_sync_data_[order.symbol].orders.size(), duration.count());
    }

    // 重构：处理成交（直接添加到对应股票的 SyncTickData）
    void onTrade(const TradeData& trade) {
        auto start_time = std::chrono::high_resolution_clock::now();
        
        stock_sync_data_[trade.symbol].trans.push_back(trade);
        
        auto end_time = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);
        
        spdlog::debug("[onTrade] {} 累计: {}条, 处理耗时:{}μs", trade.symbol, stock_sync_data_[trade.symbol].trans.size(), duration.count());
    }

    // 重构：处理Tick数据（更新 SyncTickData 并触发计算）
    void onTick(const TickData& tick) {
        auto onTick_start = std::chrono::high_resolution_clock::now();
        
        SyncTickData sync_tick;

        // 每只股票独立访问，不需要锁保护
        auto &sync_data = stock_sync_data_[tick.symbol];

        // 复制当前状态
        auto data_copy_start = std::chrono::high_resolution_clock::now();
        sync_tick = sync_data;
        auto data_copy_end = std::chrono::high_resolution_clock::now();
        auto data_copy_duration = std::chrono::duration_cast<std::chrono::microseconds>(data_copy_end - data_copy_start);

        // 更新 tick_data
        sync_tick.tick_data = tick;
        sync_tick.symbol = tick.symbol;
        sync_tick.local_time_stamp = tick.real_time;
        
        // 先更新TickDataManager和BarSeriesHolder的时间索引
        auto time_update_start = std::chrono::high_resolution_clock::now();
        update_tick_data_manager(sync_tick);
//        auto time_update_end = std::chrono::high_resolution_clock::now();
        update_bar_series_holder_time(sync_tick.symbol, sync_tick.tick_data.real_time);
        auto time_update_end = std::chrono::high_resolution_clock::now();
        auto time_update_duration = std::chrono::duration_cast<std::chrono::microseconds>(time_update_end - time_update_start);
        
        // 然后立即同步计算所有Indicator（在当前线程中）
        auto indicator_calc_start = std::chrono::high_resolution_clock::now();
        int indicator_count = 0;
        for (auto &[name, indicator]: indicators_) {
            try {
                // 修改：不再设置current_bar_holder_，让indicator直接通过股票代码获取BarSeriesHolder
                // 这样可以避免线程竞争，实现真正的并行处理
                
                auto single_indicator_start = std::chrono::high_resolution_clock::now();
                indicator->try_calculate(sync_tick);
                auto single_indicator_end = std::chrono::high_resolution_clock::now();
                auto single_indicator_duration = std::chrono::duration_cast<std::chrono::microseconds>(single_indicator_end - single_indicator_start);
                
                spdlog::debug("[onTick] Indicator[{}] 计算完成, 耗时:{}μs", name, single_indicator_duration.count());
                indicator_count++;

            } catch (const std::exception &e) {
                spdlog::error("Indicator[{}] 计算失败 for {}: {}", name, sync_tick.symbol, e.what());
            }
        }
        auto indicator_calc_end = std::chrono::high_resolution_clock::now();
        auto indicator_calc_duration = std::chrono::duration_cast<std::chrono::microseconds>(indicator_calc_end - indicator_calc_start);
        
        // 最后清理数据，准备下一个周期 - 每只股票独立访问，不需要锁保护
        auto cleanup_start = std::chrono::high_resolution_clock::now();
        auto &sync_data_for_cleanup = stock_sync_data_[tick.symbol];
        sync_data_for_cleanup.orders.clear();
        sync_data_for_cleanup.trans.clear();
        auto cleanup_end = std::chrono::high_resolution_clock::now();
        auto cleanup_duration = std::chrono::duration_cast<std::chrono::microseconds>(cleanup_end - cleanup_start);
        
        auto total_duration = std::chrono::duration_cast<std::chrono::microseconds>(cleanup_end - onTick_start);
        
        spdlog::info("[onTick] {} 处理完成: 数据复制:{}μs, 时间更新:{}μs, {}个Indicator计算:{}μs, 清理:{}μs, 总耗时:{}μs", 
                     tick.symbol, data_copy_duration.count(), time_update_duration.count(), 
                     indicator_count, indicator_calc_duration.count(), cleanup_duration.count(), total_duration.count());
    }

    // 新增：独立的Factor时间处理（按时间事件顺序，每个时间事件内按Factor多线程）
    void process_factor_time_events(const std::vector<uint64_t>& time_events) {
        spdlog::info("开始处理{}个时间事件", time_events.size());
        
        for (const auto& timestamp : time_events) {
            spdlog::debug("处理时间事件: {}", timestamp);
            
            // 创建Factor线程组，每个Factor一个线程
            std::vector<std::thread> factor_threads;
            
            for (auto& [factor_name, factor_ptr] : factors_) {
                factor_threads.emplace_back([this, factor_ptr = factor_ptr, timestamp]() {
                    try {
                        // 定义get_indicator函数
                        auto get_indicator = [this](const std::string& name) -> std::shared_ptr<Indicator> {
                            std::lock_guard<std::mutex> lock(queue_mutex_);
                            auto it = indicators_.find(name);
                            return (it != indicators_.end()) ? it->second : nullptr;
                        };
                        
                        // 优先使用新的CalculationEngine驱动的接口
                        GSeries result;
                        
                        // 检查Factor是否支持CalculationEngine驱动
                        if (factor_ptr->get_name() != "default") {  // 简单的检查，实际可以添加虚函数判断
                            // 尝试使用CalculationEngine驱动
                            int ti = calculate_time_bucket(timestamp, factor_ptr->get_frequency());
                            if (ti >= 0) {
                                result = factor_ptr->definition_with_cal_engine(shared_from_this(), stock_list_, ti);
                            }
                            
                            // 如果CalculationEngine驱动返回空结果，回退到时间戳驱动
                            if (result.get_size() == 0) {
                                result = factor_ptr->definition_with_timestamp(get_indicator, stock_list_, timestamp);
                                
                                // 如果时间戳驱动也返回空结果，回退到ti驱动
                                if (result.get_size() == 0) {
                                    if (ti >= 0) {
                                        result = factor_ptr->definition_with_accessor(get_indicator, stock_list_, ti);
                                    }
                                }
                            }
                        } else {
                            // 回退到原有的ti驱动方式
                            int ti = calculate_time_bucket(timestamp, factor_ptr->get_frequency());
                            if (ti >= 0) {
                                result = factor_ptr->definition_with_accessor(get_indicator, stock_list_, ti);
                            }
                        }
                        
                        // 将结果存储到CalculationEngine的Factor存储中（而不是Factor类内部）
                        if (result.get_size() > 0) {
                            int ti = calculate_time_bucket(timestamp, factor_ptr->get_frequency());
                            if (ti >= 0) {
                                // 使用新的批量设置方法，将GSeries数据存储到CalculationEngine中
                                set_factor_result_batch(factor_ptr->get_name(), ti, stock_list_, result);
                            }
                        }
                        
                        spdlog::debug("Factor[{}]计算完成，时间戳: {}, 有效数据: {}/{}", 
                                     factor_ptr->get_name(), timestamp, result.get_valid_num(), result.get_size());
                    } catch (const std::exception& e) {
                        spdlog::error("Factor[{}]计算失败: {}", factor_ptr->get_name(), e.what());
                    }
                });
            }
            
            // 等待当前时间事件的所有Factor线程完成
            for (auto& thread : factor_threads) {
                thread.join();
            }

            spdlog::debug("时间事件 {} 的所有Factor处理完成", timestamp);
        }
        
        spdlog::info("所有Factor时间事件处理完成");
    }

    // 新增：同步运行的Factor时间处理（与Indicator同时运行）
    void process_factor_time_events_sync(const std::vector<uint64_t>& time_events) {
        
        for (const auto& timestamp : time_events) {
            
            // 创建Factor线程组，每个Factor一个线程
            std::vector<std::thread> factor_threads;
            
            for (auto& [factor_name, factor_ptr] : factors_) {
                factor_threads.emplace_back([this, factor_ptr = factor_ptr, timestamp]() {
                    try {
                        // 定义get_indicator函数 - 不需要锁保护，因为indicators_在初始化后基本不变
                        auto get_indicator = [this](const std::string& name) -> std::shared_ptr<Indicator> {
                            auto it = indicators_.find(name);
                            return (it != indicators_.end()) ? it->second : nullptr;
                        };
                        
                        // 优先使用新的CalculationEngine驱动的接口
                        GSeries result;
                        
                        // 检查Factor是否支持CalculationEngine驱动
                        if (factor_ptr->get_name() != "default") {
                            // 尝试使用CalculationEngine驱动
                            int ti = calculate_time_bucket(timestamp, factor_ptr->get_frequency());
                            if (ti >= 0) {
                                result = factor_ptr->definition_with_cal_engine(shared_from_this(), stock_list_, ti);
                            }
                            
                            // 如果CalculationEngine驱动返回空结果，回退到ti驱动
                            if (result.get_size() == 0) {
                                result = factor_ptr->definition_with_accessor(get_indicator, stock_list_, ti);
                            }
                        } else {
                            // 回退到原有的ti驱动方式
                            int ti = calculate_time_bucket(timestamp, factor_ptr->get_frequency());
                            if (ti >= 0) {
                                result = factor_ptr->definition_with_accessor(get_indicator, stock_list_, ti);
                            }
                        }
                        
                        // 将结果存储到CalculationEngine的Factor存储中（而不是Factor类内部）
                        if (result.get_size() > 0) {
                            int ti = calculate_time_bucket(timestamp, factor_ptr->get_frequency());
                            if (ti >= 0) {
                                // 使用新的批量设置方法，将GSeries数据存储到CalculationEngine中
                                set_factor_result_batch(factor_ptr->get_name(), ti, stock_list_, result);
                            }
                        }
                        
                        spdlog::debug("Factor[{}]同步计算完成，时间戳: {}, 有效数据: {}/{}", 
                                     factor_ptr->get_name(), timestamp, result.get_valid_num(), result.get_size());
                    } catch (const std::exception& e) {
                        spdlog::error("Factor[{}]同步计算失败: {}", factor_ptr->get_name(), e.what());
                    }
                });
            }
            
            // 等待当前时间事件的所有Factor线程完成
            for (auto& thread : factor_threads) {
                thread.join();
            }

//            spdlog::debug("同步时间事件 {} 的所有Factor处理完成", timestamp);
        }
        
        spdlog::info("所有Factor时间事件处理完成");
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

    // 统一更新入口（只处理行情事件，移除时间事件处理）
    void update(const MarketAllField& field) {
        auto start_time = std::chrono::high_resolution_clock::now();
        
        // 只处理行情数据，时间事件由独立线程处理
        switch (field.type) {
            case MarketBufferType::Order: {
                auto order_start = std::chrono::high_resolution_clock::now();
                onOrder(field.get_order());
                auto order_end = std::chrono::high_resolution_clock::now();
                auto order_duration = std::chrono::duration_cast<std::chrono::microseconds>(order_end - order_start);
                
                // 更新性能统计
                perf_stats_.total_orders.fetch_add(1);
                perf_stats_.total_order_time_us.fetch_add(order_duration.count());
                uint64_t current_max = perf_stats_.max_order_time_us.load();
                while (order_duration.count() > current_max && 
                       !perf_stats_.max_order_time_us.compare_exchange_weak(current_max, order_duration.count())) {}
                
                auto total_duration = std::chrono::duration_cast<std::chrono::microseconds>(order_end - start_time);
                spdlog::debug("[update] Order处理完成: 订单处理:{}μs, 总耗时:{}μs", order_duration.count(), total_duration.count());
                break;
            }
            case MarketBufferType::Trade: {
                auto trade_start = std::chrono::high_resolution_clock::now();
                onTrade(field.get_trade());
                auto trade_end = std::chrono::high_resolution_clock::now();
                auto trade_duration = std::chrono::duration_cast<std::chrono::microseconds>(trade_end - trade_start);
                
                // 更新性能统计
                perf_stats_.total_trades.fetch_add(1);
                perf_stats_.total_trade_time_us.fetch_add(trade_duration.count());
                uint64_t current_max = perf_stats_.max_trade_time_us.load();
                while (trade_duration.count() > current_max && 
                       !perf_stats_.max_trade_time_us.compare_exchange_weak(current_max, trade_duration.count())) {}
                
                auto total_duration = std::chrono::duration_cast<std::chrono::microseconds>(trade_end - start_time);
                spdlog::debug("[update] Trade处理完成: 成交处理:{}μs, 总耗时:{}μs", trade_duration.count(), total_duration.count());
                break;
            }
            case MarketBufferType::Tick: {
                auto tick_start = std::chrono::high_resolution_clock::now();
                onTick(field.get_tick());
                auto tick_end = std::chrono::high_resolution_clock::now();
                auto tick_duration = std::chrono::duration_cast<std::chrono::microseconds>(tick_end - tick_start);
                
                // 更新性能统计
                perf_stats_.total_ticks.fetch_add(1);
                perf_stats_.total_tick_time_us.fetch_add(tick_duration.count());
                uint64_t current_max = perf_stats_.max_tick_time_us.load();
                while (tick_duration.count() > current_max && 
                       !perf_stats_.max_tick_time_us.compare_exchange_weak(current_max, tick_duration.count())) {}
                
                auto total_duration = std::chrono::duration_cast<std::chrono::microseconds>(tick_end - start_time);
                spdlog::debug("[update] Tick处理完成: Tick处理:{}μs, 总耗时:{}μs", tick_duration.count(), total_duration.count());
                break;
            }
            default:
                spdlog::warn("未知数据类型: {}", static_cast<int>(field.type));
        }
        
        // 检查是否需要输出性能统计
        auto now = std::chrono::steady_clock::now();
        if (now - last_stats_time_ >= stats_interval_) {
            perf_stats_.print_summary();
            last_stats_time_ = now;
        }
    }

    // 等待所有计算任务完成
    void wait_for_completion() {
        spdlog::info("等待所有计算任务完成...");
        
        // 等待任务队列清空
        {
            std::unique_lock<std::mutex> lock(queue_mutex_);
            while (!task_queue_.empty()) {
                spdlog::debug("等待任务队列清空，剩余任务数: {}", task_queue_.size());
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
            }
        }
        
        // 等待所有工作线程完成当前任务
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        
        spdlog::info("所有计算任务已完成");
    }
};

#endif // ALPHAFACTORFRAMEWORK_CAL_ENGINE_H
