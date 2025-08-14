//
// Created by lexyn on 25-7-16.
//
#include "my_indicator.h"
#include "data_structures.h"
#include <thread>
#include <sstream>
#include "spdlog/spdlog.h"

// 成交量指标实现
//VolumeIndicator::VolumeIndicator()
//    : Indicator("volume", "VolumeIndicator", "/data/indicators", Frequency::F15S) {}

void VolumeIndicator::Calculate(const SyncTickData& tick_data) {
    auto thread_id = std::this_thread::get_id();
    std::ostringstream oss;
    oss << thread_id;
    std::string thread_id_str = oss.str();

    spdlog::info("[Calculate-Enter] symbol={} thread_id={}", tick_data.symbol, thread_id_str);

    auto it = storage_.find(tick_data.symbol);
    if (it == storage_.end()) {
        spdlog::warn("[Calculate] symbol={} not found in storage_ (thread_id={})", tick_data.symbol, thread_id_str);
        return;
    }
    BarSeriesHolder* holder = it->second.get();

    int ti = get_time_bucket_index(tick_data.tick_data.real_time);
    spdlog::debug("[Calculate] symbol={} real_time={} ti={} (thread_id={})", 
                 tick_data.symbol, tick_data.tick_data.real_time, ti, thread_id_str);
    
    if (ti < 0) {
        spdlog::debug("[Calculate] symbol={} invalid ti (thread_id={}) real_time={}", 
                     tick_data.symbol, thread_id_str, tick_data.tick_data.real_time);
        return;
    }
    
    int bar_index = ti;

    std::string key = "volume";
    GSeries series = holder->get_m_bar(key);
    if (series.empty()) {
        series = GSeries();
        series.resize(get_bars_per_day());
        spdlog::debug("[Calculate] symbol={} new GSeries allocated (thread_id={})", tick_data.symbol, thread_id_str);
    }

    // 对每个快照数据都计算差分，然后在时间桶内累加
    double current_volume = tick_data.tick_data.volume;  // 当前累积成交量
    
    // 线程安全地获取和更新时间序列索引
    double volume_diff = 0.0;
    {
        std::lock_guard<std::mutex> lock(volume_cache_mutex_);
        
        // 获取该股票的时间序列缓存
        auto& stock_series = time_series_volume_cache_[tick_data.symbol];
        uint64_t current_time = tick_data.tick_data.real_time;
        
        // 查找前一个时间戳的累积值
        double prev_volume = 0.0;
        auto it = stock_series.lower_bound(current_time);
        if (it != stock_series.begin()) {
            --it;  // 前一个时间戳
            prev_volume = it->second;
            spdlog::debug("[Calculate] symbol={} found prev_volume={} at time={}", 
                         tick_data.symbol, prev_volume, it->first);
        } else {
            spdlog::debug("[Calculate] symbol={} first tick, setting prev_volume=0", 
                         tick_data.symbol);
        }
        
        // 计算差分
        volume_diff = current_volume - prev_volume;
        
        // 存储当前时间戳的累积值
        stock_series[current_time] = current_volume;
        
        spdlog::debug("[Calculate] symbol={} bucket={} time={} volume diff: {} - {} = {}", 
                     tick_data.symbol, bar_index, current_time, current_volume, prev_volume, volume_diff);
    }
    
    // 在时间桶内累加差分值（类似notebook中的 groupby('belong_min').sum()）
    double existing_volume = series.get(bar_index);
    
    if (!std::isnan(existing_volume)) {
        volume_diff += existing_volume;
        spdlog::debug("[Calculate] symbol={} accumulated volume: {} + {} = {}", 
                     tick_data.symbol, existing_volume, volume_diff - existing_volume, volume_diff);
    } else {
        // 如果时间桶内还没有值，直接使用差分值
        spdlog::debug("[Calculate] symbol={} first valid volume in bucket: {}", 
                     tick_data.symbol, volume_diff);
    }

    spdlog::debug("[Calculate] symbol={} ti={} bar_index={} volume_diff={} (thread_id={})", 
                 tick_data.symbol, ti, bar_index, volume_diff, thread_id_str);

    series.set(bar_index, volume_diff);
    holder->offline_set_m_bar(key, series);
    
    // 输出时间桶信息
    log_time_bucket_info(tick_data.symbol, bar_index, volume_diff);

    spdlog::info("[Calculate-Exit] symbol={} thread_id={}", tick_data.symbol, thread_id_str);
}

BarSeriesHolder* VolumeIndicator::get_bar_series_holder(const std::string& stock_code) const {
    auto it = storage_.find(stock_code);
    if (it != storage_.end()) {
        return it->second.get();  // 返回unique_ptr管理的原始指针
    }
    return nullptr;
}

void VolumeIndicator::reset_diff_storage() {
    std::lock_guard<std::mutex> lock(volume_cache_mutex_);
    time_series_volume_cache_.clear();
    spdlog::info("[VolumeIndicator] 重置时间序列缓存");
}

// AmountIndicator实现
void AmountIndicator::Calculate(const SyncTickData& tick_data) {
    auto thread_id = std::this_thread::get_id();
    std::ostringstream oss;
    oss << thread_id;
    std::string thread_id_str = oss.str();

    spdlog::info("[Calculate-Enter] symbol={} thread_id={}", tick_data.symbol, thread_id_str);

    auto it = storage_.find(tick_data.symbol);
    if (it == storage_.end()) {
        spdlog::warn("[Calculate] symbol={} not found in storage_ (thread_id={})", tick_data.symbol, thread_id_str);
        return;
    }
    BarSeriesHolder* holder = it->second.get();

    int ti = get_time_bucket_index(tick_data.tick_data.real_time);
    if (ti < 0) {
        spdlog::debug("[Calculate] symbol={} invalid ti (thread_id={}) real_time={}", 
                     tick_data.symbol, thread_id_str, tick_data.tick_data.real_time);
        return;
    }
    
    int bar_index = ti;

    std::string key = "amount";
    GSeries series = holder->get_m_bar(key);
    if (series.empty()) {
        series = GSeries();
        series.resize(get_bars_per_day());
        spdlog::debug("[Calculate] symbol={} new GSeries allocated (thread_id={})", tick_data.symbol, thread_id_str);
    }

    // 对每个快照数据都计算差分，然后在时间桶内累加
    double current_amount = tick_data.tick_data.total_value_traded;  // 当前累积成交额
    double prev_amount = std::numeric_limits<double>::quiet_NaN();
    
    // 线程安全地获取和更新时间序列索引
    double amount_diff = 0.0;
    {
        std::lock_guard<std::mutex> lock(amount_cache_mutex_);
        
        // 获取该股票的时间序列缓存
        auto& stock_series = time_series_amount_cache_[tick_data.symbol];
        uint64_t current_time = tick_data.tick_data.real_time;
        
        // 查找前一个时间戳的累积值
        double prev_amount = 0.0;
        auto it = stock_series.lower_bound(current_time);
        if (it != stock_series.begin()) {
            --it;  // 前一个时间戳
            prev_amount = it->second;
            spdlog::debug("[Calculate] symbol={} found prev_amount={} at time={}", 
                         tick_data.symbol, prev_amount, it->first);
        } else {
            spdlog::debug("[Calculate] symbol={} first tick, setting prev_amount=0", 
                         tick_data.symbol);
        }
        
        // 计算差分
        amount_diff = current_amount - prev_amount;
        
        // 存储当前时间戳的累积值
        stock_series[current_time] = current_amount;
        
        spdlog::debug("[Calculate] symbol={} bucket={} time={} amount diff: {} - {} = {}", 
                     tick_data.symbol, bar_index, current_time, current_amount, prev_amount, amount_diff);
    }
    
    // 差分计算已在上面完成，这里只需要处理时间桶累加
    
    // 在时间桶内累加差分值（类似 groupby('belong_min').sum()）
    double existing_amount = series.get(bar_index);
    
    if (!std::isnan(existing_amount)) {
        amount_diff += existing_amount;
        spdlog::debug("[Calculate] symbol={} accumulated amount: {} + {} = {}", 
                     tick_data.symbol, existing_amount, amount_diff - existing_amount, amount_diff);
    } else {
        // 如果时间桶内还没有值，直接使用差分值
        spdlog::debug("[Calculate] symbol={} first valid amount in bucket: {}", 
                     tick_data.symbol, amount_diff);
    }

    spdlog::debug("[Calculate] symbol={} ti={} bar_index={} amount_diff={} (thread_id={})", 
                 tick_data.symbol, ti, bar_index, amount_diff, thread_id_str);

    series.set(bar_index, amount_diff);
    holder->offline_set_m_bar(key, series);
    
    // 输出时间桶信息
    log_time_bucket_info(tick_data.symbol, bar_index, amount_diff);

    spdlog::info("[Calculate-Exit] symbol={} thread_id={}", tick_data.symbol, thread_id_str);
}

BarSeriesHolder* AmountIndicator::get_bar_series_holder(const std::string& stock_code) const {
    auto it = storage_.find(stock_code);
    if (it != storage_.end()) {
        return it->second.get();  // 返回unique_ptr管理的原始指针
    }
    return nullptr;
}

void AmountIndicator::reset_diff_storage() {
    std::lock_guard<std::mutex> lock(amount_cache_mutex_);
    time_series_amount_cache_.clear();
    spdlog::info("[AmountIndicator] 重置时间序列缓存");
}

// VolumeIndicator的aggregate方法实现
bool VolumeIndicator::aggregate(const std::string& target_frequency, std::map<int, std::map<std::string, double>>& aggregated_data) {
    try {
        // 如果目标频率与基础频率相同，直接返回
        if (target_frequency == "15S") {
            // 从存储中提取数据
            for (const auto& [stock, holder] : storage_) {
                if (!holder) continue;
                
                GSeries volume_series = holder->get_m_bar("volume");
                for (int ti = 0; ti < volume_series.get_size(); ++ti) {
                    double value = volume_series.get(ti);
                    if (!std::isnan(value)) {
                        aggregated_data[ti][stock] = value;
                    }
                }
            }
            return true;
        }
        
        // 对于其他频率，暂时返回false（可以根据需要实现）
        spdlog::warn("VolumeIndicator::aggregate: 不支持频率 {}", target_frequency);
        return false;
        
    } catch (const std::exception& e) {
        spdlog::error("VolumeIndicator::aggregate失败: {}", e.what());
        return false;
    }
}

// AmountIndicator的aggregate方法实现
bool AmountIndicator::aggregate(const std::string& target_frequency, std::map<int, std::map<std::string, double>>& aggregated_data) {
    try {
        // 如果目标频率与基础频率相同，直接返回
        if (target_frequency == "15S") {
            // 从存储中提取数据
            for (const auto& [stock, holder] : storage_) {
                if (!holder) continue;
                
                GSeries amount_series = holder->get_m_bar("amount");
                for (int ti = 0; ti < amount_series.get_size(); ++ti) {
                    double value = amount_series.get(ti);
                    if (!std::isnan(value)) {
                        aggregated_data[ti][stock] = value;
                    }
                }
            }
            return true;
        }
        
        // 对于其他频率，暂时返回false（可以根据需要实现）
        spdlog::warn("AmountIndicator::aggregate: 不支持频率 {}", target_frequency);
        return false;
        
    } catch (const std::exception& e) {
        spdlog::error("AmountIndicator::aggregate失败: {}", e.what());
        return false;
    }
}