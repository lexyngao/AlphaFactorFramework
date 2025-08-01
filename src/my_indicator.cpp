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

    // 按照notebook逻辑：对每个快照数据都计算差分，然后在时间桶内累加
    double current_volume = tick_data.tick_data.volume;  // 当前累积成交量
    double prev_volume = std::numeric_limits<double>::quiet_NaN();
    
    // 线程安全地获取和更新前一个累积值
    {
        std::lock_guard<std::mutex> lock(prev_volume_mutex_);
        auto it = prev_volume_map_.find(tick_data.symbol);
        if (it != prev_volume_map_.end()) {
            prev_volume = it->second;
        }
        // 更新前一个累积值
        prev_volume_map_[tick_data.symbol] = current_volume;
    }
    
    // 计算差分成交量（类似notebook中的 snap.acc_volume.diff()）
    double volume_diff = std::numeric_limits<double>::quiet_NaN();  // 默认NaN
    if (!std::isnan(prev_volume) && prev_volume > 0.0) {
        volume_diff = current_volume - prev_volume;
        spdlog::debug("[Calculate] symbol={} volume diff: {} - {} = {}", 
                     tick_data.symbol, current_volume, prev_volume, volume_diff);
    } else {
        // 第一次计算，返回NaN（与pandas diff()行为一致）
        volume_diff = std::numeric_limits<double>::quiet_NaN();
        spdlog::debug("[Calculate] symbol={} first calculation, volume diff: NaN", 
                     tick_data.symbol);
    }
    
    // 在时间桶内累加（类似notebook中的 groupby('belong_min').sum()）
    double existing_volume = series.get(bar_index);
    if (!std::isnan(volume_diff)) {
        // 如果有有效的差分值，直接累加到时间桶
        if (!std::isnan(existing_volume)) {
            volume_diff += existing_volume;
            spdlog::debug("[Calculate] symbol={} accumulated volume: {} + {} = {}", 
                         tick_data.symbol, existing_volume, volume_diff - existing_volume, volume_diff);
        } else {
            // 如果时间桶内还没有值，直接使用差分值
            spdlog::debug("[Calculate] symbol={} first valid volume in bucket: {}", 
                         tick_data.symbol, volume_diff);
        }
    } else {
        // 如果差分值是NaN，保持时间桶内的现有值
        if (!std::isnan(existing_volume)) {
            volume_diff = existing_volume;
            spdlog::debug("[Calculate] symbol={} keeping existing volume: {}", 
                         tick_data.symbol, volume_diff);
        }
        // 如果两者都是NaN，volume_diff保持NaN
    }

    spdlog::debug("[Calculate] symbol={} ti={} bar_index={} volume_diff={} (thread_id={})", 
                 tick_data.symbol, ti, bar_index, volume_diff, thread_id_str);

    series.set(bar_index, volume_diff);
    holder->offline_set_m_bar(key, series);

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
    std::lock_guard<std::mutex> lock(prev_volume_mutex_);
    prev_volume_map_.clear();
    spdlog::info("[VolumeIndicator] 重置差分存储");
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

    // 按照notebook逻辑：对每个快照数据都计算差分，然后在时间桶内累加
    double current_amount = tick_data.tick_data.total_value_traded;  // 当前累积成交额
    double prev_amount = std::numeric_limits<double>::quiet_NaN();
    
    // 线程安全地获取和更新前一个累积值
    {
        std::lock_guard<std::mutex> lock(prev_amount_mutex_);
        auto it = prev_amount_map_.find(tick_data.symbol);
        if (it != prev_amount_map_.end()) {
            prev_amount = it->second;
        }
        // 更新前一个累积值
        prev_amount_map_[tick_data.symbol] = current_amount;
    }
    
    // 计算差分成交额（类似notebook中的 snap.acc_amount.diff()）
    double amount_diff = std::numeric_limits<double>::quiet_NaN();  // 默认NaN
    if (!std::isnan(prev_amount) && prev_amount > 0.0) {
        amount_diff = current_amount - prev_amount;
        spdlog::debug("[Calculate] symbol={} amount diff: {} - {} = {}", 
                     tick_data.symbol, current_amount, prev_amount, amount_diff);
    } else {
        // 第一次计算，返回NaN（与pandas diff()行为一致）
        amount_diff = std::numeric_limits<double>::quiet_NaN();
        spdlog::debug("[Calculate] symbol={} first calculation, amount diff: NaN", 
                     tick_data.symbol);
    }
    
    // 在时间桶内累加（类似notebook中的 groupby('belong_min').sum()）
    double existing_amount = series.get(bar_index);
    if (!std::isnan(amount_diff)) {
        // 如果有有效的差分值，直接累加到时间桶
        if (!std::isnan(existing_amount)) {
            amount_diff += existing_amount;
            spdlog::debug("[Calculate] symbol={} accumulated amount: {} + {} = {}", 
                         tick_data.symbol, existing_amount, amount_diff - existing_amount, amount_diff);
        } else {
            // 如果时间桶内还没有值，直接使用差分值
            spdlog::debug("[Calculate] symbol={} first valid amount in bucket: {}", 
                         tick_data.symbol, amount_diff);
        }
    } else {
        // 如果差分值是NaN，保持时间桶内的现有值
        if (!std::isnan(existing_amount)) {
            amount_diff = existing_amount;
            spdlog::debug("[Calculate] symbol={} keeping existing amount: {}", 
                         tick_data.symbol, amount_diff);
        }
        // 如果两者都是NaN，amount_diff保持NaN
    }

    spdlog::debug("[Calculate] symbol={} ti={} bar_index={} amount_diff={} (thread_id={})", 
                 tick_data.symbol, ti, bar_index, amount_diff, thread_id_str);

    series.set(bar_index, amount_diff);
    holder->offline_set_m_bar(key, series);

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
    std::lock_guard<std::mutex> lock(prev_amount_mutex_);
    prev_amount_map_.clear();
    spdlog::info("[AmountIndicator] 重置差分存储");
}