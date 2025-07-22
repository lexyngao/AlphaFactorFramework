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
    BaseSeriesHolder* holder = it->second.get();

    int ti = get_time_bucket_index(tick_data.tick_data.real_time);
    if (ti < 0) {
        spdlog::debug("[Calculate] symbol={} invalid ti (thread_id={}) real_time={}", 
                     tick_data.symbol, thread_id_str, tick_data.tick_data.real_time);
        return;
    }
    
    // 修复：ti已经是正确的桶索引，直接使用
    int bar_index = ti;

    // 提前定义变量，避免重复定义
    std::string key = "volume";
    int current_day_index = 5;
    GSeries series = holder->his_slice_bar(key, current_day_index);
    if (series.empty()) {
        series = GSeries();
        series.resize(get_bars_per_day());
        spdlog::debug("[Calculate] symbol={} new GSeries allocated (thread_id={})", tick_data.symbol, thread_id_str);
    }

    double volume = 0.0;
    
    // 1. 计算成交数据中的成交量
    for (const auto& trans : tick_data.trans) {
        volume += trans.volume;
    }
    
    // 2. 如果成交数据为空，使用快照数据中的成交量
    if (volume == 0.0 && tick_data.tick_data.volume > 0.0) {
        volume = tick_data.tick_data.volume;
        spdlog::debug("[Calculate] symbol={} using snapshot volume: {}", tick_data.symbol, volume);
    }
    
    // 3. 对于30分钟频率，需要累积该桶内的所有成交量
    // 获取当前桶的已有数据，进行累加
    double existing_volume = series.get(bar_index);
    if (!std::isnan(existing_volume)) {
        volume += existing_volume;
        spdlog::debug("[Calculate] symbol={} accumulated volume: {} + {} = {}", 
                     tick_data.symbol, existing_volume, volume - existing_volume, volume);
    }
    
    // 4. 如果还是没有数据，记录警告
    if (volume == 0.0) {
        spdlog::debug("[Calculate] symbol={} no volume data found", tick_data.symbol);
    }

    spdlog::debug("[Calculate] symbol={} ti={} bar_index={} volume={} (thread_id={})", tick_data.symbol, ti, bar_index, volume, thread_id_str);

    series.set(bar_index, volume);
    holder->set_his_series(key, current_day_index, series);

    spdlog::info("[Calculate-Exit] symbol={} thread_id={}", tick_data.symbol, thread_id_str);
}

BaseSeriesHolder* VolumeIndicator::get_bar_series_holder(const std::string& stock_code) const {
    auto it = storage_.find(stock_code);
    if (it != storage_.end()) {
        return it->second.get();  // 返回unique_ptr管理的原始指针
    }
    return nullptr;
}