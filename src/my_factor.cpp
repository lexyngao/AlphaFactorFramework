#include "my_factor.h"

// 根据频率获取每日时间桶数量
int get_bars_per_day(Frequency frequency) {
    switch (frequency) {
        case Frequency::F15S: return 960;   // 15秒频率，960个时间桶
        case Frequency::F1MIN: return 240;  // 1分钟频率，240个时间桶
        case Frequency::F5MIN: return 48;   // 5分钟频率，48个时间桶
        case Frequency::F30MIN: return 8;   // 30分钟频率，8个时间桶
        default: return 240;  // 默认1分钟频率
    }
}

GSeries VolumeFactor::definition(
    const std::unordered_map<std::string, BarSeriesHolder*>& barRunner,
    const std::vector<std::string>& sorted_stock_list,
    int ti
) {
    GSeries result;
    int pre_length = 0;  // 需要历史数据的天数
    
    // 动态获取volume indicator的频率
    const Indicator* volume_indicator = get_indicator_by_name("volume");
    if (!volume_indicator) {
        spdlog::error("找不到volume indicator，使用默认1分钟频率");
        return result;
    }
    
    Frequency indicator_freq = volume_indicator->get_frequency();
    spdlog::debug("volume indicator频率: {}", static_cast<int>(indicator_freq));
    
    // 使用通用的频率匹配函数计算时间桶映射范围
    // Factor固定为5分钟频率，Indicator频率动态获取
    auto [start_indicator_index, end_indicator_index] = get_time_bucket_range(ti, indicator_freq, Frequency::F5MIN);
    
    spdlog::debug("因子计算: ti={}, 映射到{}频率范围: [{}, {}]", ti, static_cast<int>(indicator_freq), start_indicator_index, end_indicator_index);
    
    for (const auto& stock : sorted_stock_list) {
        double value = NAN;
        auto it = barRunner.find(stock);
        if (it != barRunner.end() && it->second) {
            // 计算5分钟时间桶内的平均成交量
            double total_volume = 0.0;
            int valid_count = 0;
            
            for (int i = start_indicator_index; i <= end_indicator_index; ++i) {
                // 获取融合数据
                GSeries series = it->second->get_today_min_series("volume", pre_length, i);
                
                // get_today_min_series返回的数据结构：
                // [历史数据(pre_length * indicator_bars_per_day)] + [当日数据(i + 1)]
                // 我们要访问的是当日数据的第i个位置
                // 当日数据从索引 pre_length * indicator_bars_per_day 开始
                int indicator_bars_per_day = get_bars_per_day(indicator_freq);  // 动态获取indicator频率
                int today_data_start = pre_length * indicator_bars_per_day;
                int today_index = today_data_start + i;  // 当日数据的第i个位置
                
                spdlog::debug("股票{}: {}频率索引={}, 当日数据起始={}, 当日索引={}, 序列大小={}", 
                             stock, static_cast<int>(indicator_freq), i, today_data_start, today_index, series.get_size());
                
                // 修复：检查索引是否在有效范围内
                if (today_index >= 0 && today_index < series.get_size() && series.is_valid(today_index)) {
                    double volume = series.get(today_index);
                    if (!std::isnan(volume)) {
                        total_volume += volume;
                        valid_count++;
                        spdlog::debug("股票{}: 获取到有效数据 volume={}", stock, volume);
                    }
                } else {
                    spdlog::debug("股票{}: 当日索引{}无效或超出范围", stock, today_index);
                }
            }
            
            // 计算平均值
            if (valid_count > 0) {
                value = total_volume / valid_count;
                spdlog::debug("股票{}: 计算平均值 value={}, valid_count={}", stock, value, valid_count);
            } else {
                spdlog::debug("股票{}: 没有有效数据", stock);
            }
        } else {
            spdlog::debug("股票{}: 找不到BarSeriesHolder", stock);
        }
        result.push(value);
    }
    return result;
} 