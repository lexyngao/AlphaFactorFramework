#include "my_factor.h"

// 根据频率获取每日时间桶数量（与data_structures.h中的逻辑保持一致）
int get_bars_per_day(Frequency frequency) {
    int normal_trading_minutes = 237;  // 上午120分钟 + 下午117分钟
    
    switch (frequency) {
        case Frequency::F15S: return normal_trading_minutes * 4;   // 237 * 4 = 948
        case Frequency::F1MIN: return normal_trading_minutes;      // 237
        case Frequency::F5MIN: return (120 / 5) + (117 / 5) + ((117 % 5) > 0 ? 1 : 0);  // 48
        case Frequency::F30MIN: return (120 / 30) + (117 / 30) + ((117 % 30) > 0 ? 1 : 0);  // 8
        default: return normal_trading_minutes;  // 默认1分钟频率
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
//    TODO：临时调整Factor频率
    auto [start_indicator_index, end_indicator_index] = get_time_bucket_range(ti, indicator_freq, Frequency::F1MIN);
    
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



// VolumeFactor的访问器模式实现
GSeries VolumeFactor::definition_with_accessor(
    std::function<std::shared_ptr<Indicator>(const std::string&)> get_indicator,
    const std::vector<std::string>& sorted_stock_list,
    int ti
) {
    GSeries result;
    
    // 动态获取volume indicator的频率
    auto volume_indicator = get_indicator("volume");
    if (!volume_indicator) {
        spdlog::error("找不到volume indicator，使用默认1分钟频率");
        return result;
    }
    
    Frequency indicator_freq = volume_indicator->frequency();
    spdlog::debug("volume indicator频率: {}", static_cast<int>(indicator_freq));
    
    // 使用通用的频率匹配函数计算时间桶映射范围
    auto [start_indicator_index, end_indicator_index] = get_time_bucket_range(ti, indicator_freq, Frequency::F1MIN);
    
    spdlog::debug("因子计算: ti={}, 映射到{}频率范围: [{}, {}]", ti, static_cast<int>(indicator_freq), start_indicator_index, end_indicator_index);
    
    // 构建临时的bar_runners用于调用原有逻辑
    std::unordered_map<std::string, BarSeriesHolder*> temp_bar_runners;
    
    // 从indicator获取数据并构建bar_runners
    const auto& volume_storage = volume_indicator->get_storage();
    for (const auto& stock : sorted_stock_list) {
        auto stock_it = volume_storage.find(stock);
        if (stock_it != volume_storage.end() && stock_it->second) {
            temp_bar_runners[stock] = stock_it->second.get();
        }
    }
    
    // 调用原有的definition方法
    return definition(temp_bar_runners, sorted_stock_list, ti);
}

// 新增：VolumeFactor的时间戳驱动实现
GSeries VolumeFactor::definition_with_timestamp(
    std::function<std::shared_ptr<Indicator>(const std::string&)> get_indicator,
    const std::vector<std::string>& sorted_stock_list,
    uint64_t timestamp
) {
    GSeries result;
    
    // 动态获取volume indicator的频率
    auto volume_indicator = get_indicator("volume");
    if (!volume_indicator) {
        spdlog::error("找不到volume indicator，使用默认1分钟频率");
        return result;
    }
    
    Frequency indicator_freq = volume_indicator->frequency();
    spdlog::debug("volume indicator频率: {}, 时间戳: {}", static_cast<int>(indicator_freq), timestamp);
    
    // 使用新的时间戳驱动函数计算可用的数据范围
    auto [start_indicator_index, end_indicator_index] = get_available_data_range_from_timestamp(timestamp, indicator_freq);
    
    if (start_indicator_index < 0 || end_indicator_index < 0) {
        spdlog::warn("时间戳{}不在交易时间内", timestamp);
        return result;
    }
    
    spdlog::debug("时间戳驱动因子计算: timestamp={}, 映射到{}频率范围: [{}, {}]", 
                  timestamp, static_cast<int>(indicator_freq), start_indicator_index, end_indicator_index);
    
    // 构建临时的bar_runners用于调用原有逻辑
    std::unordered_map<std::string, BarSeriesHolder*> temp_bar_runners;
    // 这里需要根据实际情况构建，或者直接实现计算逻辑
    
    // 临时实现：直接计算逻辑
    for (const auto& stock : sorted_stock_list) {
        double value = NAN;
        
        // 获取indicator数据
        auto indicator = get_indicator("volume");
        if (indicator) {
            // 计算从开盘到当前时间的所有可用数据的平均值
            double total_volume = 0.0;
            int valid_count = 0;
            
            for (int i = start_indicator_index; i <= end_indicator_index; ++i) {
                // 这里需要根据indicator的实际存储结构获取数据
                // 暂时使用占位符
                spdlog::debug("股票{}: 需要获取{}频率索引{}的数据", stock, static_cast<int>(indicator_freq), i);
            }
            
            if (valid_count > 0) {
                value = total_volume / valid_count;
            }
        }
        
        result.push(value);
    }
    
    return result;
}

GSeries PriceFactor::definition(
    const std::unordered_map<std::string, BarSeriesHolder*>& barRunner,
    const std::vector<std::string>& sorted_stock_list,
    int ti
) {
    GSeries result;
    int pre_length = 0;  // 需要历史数据的天数
    
    // 动态获取amount和volume indicator的频率
    const Indicator* amount_indicator = get_indicator_by_name("amount");
    const Indicator* volume_indicator = get_indicator_by_name("volume");
    
    if (!amount_indicator || !volume_indicator) {
        spdlog::error("找不到amount或volume indicator");
        return result;
    }
    
    // 确保两个indicator使用相同的频率
    Frequency amount_freq = amount_indicator->frequency();
    Frequency volume_freq = volume_indicator->frequency();
    
    if (amount_freq != volume_freq) {
        spdlog::error("amount和volume indicator频率不一致: amount={}, volume={}", 
                     static_cast<int>(amount_freq), static_cast<int>(volume_freq));
        return result;
    }
    
    spdlog::debug("price factor计算: ti={}, indicator频率={}", ti, static_cast<int>(amount_freq));
    
    // 使用通用的频率匹配函数计算时间桶映射范围
    // Factor固定为5分钟频率，Indicator频率动态获取
    auto [start_indicator_index, end_indicator_index] = get_time_bucket_range(ti, amount_freq, Frequency::F5MIN);
    
    spdlog::debug("因子计算: ti={}, 映射到{}频率范围: [{}, {}]", ti, static_cast<int>(amount_freq), start_indicator_index, end_indicator_index);
    
    for (const auto& stock : sorted_stock_list) {
        double value = NAN;
        auto it = barRunner.find(stock);
        if (it != barRunner.end() && it->second) {
            // 计算VWAP：使用当前时间桶内最后一个有效数据点的amount/volume
            // 由于amount和volume都是差分形式，VWAP = amount / volume
            double vwap_value = NAN;
            
            for (int i = start_indicator_index; i <= end_indicator_index; ++i) {
                // 修复：直接获取完整序列，然后访问第i个元素
                GSeries amount_series = it->second->get_m_bar("amount");
                GSeries volume_series = it->second->get_m_bar("volume");
                
                // 直接访问第i个索引
                if (i >= 0 && i < amount_series.get_size() && i < volume_series.get_size() &&
                    amount_series.is_valid(i) && volume_series.is_valid(i)) {
                    
                    double amount = amount_series.get(i);
                    double volume = volume_series.get(i);
                    
                    if (!std::isnan(amount) && !std::isnan(volume) && volume > 0.0) {
                        vwap_value = amount / volume;  // 计算当前时间点的VWAP
                        spdlog::debug("股票{}: 时间点{} VWAP={}, amount={}, volume={}", stock, i, vwap_value, amount, volume);
                    }
                } else {
                    spdlog::debug("股票{}: 索引{}无效或超出范围", stock, i);
                }
            }
            
            // 使用最后一个有效的VWAP值
            if (!std::isnan(vwap_value)) {
                value = vwap_value;
                spdlog::debug("股票{}: 使用VWAP值 value={}", stock, value);
            } else {
                spdlog::debug("股票{}: 没有有效的VWAP数据", stock);
            }
        } else {
            spdlog::debug("股票{}: 找不到BarSeriesHolder", stock);
        }
        result.push(value);
    }
    return result;
}


// PriceFactor的访问器模式实现
GSeries PriceFactor::definition_with_accessor(
    std::function<std::shared_ptr<Indicator>(const std::string&)> get_indicator,
    const std::vector<std::string>& sorted_stock_list,
    int ti
) {
    GSeries result;
    
    // 从DiffIndicator获取数据
    auto diff_indicator = get_indicator("diff_volume_amount");
    
    if (!diff_indicator) {
        spdlog::error("找不到DiffIndicator");
        return result;
    }
    
    // 获取DiffIndicator的频率
    Frequency diff_freq = diff_indicator->get_frequency();
    
    // 使用通用的频率匹配函数计算时间桶映射范围
    // Factor和Indicator都是1min频率，所以映射是1:1
    auto [start_indicator_index, end_indicator_index] = get_time_bucket_range(ti, diff_freq, get_frequency());
    
    spdlog::debug("PriceFactor计算: ti={}, 映射到{}频率范围: [{}, {}]", 
                  ti, static_cast<int>(diff_freq), start_indicator_index, end_indicator_index);
    
    // 初始化结果序列
    result.resize(sorted_stock_list.size());
    
    for (size_t i = 0; i < sorted_stock_list.size(); ++i) {
        const std::string& stock = sorted_stock_list[i];
        double value = NAN;
        
        // 从DiffIndicator的存储中获取数据
        const auto& diff_storage = diff_indicator->get_storage();
        
        auto stock_it = diff_storage.find(stock);
        
        if (stock_it != diff_storage.end() && stock_it->second) {
            BarSeriesHolder* diff_holder = stock_it->second.get();
            
            // 计算VWAP：使用当前时间桶内最后一个有效数据点的amount/volume
            // 由于amount和volume都是差分形式，VWAP = amount / volume
            double vwap_value = NAN;
            
            for (int j = start_indicator_index; j <= end_indicator_index; ++j) {
                if (j >= 0 && j < diff_holder->get_m_bar("amount").get_size() && 
                    j < diff_holder->get_m_bar("volume").get_size()) {
                    
                    // 从同一个holder的不同output_key读取amount和volume
                    double amount_value = diff_holder->get_m_bar("amount").get(j);
                    double volume_value = diff_holder->get_m_bar("volume").get(j);
                    
                    if (!std::isnan(amount_value) && !std::isnan(volume_value) && volume_value > 0) {
                        vwap_value = amount_value / volume_value;  // 计算当前时间点的VWAP
                        spdlog::debug("股票{}: 时间点{} VWAP={}, amount={}, volume={}", stock, j, vwap_value, amount_value, volume_value);
                    }
                }
            }
            
            // 使用最后一个有效的VWAP值
            if (!std::isnan(vwap_value)) {
                value = vwap_value;
                spdlog::debug("股票{}: 使用VWAP值 value={}", stock, value);
            } else {
                spdlog::debug("股票{}: 没有有效的VWAP数据", stock);
            }
        } else {
            spdlog::debug("股票{}: 找不到DiffIndicator的BarSeriesHolder", stock);
        }
        
        result.set(i, value);
    }
    
    spdlog::debug("PriceFactor计算完成: 有效数据 {}/{}", result.get_valid_num(), result.get_size());
    return result;
} 

// 新增：PriceFactor的时间戳驱动实现
GSeries PriceFactor::definition_with_timestamp(
    std::function<std::shared_ptr<Indicator>(const std::string&)> get_indicator,
    const std::vector<std::string>& sorted_stock_list,
    uint64_t timestamp
) {
    GSeries result;
    
    // 从DiffIndicator获取数据
    auto diff_indicator = get_indicator("diff_volume_amount");
    
    if (!diff_indicator) {
        spdlog::error("找不到DiffIndicator");
        return result;
    }
    
    // 获取DiffIndicator的存储频率字符串
    std::string storage_freq_str = diff_indicator->get_storage_frequency_str();
    spdlog::debug("DiffIndicator存储频率: {}, 时间戳: {}", storage_freq_str, timestamp);
    
    // 将字符串频率转换为Frequency枚举类型
    Frequency storage_freq = Frequency::F15S; // 默认值
    if (storage_freq_str == "15S" || storage_freq_str == "15s") {
        storage_freq = Frequency::F15S;
    } else if (storage_freq_str == "1min") {
        storage_freq = Frequency::F1MIN;
    } else if (storage_freq_str == "5min") {
        storage_freq = Frequency::F5MIN;
    } else if (storage_freq_str == "30min") {
        storage_freq = Frequency::F30MIN;
    }
    
    // 使用新的时间戳驱动函数计算可用的数据范围
    auto [start_indicator_index, end_indicator_index] = get_available_data_range_from_timestamp(timestamp, storage_freq);


    if (start_indicator_index < 0 || end_indicator_index < 0) {
        spdlog::warn("时间戳{}不在交易时间内", timestamp);
        return result;
    }
    
    spdlog::debug("时间戳驱动PriceFactor计算: timestamp={}, 映射到{}频率范围: [{}, {}]", 
                  timestamp, static_cast<int>(storage_freq), start_indicator_index, end_indicator_index);
    
    // 初始化结果序列
    result.resize(sorted_stock_list.size());
    
    for (size_t i = 0; i < sorted_stock_list.size(); ++i) {
        const std::string& stock = sorted_stock_list[i];
        double value = NAN;
        
        // 从DiffIndicator的存储中获取数据
        const auto& diff_storage = diff_indicator->get_storage();
        
        auto stock_it = diff_storage.find(stock);
        
        if (stock_it != diff_storage.end() && stock_it->second) {
            BarSeriesHolder* diff_holder = stock_it->second.get();
            
            // 计算VWAP：使用当前时间点的amount和volume计算价格
            // 由于amount和volume都是差分形式，VWAP = amount / volume
            if (end_indicator_index >= 0 && end_indicator_index < diff_holder->get_m_bar("amount").get_size() && 
                end_indicator_index < diff_holder->get_m_bar("volume").get_size()) {

                int end_indicator_index_int = end_indicator_index;
                if(end_indicator_index_int >= 60)
                {
                    double amount_value = diff_holder->get_m_bar("amount").get(end_indicator_index_int);
                    double volume_value = diff_holder->get_m_bar("volume").get(end_indicator_index_int);
                }
                
                double amount_value = diff_holder->get_m_bar("amount").get(end_indicator_index);
                double volume_value = diff_holder->get_m_bar("volume").get(end_indicator_index);
                
                if (!std::isnan(amount_value) && !std::isnan(volume_value) && volume_value > 0) {
                    value = amount_value / volume_value;  // 计算VWAP
                    spdlog::debug("股票{}: 计算VWAP value={}, amount={}, volume={}", 
                                 stock, value, amount_value, volume_value);
                } else {
                    spdlog::debug("股票{}: 当前时间点数据无效 amount={}, volume={}", stock, amount_value, volume_value);
                }
            } else {
                spdlog::debug("股票{}: 当前时间点索引{}无效或超出范围", stock, end_indicator_index);
            }
        } else {
            spdlog::debug("股票{}: 找不到DiffIndicator的BarSeriesHolder", stock);
        }
        
        result.set(i, value);
    }
    
    return result;
} 