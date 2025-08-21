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
                // [历史数据pre_length天] + [历史数据pre_length-1天] + ... + [历史数据1天] + [当日数据(0到i)]
                // 我们要访问的是当日数据的第i个位置
                // 当日数据从索引 (历史数据总长度) 开始
                
                // 计算历史数据的总长度
                int total_history_length = 0;
                if (pre_length > 0) {
                    for (int his_index = pre_length; his_index >= 1; his_index--) {
                        GSeries his_series = it->second->his_slice_bar("volume", his_index);
                        total_history_length += his_series.get_size();
                    }
                }
                
                // 当日数据的索引 = 历史数据总长度 + 当日数据中的位置i
                int today_index = total_history_length + i;
                
                spdlog::debug("股票{}: {}频率索引={}, 历史数据总长度={}, 当日索引={}, 序列大小={}", 
                             stock, static_cast<int>(indicator_freq), i, total_history_length, today_index, series.get_size());
                
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
    
    // 使用 IndicatorStorageHelper 计算可用的数据范围
    auto [start_indicator_index, end_indicator_index] = IndicatorStorageHelper::get_available_data_range_from_timestamp(timestamp, indicator_freq);
    
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


 

// 重写基类的definition_with_timestamp方法（保持参数一致）
GSeries PriceFactor::definition_with_timestamp(
    std::function<std::shared_ptr<Indicator>(const std::string&)> get_indicator,
    const std::vector<std::string>& sorted_stock_list,
    uint64_t timestamp
) {
    Frequency factor_frenquency = get_frequency();
    return definition_with_timestamp_frequency(get_indicator, sorted_stock_list, timestamp, factor_frenquency);
}

// 新增：支持动态频率的扩展方法
GSeries PriceFactor::definition_with_timestamp_frequency(
    std::function<std::shared_ptr<Indicator>(const std::string&)> get_indicator,
    const std::vector<std::string>& sorted_stock_list,
    uint64_t timestamp,
    Frequency& target_frequency
) {
    GSeries result;
    
    // 从DiffIndicator获取数据
    auto diff_indicator = get_indicator("diff_volume_amount");
    
    if (!diff_indicator) {
        spdlog::error("找不到DiffIndicator");
        return result;
    }
    
    // 获取DiffIndicator的存储频率
//    Frequency storage_freq = diff_indicator->get_frequency();

    
    // 如果目标频率与存储频率相同，直接使用原有逻辑
//    if (storage_freq == target_frequency) {
        return definition_with_timestamp_original(get_indicator, sorted_stock_list, timestamp);
//    }

//    std::string target_frequency_str = frequency_to_string(target_frequency);

    // 需要频率转换：动态聚合读取
//    return definition_with_timestamp_aggregated(get_indicator, sorted_stock_list, timestamp, target_frequency_str);
}

// 新增：处理需要频率转换的情况
GSeries PriceFactor::definition_with_timestamp_aggregated(
    std::function<std::shared_ptr<Indicator>(const std::string&)> get_indicator,
    const std::vector<std::string>& sorted_stock_list,
    uint64_t timestamp,
    const std::string& target_frequency
) {
    GSeries result;
    result.resize(sorted_stock_list.size());
    
    auto diff_indicator = get_indicator("diff_volume_amount");
    const auto& diff_storage = diff_indicator->get_storage();
    
    // 获取聚合参数（使用静态函数）
    int ratio = get_aggregation_ratio("15S", target_frequency);
    int target_bars = get_target_bars_per_day(target_frequency);
    
    // 计算目标频率下的时间索引（转换为Frequency枚举）
    Frequency target_freq_enum = string_to_frequency(target_frequency);
    auto [start_indicator_index, end_indicator_index] = IndicatorStorageHelper::get_available_data_range_from_timestamp(timestamp, target_freq_enum);
    
    if (start_indicator_index < 0 || end_indicator_index < 0) {
        spdlog::warn("时间戳{}在{}频率下不在交易时间内", timestamp, target_frequency);
        return result;
    }
    
    spdlog::debug("动态频率转换: {} -> {}, 目标索引: {}", "15S", target_frequency, end_indicator_index);
    
    for (size_t i = 0; i < sorted_stock_list.size(); ++i) {
        const std::string& stock = sorted_stock_list[i];
        double value = NAN;
        
        auto stock_it = diff_storage.find(stock);
        if (stock_it != diff_storage.end() && stock_it->second) {
            BarSeriesHolder* diff_holder = stock_it->second.get();
            
            // 动态聚合计算VWAP
            value = calculate_aggregated_vwap(diff_holder, end_indicator_index, ratio, target_frequency);
        }
        
        result.set(i, value);
    }
    
    return result;
}

// 新增：聚合+计算聚合后的VWAP
double PriceFactor::calculate_aggregated_vwap(
    BarSeriesHolder* diff_holder, 
    int target_index, 
    int ratio, 
    const std::string& target_frequency
) {
    // 计算基础15s数据的起始和结束索引
    int base_start = target_index * ratio;
    int base_end = std::min(base_start + ratio - 1, 947); // 避免超出范围
    
    // 聚合amount和volume
    double aggregated_amount = 0.0;
    double aggregated_volume = 0.0;
    int valid_count = 0;
    
    for (int i = base_start; i <= base_end; ++i) {
        if (i >= diff_holder->get_m_bar("amount").get_size() || 
            i >= diff_holder->get_m_bar("volume").get_size()) {
            continue;
        }
        
        double amount = diff_holder->get_m_bar("amount").get(i);
        double volume = diff_holder->get_m_bar("volume").get(i);
        
        if (!std::isnan(amount) && !std::isnan(volume) && volume > 0) {
            aggregated_amount += amount;
            aggregated_volume += volume;
            valid_count++;
        }
    }
    
    if (valid_count > 0 && aggregated_volume > 0) {
        return aggregated_amount / aggregated_volume;
    }
    
    return NAN;
}

// 新增：静态聚合工具函数
int PriceFactor::get_aggregation_ratio(const std::string& from_freq, const std::string& to_freq) {
    if (from_freq == "15S" && to_freq == "1min") return 4;
    if (from_freq == "15S" && to_freq == "5min") return 20;
    if (from_freq == "15S" && to_freq == "30min") return 120;
    if (from_freq == "1min" && to_freq == "5min") return 5;
    if (from_freq == "1min" && to_freq == "30min") return 30;
    if (from_freq == "5min" && to_freq == "30min") return 6;
    return 1;
}

int PriceFactor::get_target_bars_per_day(const std::string& frequency) {
    if (frequency == "15S") return 948;
    if (frequency == "1min") return 237;
    if (frequency == "5min") return 48;
    if (frequency == "30min") return 8;
    return 237;  // 默认1min
}

// 新增：字符串频率转换为Frequency枚举
Frequency PriceFactor::string_to_frequency(const std::string& freq_str) {
    if (freq_str == "15S" || freq_str == "15s") return Frequency::F15S;
    if (freq_str == "1min") return Frequency::F1MIN;
    if (freq_str == "5min") return Frequency::F5MIN;
    if (freq_str == "30min") return Frequency::F30MIN;
    return Frequency::F15S; // 默认
}

// 新增：为Frequency枚举转换为字符串
std::string PriceFactor::frequency_to_string(Frequency& frequency) {
    if (frequency == Frequency::F15S) return "15S";
    if (frequency == Frequency::F1MIN) return "1min";
    if (frequency == Frequency::F5MIN) return "5min";
    if (frequency == Frequency::F30MIN) return "30min";
    return "15S"; // 默认
}

// 原有的方法（重命名，保持原有逻辑）
GSeries PriceFactor::definition_with_timestamp_original(
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
    
    // 从ModuleConfig或GlobalConfig获取pre_days
    int pre_days = this->get_pre_days();
    spdlog::debug("PriceFactor 使用的pre_days={}", pre_days);
    
    // 使用新的融合数据范围计算方法
    auto [start_index, end_index] = IndicatorStorageHelper::get_fused_data_range_from_timestamp(
        timestamp, storage_freq, pre_days
    );

    if (start_index < 0 || end_index < 0) {
        spdlog::warn("时间戳{}不在交易时间内", timestamp);
        return result;
    }
    
    spdlog::debug("融合数据驱动PriceFactor计算: timestamp={}, pre_days={}, 映射到{}频率范围: [{}, {}]", 
                  timestamp, pre_days, static_cast<int>(storage_freq), start_index, end_index);
    
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
            
            // 获取融合数据（历史数据 + 当日数据）
            auto [fused_amount_series, amount_today_start] = IndicatorStorageHelper::get_fused_series_with_today_index(
                diff_holder, "amount", pre_days, end_index - (pre_days * get_bars_per_day(storage_freq)), storage_freq
            );
            
            auto [fused_volume_series, volume_today_start] = IndicatorStorageHelper::get_fused_series_with_today_index(
                diff_holder, "volume", pre_days, end_index - (pre_days * get_bars_per_day(storage_freq)), storage_freq
            );
            
            // 计算当日数据在融合数据中的实际索引
            int today_end_index = end_index - (pre_days * get_bars_per_day(storage_freq));
            int amount_fused_index = amount_today_start + today_end_index;
            int volume_fused_index = volume_today_start + today_end_index;
            
            spdlog::debug("股票{}: 融合数据索引映射 - 当日结束={}, amount映射={}, volume映射={}", 
                         stock, today_end_index, amount_fused_index, volume_fused_index);
            
            // 检查索引是否在有效范围内
            if (amount_fused_index >= 0 && amount_fused_index < fused_amount_series.get_size() && 
                volume_fused_index >= 0 && volume_fused_index < fused_volume_series.get_size()) {
                
                double amount_value = fused_amount_series.get(amount_fused_index);
                double volume_value = fused_volume_series.get(volume_fused_index);
                
                if (!std::isnan(amount_value) && !std::isnan(volume_value) && volume_value > 0) {
                    value = amount_value / volume_value;  // 计算VWAP
                    spdlog::debug("股票{}: 计算VWAP value={}, amount={}, volume={}", 
                                 stock, value, amount_value, volume_value);
                } else {
                    spdlog::debug("股票{}: 当前时间点数据无效 amount={}, volume={}", stock, amount_value, volume_value);
                }
            } else {
                spdlog::debug("股票{}: 融合数据索引超出范围 amount_index={}/{}, volume_index={}/{}", 
                             stock, amount_fused_index, fused_amount_series.get_size(), 
                             volume_fused_index, fused_volume_series.get_size());
            }
        } else {
            spdlog::debug("股票{}: 找不到DiffIndicator的BarSeriesHolder", stock);
        }
        
        result.set(i, value);
    }
    
    return result;
} 