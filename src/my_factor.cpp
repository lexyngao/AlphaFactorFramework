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

// VolumeFactor的新实现：直接访问indicator存储
GSeries VolumeFactor::definition_with_indicators(
    const std::unordered_map<std::string, std::shared_ptr<Indicator>>& indicators,
    const std::vector<std::string>& sorted_stock_list,
    int ti
) {
    GSeries result;
    
    // 获取volume indicator
    auto volume_it = indicators.find("volume");
    if (volume_it == indicators.end()) {
        spdlog::error("找不到volume indicator");
        return result;
    }
    
    const auto& volume_indicator = volume_it->second;
    Frequency indicator_freq = volume_indicator->get_frequency();
    
    // 使用通用的频率匹配函数计算时间桶映射范围
    auto [start_indicator_index, end_indicator_index] = get_time_bucket_range(ti, indicator_freq, Frequency::F1MIN);
    
    spdlog::debug("VolumeFactor计算: ti={}, 映射到{}频率范围: [{}, {}]", 
                  ti, static_cast<int>(indicator_freq), start_indicator_index, end_indicator_index);
    
    // 初始化结果序列
    result.resize(sorted_stock_list.size());
    
    for (size_t i = 0; i < sorted_stock_list.size(); ++i) {
        const std::string& stock = sorted_stock_list[i];
        double value = NAN;
        
        // 从volume indicator的存储中获取数据
        const auto& volume_storage = volume_indicator->get_storage();
        auto stock_it = volume_storage.find(stock);
        
        if (stock_it != volume_storage.end() && stock_it->second) {
            BarSeriesHolder* holder = stock_it->second.get();
            
            // 计算5分钟时间桶内的平均成交量
            double total_volume = 0.0;
            int valid_count = 0;
            
            for (int j = start_indicator_index; j <= end_indicator_index; ++j) {
                if (j >= 0 && j < holder->get_m_bar("volume").get_size()) {
                    double volume_value = holder->get_m_bar("volume").get(j);
                    if (!std::isnan(volume_value)) {
                        total_volume += volume_value;
                        valid_count++;
                    }
                }
            }
            
            if (valid_count > 0) {
                value = total_volume / valid_count;  // 计算平均值
            }
        }
        
        result.set(i, value);
    }
    
    spdlog::debug("VolumeFactor计算完成: 有效数据 {}/{}", result.get_valid_num(), result.get_size());
    return result;
}

// VolumeFactor的访问器模式实现
GSeries VolumeFactor::definition_with_accessor(
    std::function<std::shared_ptr<Indicator>(const std::string&)> get_indicator,
    const std::vector<std::string>& sorted_stock_list,
    int ti
) {
    GSeries result;
    
    // 按需获取volume indicator
    auto volume_indicator = get_indicator("volume");
    if (!volume_indicator) {
        spdlog::error("找不到volume indicator");
        return result;
    }
    
    Frequency indicator_freq = volume_indicator->get_frequency();
    
    // 使用通用的频率匹配函数计算时间桶映射范围
    auto [start_indicator_index, end_indicator_index] = get_time_bucket_range(ti, indicator_freq, Frequency::F1MIN);
    
    spdlog::debug("VolumeFactor计算: ti={}, 映射到{}频率范围: [{}, {}]", 
                  ti, static_cast<int>(indicator_freq), start_indicator_index, end_indicator_index);
    
    // 初始化结果序列
    result.resize(sorted_stock_list.size());
    
    for (size_t i = 0; i < sorted_stock_list.size(); ++i) {
        const std::string& stock = sorted_stock_list[i];
        double value = NAN;
        
        // 从volume indicator的存储中获取数据
        const auto& volume_storage = volume_indicator->get_storage();
        auto stock_it = volume_storage.find(stock);
        
        if (stock_it != volume_storage.end() && stock_it->second) {
            BarSeriesHolder* holder = stock_it->second.get();
            
            // 计算5分钟时间桶内的平均成交量
            double total_volume = 0.0;
            int valid_count = 0;
            
            for (int j = start_indicator_index; j <= end_indicator_index; ++j) {
                if (j >= 0 && j < holder->get_m_bar("volume").get_size()) {
                    double volume_value = holder->get_m_bar("volume").get(j);
                    if (!std::isnan(volume_value)) {
                        total_volume += volume_value;
                        valid_count++;
                    }
                }
            }
            
            if (valid_count > 0) {
                value = total_volume / valid_count;  // 计算平均值
            }
        }
        
        result.set(i, value);
    }
    
    spdlog::debug("VolumeFactor计算完成: 有效数据 {}/{}", result.get_valid_num(), result.get_size());
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
            // 计算5分钟时间桶内的平均价格
            double total_amount = 0.0;
            double total_volume = 0.0;
            int valid_count = 0;
            
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
                        total_amount += amount;
                        total_volume += volume;
                        valid_count++;
                        spdlog::debug("股票{}: 获取到有效数据 amount={}, volume={}", stock, amount, volume);
                    }
                } else {
                    spdlog::debug("股票{}: 索引{}无效或超出范围", stock, i);
                }
            }
            
            // 计算平均价格
            if (valid_count > 0 && total_volume > 0.0) {
                value = total_amount / total_volume;  // 平均价格 = 总金额 / 总成交量
                spdlog::debug("股票{}: 计算平均价格 value={}, total_amount={}, total_volume={}, valid_count={}", 
                             stock, value, total_amount, total_volume, valid_count);
            } else {
                spdlog::debug("股票{}: 没有有效数据或成交量为0", stock);
            }
        } else {
            spdlog::debug("股票{}: 找不到BarSeriesHolder", stock);
        }
        result.push(value);
    }
    return result;
}

// PriceFactor的新实现：直接访问indicator存储
GSeries PriceFactor::definition_with_indicators(
    const std::unordered_map<std::string, std::shared_ptr<Indicator>>& indicators,
    const std::vector<std::string>& sorted_stock_list,
    int ti
) {
    GSeries result;
    
    // 获取amount和volume indicator
    auto amount_it = indicators.find("amount");
    auto volume_it = indicators.find("volume");
    
    if (amount_it == indicators.end() || volume_it == indicators.end()) {
        spdlog::error("找不到amount或volume indicator");
        return result;
    }
    
    const auto& amount_indicator = amount_it->second;
    const auto& volume_indicator = volume_it->second;
    
    // 确保两个indicator使用相同的频率
    Frequency amount_freq = amount_indicator->get_frequency();
    Frequency volume_freq = volume_indicator->get_frequency();
    
    if (amount_freq != volume_freq) {
        spdlog::error("amount和volume indicator频率不一致: amount={}, volume={}", 
                     static_cast<int>(amount_freq), static_cast<int>(volume_freq));
        return result;
    }
    
    // 使用通用的频率匹配函数计算时间桶映射范围
    auto [start_indicator_index, end_indicator_index] = get_time_bucket_range(ti, amount_freq, Frequency::F1MIN);
    
    spdlog::debug("PriceFactor计算: ti={}, 映射到{}频率范围: [{}, {}]", 
                  ti, static_cast<int>(amount_freq), start_indicator_index, end_indicator_index);
    
    // 初始化结果序列
    result.resize(sorted_stock_list.size());
    
    for (size_t i = 0; i < sorted_stock_list.size(); ++i) {
        const std::string& stock = sorted_stock_list[i];
        double value = NAN;
        
        // 从amount和volume indicator的存储中获取数据
        const auto& amount_storage = amount_indicator->get_storage();
        const auto& volume_storage = volume_indicator->get_storage();
        
        auto amount_stock_it = amount_storage.find(stock);
        auto volume_stock_it = volume_storage.find(stock);
        
        if (amount_stock_it != amount_storage.end() && volume_stock_it != volume_storage.end() &&
            amount_stock_it->second && volume_stock_it->second) {
            
            BarSeriesHolder* amount_holder = amount_stock_it->second.get();
            BarSeriesHolder* volume_holder = volume_stock_it->second.get();
            
            // 计算5分钟时间桶内的平均价格（amount/volume）
            double total_amount = 0.0;
            double total_volume = 0.0;
            int valid_count = 0;
            
            for (int j = start_indicator_index; j <= end_indicator_index; ++j) {
                if (j >= 0 && j < amount_holder->get_m_bar("amount").get_size() && 
                    j < volume_holder->get_m_bar("volume").get_size()) {
                    
                    double amount_value = amount_holder->get_m_bar("amount").get(j);
                    double volume_value = volume_holder->get_m_bar("volume").get(j);
                    
                    if (!std::isnan(amount_value) && !std::isnan(volume_value) && volume_value > 0) {
                        total_amount += amount_value;
                        total_volume += volume_value;
                        valid_count++;
                    }
                }
            }
            
            if (valid_count > 0 && total_volume > 0) {
                value = total_amount / total_volume;  // 计算加权平均价格
            }
        }
        
        result.set(i, value);
    }
    
    spdlog::debug("PriceFactor计算完成: 有效数据 {}/{}", result.get_valid_num(), result.get_size());
    return result;
}

// PriceFactor的访问器模式实现
GSeries PriceFactor::definition_with_accessor(
    std::function<std::shared_ptr<Indicator>(const std::string&)> get_indicator,
    const std::vector<std::string>& sorted_stock_list,
    int ti
) {
    GSeries result;
    
    // 按需获取amount和volume indicator
    auto amount_indicator = get_indicator("amount");
    auto volume_indicator = get_indicator("volume");
    
    if (!amount_indicator || !volume_indicator) {
        spdlog::error("找不到amount或volume indicator");
        return result;
    }
    
    // 确保两个indicator使用相同的频率
    Frequency amount_freq = amount_indicator->get_frequency();
    Frequency volume_freq = volume_indicator->get_frequency();
    
    if (amount_freq != volume_freq) {
        spdlog::error("amount和volume indicator频率不一致: amount={}, volume={}", 
                     static_cast<int>(amount_freq), static_cast<int>(volume_freq));
        return result;
    }
    
    // 使用通用的频率匹配函数计算时间桶映射范围
    auto [start_indicator_index, end_indicator_index] = get_time_bucket_range(ti, amount_freq, Frequency::F1MIN);
    
    spdlog::debug("PriceFactor计算: ti={}, 映射到{}频率范围: [{}, {}]", 
                  ti, static_cast<int>(amount_freq), start_indicator_index, end_indicator_index);
    
    // 初始化结果序列
    result.resize(sorted_stock_list.size());
    
    for (size_t i = 0; i < sorted_stock_list.size(); ++i) {
        const std::string& stock = sorted_stock_list[i];
        double value = NAN;
        
        // 从amount和volume indicator的存储中获取数据
        const auto& amount_storage = amount_indicator->get_storage();
        const auto& volume_storage = volume_indicator->get_storage();
        
        auto amount_stock_it = amount_storage.find(stock);
        auto volume_stock_it = volume_storage.find(stock);
        
        if (amount_stock_it != amount_storage.end() && volume_stock_it != volume_storage.end() &&
            amount_stock_it->second && volume_stock_it->second) {
            
            BarSeriesHolder* amount_holder = amount_stock_it->second.get();
            BarSeriesHolder* volume_holder = volume_stock_it->second.get();
            
            // 计算5分钟时间桶内的平均价格（amount/volume）
            double total_amount = 0.0;
            double total_volume = 0.0;
            int valid_count = 0;
            
            for (int j = start_indicator_index; j <= end_indicator_index; ++j) {
                if (j >= 0 && j < amount_holder->get_m_bar("amount").get_size() && 
                    j < volume_holder->get_m_bar("volume").get_size()) {
                    
                    double amount_value = amount_holder->get_m_bar("amount").get(j);
                    double volume_value = volume_holder->get_m_bar("volume").get(j);
                    
                    if (!std::isnan(amount_value) && !std::isnan(volume_value) && volume_value > 0) {
                        total_amount += amount_value;
                        total_volume += volume_value;
                        valid_count++;
                    }
                }
            }
            
            if (valid_count > 0 && total_volume > 0) {
                value = total_amount / total_volume;  // 计算加权平均价格
            }
        }
        
        result.set(i, value);
    }
    
    spdlog::debug("PriceFactor计算完成: 有效数据 {}/{}", result.get_valid_num(), result.get_size());
    return result;
} 