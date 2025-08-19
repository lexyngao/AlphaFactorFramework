#include "indicator_storage_helper.h"
#include <spdlog/spdlog.h>

// 静态成员初始化
std::unordered_map<Frequency, IndicatorStorageHelper::FrequencyConfig> IndicatorStorageHelper::frequency_configs_;
bool IndicatorStorageHelper::configs_initialized_ = false;

// 交易时间段配置（分钟）
const std::vector<std::pair<int, int>> IndicatorStorageHelper::TRADING_PERIODS = {
    {9 * 60 + 0, 9 * 60 + 30},    // 9:00-9:30
    {9 * 60 + 30, 11 * 60 + 30},  // 9:30-11:30
    {13 * 60 + 0, 14 * 60 + 57},  // 13:00-14:57
    {14 * 60 + 57, 15 * 60 + 0}   // 14:57-15:00
};

void IndicatorStorageHelper::init_frequency_configs() {
    if (configs_initialized_) return;
    
    // 计算正常交易时间（分钟）
    int normal_trading_minutes = 237; // 9:30-11:30 (120分钟) + 13:00-14:57 (117分钟)
    
    // 15秒频率配置
    frequency_configs_[Frequency::F15S] = {
        normal_trading_minutes * 4,  // 237 * 4 = 948个桶
        1,                           // 步长
        15,                          // 15秒
        TRADING_PERIODS
    };
    
    // 1分钟频率配置
    frequency_configs_[Frequency::F1MIN] = {
        normal_trading_minutes,      // 237个桶
        4,                           // 步长
        60,                          // 60秒
        TRADING_PERIODS
    };
    
    // 5分钟频率配置
    frequency_configs_[Frequency::F5MIN] = {
        (120 / 5) + (117 / 5) + ((117 % 5) > 0 ? 1 : 0),  // 24 + 23 + 1 = 48个桶
        20,                          // 步长
        300,                         // 300秒
        TRADING_PERIODS
    };
    
    // 30分钟频率配置
    frequency_configs_[Frequency::F30MIN] = {
        (120 / 30) + (117 / 30) + ((117 % 30) > 0 ? 1 : 0),  // 4 + 3 + 1 = 8个桶
        120,                         // 步长
        1800,                        // 1800秒
        TRADING_PERIODS
    };
    
    configs_initialized_ = true;
    spdlog::debug("IndicatorStorageHelper 频率配置初始化完成");
}

const IndicatorStorageHelper::FrequencyConfig& IndicatorStorageHelper::get_frequency_config(Frequency frequency) {
    if (!configs_initialized_) {
        init_frequency_configs();
    }
    
    auto it = frequency_configs_.find(frequency);
    if (it == frequency_configs_.end()) {
        spdlog::error("未找到频率 {} 的配置，使用默认15秒配置", static_cast<int>(frequency));
        return frequency_configs_[Frequency::F15S];
    }
    
    return it->second;
}

int IndicatorStorageHelper::calculate_time_bucket(uint64_t timestamp, Frequency frequency) {
    if (timestamp == 0) return -1;

    // 获取频率配置
    const auto& config = get_frequency_config(frequency);
    
    // 转换为北京时间
    int64_t utc_sec = timestamp / 1000000000;
    int64_t beijing_sec = utc_sec + 8 * 3600;  // UTC + 8小时 = 北京时间
    int64_t beijing_seconds_in_day = beijing_sec % 86400;
    int hour = static_cast<int>(beijing_seconds_in_day / 3600);
    int minute = static_cast<int>((beijing_seconds_in_day % 3600) / 60);
    int second = static_cast<int>(beijing_seconds_in_day % 60);
    int total_minutes = hour * 60 + minute;

    spdlog::debug("时间桶计算: total_ns={}, utc_sec={}, beijing_sec={}, hour={}, minute={}, second={}, total_minutes={}", 
                 timestamp, utc_sec, beijing_sec, hour, minute, second, total_minutes);

    // 检查是否在交易时间内
    bool in_trading_time = false;
    int seconds_since_open = 0;
    int time_offset = 0;  // 时间偏移，用于区分上午和下午
    
    // 检查9:00-9:30时间段
    if (total_minutes >= 9 * 60 && total_minutes < 9 * 60 + 30) {
        // 9:00:00-9:30:00 映射到 9:30 桶 (bucket=0)
        seconds_since_open = 0;
        time_offset = 0;
        in_trading_time = true;
        spdlog::debug("时间映射: {}:{} -> 9:30桶 (bucket=0)", hour, minute);
    }
    // 检查9:30-11:30时间段
    else if (total_minutes >= 9 * 60 + 30 && total_minutes < 11 * 60 + 30) {
        // 9:30:00-11:30:00 正常映射
        seconds_since_open = (total_minutes - (9 * 60 + 30)) * 60 + second;
        time_offset = 0;
        in_trading_time = true;
        spdlog::debug("正常映射: {}:{} -> 上午正常计算", hour, minute);
    }
    // 检查11:30-13:00时间段
    else if (total_minutes >= 11 * 60 + 30 && total_minutes < 13 * 60) {
        // 11:30:00-13:00:00 映射到 13:00 桶
        seconds_since_open = 0;
        time_offset = 120 * 60;  // 上午120分钟后的偏移
        in_trading_time = true;
        spdlog::debug("时间映射: {}:{} -> 13:00桶", hour, minute);
    }
    // 检查13:00-14:57时间段
    else if (total_minutes >= 13 * 60 && total_minutes < 14 * 60 + 57) {
        // 13:00:00-14:57:00 正常映射
        seconds_since_open = (total_minutes - 13 * 60) * 60 + second;
        time_offset = 120 * 60;  // 上午120分钟后的偏移
        in_trading_time = true;
        spdlog::debug("正常映射: {}:{} -> 下午正常计算", hour, minute);
    }
    // 检查14:57-15:00时间段
    else if (total_minutes >= 14 * 60 + 57 && total_minutes < 15 * 60) {
        // 14:57:00-15:00:00 映射到 14:57 桶
        seconds_since_open = 0;
        time_offset = 120 * 60;  // 上午120分钟后的偏移
        in_trading_time = true;
        spdlog::debug("时间映射: {}:{} -> 14:57桶", hour, minute);
    }
    
    if (!in_trading_time) {
        spdlog::debug("非交易时间: {}:{}", hour, minute);
        return -1;
    }
    
    // 根据频率计算时间桶索引
    int target_bucket = -1;
    
    // 计算总秒数（包括偏移）
    int total_seconds = seconds_since_open + time_offset;
    
    switch (frequency) {
        case Frequency::F15S:
            // 15秒频率：直接按秒数计算
            target_bucket = total_seconds / 15;
            break;
            
        case Frequency::F1MIN:
            // 1分钟频率：按分钟数计算
            target_bucket = total_seconds / 60;
            break;
            
        case Frequency::F5MIN:
            // 5分钟频率：按5分钟计算
            target_bucket = total_seconds / 300;
            break;
            
        case Frequency::F30MIN:
            // 30分钟频率：按30分钟计算
            target_bucket = total_seconds / 1800;
            break;
    }
    
    spdlog::debug("时间桶结果: {}:{} -> bucket={}, bars_per_day={}, total_seconds={}, time_offset={}", 
                 hour, minute, target_bucket, config.bars_per_day, total_seconds, time_offset);
    
    // 检查桶索引是否在有效范围内
    if (target_bucket < 0 || target_bucket >= config.bars_per_day) {
        spdlog::warn("时间桶索引超出范围: bucket={}, max_bars={}", target_bucket, config.bars_per_day);
        return -1;
    }
    
    return target_bucket;
}

void IndicatorStorageHelper::store_value(
    Indicator* indicator,
    const std::string& stock_code,
    const std::string& key,
    double value,
    uint64_t timestamp
) {
    if (!indicator) {
        spdlog::error("Indicator 指针为空，无法存储数据");
        return;
    }
    
    // 获取 Indicator 的频率
    Frequency frequency = indicator->frequency();
    
    // 计算时间桶索引
    int time_bucket = calculate_time_bucket(timestamp, frequency);
    
    if (time_bucket < 0) {
        spdlog::debug("时间桶计算失败，跳过存储: stock={}, key={}, timestamp={}", 
                     stock_code, key, timestamp);
        return;
    }
    
    // 获取 Indicator 的存储空间
    const auto& storage = indicator->get_storage();
    auto it = storage.find(stock_code);
    
    if (it == storage.end()) {
        spdlog::warn("未找到股票{}的存储空间", stock_code);
        return;
    }
    
    // 存储到正确的时间桶
    BarSeriesHolder* holder = it->second.get();
    if (!holder) {
        spdlog::error("股票{}的BarSeriesHolder为空", stock_code);
        return;
    }
    
    // 获取或创建GSeries
    GSeries series = holder->get_m_bar(key);
    if (series.empty()) {
        series = GSeries();
        series.resize(indicator->get_bars_per_day());
        spdlog::debug("为股票{}创建新的{} GSeries，大小={}", 
                     stock_code, key, indicator->get_bars_per_day());
    }
    
    // 在时间桶内累加值（如果已有值）
    double existing_value = series.get(time_bucket);
    if (!std::isnan(existing_value)) {
        value += existing_value;
        spdlog::debug("股票{}在桶{}中累加{}: {} + {} = {}", 
                     stock_code, time_bucket, key, existing_value, value - existing_value, value);
    }
    
    // 设置值
    series.set(time_bucket, value);
    holder->offline_set_m_bar(key, series);
    
    spdlog::debug("存储成功: stock={}, key={}, bucket={}, value={}, frequency={}", 
                 stock_code, key, time_bucket, value, static_cast<int>(frequency));
}

std::pair<int, int> IndicatorStorageHelper::get_available_data_range_from_timestamp(
    uint64_t timestamp, 
    Frequency frequency
) {
    if (timestamp == 0) return {-1, -1};

    // 转换为北京时间
    int64_t utc_sec = timestamp / 1000000000;
    int64_t beijing_sec = utc_sec + 8 * 3600;  // UTC + 8小时 = 北京时间
    int64_t beijing_seconds_in_day = beijing_sec % 86400;
    int hour = static_cast<int>(beijing_seconds_in_day / 3600);
    int minute = static_cast<int>((beijing_seconds_in_day % 3600) / 60);
    int second = static_cast<int>(beijing_seconds_in_day % 60);
    int total_minutes = hour * 60 + minute;

    spdlog::debug("时间范围计算: total_ns={}, hour={}, minute={}, second={}, total_minutes={}", 
                 timestamp, hour, minute, second, total_minutes);

    // 交易时间定义
    const int time_900 = 9 * 60 + 0;    // 9:00
    const int time_930 = 9 * 60 + 30;   // 9:30
    const int time_1130 = 11 * 60 + 30; // 11:30
    const int time_1300 = 13 * 60 + 0;  // 13:00
    const int time_1457 = 14 * 60 + 57; // 14:57

    // 检查是否在交易时间内
    if (total_minutes < time_900 || total_minutes >= time_1457) {
        spdlog::debug("非交易时间: {}:{}", hour, minute);
        return {-1, -1};  // 非交易时间
    }

    // 计算从开盘到当前时间的秒数
    int seconds_since_open = 0;
    if (total_minutes >= time_900 && total_minutes < time_930) {
        // 9:00:00-9:30:00 映射到 9:30 桶 (bucket=0)
        seconds_since_open = 0;
        spdlog::debug("时间映射: {}:{} -> 9:30桶 (bucket=0)", hour, minute);
    } else if (total_minutes >= time_930 && total_minutes < time_1130) {
        // 9:30:00-11:30:00 正常映射
        seconds_since_open = (total_minutes - time_930) * 60 + second;
        spdlog::debug("正常映射: {}:{} -> 上午正常计算", hour, minute);
    } else if (total_minutes >= time_1130 && total_minutes < time_1300) {
        // 11:30:00-13:00:00 映射到 13:00 桶
        seconds_since_open = (time_1130 - time_930) * 60;  // 上午总秒数
        spdlog::debug("时间映射: {}:{} -> 13:00桶", hour, minute);
    } else if (total_minutes >= time_1300 && total_minutes < time_1457) {
        // 13:00:00-14:57:00 正常映射
        seconds_since_open = (time_1130 - time_930) * 60 + (total_minutes - time_1300) * 60 + second;
        spdlog::debug("正常映射: {}:{} -> 下午正常计算", hour, minute);
    }

    // 根据频率计算可用的时间桶数量
    int frequency_seconds = 0;
    switch (frequency) {
        case Frequency::F15S: frequency_seconds = 15; break;
        case Frequency::F1MIN: frequency_seconds = 60; break;
        case Frequency::F5MIN: frequency_seconds = 300; break;
        case Frequency::F30MIN: frequency_seconds = 1800; break;
    }
    
    // 计算可用的时间桶范围（从0到当前时间）
    int available_buckets = seconds_since_open / frequency_seconds;
    int start_index = 0;
    int end_index = std::max(0, available_buckets);
    
    spdlog::debug("时间范围结果: {}:{} -> 可用范围[{}, {}], 频率={}秒, 可用桶数={}", 
                 hour, minute, start_index, end_index, frequency_seconds, available_buckets);
    
    return {start_index, end_index};
}

std::pair<int, int> IndicatorStorageHelper::get_data_range_from_open_to_timestamp(
    uint64_t timestamp, 
    Frequency frequency
) {
    // 这个方法是 get_available_data_range_from_timestamp 的别名，保持接口一致性
    return get_available_data_range_from_timestamp(timestamp, frequency);
}

// 新增：获取融合数据（历史数据 + 当日数据），并返回当日数据在融合数据中的索引映射
std::pair<GSeries, int> IndicatorStorageHelper::get_fused_series_with_today_index(
    BarSeriesHolder* holder,
    const std::string& output_key,
    int pre_days,
    int today_end_index,
    Frequency frequency
) {
    if (!holder) {
        spdlog::error("BarSeriesHolder为空，无法获取融合数据");
        return {GSeries(), -1};
    }
    
    GSeries fused_series;
    int today_data_start_index = 0;  // 当日数据在融合数据中的起始索引
    
    // 1. 添加历史数据（按pre_days指定的天数）
    if (pre_days > 0) {
        for (int his_index = pre_days; his_index >= 1; his_index--) {
            GSeries his_series = holder->his_slice_bar(output_key, his_index);
            if (his_series.get_size() > 0) {
                fused_series.append(his_series);
                spdlog::debug("添加历史数据: 第{}天, 大小={}, 累计大小={}", 
                             his_index, his_series.get_size(), fused_series.get_size());
            } else {
                spdlog::warn("历史数据第{}天为空", his_index);
            }
        }
        // 记录当日数据的起始位置
        today_data_start_index = fused_series.get_size();
        spdlog::debug("历史数据添加完成，当日数据起始索引: {}", today_data_start_index);
    }
    
    // 2. 添加当日数据（从0到today_end_index）
    if (holder->has_m_bar(output_key)) {
        GSeries today_series = holder->get_m_bar(output_key).head(today_end_index + 1);
        fused_series.append(today_series);
        spdlog::debug("添加当日数据: 大小={}, 总融合数据大小={}", 
                     today_series.get_size(), fused_series.get_size());
    } else {
        spdlog::warn("当日数据不存在: {}", output_key);
    }
    
    // 返回：融合数据 + 当日数据起始索引
    return {fused_series, today_data_start_index};
}

// 新增：基于时间戳和pre_days获取融合数据范围
std::pair<int, int> IndicatorStorageHelper::get_fused_data_range_from_timestamp(
    uint64_t timestamp, 
    Frequency frequency,
    int pre_days
) {
    // 首先获取当日数据的范围
    auto [start_index, end_index] = get_available_data_range_from_timestamp(timestamp, frequency);
    
    if (start_index < 0 || end_index < 0) {
        spdlog::warn("时间戳{}不在交易时间内", timestamp);
        return {-1, -1};
    }
    
    // 如果有历史数据需求，扩展数据范围
    if (pre_days > 0) {
        // 获取频率配置
        const auto& config = get_frequency_config(frequency);
        int bars_per_day = config.bars_per_day;
        
        // 计算历史数据的总长度
        int total_history_length = pre_days * bars_per_day;
        
        // 扩展起始索引，包含历史数据
        start_index = 0;  // 从历史数据开始
        end_index = total_history_length + end_index;  // 到当日数据结束
        
        spdlog::debug("融合数据范围: 历史{}天({}个桶) + 当日({}到{}), 总范围[{}, {}]", 
                     pre_days, total_history_length, start_index, end_index - total_history_length, start_index, end_index);
    }
    
    return {start_index, end_index};
}
