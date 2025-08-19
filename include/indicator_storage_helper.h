#pragma once

#include "data_structures.h"
#include <unordered_map>
#include <vector>
#include <utility>
#include <spdlog/spdlog.h>

// Indicator 存储辅助工具类
// 负责根据 Indicator 的频率计算时间桶索引，并将数据存储到正确位置
class IndicatorStorageHelper {
public:
    // 频率配置结构
    struct FrequencyConfig {
        int bars_per_day;      // 每日时间桶数量
        int step_size;         // 步长
        int bucket_seconds;    // 每个桶的秒数
        std::vector<std::pair<int, int>> trading_periods; // 交易时间段（分钟）
    };

    // 核心方法：接收 Indicator 的数据并存储到正确位置
    static void store_value(
        Indicator* indicator,                   // Indicator 实例
        const std::string& stock_code,          // 股票代码
        const std::string& key,                 // 数据键名（如 "volume", "amount"）
        double value,                           // 数据值
        uint64_t timestamp                      // 时间戳
    );
    
    // 新增：基于时间戳计算可用的数据范围（时间戳驱动）
    static std::pair<int, int> get_available_data_range_from_timestamp(
        uint64_t timestamp, 
        Frequency frequency
    );
    
    // 新增：获取从开盘到指定时间的可用数据范围
    static std::pair<int, int> get_data_range_from_open_to_timestamp(
        uint64_t timestamp, 
        Frequency frequency
    );
    
    // 新增：获取融合数据（历史数据 + 当日数据），并返回当日数据在融合数据中的索引映射
    static std::pair<GSeries, int> get_fused_series_with_today_index(
        BarSeriesHolder* holder,                    // 数据持有者
        const std::string& output_key,              // 数据键名（如 "volume", "amount"）
        int pre_days,                               // 需要的历史天数
        int today_end_index,                        // 当日数据的结束索引
        Frequency frequency                         // 数据频率
    );
    
    // 新增：基于时间戳和pre_days获取融合数据范围
    static std::pair<int, int> get_fused_data_range_from_timestamp(
        uint64_t timestamp, 
        Frequency frequency,
        int pre_days
    );

private:
    // 计算时间桶索引（支持不同频率）
    static int calculate_time_bucket(uint64_t timestamp, Frequency frequency);
    
    // 获取频率配置
    static const FrequencyConfig& get_frequency_config(Frequency frequency);
    
    // 初始化频率配置（在第一次调用时自动初始化）
    static void init_frequency_configs();
    
    // 频率配置映射（静态成员）
    static std::unordered_map<Frequency, FrequencyConfig> frequency_configs_;
    static bool configs_initialized_;
    
    // 交易时间段配置
    static const std::vector<std::pair<int, int>> TRADING_PERIODS;
};
