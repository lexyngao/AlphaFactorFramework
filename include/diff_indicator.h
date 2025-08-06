#pragma once
#include "data_structures.h"
#include <unordered_map>
#include <memory>
#include <mutex>
#include <map>
#include <vector>
#include <functional>
#include <string>

// 通用差分指标 - 可以配置任意字段进行差分计算
class DiffIndicator : public Indicator {
public:
    // 差分字段配置
    struct DiffFieldConfig {
        std::string field_name;           // 字段名（如"volume", "amount", "price"）
        std::string output_key;           // 输出键名
        std::function<double(const TickData&)> getter;  // 数据获取函数
        std::string description;          // 字段描述
    };

    explicit DiffIndicator(const ModuleConfig& module) : Indicator(module) {
        // 默认配置volume和amount差分
        setup_default_fields();
    }

    // 重写计算接口
    void Calculate(const SyncTickData& tick_data) override;
    
    // 添加差分字段
    void add_diff_field(const DiffFieldConfig& config);
    
    // 重置差分存储
    void reset_diff_storage();

    // 保存结果（支持多字段）
    bool save_results(const ModuleConfig& module, const std::string& date);

    // 获取指定字段的BarSeriesHolder
    BarSeriesHolder* get_field_bar_series_holder(const std::string& stock_code, const std::string& field_name) const;

private:
    // 差分字段配置
    std::vector<DiffFieldConfig> diff_fields_;
    
    // 时间序列缓存：字段名 -> 股票 -> 时间戳 -> 累积值
    std::unordered_map<std::string, std::unordered_map<std::string, std::map<uint64_t, double>>> time_series_caches_;
    
    // 互斥锁：字段名 -> 互斥锁
    std::unordered_map<std::string, std::unique_ptr<std::mutex>> cache_mutexes_;
    
    // 计算单个字段的差分
    double calculate_field_diff(const std::string& field_name, 
                              const std::string& stock_code,
                              uint64_t current_time,
                              double current_value);
    
    // 设置默认字段（volume和amount）
    void setup_default_fields();
}; 