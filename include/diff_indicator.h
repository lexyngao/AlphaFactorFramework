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
        // 强制设置内部计算频率为15S，但保持配置的存储频率
        storage_frequency_str_ = module.frequency;  // 保存配置的存储频率
        
        // 重新设置为15S进行内部计算
        ModuleConfig internal_config = module;
        internal_config.frequency = "15S";
        
        // 重新初始化频率参数为15S
        frequency_ = Frequency::F15S;
        init_frequency_params();
        
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
    
    // 新增：聚合到指定频率
    bool save_results_with_frequency(const ModuleConfig& module, const std::string& date, const std::string& target_frequency);

    // 获取指定字段的BarSeriesHolder
    BarSeriesHolder* get_field_bar_series_holder(const std::string& stock_code, const std::string& field_name) const;

    // 获取存储频率字符串
    const std::string& get_storage_frequency_str() const { return storage_frequency_str_; }

    // 新增：聚合到指定频率
    bool aggregate(const std::string& target_frequency,std::map<int, std::map<std::string, double>> &aggregated_data) override;

private:
    // 差分字段配置
    std::vector<DiffFieldConfig> diff_fields_;
    
    // 时间序列缓存：字段名 -> 股票 -> 时间戳 -> 累积值
    std::unordered_map<std::string, std::unordered_map<std::string, std::map<uint64_t, double>>> time_series_caches_;
    
    // 互斥锁：字段名 -> 互斥锁
    std::unordered_map<std::string, std::unique_ptr<std::mutex>> cache_mutexes_;
    
    // 存储频率（从配置文件读取）
    std::string storage_frequency_str_;
    
    // 计算单个字段的差分
    double calculate_field_diff(const std::string& field_name, 
                              const std::string& stock_code,
                              uint64_t current_time,
                              double current_value);
    
    // 设置默认字段（volume和amount）
    void setup_default_fields();
    
    // 聚合辅助函数
    int get_aggregation_ratio(const std::string& from_freq, const std::string& to_freq);
    int get_target_bars_per_day(const std::string& frequency);
    void aggregate_time_segment(const GSeries& base_series, GSeries& output_series, 
                               int base_start, int base_end, int ratio, int output_start);
}; 