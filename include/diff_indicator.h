#pragma once
#include "data_structures.h"
#include "indicator_storage_helper.h"
// 前向声明，避免循环包含
class CalculationEngine;
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

    explicit DiffIndicator(const ModuleConfig& module, int pre_days = 0) : Indicator(module), pre_days_(pre_days) {
        // 保存配置的存储频率
        storage_frequency_str_ = module.frequency;
        
        // 根据配置的频率设置内部计算频率，保持与存储频率一致
        if (module.frequency == "15S" || module.frequency == "15s") {
            frequency_ = Frequency::F15S;
        } else if (module.frequency == "1min") {
            frequency_ = Frequency::F1MIN;
        } else if (module.frequency == "5min") {
            frequency_ = Frequency::F5MIN;
        } else if (module.frequency == "30min") {
            frequency_ = Frequency::F30MIN;
        } else {
            // 默认使用15S
            frequency_ = Frequency::F15S;
            spdlog::warn("DiffIndicator[{}] 未知频率配置: {}，使用默认15S", module.name, module.frequency);
        }
        
        // 重新初始化频率参数
        init_frequency_params();
        
        spdlog::info("DiffIndicator[{}] 初始化完成: 存储频率={}, 内部频率={}, pre_days={}", 
                     module.name, module.frequency, static_cast<int>(frequency_), pre_days_);
        
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
    bool save_results(const ModuleConfig& module, const std::string& date, const std::shared_ptr<CalculationEngine>& cal_engine = nullptr);
    
    // 新增：聚合到指定频率
    bool save_results_with_frequency(const ModuleConfig& module, const std::string& date, const std::string& target_frequency);

    // 获取指定字段的BarSeriesHolder（现在从CalculationEngine获取）
    BarSeriesHolder* get_field_bar_series_holder(const std::string& stock_code, const std::string& field_name) const;

    // 实现获取指定股票BarSeriesHolder的虚函数
    BarSeriesHolder* get_stock_bar_holder(const std::string& stock_code) const override;

    // 设置CalculationEngine引用（用于获取指定股票的BarSeriesHolder）
    void set_calculation_engine(std::shared_ptr<CalculationEngine> engine);

    // 获取存储频率字符串
    const std::string& get_storage_frequency_str() const { return storage_frequency_str_; }

    // 新增：聚合到指定频率
    bool aggregate(const std::string& target_frequency,std::map<int, std::map<std::string, double>> &aggregated_data) override;

private:
    // 差分字段配置
    std::vector<DiffFieldConfig> diff_fields_;
    
    // 方案B：使用简单的成员变量存储前一个tick的TotalValueTraded
    // 字段名 -> 股票 -> 前一个tick的TotalValueTraded
    std::unordered_map<std::string, std::unordered_map<std::string, double>> prev_tick_values_;
    
    // 存储频率（从配置文件读取）
    std::string storage_frequency_str_;
    
    // 预处理天数
    int pre_days_;
    
    // 指向CalculationEngine的指针（用于获取指定股票的BarSeriesHolder）
    mutable std::shared_ptr<CalculationEngine> calculation_engine_;    
    // 计算单个字段的差分
    double calculate_field_diff(const std::string& field_name, 
                              const std::string& stock_code,
                              uint64_t current_time,
                              double current_value);
    
    // 通过时间桶索引获取累积差值（更高效）
    double get_accumulated_diff_by_bucket(const std::string& field_name, 
                                         const std::string& stock_code,
                                         int time_bucket_index,
                                         BarSeriesHolder* stock_holder);
    
    // 方案B：从TickDataManager获取前一个tick的TotalValueTraded
    double get_previous_tick_total_value(const std::string& field_name, 
                                        const std::string& stock_code);
    
    // 设置默认字段（volume和amount）
    void setup_default_fields();
    
    // 聚合辅助函数
    int get_aggregation_ratio(const std::string& from_freq, const std::string& to_freq);
    int get_target_bars_per_day(const std::string& frequency);
    void aggregate_time_segment(const GSeries& base_series, GSeries& output_series, 
                               int base_start, int base_end, int ratio, int output_start);
}; 