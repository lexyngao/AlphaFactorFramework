#pragma once
#include "data_structures.h"
#include "indicator_storage_helper.h"
// 前向声明，避免循环包含
class CalculationEngine;
#include <unordered_map>
#include <vector>
#include <string>

class VolumeFactor : public Factor {
public:
    VolumeFactor(const ModuleConfig& module) : Factor(module.name, module.id, module.path, module.frequency) {}

    // 实现Factor的纯虚函数（保持兼容性）
    void Calculate(const std::vector<const Indicator*>& indicators) override {
        // 这个函数在新的架构中不会被调用，但需要实现以保持接口兼容
        spdlog::warn("VolumeFactor::Calculate被调用，但应该使用definition函数");
    }

    // 实现Factor的definition函数
    GSeries definition(
        const std::unordered_map<std::string, BarSeriesHolder*>& barRunner,
        const std::vector<std::string>& sorted_stock_list,
        int ti
    ) override;

    // 新增：支持CalculationEngine的definition函数
    GSeries definition_with_cal_engine(
        const std::shared_ptr<CalculationEngine>& cal_engine,
        const std::vector<std::string>& sorted_stock_list,
        int ti
    ) override;



    // 新增：使用访问器模式的实现
    GSeries definition_with_accessor(
        std::function<std::shared_ptr<Indicator>(const std::string&)> get_indicator,
        const std::vector<std::string>& sorted_stock_list,
        int ti
    ) override;

    // 新增：时间戳驱动的实现
    GSeries definition_with_timestamp(
        std::function<std::shared_ptr<Indicator>(const std::string&)> get_indicator,
        const std::vector<std::string>& sorted_stock_list,
        uint64_t timestamp
    ) override;
};

class PriceFactor : public Factor {
public:
    // 构造函数 - 接受ModuleConfig参数
    PriceFactor(const ModuleConfig& module) : Factor(module.name, module.id, module.path, module.frequency) {}
    
    // 实现纯虚函数Calculate（保持兼容性）
    void Calculate(const std::vector<const Indicator*>& indicators) override {
        spdlog::warn("PriceFactor::Calculate被调用，但应该使用definition函数");
    }
    
    // 原有的定义方法 - 修复参数类型
    GSeries definition(
        const std::unordered_map<std::string, BarSeriesHolder*>& bar_runners,
        const std::vector<std::string>& sorted_stock_list,
        int ti
    ) override;

    // 新增：支持CalculationEngine的definition函数
    GSeries definition_with_cal_engine(
        const std::shared_ptr<CalculationEngine>& cal_engine,
        const std::vector<std::string>& sorted_stock_list,
        int ti
    ) override;
    
    // 重写基类的definition_with_timestamp方法（保持参数一致）
    GSeries definition_with_timestamp(
        std::function<std::shared_ptr<Indicator>(const std::string&)> get_indicator,
        const std::vector<std::string>& sorted_stock_list,
        uint64_t timestamp
    ) override;
    
    // 新增：支持动态频率的扩展方法
    GSeries definition_with_timestamp_frequency(
        std::function<std::shared_ptr<Indicator>(const std::string&)> get_indicator,
        const std::vector<std::string>& sorted_stock_list,
        uint64_t timestamp,
        Frequency& target_frequency
    );

private:
    // 新增的辅助方法
    GSeries definition_with_timestamp_aggregated(
        std::function<std::shared_ptr<Indicator>(const std::string&)> get_indicator,
        const std::vector<std::string>& sorted_stock_list,
        uint64_t timestamp,
        const std::string& target_frequency
    );
    
    double calculate_aggregated_vwap(
        BarSeriesHolder* diff_holder, 
        int target_index, 
        int ratio, 
        const std::string& target_frequency
    );
    
    // 原有的方法（重命名，保持原有逻辑）
    GSeries definition_with_timestamp_original(
        std::function<std::shared_ptr<Indicator>(const std::string&)> get_indicator,
        const std::vector<std::string>& sorted_stock_list,
        uint64_t timestamp
    );
    
    // 新增：静态聚合工具函数
    static int get_aggregation_ratio(const std::string& from_freq, const std::string& to_freq);
    static int get_target_bars_per_day(const std::string& frequency);
    
    // 新增：字符串频率转换为Frequency枚举
    static Frequency string_to_frequency(const std::string& freq_str);
    static std::string frequency_to_string(Frequency& frequency);
}; 