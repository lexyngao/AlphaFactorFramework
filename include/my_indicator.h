//
// Created by lexyn on 25-7-16.
//

#ifndef ALPHAFACTORFRAMEWORK_MY_INDICATOR_H
#define ALPHAFACTORFRAMEWORK_MY_INDICATOR_H
#pragma once
#include "data_structures.h"  // 包含基类Indicator定义
#include <unordered_map>
#include <memory> //智能指针
#include <mutex>  // 添加mutex支持

// 前向声明
class CalculationEngine;

// 成交量指标
class VolumeIndicator : public Indicator {
public:
    // 让VolumeIndicator支持ModuleConfig构造
    explicit VolumeIndicator(const ModuleConfig& module) : Indicator(module) {}
//    VolumeIndicator();  // 构造函数声明

    // 重写计算接口
    void Calculate(const SyncTickData& tick_data) override;
    BarSeriesHolder* get_bar_series_holder(const std::string& stock_code) const;

    // 实现获取指定股票BarSeriesHolder的纯虚函数
    BarSeriesHolder* get_stock_bar_holder(const std::string& stock_code) const override;

    // 设置CalculationEngine引用（用于获取指定股票的BarSeriesHolder）
    void set_calculation_engine(std::shared_ptr<CalculationEngine> engine);

    // 新增：重置差分存储
    void reset_diff_storage();
    
    // 实现纯虚函数aggregate
    bool aggregate(const std::string& target_frequency, std::map<int, std::map<std::string, double>>& aggregated_data) override;

private:
    // 使用时间序列索引存储累积值：股票 -> 时间戳 -> 累积值
    std::unordered_map<std::string, std::map<uint64_t, double>> time_series_volume_cache_;
    mutable std::mutex volume_cache_mutex_;  // 保护time_series_volume_cache_的访问
    
    // 指向CalculationEngine的指针（用于获取指定股票的BarSeriesHolder）
    mutable std::shared_ptr<CalculationEngine> calculation_engine_;
};

// 成交金额指标
class AmountIndicator : public Indicator {
public:
    // 让AmountIndicator支持ModuleConfig构造
    explicit AmountIndicator(const ModuleConfig& module) : Indicator(module) {}

    // 重写计算接口
    void Calculate(const SyncTickData& tick_data) override;
    BarSeriesHolder* get_bar_series_holder(const std::string& stock_code) const;
    
    // 实现获取指定股票BarSeriesHolder的纯虚函数
    BarSeriesHolder* get_stock_bar_holder(const std::string& stock_code) const override;
    
    // 设置CalculationEngine引用（用于获取指定股票的BarSeriesHolder）
    void set_calculation_engine(std::shared_ptr<CalculationEngine> engine);
    
    // 新增：重置差分存储
    void reset_diff_storage();
    
    // 实现纯虚函数aggregate
    bool aggregate(const std::string& target_frequency, std::map<int, std::map<std::string, double>>& aggregated_data) override;

private:
    // 使用时间序列索引存储累积值：股票 -> 时间戳 -> 累积值
    std::unordered_map<std::string, std::map<uint64_t, double>> time_series_amount_cache_;
    mutable std::mutex amount_cache_mutex_;  // 保护time_series_amount_cache_的访问
    
    // 指向CalculationEngine的指针（用于获取指定股票的BarSeriesHolder）
    mutable std::shared_ptr<CalculationEngine> calculation_engine_;
};

#endif //ALPHAFACTORFRAMEWORK_MY_INDICATOR_H
