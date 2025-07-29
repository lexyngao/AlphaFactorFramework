//
// Created by lexyn on 25-7-16.
//

#ifndef ALPHAFACTORFRAMEWORK_MY_INDICATOR_H
#define ALPHAFACTORFRAMEWORK_MY_INDICATOR_H
#pragma once
#include "data_structures.h"  // 包含基类Indicator定义
#include <unordered_map>
#include <memory> //智能指针

// 成交量指标
class VolumeIndicator : public Indicator {
public:
    // 让VolumeIndicator支持ModuleConfig构造
    explicit VolumeIndicator(const ModuleConfig& module) : Indicator(module) {}
//    VolumeIndicator();  // 构造函数声明

    // 重写计算接口
    void Calculate(const SyncTickData& tick_data) override;
    BarSeriesHolder* get_bar_series_holder(const std::string& stock_code) const;

private:




};

// 成交金额指标
class AmountIndicator : public Indicator {
public:
    // 让AmountIndicator支持ModuleConfig构造
    explicit AmountIndicator(const ModuleConfig& module) : Indicator(module) {}

    // 重写计算接口
    void Calculate(const SyncTickData& tick_data) override;
    BarSeriesHolder* get_bar_series_holder(const std::string& stock_code) const;

private:




};

#endif //ALPHAFACTORFRAMEWORK_MY_INDICATOR_H
