//
// Created by xzliu on 1/8/22.
//

#pragma once

#include <iostream>
#include <cmath>
#include <string>
#include <vector>
#include <limits>
#include <queue>
#include "spdlog/spdlog.h"

// 增量计算算子基类
class BaseIncrease {
public:
    virtual void increase(const double& new_val) = 0;
    virtual double get_value() const = 0;
    virtual void clear() = 0;
    virtual ~BaseIncrease() = default;
};

// 最大值增量计算
class IncreaseMax : public BaseIncrease {
private:
    double max_val = std::numeric_limits<double>::quiet_NaN();

public:
    void increase(const double& new_val) override;
    double get_value() const override;
    void clear() override;
};

// 最小值增量计算
class IncreaseMin : public BaseIncrease {
private:
    double min_val = std::numeric_limits<double>::quiet_NaN();

public:
    void increase(const double& new_val) override;
    double get_value() const override;
    void clear() override;
};

// 均值增量计算
class IncreaseMean : public BaseIncrease {
private:
    double mean_val = 0.0;
    int n = 0;

public:
    void increase(const double& new_val) override;
    double get_value() const override;
    void clear() override;
};

// 标准差增量计算
class IncreaseStd : public BaseIncrease {
private:
    double mean_val = 0.0;
    double sum_m2 = 0.0;
    int n = 0;

public:
    void increase(const double& new_val) override;
    double get_value() const override;
    void clear() override;
};

// 偏度增量计算
class IncreaseSkew : public BaseIncrease {
private:
    double mean_val = 0.0;
    double sum_m2 = 0.0;
    double sum_m3 = 0.0;
    int n = 0;

public:
    void increase(const double& new_val) override;
    double get_value() const override;
    void clear() override;
};

// 峰度增量计算
class IncreaseKurt : public BaseIncrease {
private:
    double mean_val = 0.0;
    double sum_m2 = 0.0;
    double sum_m3 = 0.0;
    double sum_m4 = 0.0;
    int n = 0;

public:
    void increase(const double& new_val) override;
    double get_value() const override;
    void clear() override;
};

// 中位数增量计算
class IncreaseMedian : public BaseIncrease {
private:
    std::priority_queue<double, std::vector<double>, std::less<double>> p;      // 最大堆（左半部分）
    std::priority_queue<double, std::vector<double>, std::greater<double>> q;   // 最小堆（右半部分）

public:
    void increase(const double& num) override;
    double get_value() const override;
    void clear() override;
}; 