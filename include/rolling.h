//
// Created by xzliu on 12/29/21.
//

#pragma once

#include <vector>
#include <deque>
#include <algorithm>
#include <cmath>
#include <limits>
#include "compute_utils.h"

class Rolling {
public:
    // 滚动求和
    static std::vector<double> rolling_sum(const std::vector<double>& data, int window, int min_periods = 1) {
        if (window <= 0 || min_periods <= 0 || min_periods > window) {
            return std::vector<double>(data.size(), std::numeric_limits<double>::quiet_NaN());
        }
        
        std::vector<double> result(data.size(), std::numeric_limits<double>::quiet_NaN());
        std::deque<double> window_data;
        double sum = 0.0;
        int valid_count = 0;
        
        for (size_t i = 0; i < data.size(); ++i) {
            // 添加新元素
            if (std::isfinite(data[i])) {
                window_data.push_back(data[i]);
                sum += data[i];
                valid_count++;
            } else {
                window_data.push_back(std::numeric_limits<double>::quiet_NaN());
            }
            
            // 移除过期元素
            if (window_data.size() > static_cast<size_t>(window)) {
                if (std::isfinite(window_data.front())) {
                    sum -= window_data.front();
                    valid_count--;
                }
                window_data.pop_front();
            }
            
            // 计算结果
            if (valid_count >= min_periods) {
                result[i] = sum;
            }
        }
        
        return result;
    }
    
    // 滚动均值
    static std::vector<double> rolling_mean(const std::vector<double>& data, int window, int min_periods = 1) {
        if (window <= 0 || min_periods <= 0 || min_periods > window) {
            return std::vector<double>(data.size(), std::numeric_limits<double>::quiet_NaN());
        }
        
        std::vector<double> result(data.size(), std::numeric_limits<double>::quiet_NaN());
        std::deque<double> window_data;
        double sum = 0.0;
        int valid_count = 0;
        
        for (size_t i = 0; i < data.size(); ++i) {
            // 添加新元素
            if (std::isfinite(data[i])) {
                window_data.push_back(data[i]);
                sum += data[i];
                valid_count++;
            } else {
                window_data.push_back(std::numeric_limits<double>::quiet_NaN());
            }
            
            // 移除过期元素
            if (window_data.size() > static_cast<size_t>(window)) {
                if (std::isfinite(window_data.front())) {
                    sum -= window_data.front();
                    valid_count--;
                }
                window_data.pop_front();
            }
            
            // 计算结果
            if (valid_count >= min_periods) {
                result[i] = sum / valid_count;
            }
        }
        
        return result;
    }
    
    // 滚动标准差
    static std::vector<double> rolling_std(const std::vector<double>& data, int window, int min_periods = 1) {
        if (window <= 0 || min_periods <= 0 || min_periods > window) {
            return std::vector<double>(data.size(), std::numeric_limits<double>::quiet_NaN());
        }
        
        std::vector<double> result(data.size(), std::numeric_limits<double>::quiet_NaN());
        std::deque<double> window_data;
        double sum = 0.0, sum_sq = 0.0;
        int valid_count = 0;
        
        for (size_t i = 0; i < data.size(); ++i) {
            // 添加新元素
            if (std::isfinite(data[i])) {
                window_data.push_back(data[i]);
                sum += data[i];
                sum_sq += data[i] * data[i];
                valid_count++;
            } else {
                window_data.push_back(std::numeric_limits<double>::quiet_NaN());
            }
            
            // 移除过期元素
            if (window_data.size() > static_cast<size_t>(window)) {
                if (std::isfinite(window_data.front())) {
                    sum -= window_data.front();
                    sum_sq -= window_data.front() * window_data.front();
                    valid_count--;
                }
                window_data.pop_front();
            }
            
            // 计算结果
            if (valid_count >= min_periods && valid_count > 1) {
                double mean = sum / valid_count;
                double variance = (sum_sq / valid_count) - (mean * mean);
                result[i] = std::sqrt(variance);
            }
        }
        
        return result;
    }
    
    // 滚动最大值
    static std::vector<double> rolling_max(const std::vector<double>& data, int window) {
        if (window <= 0) {
            return std::vector<double>(data.size(), std::numeric_limits<double>::quiet_NaN());
        }
        
        std::vector<double> result(data.size(), std::numeric_limits<double>::quiet_NaN());
        std::deque<double> window_data;
        
        for (size_t i = 0; i < data.size(); ++i) {
            // 添加新元素
            if (std::isfinite(data[i])) {
                window_data.push_back(data[i]);
            } else {
                window_data.push_back(std::numeric_limits<double>::quiet_NaN());
            }
            
            // 移除过期元素
            if (window_data.size() > static_cast<size_t>(window)) {
                window_data.pop_front();
            }
            
            // 计算结果
            if (!window_data.empty()) {
                double max_val = std::numeric_limits<double>::quiet_NaN();
                for (const auto& val : window_data) {
                    if (std::isfinite(val)) {
                        if (!std::isfinite(max_val) || val > max_val) {
                            max_val = val;
                        }
                    }
                }
                result[i] = max_val;
            }
        }
        
        return result;
    }
    
    // 滚动最小值
    static std::vector<double> rolling_min(const std::vector<double>& data, int window) {
        if (window <= 0) {
            return std::vector<double>(data.size(), std::numeric_limits<double>::quiet_NaN());
        }
        
        std::vector<double> result(data.size(), std::numeric_limits<double>::quiet_NaN());
        std::deque<double> window_data;
        
        for (size_t i = 0; i < data.size(); ++i) {
            // 添加新元素
            if (std::isfinite(data[i])) {
                window_data.push_back(data[i]);
            } else {
                window_data.push_back(std::numeric_limits<double>::quiet_NaN());
            }
            
            // 移除过期元素
            if (window_data.size() > static_cast<size_t>(window)) {
                window_data.pop_front();
            }
            
            // 计算结果
            if (!window_data.empty()) {
                double min_val = std::numeric_limits<double>::quiet_NaN();
                for (const auto& val : window_data) {
                    if (std::isfinite(val)) {
                        if (!std::isfinite(min_val) || val < min_val) {
                            min_val = val;
                        }
                    }
                }
                result[i] = min_val;
            }
        }
        
        return result;
    }
    
    // 滚动中位数
    static std::vector<double> rolling_median(const std::vector<double>& data, int window) {
        if (window <= 0) {
            return std::vector<double>(data.size(), std::numeric_limits<double>::quiet_NaN());
        }
        
        std::vector<double> result(data.size(), std::numeric_limits<double>::quiet_NaN());
        std::deque<double> window_data;
        
        for (size_t i = 0; i < data.size(); ++i) {
            // 添加新元素
            if (std::isfinite(data[i])) {
                window_data.push_back(data[i]);
            } else {
                window_data.push_back(std::numeric_limits<double>::quiet_NaN());
            }
            
            // 移除过期元素
            if (window_data.size() > static_cast<size_t>(window)) {
                window_data.pop_front();
            }
            
            // 计算结果
            if (!window_data.empty()) {
                std::vector<double> valid_data;
                for (const auto& val : window_data) {
                    if (std::isfinite(val)) {
                        valid_data.push_back(val);
                    }
                }
                
                if (!valid_data.empty()) {
                    std::sort(valid_data.begin(), valid_data.end());
                    int n = valid_data.size();
                    if (n % 2 == 0) {
                        result[i] = (valid_data[n/2 - 1] + valid_data[n/2]) / 2.0;
                    } else {
                        result[i] = valid_data[n/2];
                    }
                }
            }
        }
        
        return result;
    }
    
    // 滚动偏度
    static std::vector<double> rolling_skew(const std::vector<double>& data, int window) {
        if (window <= 0) {
            return std::vector<double>(data.size(), std::numeric_limits<double>::quiet_NaN());
        }
        
        std::vector<double> result(data.size(), std::numeric_limits<double>::quiet_NaN());
        std::deque<double> window_data;
        
        for (size_t i = 0; i < data.size(); ++i) {
            // 添加新元素
            if (std::isfinite(data[i])) {
                window_data.push_back(data[i]);
            } else {
                window_data.push_back(std::numeric_limits<double>::quiet_NaN());
            }
            
            // 移除过期元素
            if (window_data.size() > static_cast<size_t>(window)) {
                window_data.pop_front();
            }
            
            // 计算结果
            if (window_data.size() >= static_cast<size_t>(window)) {
                std::vector<double> valid_data;
                for (const auto& val : window_data) {
                    if (std::isfinite(val)) {
                        valid_data.push_back(val);
                    }
                }
                
                if (valid_data.size() >= 3) {
                    result[i] = ComputeUtils::nan_skewness(valid_data);
                }
            }
        }
        
        return result;
    }
    
    // 滚动峰度
    static std::vector<double> rolling_kurt(const std::vector<double>& data, int window) {
        if (window <= 0) {
            return std::vector<double>(data.size(), std::numeric_limits<double>::quiet_NaN());
        }
        
        std::vector<double> result(data.size(), std::numeric_limits<double>::quiet_NaN());
        std::deque<double> window_data;
        
        for (size_t i = 0; i < data.size(); ++i) {
            // 添加新元素
            if (std::isfinite(data[i])) {
                window_data.push_back(data[i]);
            } else {
                window_data.push_back(std::numeric_limits<double>::quiet_NaN());
            }
            
            // 移除过期元素
            if (window_data.size() > static_cast<size_t>(window)) {
                window_data.pop_front();
            }
            
            // 计算结果
            if (window_data.size() >= static_cast<size_t>(window)) {
                std::vector<double> valid_data;
                for (const auto& val : window_data) {
                    if (std::isfinite(val)) {
                        valid_data.push_back(val);
                    }
                }
                
                if (valid_data.size() >= 4) {
                    result[i] = ComputeUtils::nan_kurtosis(valid_data);
                }
            }
        }
        
        return result;
    }
}; 