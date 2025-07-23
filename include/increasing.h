//
// Created by xzliu on 12/29/21.
//

#pragma once

#include <vector>
#include <algorithm>
#include <cmath>
#include <limits>

class Increasing {
public:
    // 计算递增序列
    static std::vector<double> increasing(const std::vector<double>& data) {
        std::vector<double> result(data.size());
        double cum_sum = 0.0;
        
        for (size_t i = 0; i < data.size(); ++i) {
            if (std::isfinite(data[i])) {
                cum_sum += data[i];
                result[i] = cum_sum;
            } else {
                result[i] = std::numeric_limits<double>::quiet_NaN();
            }
        }
        
        return result;
    }
    
    // 计算递增序列的均值
    static std::vector<double> increasing_mean(const std::vector<double>& data) {
        std::vector<double> result(data.size());
        double cum_sum = 0.0;
        int count = 0;
        
        for (size_t i = 0; i < data.size(); ++i) {
            if (std::isfinite(data[i])) {
                cum_sum += data[i];
                count++;
                result[i] = cum_sum / count;
            } else {
                result[i] = std::numeric_limits<double>::quiet_NaN();
            }
        }
        
        return result;
    }
    
    // 计算递增序列的中位数
    static std::vector<double> increasing_median(const std::vector<double>& data) {
        std::vector<double> result(data.size());
        std::vector<double> valid_data;
        
        for (size_t i = 0; i < data.size(); ++i) {
            if (std::isfinite(data[i])) {
                valid_data.push_back(data[i]);
                std::sort(valid_data.begin(), valid_data.end());
                
                int n = valid_data.size();
                if (n % 2 == 0) {
                    result[i] = (valid_data[n/2 - 1] + valid_data[n/2]) / 2.0;
                } else {
                    result[i] = valid_data[n/2];
                }
            } else {
                result[i] = std::numeric_limits<double>::quiet_NaN();
            }
        }
        
        return result;
    }
    
    // 计算递增序列的75分位数
    static std::vector<double> increasing_q75(const std::vector<double>& data) {
        std::vector<double> result(data.size());
        std::vector<double> valid_data;
        
        for (size_t i = 0; i < data.size(); ++i) {
            if (std::isfinite(data[i])) {
                valid_data.push_back(data[i]);
                std::sort(valid_data.begin(), valid_data.end());
                
                int n = valid_data.size();
                if (n > 0) {
                    double index = 0.75 * (n - 1);
                    int lower = static_cast<int>(std::floor(index));
                    int upper = static_cast<int>(std::ceil(index));
                    
                    if (lower == upper) {
                        result[i] = valid_data[lower];
                    } else {
                        double weight = index - lower;
                        result[i] = valid_data[lower] * (1.0 - weight) + valid_data[upper] * weight;
                    }
                } else {
                    result[i] = std::numeric_limits<double>::quiet_NaN();
                }
            } else {
                result[i] = std::numeric_limits<double>::quiet_NaN();
            }
        }
        
        return result;
    }
}; 