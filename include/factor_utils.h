//
// Created by xzliu on 12/29/21.
//

#pragma once

#include <vector>
#include <algorithm>
#include <cmath>
#include <limits>
#include "compute_utils.h"

class FactorUtils {
public:
    // 计算排名
    static std::vector<int> rank(const std::vector<double>& data, bool ascending = true) {
        std::vector<std::pair<double, int>> indexed_data;
        for (size_t i = 0; i < data.size(); ++i) {
            if (std::isfinite(data[i])) {
                indexed_data.emplace_back(data[i], i);
            }
        }
        
        if (ascending) {
            std::sort(indexed_data.begin(), indexed_data.end());
        } else {
            std::sort(indexed_data.begin(), indexed_data.end(), std::greater<>());
        }
        
        std::vector<int> ranks(data.size(), -1);
        for (size_t i = 0; i < indexed_data.size(); ++i) {
            ranks[indexed_data[i].second] = static_cast<int>(i) + 1;
        }
        
        return ranks;
    }
    
    // 计算百分比排名
    static std::vector<double> rank_pct(const std::vector<double>& data, bool ascending = true) {
        auto ranks = rank(data, ascending);
        std::vector<double> pct_ranks(data.size(), std::numeric_limits<double>::quiet_NaN());
        
        int valid_count = 0;
        for (int rank : ranks) {
            if (rank > 0) valid_count++;
        }
        
        if (valid_count == 0) return pct_ranks;
        
        for (size_t i = 0; i < ranks.size(); ++i) {
            if (ranks[i] > 0) {
                pct_ranks[i] = static_cast<double>(ranks[i] - 1) / (valid_count - 1);
            }
        }
        
        return pct_ranks;
    }
    
    // 标准化 (z-score)
    static std::vector<double> z_score(const std::vector<double>& data) {
        double mean = ComputeUtils::nan_mean(data);
        double std_dev = ComputeUtils::nan_std(data);
        
        if (!std::isfinite(mean) || !std::isfinite(std_dev) || std_dev == 0.0) {
            return std::vector<double>(data.size(), std::numeric_limits<double>::quiet_NaN());
        }
        
        std::vector<double> z_scores(data.size());
        for (size_t i = 0; i < data.size(); ++i) {
            if (std::isfinite(data[i])) {
                z_scores[i] = (data[i] - mean) / std_dev;
            } else {
                z_scores[i] = std::numeric_limits<double>::quiet_NaN();
            }
        }
        
        return z_scores;
    }
    
    // 计算差分
    static std::vector<double> diff(const std::vector<double>& data, int periods = 1) {
        if (periods <= 0 || static_cast<size_t>(periods) >= data.size()) {
            return std::vector<double>(data.size(), std::numeric_limits<double>::quiet_NaN());
        }
        
        std::vector<double> result(data.size(), std::numeric_limits<double>::quiet_NaN());
        for (size_t i = periods; i < data.size(); ++i) {
            if (std::isfinite(data[i]) && std::isfinite(data[i - periods])) {
                result[i] = data[i] - data[i - periods];
            }
        }
        
        return result;
    }
    
    // 计算百分比变化
    static std::vector<double> pct_change(const std::vector<double>& data, int periods = 1) {
        if (periods <= 0 || static_cast<size_t>(periods) >= data.size()) {
            return std::vector<double>(data.size(), std::numeric_limits<double>::quiet_NaN());
        }
        
        std::vector<double> result(data.size(), std::numeric_limits<double>::quiet_NaN());
        for (size_t i = periods; i < data.size(); ++i) {
            if (std::isfinite(data[i]) && std::isfinite(data[i - periods]) && data[i - periods] != 0.0) {
                result[i] = (data[i] - data[i - periods]) / data[i - periods];
            }
        }
        
        return result;
    }
    
    // 计算累积和
    static std::vector<double> cumsum(const std::vector<double>& data) {
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
    
    // 计算累积最大值
    static std::vector<double> cummax(const std::vector<double>& data) {
        std::vector<double> result(data.size());
        double max_val = std::numeric_limits<double>::quiet_NaN();
        
        for (size_t i = 0; i < data.size(); ++i) {
            if (std::isfinite(data[i])) {
                if (!std::isfinite(max_val) || data[i] > max_val) {
                    max_val = data[i];
                }
                result[i] = max_val;
            } else {
                result[i] = std::numeric_limits<double>::quiet_NaN();
            }
        }
        
        return result;
    }
    
    // 计算累积最小值
    static std::vector<double> cummin(const std::vector<double>& data) {
        std::vector<double> result(data.size());
        double min_val = std::numeric_limits<double>::quiet_NaN();
        
        for (size_t i = 0; i < data.size(); ++i) {
            if (std::isfinite(data[i])) {
                if (!std::isfinite(min_val) || data[i] < min_val) {
                    min_val = data[i];
                }
                result[i] = min_val;
            } else {
                result[i] = std::numeric_limits<double>::quiet_NaN();
            }
        }
        
        return result;
    }
    
    // 前向填充
    static std::vector<double> ffill(const std::vector<double>& data) {
        std::vector<double> result(data.size());
        double last_valid = std::numeric_limits<double>::quiet_NaN();
        
        for (size_t i = 0; i < data.size(); ++i) {
            if (std::isfinite(data[i])) {
                last_valid = data[i];
                result[i] = data[i];
            } else {
                result[i] = last_valid;
            }
        }
        
        return result;
    }
    
    // 众数计算
    static double mode(const std::vector<double>& data) {
        std::map<double, int> count_map;
        for (const auto& val : data) {
            if (std::isfinite(val)) {
                count_map[val]++;
            }
        }
        
        if (count_map.empty()) return std::numeric_limits<double>::quiet_NaN();
        
        auto max_it = std::max_element(count_map.begin(), count_map.end(),
                                      [](const auto& a, const auto& b) { return a.second < b.second; });
        
        return max_it->first;
    }
}; 