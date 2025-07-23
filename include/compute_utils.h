//
// Created by xzliu on 12/29/21.
//

#pragma once

#include <cmath>
#include <limits>
#include <vector>
#include <algorithm>
#include <numeric>

class ComputeUtils {
public:
    static bool greater_than_zero(double x) {
        return x > 0.0;
    }

    static double nan_divide(double a, double b) {
        if (std::isfinite(a) && std::isfinite(b) && b != 0.0) {
            return a / b;
        }
        return std::numeric_limits<double>::quiet_NaN();
    }

    static double nan_sum(const std::vector<double>& vec) {
        double sum = 0.0;
        int count = 0;
        for (const auto& val : vec) {
            if (std::isfinite(val)) {
                sum += val;
                count++;
            }
        }
        return count > 0 ? sum : std::numeric_limits<double>::quiet_NaN();
    }

    static double nan_mean(const std::vector<double>& vec) {
        double sum = 0.0;
        int count = 0;
        for (const auto& val : vec) {
            if (std::isfinite(val)) {
                sum += val;
                count++;
            }
        }
        return count > 0 ? sum / count : std::numeric_limits<double>::quiet_NaN();
    }

    static double nan_std(const std::vector<double>& vec) {
        double mean = nan_mean(vec);
        if (!std::isfinite(mean)) return std::numeric_limits<double>::quiet_NaN();
        
        double sum_sq = 0.0;
        int count = 0;
        for (const auto& val : vec) {
            if (std::isfinite(val)) {
                sum_sq += (val - mean) * (val - mean);
                count++;
            }
        }
        return count > 1 ? std::sqrt(sum_sq / (count - 1)) : std::numeric_limits<double>::quiet_NaN();
    }

    static double nan_median(const std::vector<double>& vec) {
        std::vector<double> valid_vals;
        for (const auto& val : vec) {
            if (std::isfinite(val)) {
                valid_vals.push_back(val);
            }
        }
        
        if (valid_vals.empty()) return std::numeric_limits<double>::quiet_NaN();
        
        std::sort(valid_vals.begin(), valid_vals.end());
        int n = valid_vals.size();
        if (n % 2 == 0) {
            return (valid_vals[n/2 - 1] + valid_vals[n/2]) / 2.0;
        } else {
            return valid_vals[n/2];
        }
    }

    static double nan_skewness(const std::vector<double>& vec) {
        double mean = nan_mean(vec);
        if (!std::isfinite(mean)) return std::numeric_limits<double>::quiet_NaN();
        
        double std_dev = nan_std(vec);
        if (!std::isfinite(std_dev) || std_dev == 0.0) return std::numeric_limits<double>::quiet_NaN();
        
        double sum_cube = 0.0;
        int count = 0;
        for (const auto& val : vec) {
            if (std::isfinite(val)) {
                double z_score = (val - mean) / std_dev;
                sum_cube += z_score * z_score * z_score;
                count++;
            }
        }
        return count > 0 ? sum_cube / count : std::numeric_limits<double>::quiet_NaN();
    }

    static double nan_kurtosis(const std::vector<double>& vec) {
        double mean = nan_mean(vec);
        if (!std::isfinite(mean)) return std::numeric_limits<double>::quiet_NaN();
        
        double std_dev = nan_std(vec);
        if (!std::isfinite(std_dev) || std_dev == 0.0) return std::numeric_limits<double>::quiet_NaN();
        
        double sum_quad = 0.0;
        int count = 0;
        for (const auto& val : vec) {
            if (std::isfinite(val)) {
                double z_score = (val - mean) / std_dev;
                sum_quad += z_score * z_score * z_score * z_score;
                count++;
            }
        }
        return count > 0 ? (sum_quad / count) - 3.0 : std::numeric_limits<double>::quiet_NaN();
    }

    static double nan_corr(const std::vector<double>& vec1, const std::vector<double>& vec2) {
        if (vec1.size() != vec2.size()) return std::numeric_limits<double>::quiet_NaN();
        
        double mean1 = nan_mean(vec1);
        double mean2 = nan_mean(vec2);
        if (!std::isfinite(mean1) || !std::isfinite(mean2)) return std::numeric_limits<double>::quiet_NaN();
        
        double sum_prod = 0.0, sum_sq1 = 0.0, sum_sq2 = 0.0;
        int count = 0;
        
        for (size_t i = 0; i < vec1.size(); ++i) {
            if (std::isfinite(vec1[i]) && std::isfinite(vec2[i])) {
                double diff1 = vec1[i] - mean1;
                double diff2 = vec2[i] - mean2;
                sum_prod += diff1 * diff2;
                sum_sq1 += diff1 * diff1;
                sum_sq2 += diff2 * diff2;
                count++;
            }
        }
        
        if (count < 2) return std::numeric_limits<double>::quiet_NaN();
        
        double denominator = std::sqrt(sum_sq1 * sum_sq2);
        return denominator != 0.0 ? sum_prod / denominator : std::numeric_limits<double>::quiet_NaN();
    }

    static double nan_quantile(const std::vector<double>& vec, double q) {
        if (q < 0.0 || q > 1.0) return std::numeric_limits<double>::quiet_NaN();
        
        std::vector<double> valid_vals;
        for (const auto& val : vec) {
            if (std::isfinite(val)) {
                valid_vals.push_back(val);
            }
        }
        
        if (valid_vals.empty()) return std::numeric_limits<double>::quiet_NaN();
        
        std::sort(valid_vals.begin(), valid_vals.end());
        int n = valid_vals.size();
        
        if (q == 0.0) return valid_vals[0];
        if (q == 1.0) return valid_vals[n-1];
        
        double index = q * (n - 1);
        int lower = static_cast<int>(std::floor(index));
        int upper = static_cast<int>(std::ceil(index));
        
        if (lower == upper) return valid_vals[lower];
        
        double weight = index - lower;
        return valid_vals[lower] * (1.0 - weight) + valid_vals[upper] * weight;
    }
}; 