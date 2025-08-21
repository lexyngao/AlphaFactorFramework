#include "data_structures.h"
#include "increasing.h"
#include "compute_utils.h"
#include "factor_utils.h"
#include <algorithm>
#include <cmath>
#include <queue>
#include <map>

// ======================== 统计算子实现 ========================
double GSeries::skewness() const {
    IncreaseSkew skew;
    for (const auto& d : d_vec) {
        if (std::isfinite(d)) skew.increase(d);
    }
    return skew.get_value();
}

double GSeries::kurtosis() const {
    IncreaseKurt kurt;
    for (const auto& d : d_vec) {
        if (std::isfinite(d)) kurt.increase(d);
    }
    return kurt.get_value();
}

double GSeries::nanmedian() const {
    IncreaseMedian median;
    for (const double& d : d_vec) {
        if (std::isfinite(d)) median.increase(d);
    }
    return median.get_value();
}

// ======================== 分位数算子实现 ========================
double GSeries::nanquantile(const double& q) const {
    std::vector<double> valid_vec;
    for (const auto& d : d_vec) {
        if (std::isfinite(d)) valid_vec.push_back(d);
    }
    const int n = int(valid_vec.size());

    if (n == 0) {
        return std::numeric_limits<double>::quiet_NaN();
    } else {
        std::sort(valid_vec.begin(), valid_vec.end());
        double id = (n - 1) * q;
        id = std::max(0.0, std::min(id, double(n - 1)));
        int lo = std::floor(id);
        lo = std::max(0, std::min(n - 1, lo));
        int hi = std::ceil(id);
        hi = std::max(0, std::min(n - 1, hi));
        double qs = valid_vec[lo];
        double h = (id - lo);
        return (1.0 - h) * qs + h * valid_vec[hi];
    }
}

std::vector<double> GSeries::nanquantile(const std::vector<double>& q_list) const {
    std::vector<double> valid_vec;
    for (const auto& d : d_vec) {
        if (std::isfinite(d)) valid_vec.push_back(d);
    }
    const int n = int(valid_vec.size());

    if (n == 0) {
        return std::vector<double>(q_list.size(), std::numeric_limits<double>::quiet_NaN());
    } else {
        std::sort(valid_vec.begin(), valid_vec.end());
        std::vector<double> q_vector;
        for (const auto& q : q_list) {
            double id = (n - 1) * q;
            id = std::max(0.0, std::min(id, double(n - 1)));
            int lo = std::floor(id);
            lo = std::max(0, std::min(n - 1, lo));
            int hi = std::ceil(id);
            hi = std::max(0, std::min(n - 1, hi));
            double qs = valid_vec[lo];
            double h = (id - lo);
            double q_val = (1.0 - h) * qs + h * valid_vec[hi];
            q_vector.push_back(q_val);
        }
        return q_vector;
    }
}

// ======================== 排序算子实现 ========================
std::vector<int> GSeries::arg_sort() const {
    std::vector<int> idx(d_vec.size());
    for (int i = 0; i < d_vec.size(); i++) idx[i] = i;
    std::sort(idx.begin(), idx.end(), [this](int i1, int i2) {
        return d_vec[i1] < d_vec[i2];
    });
    return idx;
}

GSeries GSeries::nan_reduce_sort(const bool& reverse) const {
    std::vector<double> sort_value;
    for (const double& d : d_vec) {
        if (std::isfinite(d)) sort_value.push_back(d);
    }
    if (reverse) {
        std::sort(sort_value.rbegin(), sort_value.rend());
    } else {
        std::sort(sort_value.begin(), sort_value.end());
    }
    return GSeries(sort_value);
}

// ======================== 滚动算子实现 ========================
GSeries GSeries::rolling_sum(const int& num, const int& min_period) const {
    RollingSum rollingSum(num);
    rollingSum.set_min_periods(min_period);
    std::vector<double> vec_roll(size);
    for (int i = 0; i < size; i++) {
        rollingSum.increase(d_vec[i]);
        vec_roll[i] = rollingSum.get_value();
    }
    return GSeries(vec_roll);
}

GSeries GSeries::rolling_mean(const int& num, const int& min_period) const {
    RollingMean rollingMean(num);
    rollingMean.set_min_periods(min_period);
    std::vector<double> vec_roll(size);
    for (int i = 0; i < size; i++) {
        rollingMean.increase(d_vec[i]);
        vec_roll[i] = rollingMean.get_value();
    }
    return GSeries(vec_roll);
}

// 其他滚动算子实现类似，使用对应的Rolling类

// ======================== 跳跃滚动算子实现 ========================
GSeries GSeries::rolling_jump_sum(const int& jump_num, const int& start_point) const {
    double sum_num = 0.0;
    int sum_n = 0;
    std::vector<double> max_values;
    for (int i = 0; i < size; i++) {
        if (std::isfinite(d_vec[i])) {
            sum_num += d_vec[i];
            sum_n++;
        }
        if ((i == start_point) || ((i > start_point) && ((i - start_point) % jump_num == 0)) || (i == size - 1)) {
            if (sum_n == 0) {
                max_values.push_back(std::numeric_limits<double>::quiet_NaN());
            } else {
                max_values.push_back(sum_num);
            }
            sum_num = 0.0;
            sum_n = 0;
        }
    }
    return GSeries(max_values);
}

// 其他跳跃滚动算子实现类似

// ======================== 累积算子实现 ========================
GSeries GSeries::cumsum() const {
    double sum = 0;
    std::vector<double> cum_sum;
    for (const double& d : d_vec) {
        if (std::isfinite(d)) sum += d;
        cum_sum.push_back(sum);
    }
    return GSeries(cum_sum);
}

// ======================== 其他算子实现 ========================
double GSeries::mode() const {
    std::map<double, int> mode_map;
    for (const double& d : d_vec) {
        if (std::isfinite(d)) mode_map[d] += 1;
    }

    if (mode_map.empty()) return std::numeric_limits<double>::quiet_NaN();

    auto it = mode_map.begin();
    int count = it->second;
    double max = it->first;
    it++;
    for (; it != mode_map.end(); it++) {
        if (count < it->second) {
            count = it->second;
            max = it->first;
        }
    }
    return max;
}

// 其他算子实现类似...