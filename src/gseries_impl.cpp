//
// Created by xzliu on 12/29/21.
//

#include "../include/data_structures.h"
#include <algorithm>
#include <numeric>
#include <cmath>
#include <limits>

// 统计方法实现
double GSeries::nansum() const {
    return ComputeUtils::nan_sum(d_vec);
}

double GSeries::nansum(const int & head_num) const {
    if (head_num <= 0 || head_num > size) return std::numeric_limits<double>::quiet_NaN();
    std::vector<double> head_vec(d_vec.begin(), d_vec.begin() + head_num);
    return ComputeUtils::nan_sum(head_vec);
}

double GSeries::nanmean() const {
    return ComputeUtils::nan_mean(d_vec);
}

double GSeries::nanmean(const int & head_num) const {
    if (head_num <= 0 || head_num > size) return std::numeric_limits<double>::quiet_NaN();
    std::vector<double> head_vec(d_vec.begin(), d_vec.begin() + head_num);
    return ComputeUtils::nan_mean(head_vec);
}

double GSeries::locate(const int & idx) const {
    if (idx < 0 || idx >= size) {
        spdlog::error("GSeries index out of range: {} (size: {})", idx, size);
        return std::numeric_limits<double>::quiet_NaN();
    }
    return d_vec[idx];
}

double GSeries::r_locate(const int & idx) const {
    if (idx < 0 || idx >= size) {
        spdlog::error("GSeries index out of range: {} (size: {})", idx, size);
        return std::numeric_limits<double>::quiet_NaN();
    }
    return d_vec[size - 1 - idx];
}

double GSeries::nanmedian() const {
    return ComputeUtils::nan_median(d_vec);
}

double GSeries::nanstd() const {
    return ComputeUtils::nan_std(d_vec);
}

double GSeries::skewness() const {
    return ComputeUtils::nan_skewness(d_vec);
}

double GSeries::kurtosis() const {
    return ComputeUtils::nan_kurtosis(d_vec);
}

int GSeries::count() const {
    return valid_num;
}

double GSeries::max() const {
    double max_val = std::numeric_limits<double>::quiet_NaN();
    for (const auto& val : d_vec) {
        if (std::isfinite(val)) {
            if (!std::isfinite(max_val) || val > max_val) {
                max_val = val;
            }
        }
    }
    return max_val;
}

double GSeries::min() const {
    double min_val = std::numeric_limits<double>::quiet_NaN();
    for (const auto& val : d_vec) {
        if (std::isfinite(val)) {
            if (!std::isfinite(min_val) || val < min_val) {
                min_val = val;
            }
        }
    }
    return min_val;
}

int GSeries::argmax() const {
    double max_val = std::numeric_limits<double>::quiet_NaN();
    int max_idx = -1;
    for (int i = 0; i < size; ++i) {
        if (std::isfinite(d_vec[i])) {
            if (!std::isfinite(max_val) || d_vec[i] > max_val) {
                max_val = d_vec[i];
                max_idx = i;
            }
        }
    }
    return max_idx;
}

int GSeries::argmin() const {
    double min_val = std::numeric_limits<double>::quiet_NaN();
    int min_idx = -1;
    for (int i = 0; i < size; ++i) {
        if (std::isfinite(d_vec[i])) {
            if (!std::isfinite(min_val) || d_vec[i] < min_val) {
                min_val = d_vec[i];
                min_idx = i;
            }
        }
    }
    return min_idx;
}

int GSeries::length() const {
    return size;
}

double GSeries::corrwith(const GSeries &other) const {
    return ComputeUtils::nan_corr(d_vec, other.d_vec);
}

void GSeries::fillna_inplace(const double & f_val) {
    for (auto& val : d_vec) {
        if (!std::isfinite(val)) {
            val = f_val;
        }
    }
    valid_num = std::count_if(d_vec.begin(), d_vec.end(), [](double v) { return !std::isnan(v); });
}

void GSeries::ffill_inplace() {
    double last_valid = std::numeric_limits<double>::quiet_NaN();
    for (auto& val : d_vec) {
        if (std::isfinite(val)) {
            last_valid = val;
        } else {
            val = last_valid;
        }
    }
    valid_num = std::count_if(d_vec.begin(), d_vec.end(), [](double v) { return !std::isnan(v); });
}

GSeries GSeries::fillna(const double & f_val) const {
    GSeries result = *this;
    result.fillna_inplace(f_val);
    return result;
}

GSeries GSeries::ffill() const {
    GSeries result = *this;
    result.ffill_inplace();
    return result;
}

GSeries GSeries::nan_reduce_sort(const bool & reverse) const {
    std::vector<double> valid_data;
    for (const auto& val : d_vec) {
        if (std::isfinite(val)) {
            valid_data.push_back(val);
        }
    }
    
    if (reverse) {
        std::sort(valid_data.begin(), valid_data.end(), std::greater<>());
    } else {
        std::sort(valid_data.begin(), valid_data.end());
    }
    
    return GSeries(valid_data);
}

GSeries GSeries::pos_shift(const int & n) const {
    if (n <= 0) return *this;
    
    std::vector<double> result(size, std::numeric_limits<double>::quiet_NaN());
    for (int i = n; i < size; ++i) {
        result[i] = d_vec[i - n];
    }
    return GSeries(result);
}

GSeries GSeries::neg_shift(const int & n) const {
    if (n <= 0) return *this;
    
    std::vector<double> result(size, std::numeric_limits<double>::quiet_NaN());
    for (int i = 0; i < size - n; ++i) {
        result[i] = d_vec[i + n];
    }
    return GSeries(result);
}

double GSeries::nanquantile(const double & q) const {
    return ComputeUtils::nan_quantile(d_vec, q);
}

std::vector<double> GSeries::nanquantile(const std::vector<double> & q_list) const {
    std::vector<double> result;
    for (const auto& q : q_list) {
        result.push_back(nanquantile(q));
    }
    return result;
}

std::vector<int> GSeries::slice_idx_equal(const double & val) const {
    std::vector<int> indices;
    for (int i = 0; i < size; ++i) {
        if (std::isfinite(d_vec[i]) && std::abs(d_vec[i] - val) < 1e-10) {
            indices.push_back(i);
        }
    }
    return indices;
}

std::vector<int> GSeries::slice_idx_greater(const double & val) const {
    std::vector<int> indices;
    for (int i = 0; i < size; ++i) {
        if (std::isfinite(d_vec[i]) && d_vec[i] > val) {
            indices.push_back(i);
        }
    }
    return indices;
}

std::vector<int> GSeries::slice_idx_greater_equal(const double & val) const {
    std::vector<int> indices;
    for (int i = 0; i < size; ++i) {
        if (std::isfinite(d_vec[i]) && d_vec[i] >= val) {
            indices.push_back(i);
        }
    }
    return indices;
}

std::vector<int> GSeries::slice_idx_less(const double & val) const {
    std::vector<int> indices;
    for (int i = 0; i < size; ++i) {
        if (std::isfinite(d_vec[i]) && d_vec[i] < val) {
            indices.push_back(i);
        }
    }
    return indices;
}

std::vector<int> GSeries::slice_idx_less_equal(const double & val) const {
    std::vector<int> indices;
    for (int i = 0; i < size; ++i) {
        if (std::isfinite(d_vec[i]) && d_vec[i] <= val) {
            indices.push_back(i);
        }
    }
    return indices;
}

std::vector<int> GSeries::slice_idx_range(const double & lower, const double & upper) const {
    std::vector<int> indices;
    for (int i = 0; i < size; ++i) {
        if (std::isfinite(d_vec[i]) && d_vec[i] >= lower && d_vec[i] <= upper) {
            indices.push_back(i);
        }
    }
    return indices;
}

std::vector<int> GSeries::non_null_index() const {
    std::vector<int> indices;
    for (int i = 0; i < size; ++i) {
        if (std::isfinite(d_vec[i])) {
            indices.push_back(i);
        }
    }
    return indices;
}

double GSeries::slice_mean(const std::vector<int> & idx) const {
    double sum = 0.0;
    int count = 0;
    for (int i : idx) {
        if (i >= 0 && i < size && std::isfinite(d_vec[i])) {
            sum += d_vec[i];
            count++;
        }
    }
    return count > 0 ? sum / count : std::numeric_limits<double>::quiet_NaN();
}

double GSeries::slice_sum(const std::vector<int> & idx) const {
    double sum = 0.0;
    for (int i : idx) {
        if (i >= 0 && i < size && std::isfinite(d_vec[i])) {
            sum += d_vec[i];
        }
    }
    return sum;
}

double GSeries::slice_max(const std::vector<int> & idx) const {
    double max_val = std::numeric_limits<double>::quiet_NaN();
    for (int i : idx) {
        if (i >= 0 && i < size && std::isfinite(d_vec[i])) {
            if (!std::isfinite(max_val) || d_vec[i] > max_val) {
                max_val = d_vec[i];
            }
        }
    }
    return max_val;
}

double GSeries::slice_min(const std::vector<int> & idx) const {
    double min_val = std::numeric_limits<double>::quiet_NaN();
    for (int i : idx) {
        if (i >= 0 && i < size && std::isfinite(d_vec[i])) {
            if (!std::isfinite(min_val) || d_vec[i] < min_val) {
                min_val = d_vec[i];
            }
        }
    }
    return min_val;
}

double GSeries::slice_std(const std::vector<int> & idx) const {
    std::vector<double> slice_data;
    for (int i : idx) {
        if (i >= 0 && i < size && std::isfinite(d_vec[i])) {
            slice_data.push_back(d_vec[i]);
        }
    }
    return ComputeUtils::nan_std(slice_data);
}

GSeries GSeries::slice(const std::vector<int> & idx) const {
    std::vector<double> slice_data;
    for (int i : idx) {
        if (i >= 0 && i < size) {
            slice_data.push_back(d_vec[i]);
        } else {
            slice_data.push_back(std::numeric_limits<double>::quiet_NaN());
        }
    }
    return GSeries(slice_data);
}

GSeries GSeries::cumsum() const {
    return GSeries(FactorUtils::cumsum(d_vec));
}

GSeries GSeries::cummax() const {
    return GSeries(FactorUtils::cummax(d_vec));
}

GSeries GSeries::cummin() const {
    return GSeries(FactorUtils::cummin(d_vec));
}

double GSeries::mode() const {
    return FactorUtils::mode(d_vec);
}

GSeries GSeries::diff(const int & num, const bool & is_ffill) const {
    auto diff_data = FactorUtils::diff(d_vec, num);
    if (is_ffill) {
        diff_data = FactorUtils::ffill(diff_data);
    }
    return GSeries(diff_data);
}

GSeries GSeries::z_score() const {
    return GSeries(FactorUtils::z_score(d_vec));
}

GSeries GSeries::mean_fold(const bool & mean_first) const {
    return GSeries(Increasing::increasing_mean(d_vec));
}

void GSeries::mean_fold_inplace(const bool & mean_first) {
    auto new_data = Increasing::increasing_mean(d_vec);
    d_vec = new_data;
    valid_num = std::count_if(d_vec.begin(), d_vec.end(), [](double v) { return !std::isnan(v); });
}

void GSeries::median_fold_inplace(const bool & mean_first) {
    auto new_data = Increasing::increasing_median(d_vec);
    d_vec = new_data;
    valid_num = std::count_if(d_vec.begin(), d_vec.end(), [](double v) { return !std::isnan(v); });
}

void GSeries::q75_fold_inplace(const bool & mean_first) {
    auto new_data = Increasing::increasing_q75(d_vec);
    d_vec = new_data;
    valid_num = std::count_if(d_vec.begin(), d_vec.end(), [](double v) { return !std::isnan(v); });
}

GSeries GSeries::pct_change(const int & num, const bool & is_ffill) const {
    auto pct_data = FactorUtils::pct_change(d_vec, num);
    if (is_ffill) {
        pct_data = FactorUtils::ffill(pct_data);
    }
    return GSeries(pct_data);
}

GSeries GSeries::pct_change(const int & limits) const {
    return pct_change(limits, false);
}

GSeries GSeries::rank(const bool & is_pct, const bool & is_ascending) const {
    if (is_pct) {
        return GSeries(FactorUtils::rank_pct(d_vec, is_ascending));
    } else {
        auto ranks = FactorUtils::rank(d_vec, is_ascending);
        std::vector<double> rank_doubles(ranks.begin(), ranks.end());
        return GSeries(rank_doubles);
    }
}

std::vector<int> GSeries::arg_sort() const {
    std::vector<std::pair<double, int>> indexed_data;
    for (int i = 0; i < size; ++i) {
        if (std::isfinite(d_vec[i])) {
            indexed_data.emplace_back(d_vec[i], i);
        }
    }
    
    std::sort(indexed_data.begin(), indexed_data.end());
    
    std::vector<int> result;
    for (const auto& pair : indexed_data) {
        result.push_back(pair.second);
    }
    return result;
}

GSeries GSeries::tail(const int & num) const {
    if (num <= 0) return GSeries();
    int start = std::max(0, size - num);
    std::vector<double> tail_data(d_vec.begin() + start, d_vec.end());
    return GSeries(tail_data);
}

GSeries GSeries::tail_rn(const int & num) const {
    return tail(num);
}

GSeries GSeries::head(const int & num) const {
    if (num <= 0) return GSeries();
    int end = std::min(size, num);
    std::vector<double> head_data(d_vec.begin(), d_vec.begin() + end);
    return GSeries(head_data);
}

GSeries GSeries::head_rn(const int & num) const {
    return head(num);
}

// 滚动窗口方法
GSeries GSeries::rolling_sum(const int & num, const int & min_period) const {
    auto rolling_data = Rolling::rolling_sum(d_vec, num, min_period);
    return GSeries(rolling_data);
}

GSeries GSeries::rolling_skew(const int & num) const {
    auto rolling_data = Rolling::rolling_skew(d_vec, num);
    return GSeries(rolling_data);
}

GSeries GSeries::rolling_kurt(const int & num) const {
    auto rolling_data = Rolling::rolling_kurt(d_vec, num);
    return GSeries(rolling_data);
}

GSeries GSeries::rolling_max(const int & num) const {
    auto rolling_data = Rolling::rolling_max(d_vec, num);
    return GSeries(rolling_data);
}

GSeries GSeries::rolling_min(const int & num) const {
    auto rolling_data = Rolling::rolling_min(d_vec, num);
    return GSeries(rolling_data);
}

GSeries GSeries::rolling_mean(const int & num, const int & min_period) const {
    auto rolling_data = Rolling::rolling_mean(d_vec, num, min_period);
    return GSeries(rolling_data);
}

GSeries GSeries::rolling_median(const int & num) const {
    auto rolling_data = Rolling::rolling_median(d_vec, num);
    return GSeries(rolling_data);
}

GSeries GSeries::rolling_std(const int & num, const int & min_period) const {
    auto rolling_data = Rolling::rolling_std(d_vec, num, min_period);
    return GSeries(rolling_data);
}

// 跳跃滚动窗口方法（简化实现）
GSeries GSeries::rolling_jump_min(const int & jump_num, const int & start_point) const {
    if (jump_num <= 0 || start_point < 0 || start_point >= size) {
        return GSeries(std::vector<double>(size, std::numeric_limits<double>::quiet_NaN()));
    }
    
    std::vector<double> result(size, std::numeric_limits<double>::quiet_NaN());
    for (int i = start_point; i < size; i += jump_num) {
        if (i < size && std::isfinite(d_vec[i])) {
            result[i] = d_vec[i];
        }
    }
    return GSeries(result);
}

GSeries GSeries::rolling_jump_max(const int & jump_num, const int & start_point) const {
    return rolling_jump_min(jump_num, start_point);
}

GSeries GSeries::rolling_jump_last(const int & jump_num, const int & start_point) const {
    return rolling_jump_min(jump_num, start_point);
}

GSeries GSeries::rolling_jump_first(const int & jump_num, const int & start_point) const {
    return rolling_jump_min(jump_num, start_point);
}

GSeries GSeries::rolling_jump_sum(const int & jump_num, const int & start_point) const {
    return rolling_jump_min(jump_num, start_point);
}

GSeries GSeries::rolling_jump_mean(const int & jump_num, const int & start_point) const {
    return rolling_jump_min(jump_num, start_point);
}

// 元素运算方法
GSeries GSeries::neutralize(const GSeries &other) const {
    // 简化实现：返回this
    return *this;
}

GSeries GSeries::element_mul(const GSeries &other) const {
    int min_size = std::min(get_size(), other.get_size());
    std::vector<double> result(min_size);
    for (int i = 0; i < min_size; ++i) {
        if (std::isfinite(d_vec[i]) && std::isfinite(other.d_vec[i])) {
            result[i] = d_vec[i] * other.d_vec[i];
        } else {
            result[i] = std::numeric_limits<double>::quiet_NaN();
        }
    }
    return GSeries(result);
}

void GSeries::element_mul_inplace(const GSeries &other) {
    int min_size = std::min(get_size(), other.get_size());
    for (int i = 0; i < min_size; ++i) {
        if (std::isfinite(d_vec[i]) && std::isfinite(other.d_vec[i])) {
            d_vec[i] *= other.d_vec[i];
        } else {
            d_vec[i] = std::numeric_limits<double>::quiet_NaN();
        }
    }
    valid_num = std::count_if(d_vec.begin(), d_vec.end(), [](double v) { return !std::isnan(v); });
}

GSeries GSeries::element_div(const GSeries &other) const {
    int min_size = std::min(get_size(), other.get_size());
    std::vector<double> result(min_size);
    for (int i = 0; i < min_size; ++i) {
        if (std::isfinite(d_vec[i]) && std::isfinite(other.d_vec[i]) && other.d_vec[i] != 0.0) {
            result[i] = d_vec[i] / other.d_vec[i];
        } else {
            result[i] = std::numeric_limits<double>::quiet_NaN();
        }
    }
    return GSeries(result);
}

void GSeries::element_div_inplace(const GSeries &other) {
    int min_size = std::min(get_size(), other.get_size());
    for (int i = 0; i < min_size; ++i) {
        if (std::isfinite(d_vec[i]) && std::isfinite(other.d_vec[i]) && other.d_vec[i] != 0.0) {
            d_vec[i] /= other.d_vec[i];
        } else {
            d_vec[i] = std::numeric_limits<double>::quiet_NaN();
        }
    }
    valid_num = std::count_if(d_vec.begin(), d_vec.end(), [](double v) { return !std::isnan(v); });
}

GSeries GSeries::element_add(const GSeries &other) const {
    int min_size = std::min(get_size(), other.get_size());
    std::vector<double> result(min_size);
    for (int i = 0; i < min_size; ++i) {
        if (std::isfinite(d_vec[i]) && std::isfinite(other.d_vec[i])) {
            result[i] = d_vec[i] + other.d_vec[i];
        } else {
            result[i] = std::numeric_limits<double>::quiet_NaN();
        }
    }
    return GSeries(result);
}

void GSeries::element_add_inplace(const GSeries &other) {
    int min_size = std::min(get_size(), other.get_size());
    for (int i = 0; i < min_size; ++i) {
        if (std::isfinite(d_vec[i]) && std::isfinite(other.d_vec[i])) {
            d_vec[i] += other.d_vec[i];
        } else {
            d_vec[i] = std::numeric_limits<double>::quiet_NaN();
        }
    }
    valid_num = std::count_if(d_vec.begin(), d_vec.end(), [](double v) { return !std::isnan(v); });
}

GSeries GSeries::element_sub(const GSeries &other) const {
    int min_size = std::min(get_size(), other.get_size());
    std::vector<double> result(min_size);
    for (int i = 0; i < min_size; ++i) {
        if (std::isfinite(d_vec[i]) && std::isfinite(other.d_vec[i])) {
            result[i] = d_vec[i] - other.d_vec[i];
        } else {
            result[i] = std::numeric_limits<double>::quiet_NaN();
        }
    }
    return GSeries(result);
}

void GSeries::element_sub_inplace(const GSeries &other) {
    int min_size = std::min(get_size(), other.get_size());
    for (int i = 0; i < min_size; ++i) {
        if (std::isfinite(d_vec[i]) && std::isfinite(other.d_vec[i])) {
            d_vec[i] -= other.d_vec[i];
        } else {
            d_vec[i] = std::numeric_limits<double>::quiet_NaN();
        }
    }
    valid_num = std::count_if(d_vec.begin(), d_vec.end(), [](double v) { return !std::isnan(v); });
}

GSeries GSeries::element_abs() const {
    std::vector<double> result(get_size());
    for (int i = 0; i < get_size(); ++i) {
        if (std::isfinite(d_vec[i])) {
            result[i] = std::abs(d_vec[i]);
        } else {
            result[i] = std::numeric_limits<double>::quiet_NaN();
        }
    }
    return GSeries(result);
}

void GSeries::element_abs_inplace() {
    for (auto& val : d_vec) {
        if (std::isfinite(val)) {
            val = std::abs(val);
        }
    }
}

GSeries GSeries::element_pow(const double &_x) const {
    std::vector<double> result(get_size());
    for (int i = 0; i < get_size(); ++i) {
        if (std::isfinite(d_vec[i])) {
            result[i] = std::pow(d_vec[i], _x);
        } else {
            result[i] = std::numeric_limits<double>::quiet_NaN();
        }
    }
    return GSeries(result);
}

void GSeries::element_pow_inplace(const double &_x) {
    for (auto& val : d_vec) {
        if (std::isfinite(val)) {
            val = std::pow(val, _x);
        }
    }
    valid_num = std::count_if(d_vec.begin(), d_vec.end(), [](double v) { return !std::isnan(v); });
}

GSeries GSeries::element_add(const double &_x) const {
    std::vector<double> result(get_size());
    for (int i = 0; i < get_size(); ++i) {
        if (std::isfinite(d_vec[i])) {
            result[i] = d_vec[i] + _x;
        } else {
            result[i] = std::numeric_limits<double>::quiet_NaN();
        }
    }
    return GSeries(result);
}

void GSeries::element_add_inplace(const double &_x) {
    for (auto& val : d_vec) {
        if (std::isfinite(val)) {
            val += _x;
        }
    }
}

GSeries GSeries::element_sub(const double &_x) const {
    std::vector<double> result(get_size());
    for (int i = 0; i < get_size(); ++i) {
        if (std::isfinite(d_vec[i])) {
            result[i] = d_vec[i] - _x;
        } else {
            result[i] = std::numeric_limits<double>::quiet_NaN();
        }
    }
    return GSeries(result);
}

void GSeries::element_sub_inplace(const double &_x) {
    for (auto& val : d_vec) {
        if (std::isfinite(val)) {
            val -= _x;
        }
    }
}

GSeries GSeries::element_rsub(const double &_x) const {
    std::vector<double> result(get_size());
    for (int i = 0; i < get_size(); ++i) {
        if (std::isfinite(d_vec[i])) {
            result[i] = _x - d_vec[i];
        } else {
            result[i] = std::numeric_limits<double>::quiet_NaN();
        }
    }
    return GSeries(result);
}

void GSeries::element_rsub_inplace(const double &_x) {
    for (auto& val : d_vec) {
        if (std::isfinite(val)) {
            val = _x - val;
        }
    }
    valid_num = std::count_if(d_vec.begin(), d_vec.end(), [](double v) { return !std::isnan(v); });
}

GSeries GSeries::element_div(const double &_x) const {
    if (_x == 0.0) {
        return GSeries(std::vector<double>(get_size(), std::numeric_limits<double>::quiet_NaN()));
    }
    
    std::vector<double> result(get_size());
    for (int i = 0; i < get_size(); ++i) {
        if (std::isfinite(d_vec[i])) {
            result[i] = d_vec[i] / _x;
        } else {
            result[i] = std::numeric_limits<double>::quiet_NaN();
        }
    }
    return GSeries(result);
}

void GSeries::element_div_inplace(const double &_x) {
    if (_x == 0.0) {
        for (auto& val : d_vec) {
            val = std::numeric_limits<double>::quiet_NaN();
        }
    } else {
        for (auto& val : d_vec) {
            if (std::isfinite(val)) {
                val /= _x;
            }
        }
    }
    valid_num = std::count_if(d_vec.begin(), d_vec.end(), [](double v) { return !std::isnan(v); });
}

GSeries GSeries::element_mul(const double &_x) const {
    std::vector<double> result(get_size());
    for (int i = 0; i < get_size(); ++i) {
        if (std::isfinite(d_vec[i])) {
            result[i] = d_vec[i] * _x;
        } else {
            result[i] = std::numeric_limits<double>::quiet_NaN();
        }
    }
    return GSeries(result);
}

void GSeries::element_mul_inplace(const double &_x) {
    for (auto& val : d_vec) {
        if (std::isfinite(val)) {
            val *= _x;
        }
    }
}

GSeries GSeries::element_rdiv(const double &_x) const {
    std::vector<double> result(get_size());
    for (int i = 0; i < get_size(); ++i) {
        if (std::isfinite(d_vec[i]) && d_vec[i] != 0.0) {
            result[i] = _x / d_vec[i];
        } else {
            result[i] = std::numeric_limits<double>::quiet_NaN();
        }
    }
    return GSeries(result);
}

void GSeries::element_rdiv_inplace(const double &_x) {
    for (auto& val : d_vec) {
        if (std::isfinite(val) && val != 0.0) {
            val = _x / val;
        } else {
            val = std::numeric_limits<double>::quiet_NaN();
        }
    }
    valid_num = std::count_if(d_vec.begin(), d_vec.end(), [](double v) { return !std::isnan(v); });
} 