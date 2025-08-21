#pragma once

#include "data_structures.h"
#include "increasing.h"
#include <vector>
#include <algorithm>
#include <cmath>
#include <map>
#include <queue>

// Series算子工具类 - 提供各种统计和计算方法
class SeriesOperators {
public:
    // 统计方法
    static double nansum(const GSeries& series);
    static double nansum(const GSeries& series, int head_num);
    static double nanmean(const GSeries& series);
    static double nanmean(const GSeries& series, int head_num);
    static double nanstd(const GSeries& series);
    static double nanmedian(const GSeries& series);
    static double skewness(const GSeries& series);
    static double kurtosis(const GSeries& series);
    static int count(const GSeries& series);
    static double max(const GSeries& series);
    static double min(const GSeries& series);
    static int argmax(const GSeries& series);
    static int argmin(const GSeries& series);
    
    // 分位数方法
    static double nanquantile(const GSeries& series, double q);
    static std::vector<double> nanquantile(const GSeries& series, const std::vector<double>& q_list);
    
    // 排序方法
    static std::vector<int> arg_sort(const GSeries& series, bool reverse = false);
    static GSeries nan_reduce_sort(const GSeries& series, bool reverse = false);
    
    // 滚动窗口方法
    static GSeries rolling_sum(const GSeries& series, int window, int min_periods = 1);
    static GSeries rolling_mean(const GSeries& series, int window, int min_periods = 1);
    static GSeries rolling_std(const GSeries& series, int window, int min_periods = 1);
    static GSeries rolling_median(const GSeries& series, int window);
    static GSeries rolling_max(const GSeries& series, int window);
    static GSeries rolling_min(const GSeries& series, int window);
    static GSeries rolling_skew(const GSeries& series, int window);
    static GSeries rolling_kurt(const GSeries& series, int window);
    
    // 跳跃滚动方法
    static GSeries rolling_jump_sum(const GSeries& series, int jump_num, int start_point = 0);
    static GSeries rolling_jump_mean(const GSeries& series, int jump_num, int start_point = 0);
    static GSeries rolling_jump_max(const GSeries& series, int jump_num, int start_point = 0);
    static GSeries rolling_jump_min(const GSeries& series, int jump_num, int start_point = 0);
    static GSeries rolling_jump_first(const GSeries& series, int jump_num, int start_point = 0);
    static GSeries rolling_jump_last(const GSeries& series, int jump_num, int start_point = 0);
    
    // 累积方法
    static GSeries cumsum(const GSeries& series);
    static GSeries cummax(const GSeries& series);
    static GSeries cummin(const GSeries& series);
    
    // 其他方法
    static double mode(const GSeries& series);
    static GSeries ffill(const GSeries& series);
    static GSeries fillna(const GSeries& series, double fill_value);
    static GSeries diff(const GSeries& series, int periods = 1, bool ffill = false);
    static GSeries pct_change(const GSeries& series, int periods = 1, bool ffill = false);
    static GSeries pct_change(const GSeries& series, int limit);
    static GSeries rank(const GSeries& series, bool pct = false, bool ascending = true);
    static GSeries z_score(const GSeries& series);
    static GSeries mean_fold(const GSeries& series, bool mean_first = true);
    
    // 切片方法
    static GSeries slice(const GSeries& series, const std::vector<int>& indices);
    static double slice_mean(const GSeries& series, const std::vector<int>& indices);
    static double slice_sum(const GSeries& series, const std::vector<int>& indices);
    static double slice_max(const GSeries& series, const std::vector<int>& indices);
    static double slice_min(const GSeries& series, const std::vector<int>& indices);
    static double slice_std(const GSeries& series, const std::vector<int>& indices);
    
    // 索引查找方法
    static std::vector<int> slice_idx_equal(const GSeries& series, double value);
    static std::vector<int> slice_idx_greater(const GSeries& series, double value);
    static std::vector<int> slice_idx_less(const GSeries& series, double value);
    static std::vector<int> slice_idx_range(const GSeries& series, double lower, double upper);
    static std::vector<int> slice_idx_greater_equal(const GSeries& series, double value);
    static std::vector<int> slice_idx_less_equal(const GSeries& series, double value);
    static std::vector<int> non_null_index(const GSeries& series);
    
    // 头部尾部方法
    static GSeries head(const GSeries& series, int n);
    static GSeries head_rn(const GSeries& series, int n);
    static GSeries tail(const GSeries& series, int n);
    static GSeries tail_rn(const GSeries& series, int n);
    
    // 移位方法
    static GSeries pos_shift(const GSeries& series, int n);
    static GSeries neg_shift(const GSeries& series, int n);
    
    // 元素级运算方法
    static GSeries element_add(const GSeries& series, double value);
    static GSeries element_sub(const GSeries& series, double value);
    static GSeries element_mul(const GSeries& series, double value);
    static GSeries element_div(const GSeries& series, double value);
    static GSeries element_rsub(const GSeries& series, double value);
    static GSeries element_rdiv(const GSeries& series, double value);
    static GSeries element_abs(const GSeries& series);
    static GSeries element_pow(const GSeries& series, double exponent);
    
    // 序列间运算方法
    static GSeries element_add(const GSeries& series1, const GSeries& series2);
    static GSeries element_sub(const GSeries& series1, const GSeries& series2);
    static GSeries element_mul(const GSeries& series1, const GSeries& series2);
    static GSeries element_div(const GSeries& series1, const GSeries& series2);
    
    // 相关性方法
    static double corrwith(const GSeries& series1, const GSeries& series2);
    static GSeries neutralize(const GSeries& series, const GSeries& other);
    
private:
    // 辅助方法
    static void set_average(std::vector<int>& idx_equal,
                           std::vector<double>& rank_result,
                           std::vector<std::pair<double, int>>& rank_vec);
};
