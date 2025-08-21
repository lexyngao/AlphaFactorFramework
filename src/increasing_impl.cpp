#include "increasing.h"
#include "compute_utils.h"
#include <cmath>
#include <algorithm>

// IncreaseMax 实现
void IncreaseMax::increase(const double& new_val) {
    if (!std::isfinite(new_val)) return;
    max_val = std::isfinite(max_val) ? std::max(max_val, new_val) : new_val;
}

double IncreaseMax::get_value() const {
    return max_val;
}

void IncreaseMax::clear() {
    max_val = std::numeric_limits<double>::quiet_NaN();
}

// IncreaseMin 实现
void IncreaseMin::increase(const double& new_val) {
    if (!std::isfinite(new_val)) return;
    min_val = std::isfinite(min_val) ? std::min(min_val, new_val) : new_val;
}

double IncreaseMin::get_value() const {
    return min_val;
}

void IncreaseMin::clear() {
    min_val = std::numeric_limits<double>::quiet_NaN();
}

// IncreaseMean 实现
void IncreaseMean::increase(const double& new_val) {
    if (!std::isfinite(new_val)) return;
    double new_n = n + 1.0;
    mean_val = mean_val + ((new_val - mean_val) / new_n);
    n += 1;
}

double IncreaseMean::get_value() const {
    if (n > 0) {
        return mean_val;
    } else {
        return std::numeric_limits<double>::quiet_NaN();
    }
}

void IncreaseMean::clear() {
    mean_val = 0.0;
    n = 0;
}

// IncreaseStd 实现
void IncreaseStd::increase(const double& new_val) {
    if (!std::isfinite(new_val)) return;
    double new_n = n + 1.0;
    sum_m2 = sum_m2 + ((1 - (1.0 / new_n)) * (new_val - mean_val) * (new_val - mean_val));
    mean_val = mean_val + ((new_val - mean_val) / new_n);
    n += 1;
}

double IncreaseStd::get_value() const {
    if (n > 1) {
        return std::sqrt(sum_m2 / (n - 1.0));
    } else {
        return std::numeric_limits<double>::quiet_NaN();
    }
}

void IncreaseStd::clear() {
    mean_val = 0.0;
    sum_m2 = 0.0;
    n = 0;
}

// IncreaseSkew 实现
void IncreaseSkew::increase(const double& new_val) {
    if (!std::isfinite(new_val)) return;
    double delta = new_val - mean_val;
    double delta_3 = std::pow(delta, 3.0);
    double new_n = n + 1.0;
    sum_m3 = sum_m3 + (delta_3 * (new_n - 1) * (new_n - 2) / (new_n * new_n)) - (3 * delta * sum_m2 / new_n);
    sum_m2 = sum_m2 + ((1 - (1.0 / new_n)) * (new_val - mean_val) * (new_val - mean_val));
    mean_val = mean_val + ((new_val - mean_val) / new_n);
    n += 1;
}

double IncreaseSkew::get_value() const {
    if (n < 2) {
        return std::numeric_limits<double>::quiet_NaN();
    } else {
        double g1 = ComputeUtils::nan_divide(sum_m3, std::pow(sum_m2, 1.5));
        double n_scalar = (double(n) / double(n - 2.0)) * std::sqrt(n - 1.0);
        return n_scalar * g1;
    }
}

void IncreaseSkew::clear() {
    mean_val = 0.0;
    sum_m2 = 0.0;
    sum_m3 = 0.0;
    n = 0;
}

// IncreaseKurt 实现
void IncreaseKurt::increase(const double& new_val) {
    if (!std::isfinite(new_val)) return;
    double delta = new_val - mean_val;
    double delta_4 = std::pow(delta, 4.0);
    double delta_3 = std::pow(delta, 3.0);
    double delta_2 = std::pow(delta, 2.0);
    double new_n = n + 1.0;
    double n_2 = std::pow(new_n, 2.0);
    double n_3 = std::pow(new_n, 3.0);
    sum_m4 = sum_m4 + (delta_4 * (new_n - 1.0) * (n_2 - 3 * new_n + 3) / n_3) + (6 * delta_2 * sum_m2 / n_2) - (4 * delta * sum_m3 / new_n);
    sum_m3 = sum_m3 + (delta_3 * (new_n - 1) * (new_n - 2) / (new_n * new_n)) - (3 * delta * sum_m2 / new_n);
    sum_m2 = sum_m2 + ((1 - (1.0 / new_n)) * (new_val - mean_val) * (new_val - mean_val));
    mean_val = mean_val + ((new_val - mean_val) / new_n);
    n += 1;
}

double IncreaseKurt::get_value() const {
    if (n < 3) {
        return std::numeric_limits<double>::quiet_NaN();
    } else {
        double g2 = ComputeUtils::nan_divide(double(n) * sum_m4, std::pow(sum_m2, 2)) - 3.0;
        double n_scalar = (double(n - 1.0) / ((n - 2.0) * (n - 3.0)));
        return n_scalar * ((n + 1.0) * g2 + 6.0);
    }
}

void IncreaseKurt::clear() {
    mean_val = 0.0;
    sum_m2 = 0.0;
    sum_m3 = 0.0;
    sum_m4 = 0.0;
    n = 0;
}

// IncreaseMedian 实现
void IncreaseMedian::increase(const double& num) {
    if (!std::isfinite(num)) return;
    if (p.empty() || num < p.top()) p.push(num);
    else q.push(num);
    if (p.size() == q.size() + 2) q.push(p.top()), p.pop();
    if (p.size() + 1 == q.size()) p.push(q.top()), q.pop();
}

double IncreaseMedian::get_value() const {
    if (p.empty() && q.empty()) {
        return std::numeric_limits<double>::quiet_NaN();
    } else {
        return p.size() == q.size() ? (p.top() + q.top()) / 2.0 : p.empty() ? std::numeric_limits<double>::quiet_NaN() : p.top();
    }
}

void IncreaseMedian::clear() {
    while (!p.empty()) p.pop();
    while (!q.empty()) q.pop();
}
