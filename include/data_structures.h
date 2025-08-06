//
// Created by lexyn on 25-7-14.
//

#ifndef ALPHAFACTORFRAMEWORK_DATA_STRUCTURES_H
#define ALPHAFACTORFRAMEWORK_DATA_STRUCTURES_H

#include <string>
#include <unordered_map>
#include <map>
#include <vector>
#include "spdlog/spdlog.h"
#include <memory>
#include "config.h"
#include "factor_utils.h"
#include "compute_utils.h"
#include "increasing.h"
#include "rolling.h"
#include <iomanip>
#include <fstream>
#include <queue>
#include <limits>
#include <atomic>


// 存储计算结果的序列（PDF 1.3节）
class GSeries {
private:
    std::vector<double> d_vec;
    int valid_num = 0;
    int size = 0;

public:
    GSeries() = default;

    GSeries(const GSeries &other) {
        this->d_vec = other.d_vec;
        this->size  = other.get_size();
        this->valid_num = other.valid_num;
    }

    //重载=号运算符
    GSeries& operator= (const GSeries &other){
        if(this == &other){
            return *this;
        } else {
            this->d_vec = other.d_vec;
            this->size  = other.get_size();
            this->valid_num = other.valid_num;
            return *this;
        }
    }

    explicit GSeries(const std::vector<double> & new_vec) {
        this->d_vec = new_vec;
        this->size = int(d_vec.size());
        this->valid_num = std::count_if(d_vec.begin(), d_vec.end(), [](double v) { return !std::isnan(v); });
    }

    GSeries(int n, double val): d_vec(n, val) {
        this->size = n;
        this->valid_num = std::count_if(d_vec.begin(), d_vec.end(), [](double v) { return !std::isnan(v); });
    }

    explicit GSeries(int n): d_vec(n, std::numeric_limits<double>::quiet_NaN()) {
        this->size = n;
        this->valid_num = 0;
    }

    void push(const double &_x) {
        this->d_vec.push_back(_x);
        this->size = int(d_vec.size());
        if (!std::isnan(_x)) valid_num++;
    }

    bool empty() const{
        return d_vec.empty();}

    void to_csv(const std::string & out_file, const std::vector<std::string> & alia_index) const{
        std::ofstream outfile(out_file, std::ios::trunc);
        for (int i=0;i<size;i++){
            if (std::isnan(d_vec[i])) {
                outfile << alia_index[i] << ",\n";
            } else {
                outfile << alia_index[i] << "," << std::setprecision(10) << d_vec[i] << "\n";
            }
        }
    }

    void to_csv(const std::string & out_file) const{
        std::ofstream outfile(out_file, std::ios::trunc);
        for (int i=0;i<size;i++){
            if (std::isnan(d_vec[i])) {
                outfile << "\n";
            } else {
                outfile << std::setprecision(10) << d_vec[i] << "\n";
            }
        }
    }

    void read_csv(const std::string & out_file){
        std::ifstream infile(out_file);
        std::string line;
        while(std::getline(infile, line)){
            d_vec.push_back(std::stod(line));
        }
        size = int(d_vec.size());
        valid_num = std::count_if(d_vec.begin(), d_vec.end(), [](double v) { return !std::isnan(v); });
    }

    std::vector<double> vec() const{
        return d_vec;}

    double back() const{
        if (d_vec.empty()) return std::numeric_limits<double>::quiet_NaN();
        else return d_vec.back();
    }

    double back_default(const double & d) const{
        if (d_vec.empty()) {
            return std::numeric_limits<double>::quiet_NaN();
        } else {
            double back_val = d_vec.back();
            if (std::isfinite(back_val)){
                return back_val;
            } else {
                return d;
            }
        }
    }

    double front() const{
        if (d_vec.empty()) return std::numeric_limits<double>::quiet_NaN();
        else return d_vec[0];
    }

    double first_valid() const{
        double first_val = std::numeric_limits<double>::quiet_NaN();
        for (const auto & d : d_vec)
            if (std::isfinite(d)) {
                first_val = d;
                break;
            }
        return first_val;
    }

    double last_valid() const{
        double last_valid_value = std::numeric_limits<double>::quiet_NaN();
        for (int i=size-1;i>=0;i--){
            if (std::isfinite(d_vec[i])) {
                last_valid_value = d_vec[i];
                break;
            }
        }
        return last_valid_value;
    }

    void set_locate(int location, const double & set_value){
        if (location >= size || location < 0) return;
        bool was_valid = !std::isnan(d_vec[location]);
        d_vec[location] = set_value;
        bool is_valid = !std::isnan(set_value);
        if (is_valid && !was_valid) valid_num++;
        else if (!is_valid && was_valid) valid_num--;
    }

    void set_zero_nan_inplace(const double & eps){
        for (auto & d : d_vec){
            if (std::abs(d) < eps) d = std::numeric_limits<double>::quiet_NaN();
        }
        valid_num = std::count_if(d_vec.begin(), d_vec.end(), [](double v) { return !std::isnan(v); });
    }

    void set_nan_if_less(const double & eps){
        for (auto & d : d_vec)
            if (d < eps) d = std::numeric_limits<double>::quiet_NaN();
        valid_num = std::count_if(d_vec.begin(), d_vec.end(), [](double v) { return !std::isnan(v); });
    }

    void set_nan_if_greater(const double & eps){
        for (auto & d : d_vec)
            if (d > eps) d = std::numeric_limits<double>::quiet_NaN();
        valid_num = std::count_if(d_vec.begin(), d_vec.end(), [](double v) { return !std::isnan(v); });
    }

    void set_nan_if_abs_zero(double eps=1e-8){
        for (auto & d : d_vec)
            if (std::isfinite(d) && std::abs(d) < eps) {
                d = std::numeric_limits<double>::quiet_NaN();
            }
        valid_num = std::count_if(d_vec.begin(), d_vec.end(), [](double v) { return !std::isnan(v); });
    }

    bool is_location_not_nan(const int & idx) const{
        if (idx >= size || idx < 0){
            spdlog::critical("series locate out of index");
            return false;
        } else {
            return !std::isnan(d_vec[idx]);
        }
    }

    // 兼容性方法
    double get(int idx) const { return locate(idx); }
    void set(int idx, double value) { set_locate(idx, value); }
    bool is_valid(int idx) const { return is_location_not_nan(idx); }
    const std::vector<double>& data() const { return d_vec; }
    int get_size() const { return size; }
    int get_valid_num() const { return valid_num; }

    // 重置数据（用于reindex）
    void reindex(const std::vector<std::string>& new_stock_list, const std::unordered_map<std::string, int>& old_index_map) {
        std::vector<double> new_d_vec;
        new_d_vec.reserve(new_stock_list.size());
        for (const auto& stock : new_stock_list) {
            if (old_index_map.count(stock)) {
                new_d_vec.push_back(d_vec[old_index_map.at(stock)]);
            } else {
                new_d_vec.push_back(std::numeric_limits<double>::quiet_NaN());
            }
        }
        d_vec = new_d_vec;
        size = new_d_vec.size();
        valid_num = std::count_if(new_d_vec.begin(), new_d_vec.end(), [](double v) { return !std::isnan(v); });
    }

    // 调整大小
    void resize(int new_size) {
        if (new_size <= size) return;
        d_vec.resize(new_size, std::numeric_limits<double>::quiet_NaN());
        size = new_size;
        valid_num = std::count_if(d_vec.begin(), d_vec.end(), [](double v) { return !std::isnan(v); });
    }

    // 统计方法
    double nansum() const;
    double nansum(const int & head_num) const;
    double nanmean() const;
    double nanmean(const int & head_num) const;
    double locate(const int & idx) const;
    double r_locate(const int & idx) const;
    double nanmedian() const;
    double nanstd() const;
    double skewness() const;
    double kurtosis() const;
    int count() const;
    double max() const;
    double min() const;
    int argmax() const;
    int argmin() const;
    int length() const;
    double corrwith(const GSeries &other) const;

    void fillna_inplace(const double & f_val);
    void ffill_inplace();

    GSeries fillna(const double & f_val) const;
    GSeries ffill() const;
    GSeries nan_reduce_sort(const bool & reverse) const;

    GSeries pos_shift(const int & n) const;
    GSeries neg_shift(const int & n) const;

    double nanquantile(const double & q) const;

    double quantile(const double & q) const{
        if (count() == size){
            return nanquantile(q);
        } else {
            return std::numeric_limits<double>::quiet_NaN();
        }
    }

    std::vector<double> nanquantile(const std::vector<double> & q_list) const;

    std::vector<int> slice_idx_equal(const double & val) const;
    std::vector<int> slice_idx_greater(const double & val) const;
    std::vector<int> slice_idx_greater_equal(const double & val) const;
    std::vector<int> slice_idx_less(const double & val) const;
    std::vector<int> slice_idx_less_equal(const double & val) const;
    std::vector<int> slice_idx_range(const double & lower, const double & upper) const;
    std::vector<int> non_null_index() const; // select valid index

    std::vector<int> null_index() const{
        std::vector<int> null_idx;
        for (int i=0;i<d_vec.size();i++){
            if (!std::isfinite(d_vec[i])) {
                null_idx.push_back(i);
            }
        }
        return null_idx;
    }

    double slice_mean(const std::vector<int> & idx) const;
    double slice_sum(const std::vector<int> & idx) const;

    double slice_max(const std::vector<int> & idx) const;
    double slice_min(const std::vector<int> & idx) const;
    double slice_std(const std::vector<int> & idx) const;
    GSeries slice(const std::vector<int> & idx) const;

    GSeries cumsum() const;
    GSeries cummax() const;
    GSeries cummin() const;
    double mode() const;
    GSeries diff(const int & num, const bool & is_ffill) const;

    GSeries z_score() const;
    GSeries mean_fold(const bool & mean_first=true) const;

    void mean_fold_inplace(const bool & mean_first=true);
    void median_fold_inplace(const bool & mean_first=true);
    void q75_fold_inplace(const bool & mean_first=true);

    GSeries pct_change(const int & num, const bool & is_ffill) const;

    GSeries pct_change(const int & limits) const;

    GSeries rank(const bool & is_pct, const bool & is_ascending) const;

    std::vector<int> arg_sort() const;

    GSeries tail(const int & num) const;

    GSeries tail_rn(const int & num) const;

    GSeries head(const int & num) const;

    GSeries head_rn(const int & num) const;

    GSeries rolling_sum(const int & num, const int & min_period) const;

    GSeries rolling_skew(const int & num) const;

    GSeries rolling_kurt(const int & num) const;

    GSeries rolling_max(const int & num) const;

    GSeries rolling_min(const int & num) const;

    GSeries rolling_mean(const int & num, const int & min_period) const;

    GSeries rolling_median(const int & num) const;

    GSeries rolling_std(const int & num, const int & min_period) const;

    GSeries rolling_jump_min(const int & jump_num, const int & start_point) const;

    GSeries rolling_jump_max(const int & jump_num, const int & start_point) const;

    GSeries rolling_jump_last(const int & jump_num, const int & start_point) const;
    GSeries rolling_jump_first(const int & jump_num, const int & start_point) const;

    GSeries rolling_jump_sum(const int & jump_num, const int & start_point) const;

    GSeries rolling_jump_mean(const int & jump_num, const int & start_point) const;

    void append(const GSeries & other){
        d_vec.insert(d_vec.end(), other.d_vec.begin(), other.d_vec.end());
        size = int(d_vec.size());
        valid_num = std::count_if(d_vec.begin(), d_vec.end(), [](double v) { return !std::isnan(v); });
    }

    static GSeries concat(const GSeries & series1, const GSeries & series2){
        std::vector<double> cnt(series1.d_vec);
        cnt.insert(cnt.end(), series2.d_vec.begin(), series2.d_vec.end());
        return GSeries(cnt);
    }

    GSeries neutralize(const GSeries &other) const; // neu = this - b * other

    GSeries element_mul(const GSeries &other) const;
    void element_mul_inplace(const GSeries &other);

    GSeries element_div(const GSeries &other) const;
    void element_div_inplace(const GSeries &other);

    GSeries element_add(const GSeries &other) const;
    void element_add_inplace(const GSeries &other);

    GSeries element_sub(const GSeries &other) const;
    void element_sub_inplace(const GSeries &other);

    GSeries element_abs() const;
    void element_abs_inplace();

    GSeries element_pow(const double &_x) const;
    void element_pow_inplace(const double &_x);

    GSeries element_add(const double &_x) const;
    void element_add_inplace(const double &_x);

    GSeries element_sub(const double &_x) const;
    void element_sub_inplace(const double &_x);

    GSeries element_rsub(const double &_x) const;
    void element_rsub_inplace(const double &_x);

    GSeries element_div(const double &_x) const;
    void element_div_inplace(const double &_x);

    GSeries element_mul(const double &_x) const;
    void element_mul_inplace(const double &_x);

    GSeries element_rdiv(const double &_x) const;
    void element_rdiv_inplace(const double &_x);

    GSeries element_log() const{
        std::vector<double> new_vec(size, 0);
        for (int i=0;i<size;i++){
            if (std::isfinite(d_vec[i]) && ComputeUtils::greater_than_zero(d_vec[i])) new_vec[i] = std::log(d_vec[i]);
            else new_vec[i] = std::numeric_limits<double>::quiet_NaN();
        }
        return GSeries(new_vec);
    }

    void element_log_inplace(){
        for (auto & d: d_vec){
            if (std::isfinite(d) && ComputeUtils::greater_than_zero(d)) d = std::log(d);
            else d = std::numeric_limits<double>::quiet_NaN();
        }
        valid_num = std::count_if(d_vec.begin(), d_vec.end(), [](double v) { return !std::isnan(v); });
    }

    GSeries element_exp(){
        std::vector<double> new_vec(size, 0);
        for (int i=0;i<size;i++){
            if (std::isfinite(d_vec[i])) new_vec[i] = std::exp(d_vec[i]);
            else new_vec[i] = std::numeric_limits<double>::quiet_NaN();
        }
        return GSeries(new_vec);
    }

    void element_exp_inplace(){
        for (auto & d: d_vec){
            if (std::isfinite(d)) d = std::exp(d);
            else d = std::numeric_limits<double>::quiet_NaN();
        }
        valid_num = std::count_if(d_vec.begin(), d_vec.end(), [](double v) { return !std::isnan(v); });
    }

    static GSeries maximum(const GSeries &series1, const GSeries &series2){
        int full_size = std::min(series1.get_size(), series2.get_size());

        std::vector<double> max_data;

        for (int i=0;i< full_size;i++){
            if (std::isfinite(series1.d_vec[i]) && std::isfinite(series2.d_vec[i])){
                max_data.push_back(std::max(series1.d_vec[i], series2.d_vec[i]));
            } else {
                max_data.push_back(std::numeric_limits<double>::quiet_NaN());
            }
        }

        return GSeries(max_data);
    }

    double max_draw_down() const{
        double local_max = std::numeric_limits<double>::quiet_NaN();
        double mdd = std::numeric_limits<double>::quiet_NaN();
        for (const auto & d: d_vec){
            if (std::isfinite(d)){
                if (!std::isfinite(local_max)) local_max = d;
                else local_max = std::max(local_max, d);
                double draw_down = ComputeUtils::nan_divide(d, local_max);
                if (!std::isfinite(mdd)) mdd = draw_down;
                else mdd = std::min(mdd, draw_down);
            }
        }
        return mdd;
    }

    double max_rise() const{
        double local_min = std::numeric_limits<double>::quiet_NaN();
        double m_rise = std::numeric_limits<double>::quiet_NaN();
        for (const auto & d: d_vec){
            if (std::isfinite(d)){
                if (!std::isfinite(local_min)) local_min = d;
                else local_min = std::min(local_min, d);

                double mr = ComputeUtils::nan_divide(d, local_min);

                if (!std::isfinite(m_rise)) m_rise = mr;
                else m_rise = std::max(m_rise, mr);
            }
        }
        return m_rise;
    }
    
    static GSeries minimum(const GSeries &series1, const GSeries &series2) {
        int full_size = std::min(series1.get_size(), series2.get_size());

        std::vector<double> min_data;

        for (int i=0;i< full_size;i++){
            if (std::isfinite(series1.d_vec[i]) && std::isfinite(series2.d_vec[i])){
                min_data.push_back(std::min(series1.d_vec[i], series2.d_vec[i]));
            } else {
                min_data.push_back(std::numeric_limits<double>::quiet_NaN());
            }
        }

        return GSeries(min_data);
    }
};

// 订单数据（PDF 2.1节）
struct OrderData {
    int64_t order_number = 0;
    char order_kind = '\0';
    double price = 0.0;
    double volume = 0.0;
    char bs_flag = '\0';  // B:买, S:卖
    uint64_t real_time = 0;    // 时间戳（秒级或毫秒级）
    int64_t appl_seq_num = 0;  // 序列号
    std::string symbol;        // 股票代码（Symbol）
};

// 成交数据（PDF 2.2节）
struct TradeData {
    int64_t ask_no = 0;
    int64_t bid_no = 0;
    int64_t trade_no = 0;
    char side = '\0';     // B:主动买, S:主动卖
    char cancel_flag = '\0';  // N:正常, C:撤销
    double price = 0.0;
    double volume = 0.0;
    double trade_money = 0.0;  // 成交金额（TradeMoney字段）
    uint64_t real_time = 0;
    int64_t appl_seq_num = 0;
    std::string symbol;        // 股票代码（Symbol）
};

// 快照数据
struct TickData {
    // 盘口价格（买一到买五，卖一到卖五）
    double bid_price_v[5]{};   // BidPrice1~BidPrice5
    double ask_price_v[5]{};   // AskPrice1~AskPrice5

    // 盘口数量（买一到买五，卖一到卖五）
    double bid_volume_v[5]{};  // BidVol1~BidVol5
    double ask_volume_v[5]{};  // AskVol1~AskVol5

    // 价格信息
    double last_price = 0.0;   // 最新价（LastPrice）
    double pre_close = 0.0;    // 前收盘价（PreClose）
    double open_price = 0.0;   // 开盘价（Open）
    double close_price = 0.0;  // 收盘价（Close，此处数据中可能为当日收盘前的临时值）
    double high_price = 0.0;   // 最高价（High）
    double low_price = 0.0;    // 最低价（Low）
    double limit_high = 0.0;   // 涨停价（LimitHigh）
    double limit_low = 0.0;    // 跌停价（LimitLow）

    // 成交量与成交额
    double volume = 0.0;       // 成交量（Volume）
    double total_value_traded = 0.0;  // 总成交额（TotalValueTraded）

    // 时间与序列信息
    uint64_t real_time = 0;         // 时间戳（转换为秒级，来自TimeStamp）
    std::string symbol;        // 股票代码（Symbol）
    int64_t appl_seq_num = 0;  // 序列号（可基于ExchangeTime生成，数据中无直接字段）
};

// 同步的行情数据（含快照+关联订单+成交，PDF 3.3节）
struct SyncTickData {
    std::string symbol;               // 股票代码（如603103.SH）
    double local_time_stamp = 0;      // 本地接收时间戳
    TickData tick_data;               // 快照数据
    std::vector<TradeData> trans;     // 关联成交数据
    std::vector<OrderData> orders;    // 关联订单数据
};

// 用于排序的统一数据结构
enum class MarketBufferType { Order, Trade, Tick, Time };

struct MarketAllField {
    MarketBufferType type;          // 数据类型
    std::string symbol;             // 股票代码（可复制）
    uint64_t timestamp;             // 时间戳（可复制）即real_time
    uint64_t appl_seq_num;          // 序列号（可复制）
    union {
        OrderData order;
        TradeData trade;
        TickData tick;
        uint64_t time_trigger;  // Time类型的时间戳
    };

    // 1. 默认构造函数（必须显式定义）
    MarketAllField()
            : type(MarketBufferType::Tick), timestamp(0), appl_seq_num(0) {
        // 初始化union中的一个成员（避免未初始化的风险）
        new(&tick) TickData();  //  placement new：显式构造TickData
    }

    // 2. 带参构造函数（按类型初始化union）
    MarketAllField(MarketBufferType t, const std::string &sym, uint64_t ts, uint64_t seq)
            : type(t), symbol(sym), timestamp(ts), appl_seq_num(seq) {
        // 根据类型显式构造union成员
        switch (type) {
            case MarketBufferType::Order:
                new(&order) OrderData();
                break;
            case MarketBufferType::Trade:
                new(&trade) TradeData();
                break;
            case MarketBufferType::Tick:
                new(&tick) TickData();
                break;
            case MarketBufferType::Time:
                time_trigger = ts;  // 直接赋值时间戳
                break;
        }
    }

    // 3. 复制构造函数（关键：手动复制union成员）
    MarketAllField(const MarketAllField &other)
            : type(other.type), symbol(other.symbol),
              timestamp(other.timestamp), appl_seq_num(other.appl_seq_num) {
        // 根据类型复制对应的union成员
        switch (type) {
            case MarketBufferType::Order:
                new(&order) OrderData(other.order);
                break;
            case MarketBufferType::Trade:
                new(&trade) TradeData(other.trade);
                break;
            case MarketBufferType::Tick:
                new(&tick) TickData(other.tick);
                break;
            case MarketBufferType::Time:
                time_trigger = other.time_trigger;
                break;
        }
    }

    // 4. 移动构造函数（关键：手动移动union成员）
    MarketAllField(MarketAllField &&other) noexcept
            : type(other.type), symbol(std::move(other.symbol)),
              timestamp(other.timestamp), appl_seq_num(other.appl_seq_num) {
        // 根据类型移动对应的union成员
        switch (type) {
            case MarketBufferType::Order:
                new(&order) OrderData(std::move(other.order));
                break;
            case MarketBufferType::Trade:
                new(&trade) TradeData(std::move(other.trade));
                break;
            case MarketBufferType::Tick:
                new(&tick) TickData(std::move(other.tick));
                break;
            case MarketBufferType::Time:
                time_trigger = other.time_trigger;
                break;
        }
    }

    // 5. 析构函数（手动销毁union成员）
    ~MarketAllField() {
        // 根据类型显式销毁union成员
        switch (type) {
            case MarketBufferType::Order:
                order.~OrderData();
                break;
            case MarketBufferType::Trade:
                trade.~TradeData();
                break;
            case MarketBufferType::Tick:
                tick.~TickData();
                break;
            case MarketBufferType::Time:
                // Time类型不需要析构
                break;
        }
    }

    // 6. 复制赋值运算符（可选，根据需要添加）
    MarketAllField &operator=(const MarketAllField &other) {
        if (this != &other) {
            // 先销毁当前union成员
            this->~MarketAllField();
            // 再复制其他成员
            type = other.type;
            symbol = other.symbol;
            timestamp = other.timestamp;
            appl_seq_num = other.appl_seq_num;
            // 复制union成员
            switch (type) {
                case MarketBufferType::Order:
                    new(&order) OrderData(other.order);
                    break;
                case MarketBufferType::Trade:
                    new(&trade) TradeData(other.trade);
                    break;
                case MarketBufferType::Tick:
                    new(&tick) TickData(other.tick);
                    break;
                case MarketBufferType::Time:
                    time_trigger = other.time_trigger;
                    break;
            }
        }
        return *this;
    }

    // 7. Getter方法（带类型校验）
    const OrderData& get_order() const {
        if (type != MarketBufferType::Order) {
            throw std::logic_error("MarketAllField类型错误：当前不是Order类型");
        }
        return order;
    }

    const TradeData& get_trade() const {
        if (type != MarketBufferType::Trade) {
            throw std::logic_error("MarketAllField类型错误：当前不是Trade类型");
        }
        return trade;
    }

    const TickData& get_tick() const {
        if (type != MarketBufferType::Tick) {
            throw std::logic_error("MarketAllField类型错误：当前不是Tick类型");
        }
        return tick;
    }

    uint64_t get_time_trigger() const {
        if (type != MarketBufferType::Time) {
            throw std::logic_error("MarketAllField类型错误：当前不是Time类型");
        }
        return time_trigger;
    }

};

// Indicator历史数据持有者（PDF 1.3节）
class BaseSeriesHolder {  // PDF中为BaseSeriesHolder，修正类名对齐
protected:
    std::string stock;  // 股票代码
    
    // T日之前的历史数据：结构：indicator_name -> 日期索引（1=往前1日, 2=往前2日, ..., pre_days=往前pre_days日）-> GSeries
    std::unordered_map<std::string, std::unordered_map<int, GSeries>> HisBarSeries;

public:
    // 1. 构造函数（初始化智能指针）
    explicit BaseSeriesHolder(std::string stock_code)
            : stock(std::move(stock_code)) {}

    // 2. 移动构造函数（允许对象移动）
    BaseSeriesHolder(BaseSeriesHolder&& other) noexcept
            : stock(std::move(other.stock)),
              HisBarSeries(std::move(other.HisBarSeries)) {}

    // 3. 移动赋值运算符（允许对象移动赋值）
    BaseSeriesHolder& operator=(BaseSeriesHolder&& other) noexcept {
        if (this != &other) {
            stock = std::move(other.stock);
            HisBarSeries = std::move(other.HisBarSeries);
        }
        return *this;
    }

    // 4. 禁止复制（保持原有设计，避免线程安全问题）
    BaseSeriesHolder(const BaseSeriesHolder&) = delete;
    BaseSeriesHolder& operator=(const BaseSeriesHolder&) = delete;

    // 设置历史序列（确保his_day_index>0，PDF 1.3节）
    // 新的索引逻辑：1=往前1日, 2=往前2日, ..., pre_days=往前pre_days日
    void set_his_series(const std::string& indicator_name, int his_day_index, const GSeries& series) {
        if (his_day_index <= 0) {
            spdlog::error("{}: his_day_index must be > 0 (got {})", stock, his_day_index);
            return;
        }
        HisBarSeries[indicator_name][his_day_index] = series;
    }

    // 获取历史序列片段（PDF 1.3节）
    // 新的索引逻辑：1=往前1日, 2=往前2日, ..., pre_days=往前pre_days日
    GSeries his_slice_bar(const std::string& indicator_name, int his_day_index) const {
        if (!HisBarSeries.count(indicator_name)) {
            spdlog::error("{}: Indicator {} not found in HisBarSeries", stock, indicator_name);
            return GSeries();
        }
        const auto& day_map = HisBarSeries.at(indicator_name);
        if (!day_map.count(his_day_index)) {
            spdlog::error("{}: Indicator {} day {} not found", stock, indicator_name, his_day_index);
            return GSeries();
        }
        return day_map.at(his_day_index);
    }

    // 获取股票代码
    const std::string& get_stock() const { return stock; }

    // 新增：获取所有indicator key
    std::vector<std::string> get_all_indicator_keys() const {
        std::vector<std::string> keys;
        for (const auto& kv : HisBarSeries) {
            keys.push_back(kv.first);
        }
        return keys;
    }
};

// BarSeriesHolder：包含T日数据的子类
class BarSeriesHolder : public BaseSeriesHolder {
private:
    int current_time = 0;
    double current_minute_close = 0.0;
    double pre_close = 0.0;
    std::unordered_map<std::string, GSeries> MBarSeries; // today m bar
    mutable std::mutex m_bar_mutex_; // 保护MBarSeries的互斥锁

public:
    bool status = false;

    // 继承构造函数
    explicit BarSeriesHolder(std::string stock_code) : BaseSeriesHolder(std::move(stock_code)) {}
    
    // 继承移动构造函数
    BarSeriesHolder(BarSeriesHolder&& other) noexcept 
        : BaseSeriesHolder(std::move(other)),
          current_time(other.current_time),
          current_minute_close(other.current_minute_close),
          pre_close(other.pre_close),
          MBarSeries(std::move(other.MBarSeries)),
          status(other.status) {}
    
    // 继承移动赋值运算符
    BarSeriesHolder& operator=(BarSeriesHolder&& other) noexcept {
        if (this != &other) {
            BaseSeriesHolder::operator=(std::move(other));
            current_time = other.current_time;
            current_minute_close = other.current_minute_close;
            pre_close = other.pre_close;
            MBarSeries = std::move(other.MBarSeries);
            status = other.status;
        }
        return *this;
    }
    
    // 禁止复制
    BarSeriesHolder(const BarSeriesHolder&) = delete;
    BarSeriesHolder& operator=(const BarSeriesHolder&) = delete;

    double get_pre_close() const {
        return pre_close;
    }

    bool check_data_exist(const std::string& name) const {
        std::lock_guard<std::mutex> lock(m_bar_mutex_);
        return MBarSeries.count(name) > 0;
    }

    const GSeries& get_data(const std::string& name) const {
        std::lock_guard<std::mutex> lock(m_bar_mutex_);
        return MBarSeries.at(name);
    }

    GSeries get_today_min_series(
            const std::string& factor_name, const int& pre_length, const int& today_minute_index) const {
        std::lock_guard<std::mutex> lock(m_bar_mutex_);
        int minute_len = today_minute_index + 1;
        GSeries today_series;
        if (pre_length > 0) {
            for (int his_index = pre_length; his_index >= 1; his_index--) {
                GSeries his_series = his_slice_bar(factor_name, his_index);
                today_series.append(his_series);
            }
        }
        if (MBarSeries.count(factor_name)) {
            GSeries cur_series = MBarSeries.at(factor_name).head(minute_len);
            today_series.append(cur_series);
        } else {
            spdlog::critical("{} m bar no factor {}", stock, factor_name);
        }

        return today_series;
    }

    void offline_set_m_bar(const std::string& factor_name, const GSeries& val) {
        std::lock_guard<std::mutex> lock(m_bar_mutex_);
        MBarSeries[factor_name] = val;
        status = true;
    }

    // 新增：获取T日（今天）的数据
    GSeries get_m_bar(const std::string& factor_name) const {
        std::lock_guard<std::mutex> lock(m_bar_mutex_);
        if (!MBarSeries.count(factor_name)) {
            spdlog::error("{}: Factor {} not found in MBarSeries", stock, factor_name);
            return GSeries();
        }
        return MBarSeries.at(factor_name);
    }

    // 新增：检查T日数据是否存在
    bool has_m_bar(const std::string& factor_name) const {
        std::lock_guard<std::mutex> lock(m_bar_mutex_);
        return MBarSeries.count(factor_name) > 0;
    }

    // 新增：获取T日数据的状态
    bool get_status() const { return status; }

    // 新增：获取所有T日factor key
    std::vector<std::string> get_all_m_bar_keys() const {
        std::lock_guard<std::mutex> lock(m_bar_mutex_);
        std::vector<std::string> keys;
        for (const auto& kv : MBarSeries) {
            keys.push_back(kv.first);
        }
        return keys;
    }
};

// Factor基类（PDF 3.4节）
class BaseFactor {
public:
    BaseFactor() = default;
    BaseFactor(const std::string& name, const std::string& id, const std::string& path)
        : name_(name), id_(id), path_(path) {}
    virtual ~BaseFactor() = default;  // 确保多态析构

    // 计算因子（输入：所有股票的Indicator数据、排序后的股票列表、时间bar索引ti）
    virtual GSeries definition(
            const std::unordered_map<std::string, BaseSeriesHolder*>& bar_runners,
            const std::vector<std::string>& sorted_stock_list,
            int ti
    ) {
        spdlog::critical("BaseFactor::definition not implement yet!!");
        return GSeries();
    }

    // 获取因子名称
    const std::string& get_name() const { return name_; }
    const std::string& get_id() const { return id_; }
    const std::string& get_path() const { return path_; }

protected:
    std::string name_;
    std::string id_;
    std::string path_;
};

//indicator类
// 频率类型定义
enum class Frequency {
    F15S,   // 15秒
    F1MIN,  // 1分钟
    F5MIN,  // 5分钟
    F30MIN  // 30分钟
};

class Indicator {
protected:
    std::string name_;          // 指标名称（如"volume"）
    std::string id_;            // 类名（如"VolumeIndicator"）
    std::string path_;          // 持久化基础路径
    Frequency frequency_;       // 更新频率
    
    // 计算状态标记（线程安全）
    mutable std::atomic<bool> is_calculated_{false};  // 是否已计算完成
    int step_ = 1; // 步长，默认每个bar都输出
    int bars_per_day_ = 960; // 默认15s

    void init_frequency_params() {
        // 新的时间桶逻辑包含的时间段：
        // 1. 9:00-9:30:00 -> 映射到9:30桶 (30分钟)
        // 2. 9:30:00-11:30:00 -> 正常映射 (120分钟)
        // 3. 11:30:00-13:00:00 -> 映射到13:00桶 (90分钟)
        // 4. 13:00:00-14:57:00 -> 正常映射 (117分钟)
        // 5. 14:57:00-15:00+ -> 映射到14:57桶 (3分钟+)
        
        // 计算实际需要的时间桶数量
        // 正常映射的时间：9:30-11:30 (120分钟) + 13:00-14:57 (117分钟) = 237分钟
        // 转换为15秒桶：237 * 4 = 948个桶
        int normal_trading_minutes = 237;
        
        switch (frequency_) {
            case Frequency::F15S:
                step_ = 1;
                bars_per_day_ = normal_trading_minutes * 4;  // 237 * 4 = 948
                break;
            case Frequency::F1MIN:
                step_ = 4;
                bars_per_day_ = normal_trading_minutes;  // 237
                break;
            case Frequency::F5MIN:
                step_ = 20;
                bars_per_day_ = (120 / 5) + (117 / 5) + ((117 % 5) > 0 ? 1 : 0);  // 24 + 23 + 1 = 48
                break;
            case Frequency::F30MIN:
                step_ = 120;
                bars_per_day_ = (120 / 30) + (117 / 30) + ((117 % 30) > 0 ? 1 : 0);  // 4 + 3 + 1 = 8
                break;
        }
    }

    // 用unique_ptr自动管理BarSeriesHolder的生命周期
    std::unordered_map<std::string, std::unique_ptr<BarSeriesHolder>> storage_;


public:
    // 通过ModuleConfig的参数初始化
    Indicator(const ModuleConfig& module)
            : name_(module.name), id_(module.id), path_(module.path), frequency_([](const std::string& freq_str) {
        if (freq_str == "15S" || freq_str == "15s") return Frequency::F15S;
        if (freq_str == "1min") return Frequency::F1MIN;
        if (freq_str == "5min") return Frequency::F5MIN;
        if (freq_str == "30min") return Frequency::F30MIN;
        return Frequency::F15S; // 默认
    }(module.frequency)) {
        init_frequency_params();
    }

    virtual ~Indicator()=default;

    // 纯虚函数：计算指标（由子类实现具体逻辑）
    virtual void Calculate(const SyncTickData& tick_data) = 0;

    // 获取完整存储路径（path/date/frequency/name.gz）
    std::string get_full_storage_path(const std::string& date) const {
        std::string freq_str;
        switch (frequency_) {
            case Frequency::F15S:  freq_str = "15s"; break;
            case Frequency::F1MIN: freq_str = "1min"; break;
            case Frequency::F5MIN: freq_str = "5min"; break;
            case Frequency::F30MIN: freq_str = "30min"; break;
        }
        return path_ + "/" + date + "/" + freq_str + "/" + name_ + ".gz";
    }

    // 获取指标名称
    const std::string& name() const { return name_; }
    // 获取更新频率
    Frequency frequency() const { return frequency_; }
    
    // 新增：输出时间桶信息的辅助函数
    void log_time_bucket_info(const std::string& symbol, int bucket_index, double value) const {
        std::string time_str = format_time_bucket(bucket_index);
        spdlog::info("[{}] symbol={} bucket[{}]={} value={}", 
                     name_, symbol, bucket_index, time_str, value);
    }
    // 获取存储结构（供Factor访问）
    const std::unordered_map<std::string, std::unique_ptr<BarSeriesHolder>>& get_storage()  const {
        return storage_;
    }

    // 初始化指标存储（预创建所有股票的BaseSeriesHolder）
    void init_storage(const std::vector<std::string>& stock_list) {
        storage_.clear();  // 清空原有存储

        for (const auto& stock : stock_list) {
            auto holder = std::make_unique<BarSeriesHolder>(stock);
            try {
                load_historical_data(stock, *holder);  // 加载该股票的历史数据
            } catch (const std::exception& e) {
                spdlog::error("指标[{}]加载{}历史数据失败: {}", name_, stock, e.what());
            }
            storage_[stock] = std::move(holder);  // 存入指标自身的storage_
        }

        spdlog::info("指标[{}]初始化{}只股票的存储", name_, stock_list.size());
    }

    // 辅助函数：加载单只股票的历史数据（可在子类重写）
    virtual void load_historical_data(const std::string& stock_code, BarSeriesHolder& holder) {
        spdlog::debug("指标[{}]暂未实现{}的历史数据加载", name_, stock_code);
    }

    // 新增：状态管理方法
    void mark_as_calculated() const { is_calculated_ = true; }

    bool is_calculated() const { return is_calculated_; }

    void reset_calculation_status() const { is_calculated_ = false; }

    
    // 修改：尝试计算（增加状态检查）
    void try_calculate(const SyncTickData& sync_tick) {
        if (is_calculated_) {
            spdlog::debug("指标[{}]已计算完成，跳过", name_);
            return;
        }
        Calculate(sync_tick);
    }




    // 统一的时间桶索引计算
    int get_time_bucket_index(uint64_t total_ns) const {
        if (total_ns == 0) return -1;

        // 1. 转换为北京时间
        int64_t utc_sec = total_ns / 1000000000;
        int64_t beijing_sec = utc_sec + 8 * 3600;  // UTC + 8小时 = 北京时间
        int64_t beijing_seconds_in_day = beijing_sec % 86400;
        int hour = static_cast<int>(beijing_seconds_in_day / 3600);
        int minute = static_cast<int>((beijing_seconds_in_day % 3600) / 60);
        int second = static_cast<int>(beijing_seconds_in_day % 60);
        int total_minutes = hour * 60 + minute;

        // 添加调试信息
        spdlog::debug("时间桶计算: total_ns={}, utc_sec={}, beijing_sec={}, hour={}, minute={}, second={}, total_minutes={}", 
                     total_ns, utc_sec, beijing_sec, hour, minute, second, total_minutes);

        // 新的时间桶映射逻辑
        // 定义关键时间点（以分钟为单位）
        const int time_900 = 9 * 60 + 0;    // 9:00
        const int time_930 = 9 * 60 + 30;   // 9:30
        const int time_1130 = 11 * 60 + 30; // 11:30
        const int time_1300 = 13 * 60 + 0;  // 13:00
        const int time_1457 = 14 * 60 + 57; // 14:57
        const int time_1500 = 15 * 60 + 0;  // 15:00

        // 检查时间范围并映射到对应桶
        int target_bucket = -1;
        
        if (total_minutes >= time_900 && total_minutes < time_930) {
            // 9:00:00-9:30:00 映射到 9:30 桶 (bucket=0)
            target_bucket = 0;
            spdlog::debug("时间映射: {}:{} -> 9:30桶 (bucket={})", hour, minute, target_bucket);
        } else if (total_minutes >= time_930 && total_minutes < time_1130) {
            // 9:30:00-11:30:00 正常映射 (9:30:00-9:30:15就是bucket=0)
            int seconds_since_930 = (total_minutes - time_930) * 60 + second;
            target_bucket = seconds_since_930 / 15;
            spdlog::debug("正常映射: {}:{} -> bucket={}", hour, minute, target_bucket);
        } else if (total_minutes >= time_1130 && total_minutes < time_1300) {
            // 11:30:00-13:00:00 映射到 13:00 桶
            target_bucket = 480;
            spdlog::debug("时间映射: {}:{} -> 13:00桶 (bucket={})", hour, minute, target_bucket);
        } else if (total_minutes >= time_1300 && total_minutes < time_1457) {
            // 13:00:00-14:57:00 正常映射 (从bucket=480开始)
            int seconds_since_1300 = (total_minutes - time_1300) * 60 + second;
            target_bucket = seconds_since_1300 / 15 + 480;
            spdlog::debug("正常映射: {}:{} -> bucket={}", hour, minute, target_bucket);
        } else {
            // 其他时间返回-1
            spdlog::debug("非交易时间: {}:{}", hour, minute);
            return -1;
        }
        
        spdlog::debug("时间桶结果: {}:{} -> bucket={}, bars_per_day={}", 
                     hour, minute, target_bucket, bars_per_day_);
        
        if (target_bucket < 0 || target_bucket >= bars_per_day_) return -1;
        return target_bucket;
    }

    int get_step() const { return step_; }
    int get_bars_per_day() const { return bars_per_day_; }
    Frequency get_frequency() const { return frequency_; }
    
    // 格式化时间桶索引为可读时间
    std::string format_time_bucket(int bucket_index) const {
        if (bucket_index < 0 || bucket_index >= bars_per_day_) {
            return "INVALID";
        }
        
        // 根据新的时间桶映射逻辑，计算对应的实际时间
        // 定义关键时间点（以分钟为单位）
        const int time_930 = 9 * 60 + 30;   // 9:30
        const int time_1130 = 11 * 60 + 30; // 11:30
        const int time_1300 = 13 * 60 + 0;  // 13:00
        const int time_1457 = 14 * 60 + 57; // 14:57
        
        // 计算从9:30开始的秒数
        int total_seconds = 0;
        switch (frequency_) {
            case Frequency::F15S:
                total_seconds = bucket_index * 15;
                break;
            case Frequency::F1MIN:
                total_seconds = bucket_index * 60;
                break;
            case Frequency::F5MIN:
                total_seconds = bucket_index * 300;
                break;
            case Frequency::F30MIN:
                total_seconds = bucket_index * 1800;
                break;
        }
        
        // 转换为从9:30开始的分钟数
        int minutes_since_930 = total_seconds / 60;
        
        // 计算实际时间
        int hour, minute;
        if (minutes_since_930 < 120) {
            // 上午：9:30 + minutes_since_930
            hour = 9 + (minutes_since_930 / 60);
            minute = 30 + (minutes_since_930 % 60);
            if (minute >= 60) {
                hour += minute / 60;
                minute = minute % 60;
            }
        } else {
            // 下午：13:00 + (minutes_since_930 - 120)
            int afternoon_minutes = minutes_since_930 - 120;
            hour = 13 + (afternoon_minutes / 60);
            minute = afternoon_minutes % 60;
        }
        
        // 检查是否是特殊映射的时间点
        // 9:30对应的桶索引 = 0
        // 13:00对应的桶索引 = 480
        // 14:57对应的桶索引需要计算
        int seconds_since_1300 = (time_1457 - time_1300) * 60; // 到14:57的秒数
        int bucket_1457 = seconds_since_1300 / 15 + 480;
        
        char time_str[20];
        if (bucket_index == 0) {
            snprintf(time_str, sizeof(time_str), "09:30(特殊)");
        } else if (bucket_index == 480) {
            snprintf(time_str, sizeof(time_str), "13:00(特殊)");
        } else if (bucket_index == bucket_1457) {
            snprintf(time_str, sizeof(time_str), "14:57(特殊)");
        } else {
            snprintf(time_str, sizeof(time_str), "%02d:%02d", hour, minute);
        }
        return std::string(time_str);
    }

    // 写入T日数据到storage_
    void set_bar_series(const std::string& stock, const std::string& indicator_name, const GSeries& series) {
        auto it = storage_.find(stock);
        if (it == storage_.end() || !it->second) {
            // 如果没有对应股票的holder，则新建
            storage_[stock] = std::make_unique<BarSeriesHolder>(stock);
            it = storage_.find(stock);
        }
        // T日数据使用offline_set_m_bar方法
        it->second->offline_set_m_bar(indicator_name, series);
    }
};

// 因子类：依赖Indicator结果计算，结果存储在factor_storage
class Factor {
protected:
    std::string name_;          // 因子名称（如"volatility"）
    std::string id_;            // 类名（如"VolatilityFactor"）
    std::string path_;          // 持久化基础路径
    const Frequency frequency_ = Frequency::F5MIN;  // 因子固定为5分钟频率
    // 存储结构：key为时间bar索引（如9:35为0），内层key为因子名
    std::map<int, std::map<std::string, GSeries>> factor_storage;
    // 新增：存储依赖的indicators
    std::vector<const Indicator*> dependent_indicators_;

public:
    Factor(std::string name, std::string id, std::string path)
            : name_(std::move(name)), id_(std::move(id)), path_(std::move(path)) {}

    virtual ~Factor() = default;

    // 纯虚函数：计算因子（由子类实现具体逻辑，依赖Indicator结果）
    virtual void Calculate(const std::vector<const Indicator*>& indicators) = 0;

    // 新增：definition方法，用于计算因子
    virtual GSeries definition(
            const std::unordered_map<std::string, BarSeriesHolder*>& bar_runners,
            const std::vector<std::string>& sorted_stock_list,
            int ti
    ) {
        spdlog::critical("Factor::definition not implement yet!!");
        return GSeries();
    }


    // 使用访问器模式的definition方法
    virtual GSeries definition_with_accessor(
            std::function<std::shared_ptr<Indicator>(const std::string&)> get_indicator,
            const std::vector<std::string>& sorted_stock_list,
            int ti
    ) {
        // 默认实现：调用原有的definition方法
        std::unordered_map<std::string, BarSeriesHolder*> bar_runners;
        
        // 这里需要子类重写，因为基类不知道需要哪些indicator
        spdlog::warn("Factor::definition_with_accessor需要子类重写");
        return GSeries();
    }

    // 设置因子计算结果
    void set_factor_result(int ti, const GSeries& result) {
        factor_storage[ti][name_] = result;
    }

    // 获取完整存储路径（path/date/5min/name.gz）
    std::string get_full_storage_path(const std::string& date) const {
        return path_ + "/" + date + "/5min/" + name_ + ".gz";
    }

    // 获取因子名称
    const std::string& name() const { return name_; }
    const std::string& get_name() const { return name_; }
    const std::string& get_id() const { return id_; }
    const std::string& get_path() const { return path_; }
    
    // 获取存储结构
    const std::map<int, std::map<std::string, GSeries>>& get_storage() const {
        return factor_storage;
    }
    
    // 设置依赖的indicators
    void set_dependent_indicators(const std::vector<const Indicator*>& indicators) {
        dependent_indicators_ = indicators;
    }
    
    // 获取依赖的indicators
    const std::vector<const Indicator*>& get_dependent_indicators() const {
        return dependent_indicators_;
    }
    
    // 根据indicator名称获取indicator
    const Indicator* get_indicator_by_name(const std::string& indicator_name) const {
        for (const auto& indicator : dependent_indicators_) {
            if (indicator && indicator->name() == indicator_name) {
                return indicator;
            }
        }
        return nullptr;
    }
};

// 计算时间桶映射范围（从factor时间桶映射到indicator时间桶范围）
inline std::pair<int, int> get_time_bucket_range(int factor_ti, Frequency indicator_freq, Frequency factor_freq) {
    int indicator_seconds = 0;
    int factor_seconds = 0;
    
    // 获取indicator频率的秒数
    switch (indicator_freq) {
        case Frequency::F15S: indicator_seconds = 15; break;
        case Frequency::F1MIN: indicator_seconds = 60; break;
        case Frequency::F5MIN: indicator_seconds = 300; break;
        case Frequency::F30MIN: indicator_seconds = 1800; break;
    }
    
    // 获取factor频率的秒数
    switch (factor_freq) {
        case Frequency::F15S: factor_seconds = 15; break;
        case Frequency::F1MIN: factor_seconds = 60; break;
        case Frequency::F5MIN: factor_seconds = 300; break;
        case Frequency::F30MIN: factor_seconds = 1800; break;
    }

    // 计算比例（indicator频率 / factor频率）
    int ratio = indicator_seconds / factor_seconds;
    
    if (ratio >= 1) {
        // indicator频率 >= factor频率（如30min indicator vs 5min factor）
        // 一个indicator时间桶对应多个factor时间桶
        int indicator_ti = factor_ti / ratio;
        return {indicator_ti, indicator_ti};  // 返回同一个indicator时间桶
    } else {
        // indicator频率 < factor频率（如1min indicator vs 5min factor）
        // 一个factor时间桶对应多个indicator时间桶
        int start_index = factor_ti * (factor_seconds / indicator_seconds);
        int end_index = start_index + (factor_seconds / indicator_seconds) - 1;
        return {start_index, end_index};
    }
}

#endif //ALPHAFACTORFRAMEWORK_DATA_STRUCTURES_H
