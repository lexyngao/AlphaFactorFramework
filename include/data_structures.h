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


// 存储计算结果的序列（PDF 1.3节）
class GSeries {
private:
    std::vector<double> d_vec;  // 每个时间bar的计算结果
    // 总长度
    int valid_num = 0;          // 有效值数量（非空值）

public:
    GSeries() = default;
    explicit GSeries(const std::vector<double>& vec) : d_vec(vec), size(vec.size()) {
        valid_num = std::count_if(vec.begin(), vec.end(), [](double v) { return !std::isnan(v); });
    }

    // 添加值（自动更新size和valid_num）
    void push(double value) {
        d_vec.push_back(value);
        size++;
        if (!std::isnan(value)) valid_num++;
    }

    // 获取值（带边界检查）
    double get(int idx) const {
        if (idx < 0 || idx >= size) {
            spdlog::error("GSeries index out of range: {} (size: {})", idx, size);
            return std::nan("");
        }
        return d_vec[idx];
    }

    bool is_valid(int idx) const {
        if (idx < 0 || idx >= size) return false;  // 索引越界 → 无效
        return !std::isnan(d_vec[idx]);            // 非NaN → 有效
    }

    // 重置数据（用于reindex）
    void reindex(const std::vector<std::string>& new_stock_list, const std::unordered_map<std::string, int>& old_index_map) {
        std::vector<double> new_d_vec;
        new_d_vec.reserve(new_stock_list.size());
        for (const auto& stock : new_stock_list) {
            if (old_index_map.count(stock)) {
                new_d_vec.push_back(d_vec[old_index_map.at(stock)]);
            } else {
                new_d_vec.push_back(std::nan(""));  // 缺失值用NaN填充（PDF要求）
            }
        }
        d_vec = new_d_vec;
        size = new_d_vec.size();
        valid_num = std::count_if(new_d_vec.begin(), new_d_vec.end(), [](double v) { return !std::isnan(v); });
    }

    // getter
    const std::vector<double>& data() const { return d_vec; }
    int get_size() const { return size; }
    int get_valid_num() const { return valid_num; }

    // 判断是否为空（新增函数）
    bool empty() const {
        return size == 0 || d_vec.empty();  // 两种判断方式，确保准确性
    }

    // 设置指定索引的值（之前用到的set函数，新增）
    void set(int idx, double value) {
        if (idx < 0) {
            spdlog::error("GSeries set index out of range: {}", idx);
            return;
        }
        // 若索引超过当前size，扩容并填充NaN
        if (idx >= size) {
            int new_size = idx + 1;
            d_vec.resize(new_size, std::nan(""));
            size = new_size;
        }
        // 更新值
        bool was_valid = !std::isnan(d_vec[idx]);
        d_vec[idx] = value;
        bool is_valid = !std::isnan(value);

        // 更新valid_num
        if (is_valid && !was_valid) valid_num++;
        else if (!is_valid && was_valid) valid_num--;
    }

    // 调整大小（之前用到的resize函数，新增）
    void resize(int new_size) {
        if (new_size <= size) return;
        d_vec.resize(new_size, std::nan(""));
        size = new_size;
    }

    int size = 0;
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
enum class MarketBufferType { Order, Trade, Tick };

struct MarketAllField {
    MarketBufferType type;          // 数据类型
    std::string symbol;             // 股票代码（可复制）
    uint64_t timestamp;             // 时间戳（可复制）即real_time
    uint64_t appl_seq_num;          // 序列号（可复制）
    union {
        OrderData order;
        TradeData trade;
        TickData tick;
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

};

// Indicator历史数据持有者（PDF 1.3节）
class BaseSeriesHolder {  // PDF中为BaseSeriesHolder，修正类名对齐
private:
    std::string stock;  // 股票代码
    // 结构：indicator_name -> 日期索引（T-5=1,...,T-1=pre_days）-> GSeries
    std::unordered_map<std::string, std::unordered_map<int, GSeries>> HisBarSeries;


public:
    // 1. 构造函数（初始化智能指针）
    explicit BaseSeriesHolder(std::string stock_code)
            : stock(std::move(stock_code)) {}

    // 2. 移动构造函数（允许对象移动）
    BaseSeriesHolder(BaseSeriesHolder&& other) noexcept
            : stock(std::move(other.stock)),
              HisBarSeries(std::move(other.HisBarSeries)) {}  // 移动智能指针

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
    void set_his_series(const std::string& indicator_name, int his_day_index, const GSeries& series) {
        if (his_day_index <= 0) {
            spdlog::error("{}: his_day_index must be > 0 (got {})", stock, his_day_index);
            return;
        }
        HisBarSeries[indicator_name][his_day_index] = series;
    }

    // 获取历史序列片段（PDF 1.3节）
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

// Factor基类（PDF 3.4节）
class BaseFactor {
public:
    virtual ~BaseFactor() = default;  // 确保多态析构

    // 计算因子（输入：所有股票的Indicator数据、排序后的股票列表、时间bar索引ti）
    virtual GSeries definition(
            const std::unordered_map<std::string, BaseSeriesHolder*>& bar_runners,
            const std::vector<std::string>& sorted_stock_list,
            int ti
    ) {
        spdlog::critical("BaseFactor::definition not implemented for factor");
        return GSeries();
    }
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
    int step_ = 1; // 步长，默认每个bar都输出
    int bars_per_day_ = 960; // 默认15s

    void init_frequency_params() {
        switch (frequency_) {
            case Frequency::F15S:
                step_ = 1;
                bars_per_day_ = 960;
                break;
            case Frequency::F1MIN:
                step_ = 4;
                bars_per_day_ = 240;
                break;
            case Frequency::F5MIN:
                step_ = 20;
                bars_per_day_ = 48;
                break;
            case Frequency::F30MIN:
                step_ = 120;
                bars_per_day_ = 8;
                break;
        }
    }

    // 关键修改：用unique_ptr自动管理BaseSeriesHolder的生命周期
    std::unordered_map<std::string, std::unique_ptr<BaseSeriesHolder>> storage_;

    std::unordered_map<std::string, uint64_t> last_calculation_time_; // 股票->上次计算时间（纳秒）


public:
    Indicator(std::string name, std::string id, std::string path, Frequency freq)
            : name_(std::move(name)), id_(std::move(id)), path_(std::move(path)), frequency_(freq) {
        init_frequency_params();
    }

    // 新增：支持通过ModuleConfig直接初始化
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
    // 获取存储结构（供Factor访问）
    const std::unordered_map<std::string, std::unique_ptr<BaseSeriesHolder>>& get_storage()  const {
        return storage_;
    }

    // 初始化指标存储（预创建所有股票的BaseSeriesHolder）
    void init_storage(const std::vector<std::string>& stock_list) {
        storage_.clear();  // 清空原有存储

        for (const auto& stock : stock_list) {
            auto holder = std::make_unique<BaseSeriesHolder>(stock);
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
    virtual void load_historical_data(const std::string& stock_code, BaseSeriesHolder& holder) {
        spdlog::debug("指标[{}]暂未实现{}的历史数据加载", name_, stock_code);
    }

    // 新增：尝试计算（内部判断频率，解耦engine）
    void try_calculate(const SyncTickData& sync_tick) {
        Calculate(sync_tick);
    }




    // 统一的时间桶索引计算
    int get_time_bucket_index(uint64_t total_ns) const {
        if (total_ns == 0) return -1;

        // 1. 转换为北京时间
        int64_t utc_sec = total_ns / 1000000000;
        int64_t beijing_sec = utc_sec;
        int64_t beijing_seconds_in_day = beijing_sec % 86400;
        int hour = static_cast<int>(beijing_seconds_in_day / 3600);
        int minute = static_cast<int>((beijing_seconds_in_day % 3600) / 60);
        int second = static_cast<int>(beijing_seconds_in_day % 60);
        int total_minutes = hour * 60 + minute;

        // 交易时间
        const int morning_start = 9 * 60 + 30;   // 9:30
        const int morning_end = 11 * 60 + 30;    // 11:30
        const int afternoon_start = 13 * 60;     // 13:00
        const int afternoon_end = 15 * 60;       // 15:00

        bool is_morning = (total_minutes >= morning_start && total_minutes < morning_end);
        bool is_afternoon = (total_minutes >= afternoon_start && total_minutes < afternoon_end);
        if (!is_morning && !is_afternoon) return -1;

        int seconds_since_open = 0;
        if (is_morning) {
            seconds_since_open = (total_minutes - morning_start) * 60 + second;
        } else {
            seconds_since_open = (morning_end - morning_start) * 60 // 上午总秒数
                                + (total_minutes - afternoon_start) * 60 + second;
        }

        int bucket_len = 15; // 默认15s
        switch (frequency_) {
            case Frequency::F15S: bucket_len = 15; break;
            case Frequency::F1MIN: bucket_len = 60; break;
            case Frequency::F5MIN: bucket_len = 300; break;
            case Frequency::F30MIN: bucket_len = 1800; break;
        }
        int ti = seconds_since_open / bucket_len;
        if (ti < 0 || ti >= bars_per_day_) return -1;
        return ti;
    }

    int get_step() const { return step_; }
    int get_bars_per_day() const { return bars_per_day_; }
    Frequency get_frequency() const { return frequency_; }

    // 新增：写入T日数据到storage_
    void set_bar_series(const std::string& stock, const std::string& indicator_name, const GSeries& series) {
        auto it = storage_.find(stock);
        if (it == storage_.end() || !it->second) {
            // 如果没有对应股票的holder，则新建
            storage_[stock] = std::make_unique<BaseSeriesHolder>(stock);
            it = storage_.find(stock);
        }
        // T日一般用his_day_index=0
        it->second->set_his_series(indicator_name, 0, series);
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

public:
    Factor(std::string name, std::string id, std::string path)
            : name_(std::move(name)), id_(std::move(id)), path_(std::move(path)) {}

    virtual ~Factor() = default;

    // 纯虚函数：计算因子（由子类实现具体逻辑，依赖Indicator结果）
    virtual void Calculate(const std::vector<const Indicator*>& indicators) = 0;

    // 获取完整存储路径（path/date/5min/name.gz）
    std::string get_full_storage_path(const std::string& date) const {
        return path_ + "/" + date + "/5min/" + name_ + ".gz";
    }

    // 获取因子名称
    const std::string& name() const { return name_; }
    // 获取存储结构
    const std::map<int, std::map<std::string, GSeries>>& get_storage() const {
        return factor_storage;
    }
};

#endif //ALPHAFACTORFRAMEWORK_DATA_STRUCTURES_H
