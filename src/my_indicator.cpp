//
// Created by lexyn on 25-7-16.
//
#include "my_indicator.h"
#include "data_structures.h"

// 成交量指标实现
VolumeIndicator::VolumeIndicator()
        : Indicator("volume", "VolumeIndicator", "/data/indicators", Frequency::F5MIN) {}

void VolumeIndicator::Calculate(const SyncTickData& tick_data) {
    // 1. 从基类storage_中获取或创建BaseSeriesHolder（线程安全）
    BaseSeriesHolder* holder = nullptr;
    {
        // 加锁保护storage_（需在基类或子类中添加storage_mutex_）
        static std::mutex storage_mutex_;  // 或作为基类的protected成员
        std::lock_guard<std::mutex> lock(storage_mutex_);

        // 若股票不存在，创建新的BaseSeriesHolder（通过unique_ptr管理）
        if (!storage_.count(tick_data.symbol)) {
            storage_[tick_data.symbol] = std::make_unique<BaseSeriesHolder>(tick_data.symbol);
            spdlog::debug("[初始化] 为股票{}创建BaseSeriesHolder", tick_data.symbol);
        }
        holder = storage_[tick_data.symbol].get();  // 获取原始指针（不转移所有权）
    }

    // 2. 锁定当前股票的BaseSeriesHolder，确保线程安全
    std::lock_guard<std::mutex> lock(holder->get_mutex());


    // 2. 后续逻辑（累加成交量、计算ti、更新GSeries等）
    double volume = 0.0;
    for (const auto& trans : tick_data.trans) {
        volume += trans.volume;
        spdlog::debug("[成交明细] 单笔volume: {}, 累计: {}", trans.volume, volume);
    }

    // 日志3：打印总成交量（若为0，说明trans为空或volume全0）
    spdlog::debug("[成交量汇总] 本次累计volume: {}", volume);

    // 2. 解析纳秒级时间戳（假设tick_data.tick_data.real_time是uint64_t类型的总纳秒数）
    uint64_t total_ns = tick_data.tick_data.real_time;
    if (total_ns == 0) {
        spdlog::debug("{}: 无效的纳秒时间戳", tick_data.symbol);
        return;
    }

    // 3. 计算ti
    int ti = cal_time_bucket_ns(total_ns);

    if (ti < 0) {
        spdlog::debug("{}: 跳过非交易时间数据", tick_data.symbol);
        return;
    }

    // 3. 更新成交量（关键逻辑：获取当前值并累加）
    std::string indicator_name = "volume_indicator";
    int current_day_index = 5;  // 假设T日索引为5（根据实际情况调整）

    // 3.1 获取当前时间桶的现有成交量
    double current_volume = 0.0;
    GSeries current_series = holder->his_slice_bar(indicator_name, current_day_index);
    spdlog::debug("[读取GSeries] 初始长度: {}, 有效值数量: {}, current_day_index: {}",
                  current_series.get_size(), current_series.get_valid_num(), current_day_index);

    if (!current_series.empty() && current_series.get_valid_num() > ti) {
        current_volume = current_series.get(ti);
        spdlog::debug("[读取当前ti值] ti={}, 现有volume: {}", ti, current_volume);
    }

    // 3.2 累加新成交量
    current_volume += volume;

    // 3.3 更新GSeries（将当前时间桶的值设置为累加后的值）
    GSeries updated_series = current_series;
    if (updated_series.empty()) {
        updated_series = GSeries();  // 创建新GSeries
        updated_series.resize(48);   // 假设一天48个5分钟bar
    }
    updated_series.set(ti, current_volume);

    // 4. 将更新后的GSeries存入BaseSeriesHolder
    holder->set_his_series(indicator_name, current_day_index, updated_series);
    spdlog::info("[更新value]{}: Volume updated to {} at ti={}",
                 tick_data.symbol, current_volume, ti);


}

// 输入：UTC时间的纳秒级时间戳（total_ns，由parse_datetime_ns返回）
// 返回：时间桶索引ti（0-47，-1表示非交易时间）
int VolumeIndicator::cal_time_bucket_ns(uint64_t total_ns) {
    if (total_ns == 0) {
        spdlog::debug("无效的total_ns：0");
        return -1;
    }

    // 1. 将UTC纳秒时间戳转换为北京时间（UTC+8）
    // 北京时间 = UTC时间 + 8小时（28800秒）
    const int64_t BEIJING_UTC_OFFSET_SEC = 8 * 3600;  // 北京时间比UTC快8小时
    int64_t utc_sec = total_ns / 1000000000;          // UTC总秒数
    int64_t beijing_sec = utc_sec;  // 北京时间总秒数

    // 2. 提取北京时间的时、分（忽略日期，只关心当日时间）
    int64_t beijing_seconds_in_day = beijing_sec % 86400;  // 北京时间当日秒数（0-86399）
    int hour = static_cast<int>(beijing_seconds_in_day / 3600);  // 北京小时（0-23）
    int minute = static_cast<int>((beijing_seconds_in_day % 3600) / 60);  // 北京分钟（0-59）
    int total_minutes = hour * 60 + minute;  // 北京时间当日总分钟数（0-1439）

    // 3. 交易时间边界（北京时间）
    const int morning_start = 9 * 60 + 30;   // 9:30
    const int morning_end = 11 * 60 + 30;     // 11:30（不含）
    const int afternoon_start = 13 * 60;      // 13:00
    const int afternoon_end = 15 * 60;        // 15:00（不含）

    // 4. 判断是否在交易时间内
    bool is_morning = (total_minutes >= morning_start && total_minutes < morning_end);
    bool is_afternoon = (total_minutes >= afternoon_start && total_minutes < afternoon_end);
    if (!is_morning && !is_afternoon) {
        spdlog::debug("非交易时间（北京时间）：{}:{}（总分钟数：{}）", hour, minute, total_minutes);
        return -1;
    }

    // 5. 计算时间桶索引ti
    int ti;
    if (is_morning) {
        // 上午时段：9:30-11:30（每5分钟一个桶，ti=0-23）
        int minutes_in_morning = total_minutes - morning_start;
        ti = minutes_in_morning / 5;
    } else {
        // 下午时段：13:00-15:00（每5分钟一个桶，ti=24-47）
        int minutes_in_afternoon = total_minutes - afternoon_start;
        ti = 24 + (minutes_in_afternoon / 5);
    }

    // 校验ti范围
    if (ti < 0 || ti >= 48) {
        spdlog::warn("ti超出范围：{}（北京时间：{}:{}）", ti, hour, minute);
        return -1;
    }

    return ti;
}

BaseSeriesHolder* VolumeIndicator::get_bar_series_holder(const std::string& stock_code) const {
    std::lock_guard<std::mutex> lock(storage_mutex_);  // 保护storage_的线程安全
    auto it = storage_.find(stock_code);
    if (it != storage_.end()) {
        return it->second.get();  // 返回unique_ptr管理的原始指针
    }
    return nullptr;
}