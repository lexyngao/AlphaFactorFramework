#pragma once
#include "config.h"
#include "data_loader.h"
#include "cal_engine.h"
#include "result_storage.h"
#include "my_indicator.h"
#include "my_factor.h"
#include <memory>
#include <vector>
#include <unordered_map>
#include <set>

class Framework {
public:
    Framework(const GlobalConfig& config)
        : config_(config), engine_(config) {
        stock_list_ = DataLoader().get_stock_list_from_data();
    }

    void register_indicators_factors(const std::vector<ModuleConfig>& modules) {
        for (const auto& module : modules) {
            if (module.handler == "Indicator") {
                auto indicator = std::make_shared<VolumeIndicator>(module); // 可扩展为工厂
                engine_.add_indicator(module.name, indicator);
                indicator_map_[module.name] = indicator;
            } else if (module.handler == "Factor") {
                auto factor = std::make_shared<VolumeFactor>(module); // 可扩展为工厂
                engine_.add_factor(factor);
                factor_map_[module.name] = factor;
            }
        }
        engine_.init_indicator_storage(stock_list_);
    }

    void load_all_indicators() {
        for (const auto& module : config_.modules) {
            if (module.handler == "Indicator") {
                auto it = indicator_map_.find(module.name);
                if (it != indicator_map_.end()) {
                    ResultStorage::load_multi_day_indicators(it->second, module, config_);
                }
            }
        }
    }

    std::vector<MarketAllField> load_and_sort_market_data(DataLoader& data_loader) {
        std::vector<MarketAllField> all_tick_datas;
        for (const auto& stock : stock_list_) {
            auto stock_data = data_loader.load_stock_data_to_Market(stock, config_.calculate_date);
            all_tick_datas.insert(all_tick_datas.end(), stock_data.begin(), stock_data.end());
        }
        data_loader.sort_market_datas(all_tick_datas);
        return all_tick_datas;
    }

    void run_engine(const std::vector<MarketAllField>& all_tick_datas) {
        // 生成Time触发事件
        std::vector<MarketAllField> all_data_with_time = all_tick_datas;
        
        // 只生成Factor需要的Time事件（5min间隔）
        std::set<int> time_intervals;
        if (!factor_map_.empty()) {
            time_intervals.insert(300);  // Factor固定为5min
        }
        
        // 为每个时间间隔生成Time事件
        for (int interval_seconds : time_intervals) {
            // 计算交易时间内的所有时间点
            std::vector<uint64_t> time_points = generate_time_points(interval_seconds);
            
            for (uint64_t time_point : time_points) {
                MarketAllField time_field(
                    MarketBufferType::Time,
                    "TIME",  // 股票代码设为TIME
                    time_point,  // 时间戳
                    0  // 序列号
                );
                all_data_with_time.push_back(time_field);
            }
        }
        
        // 重新排序所有数据（包括Time事件）
        DataLoader().sort_market_datas(all_data_with_time);
        
        // 运行引擎
        for (const auto& field : all_data_with_time) {
            engine_.update(field);
        }
    }

    void save_all_results() {
        for (const auto& module : config_.modules) {
            if (module.handler == "Indicator") {
                auto it = indicator_map_.find(module.name);
                if (it != indicator_map_.end()) {
                    ResultStorage::save_indicator(it->second, module, config_.calculate_date);
                }
            } else if (module.handler == "Factor") {
                auto it = factor_map_.find(module.name);
                if (it != factor_map_.end()) {
                    ResultStorage::save_factor(it->second, module, config_.calculate_date, stock_list_);
                }
            }
        }
    }

    const std::vector<std::string>& get_stock_list() const { return stock_list_; }
    CalculationEngine& get_engine() { return engine_; }
    const GlobalConfig& get_config() const { return config_; }
    const std::unordered_map<std::string, std::shared_ptr<Indicator>>& get_indicator_map() const { return indicator_map_; }
    const std::unordered_map<std::string, std::shared_ptr<Factor>>& get_factor_map() const { return factor_map_; }

private:
    // 生成指定间隔的时间点
    std::vector<uint64_t> generate_time_points(int interval_seconds) {
        std::vector<uint64_t> time_points;
        
        // 交易时间：9:30-11:30, 13:00-15:00
        const int morning_start = 9 * 3600 + 30 * 60;   // 9:30
        const int morning_end = 11 * 3600 + 30 * 60;    // 11:30
        const int afternoon_start = 13 * 3600;           // 13:00
        const int afternoon_end = 15 * 3600;             // 15:00
        
        // 生成上午的时间点
        for (int time = morning_start; time < morning_end; time += interval_seconds) {
            // 转换为纳秒级时间戳（假设是2024年7月1日）
            uint64_t timestamp = convert_to_timestamp(2024, 7, 1, time);
            time_points.push_back(timestamp);
        }
        
        // 生成下午的时间点
        for (int time = afternoon_start; time < afternoon_end; time += interval_seconds) {
            uint64_t timestamp = convert_to_timestamp(2024, 7, 1, time);
            time_points.push_back(timestamp);
        }
        
        return time_points;
    }
    
    // 将时间转换为纳秒级时间戳
    uint64_t convert_to_timestamp(int year, int month, int day, int seconds_in_day) {
        // 简化的时间转换，实际应该使用更精确的日期时间库
        // 这里假设是2024年7月1日，转换为UTC时间戳
        int hour = seconds_in_day / 3600;
        int minute = (seconds_in_day % 3600) / 60;
        int second = seconds_in_day % 60;
        
        // 转换为北京时间（UTC+8）
        // 这里简化处理，实际应该使用标准的时间库
        uint64_t timestamp = 1720000000000000000ULL;  // 2024年7月1日的基础时间戳
        timestamp += (hour * 3600 + minute * 60 + second) * 1000000000ULL;
        return timestamp;
    }

    GlobalConfig config_;
    CalculationEngine engine_;
    std::vector<std::string> stock_list_;
    std::unordered_map<std::string, std::shared_ptr<Indicator>> indicator_map_;
    std::unordered_map<std::string, std::shared_ptr<Factor>> factor_map_;
}; 