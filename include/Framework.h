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
#include <thread>
#include <spdlog/spdlog.h>

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
        spdlog::info("开始加载所有指标数据...");
        for (const auto& module : config_.modules) {
            if (module.handler == "Indicator") {
                spdlog::info("处理指标模块: {}", module.name);
                auto it = indicator_map_.find(module.name);
                if (it != indicator_map_.end()) {
                    spdlog::info("调用load_multi_day_indicators for {}", module.name);
                    ResultStorage::load_multi_day_indicators(it->second, module, config_);
                } else {
                    spdlog::error("未找到指标: {}", module.name);
                }
            }
        }
        spdlog::info("指标数据加载完成");
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
            std::vector<uint64_t> time_points = generate_time_points(interval_seconds, config_.calculate_date);
            
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
        
        // 按股票分组数据
        std::unordered_map<std::string, std::vector<MarketAllField>> stock_data;
        std::vector<MarketAllField> time_events;
        
        for (const auto& field : all_data_with_time) {
            if (field.type == MarketBufferType::Time) {
                time_events.push_back(field);
            } else {
                stock_data[field.symbol].push_back(field);
            }
        }
        
        spdlog::info("数据分组完成: {}只股票, {}个Time事件", stock_data.size(), time_events.size());
        
        // 多线程处理：每个股票一个线程，保持单股票内的串行
        std::vector<std::thread> stock_threads;
        
        // 处理股票数据
        for (const auto& [symbol, data] : stock_data) {
            stock_threads.emplace_back([this, symbol = symbol, data = data]() {
                spdlog::debug("开始处理股票 {} 的 {} 条数据", symbol, data.size());
                for (const auto& field : data) {
                    engine_.update(field);
                }
                spdlog::debug("完成处理股票 {}", symbol);
            });
        }
        
        // 等待所有股票线程完成
        for (auto& thread : stock_threads) {
            thread.join();
        }
        
        // 串行处理Time事件（确保在所有股票数据处理完成后）
        spdlog::info("开始处理 {} 个Time事件", time_events.size());
        for (const auto& time_event : time_events) {
            engine_.update(time_event);
        }
        
        spdlog::info("引擎运行完成");
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
    std::vector<uint64_t> generate_time_points(int interval_seconds, const std::string& date_str) {
        std::vector<uint64_t> time_points;
        
        // 解析日期字符串 (格式: YYYYMMDD)
        if (date_str.length() != 8) {
            spdlog::error("日期格式错误: {}, 期望格式: YYYYMMDD", date_str);
            return time_points;
        }
        
        int year = std::stoi(date_str.substr(0, 4));
        int month = std::stoi(date_str.substr(4, 2));
        int day = std::stoi(date_str.substr(6, 2));
        
        spdlog::debug("生成时间点: 日期={}-{:02d}-{:02d}, 间隔={}秒", year, month, day, interval_seconds);
        
        // 交易时间：9:30-11:30, 13:00-15:00
        const int morning_start = 9 * 3600 + 30 * 60;   // 9:30
        const int morning_end = 11 * 3600 + 30 * 60;    // 11:30
        const int afternoon_start = 13 * 3600;           // 13:00
        const int afternoon_end = 15 * 3600;             // 15:00
        
        // 生成上午的时间点
        for (int time = morning_start; time < morning_end; time += interval_seconds) {
            uint64_t timestamp = convert_to_timestamp(year, month, day, time);
            time_points.push_back(timestamp);
        }
        
        // 生成下午的时间点
        for (int time = afternoon_start; time < afternoon_end; time += interval_seconds) {
            uint64_t timestamp = convert_to_timestamp(year, month, day, time);
            time_points.push_back(timestamp);
        }
        
        spdlog::debug("生成了 {} 个时间点", time_points.size());
        return time_points;
    }
    
    // 将时间转换为纳秒级时间戳
    uint64_t convert_to_timestamp(int year, int month, int day, int seconds_in_day) {
        int hour = seconds_in_day / 3600;
        int minute = (seconds_in_day % 3600) / 60;
        int second = seconds_in_day % 60;
        
        // 构建日期时间字符串
        std::string datetime_str = fmt::format("{:04d}-{:02d}-{:02d} {:02d}:{:02d}:{:02d}.000000000", 
                                              year, month, day, hour, minute, second);
        
        // 使用数据加载器的解析函数
        uint64_t timestamp = DataLoader::parse_datetime_ns(datetime_str);
        
        spdlog::debug("时间转换: {}-{:02d}-{:02d} {:02d}:{:02d}:{:02d} -> {} ns", 
                     year, month, day, hour, minute, second, timestamp);
        
        return timestamp;
    }

    GlobalConfig config_;
    CalculationEngine engine_;
    std::vector<std::string> stock_list_;
    std::unordered_map<std::string, std::shared_ptr<Indicator>> indicator_map_;
    std::unordered_map<std::string, std::shared_ptr<Factor>> factor_map_;
}; 