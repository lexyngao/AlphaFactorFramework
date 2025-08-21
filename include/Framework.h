#pragma once
#include "config.h"
#include "data_loader.h"
#include "cal_engine.h"
#include "result_storage.h"
#include "my_indicator.h"
#include "my_factor.h"
#include "diff_indicator.h"
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
                std::shared_ptr<Indicator> indicator;
                
                // 根据id创建对应的Indicator实例
                if (module.id == "VolumeIndicator") {
                    indicator = std::make_shared<VolumeIndicator>(module);
                } else if (module.id == "AmountIndicator") {
                    indicator = std::make_shared<AmountIndicator>(module);
                } else if (module.id == "DiffIndicator") {
                    indicator = std::make_shared<DiffIndicator>(module);
                } else {
                    spdlog::error("未知的Indicator类型: {}", module.id);
                    continue;
                }
                
                engine_.add_indicator(module.name, indicator);
                indicator_map_[module.name] = indicator;
                
            } else if (module.handler == "Factor") {
                std::shared_ptr<Factor> factor;
                
                // 根据id创建对应的Factor实例
                if (module.id == "VolumeFactor") {
                    factor = std::make_shared<VolumeFactor>(module);
                } else if (module.id == "PriceFactor") {
                    factor = std::make_shared<PriceFactor>(module);
                } else {
                    spdlog::error("未知的Factor类型: {}", module.id);
                    continue;
                }
                
                // 注入pre_days配置
                factor->set_pre_days(config_.pre_days);
                
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
        spdlog::info("开始运行引擎，数据量: {}", all_tick_datas.size());
        
        // 重置所有指标的计算状态和差分存储
//        engine_.reset_all_indicator_status();
        engine_.reset_diff_storage();

        // 设置factor依赖关系
        setup_factor_dependencies();
        
        // 生成时间事件（不插入数据流）
        std::vector<uint64_t> time_points = generate_time_points(60, config_.calculate_date);
        spdlog::info("生成了 {} 个时间事件", time_points.size());
        
        // 按股票分组处理行情数据（不包含时间事件）
        std::unordered_map<std::string, std::vector<MarketAllField>> stock_data_map;
        for (const auto& data : all_tick_datas) {
            // 只处理行情数据，不处理时间事件
            stock_data_map[data.symbol].push_back(data);
        }
        
        spdlog::info("数据分组完成，共{}只股票", stock_data_map.size());
        
        // 启动Indicator线程组（按股票）
        std::vector<std::thread> indicator_threads;
        for (const auto& [stock, stock_data] : stock_data_map) {
            indicator_threads.emplace_back([this, stock_code = stock, data = stock_data]() {
                spdlog::info("开始处理股票{}的行情数据，共{}条", stock_code, data.size());
                for (const auto& tick_data : data) {
                    engine_.update(tick_data);
                }
                spdlog::info("股票{}行情数据处理完成", stock_code);

            });
        }

        
        // 等待所有Indicator线程完成
        spdlog::info("等待所有Indicator线程完成...");
        for (auto& thread : indicator_threads) {
            thread.join();
        }

        // 启动Factor线程组（按时间事件顺序，每个时间事件内按Factor多线程）
        spdlog::info("启动Factor线程组，处理时间事件");
        engine_.process_factor_time_events(time_points);
        
        spdlog::info("引擎运行完成");
    }

    void setup_factor_dependencies() {
        spdlog::info("设置factor依赖关系...");
        for (auto& [factor_name, factor] : factor_map_) {
            // 收集所有indicators作为依赖
            std::vector<const Indicator*> dependent_indicators;
            for (auto& [indicator_name, indicator] : indicator_map_) {
                dependent_indicators.push_back(indicator.get());
            }
            factor->set_dependent_indicators(dependent_indicators);
            spdlog::debug("Factor[{}]设置了{}个indicator依赖", factor_name, dependent_indicators.size());
        }
    }

    void save_all_results() {
        spdlog::info("开始保存所有结果...");
        
        // 添加线程同步，确保所有计算线程完成
        // 等待引擎完成所有计算
        engine_.wait_for_completion();
        
        try {
            for (const auto& module : config_.modules) {
                try {
                    if (module.handler == "Indicator") {
                        auto it = indicator_map_.find(module.name);
                        if (it != indicator_map_.end() && it->second) {
                            spdlog::info("保存指标: {}", module.name);
                            if (!ResultStorage::save_indicator(it->second, module, config_.calculate_date)) {
                                spdlog::error("保存指标[{}]失败", module.name);
                            }
                        } else {
                            spdlog::warn("指标[{}]不存在或为空", module.name);
                        }
                    } else if (module.handler == "Factor") {
                        auto it = factor_map_.find(module.name);
                        if (it != factor_map_.end() && it->second) {
                            spdlog::info("保存因子: {}", module.name);
                            if (!ResultStorage::save_factor(it->second, module, config_.calculate_date, stock_list_)) {
                                spdlog::error("保存因子[{}]失败", module.name);
                            }
                        } else {
                            spdlog::warn("因子[{}]不存在或为空", module.name);
                        }
                    }
                } catch (const std::exception& e) {
                    spdlog::error("保存模块[{}]时发生异常: {}", module.name, e.what());
                } catch (...) {
                    spdlog::error("保存模块[{}]时发生未知异常", module.name);
                }
            }
        } catch (const std::exception& e) {
            spdlog::error("保存结果时发生异常: {}", e.what());
            throw; // 重新抛出异常，让上层处理
        }
        
        spdlog::info("所有结果保存完成");
    }
    
    // 新增：保存指定频率的DiffIndicator结果
    void save_diff_indicator_with_frequencies(const std::string& indicator_name, 
                                             const std::vector<std::string>& target_frequencies) {
        auto it = indicator_map_.find(indicator_name);
        if (it == indicator_map_.end()) {
            spdlog::error("未找到指标: {}", indicator_name);
            return;
        }
        
        // 检查是否为DiffIndicator
        auto diff_indicator = std::dynamic_pointer_cast<DiffIndicator>(it->second);
        if (!diff_indicator) {
            spdlog::error("指标[{}]不是DiffIndicator类型", indicator_name);
            return;
        }
        
        // 查找对应的模块配置
        ModuleConfig module_config;
        bool found = false;
        for (const auto& module : config_.modules) {
            if (module.name == indicator_name && module.handler == "Indicator") {
                module_config = module;
                found = true;
                break;
            }
        }
        
        if (!found) {
            spdlog::error("未找到指标[{}]的配置", indicator_name);
            return;
        }
        
        // 为每个目标频率保存数据
        for (const auto& freq : target_frequencies) {
            spdlog::info("保存指标[{}]的{}频率数据", indicator_name, freq);
            diff_indicator->save_results_with_frequency(module_config, config_.calculate_date, freq);
        }
    }

    // 新增：生成时间点（供外部调用）
    std::vector<uint64_t> generate_time_points(int interval_seconds, const std::string& date_str) const {
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
        
        // 交易时间：9:30-11:30, 13:00-14:57
        const int morning_start = 9 * 3600 + 30 * 60;   // 9:30
        const int morning_end = 11 * 3600 + 30 * 60;    // 11:30
        const int afternoon_start = 13 * 3600;           // 13:00
        const int afternoon_end = 14 * 3600 + 57 * 60;  // 14:57
        
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

    const std::vector<std::string>& get_stock_list() const { return stock_list_; }
    CalculationEngine& get_engine() { return engine_; }
    const GlobalConfig& get_config() const { return config_; }
    const std::unordered_map<std::string, std::shared_ptr<Indicator>>& get_indicator_map() const { return indicator_map_; }
    const std::unordered_map<std::string, std::shared_ptr<Factor>>& get_factor_map() const { return factor_map_; }

private:
    
    // 将时间转换为纳秒级时间戳
    uint64_t convert_to_timestamp(int year, int month, int day, int seconds_in_day) const {
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