#pragma once
#include "config.h"
#include "data_loader.h"
#include "cal_engine.h"
#include "result_storage.h"
#include "my_indicator.h"
#include <memory>
#include <vector>
#include <unordered_map>

class Framework {
public:
    Framework(const GlobalConfig& config)
        : config_(config), engine_(config) {
        stock_list_ = DataLoader().get_stock_list_from_data();
    }

    void register_indicators(const std::vector<ModuleConfig>& modules) {
        for (const auto& module : modules) {
            if (module.handler == "Indicator") {
                auto indicator = std::make_shared<VolumeIndicator>(module); // 可扩展为工厂
                engine_.add_indicator(module.name, indicator);
                indicator_map_[module.name] = indicator;
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
        for (const auto& field : all_tick_datas) {
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
            }
        }
    }

    const std::vector<std::string>& get_stock_list() const { return stock_list_; }
    CalculationEngine& get_engine() { return engine_; }
    const GlobalConfig& get_config() const { return config_; }
    const std::unordered_map<std::string, std::shared_ptr<Indicator>>& get_indicator_map() const { return indicator_map_; }

private:
    GlobalConfig config_;
    CalculationEngine engine_;
    std::vector<std::string> stock_list_;
    std::unordered_map<std::string, std::shared_ptr<Indicator>> indicator_map_;
}; 