#pragma once
#include "data_structures.h"
#include <unordered_map>
#include <vector>
#include <string>

class VolumeFactor : public Factor {
public:
    VolumeFactor(const ModuleConfig& module) : Factor(module.name, module.id, module.path) {}

    // 实现Factor的纯虚函数（保持兼容性）
    void Calculate(const std::vector<const Indicator*>& indicators) override {
        // 这个函数在新的架构中不会被调用，但需要实现以保持接口兼容
        spdlog::warn("VolumeFactor::Calculate被调用，但应该使用definition函数");
    }

    // 实现Factor的definition函数
    GSeries definition(
        const std::unordered_map<std::string, BarSeriesHolder*>& barRunner,
        const std::vector<std::string>& sorted_stock_list,
        int ti
    ) override;



    // 新增：使用访问器模式的实现
    GSeries definition_with_accessor(
        std::function<std::shared_ptr<Indicator>(const std::string&)> get_indicator,
        const std::vector<std::string>& sorted_stock_list,
        int ti
    ) override;
};

class PriceFactor : public Factor {
public:
    PriceFactor(const ModuleConfig& module) : Factor(module.name, module.id, module.path) {}

    // 实现Factor的纯虚函数（保持兼容性）
    void Calculate(const std::vector<const Indicator*>& indicators) override {
        // 这个函数在新的架构中不会被调用，但需要实现以保持接口兼容
        spdlog::warn("PriceFactor::Calculate被调用，但应该使用definition函数");
    }

    // 实现Factor的definition函数
    GSeries definition(
        const std::unordered_map<std::string, BarSeriesHolder*>& barRunner,
        const std::vector<std::string>& sorted_stock_list,
        int ti
    ) override;



    // 新增：使用访问器模式的实现
    GSeries definition_with_accessor(
        std::function<std::shared_ptr<Indicator>(const std::string&)> get_indicator,
        const std::vector<std::string>& sorted_stock_list,
        int ti
    ) override;
}; 