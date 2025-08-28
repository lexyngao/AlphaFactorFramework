//
// Created by lexyn on 25-7-14.
//
#ifndef ALPHAFACTORFRAMEWORK_CONFIG_H
#define ALPHAFACTORFRAMEWORK_CONFIG_H

#include <string>
#include <vector>
#include <tinyxml2.h>
#include "spdlog/spdlog.h"

// 模块配置（Indicator/Factor，PDF 1.2节）
struct ModuleConfig {
    std::string handler;   // "Indicator"或"Factor"
    std::string name;      // 因子/指标名称
    std::string id;        // 计算类名（如MAIndicator、VolFactor）
    std::string path ;      // 存储路径（如/dat/indicator）
    std::string frequency; // 频率（Indicator:15S/1min/5min/30min；Factor:5min）
};

// 全局配置（PDF 1.2节）
struct GlobalConfig {
    std::string calculate_date = "20240701.csv";   // 计算日期（如20240701）
    std::string stock_universe;   // 股票池名称（如1800）
    int pre_days;             // 提前加载的Indicator天数（如5）
    std::vector<ModuleConfig> modules;  // 所有模块配置
    uint64_t  factor_frequency = 300000; //因子factor计算触发间隔（毫秒，如5分钟=300000ms）
    size_t  worker_thread_count = 0; //cal_engine的线程数
    // indicator指标线程数（0表示自动根据CPU核心数确定）
    size_t indicator_thread_count = 0;
    // factor因子线程数（0表示自动根据CPU核心数确定）
    size_t factor_thread_count = 0;
};

// 配置加载器（解析XML配置文件）
class ConfigLoader {
public:
    // 从XML文件加载配置（严格对齐PDF的<Tsaigu>结构）
    bool load(const std::string& config_path, GlobalConfig& config) {
        tinyxml2::XMLDocument doc;
        if (doc.LoadFile(config_path.c_str()) != tinyxml2::XML_SUCCESS) {
            spdlog::critical("Failed to load config file: {} (error: {})", config_path, doc.ErrorStr());
            return false;
        }

        // 解析<Tsaigu>-><Universe>（PDF 1.2节）
        auto* tsaigu_node = doc.FirstChildElement("Tsaigu");
        if (!tsaigu_node) {
            spdlog::critical("Config missing <Tsaigu> root node");
            return false;
        }
        auto* universe_node = tsaigu_node->FirstChildElement("Universe");
        if (!universe_node) {
            spdlog::critical("Config missing <Universe> node");
            return false;
        }
        // 提取Universe字段
        const char* calc_date = universe_node->Attribute("calculate_date");
        const char* stock_univ = universe_node->Attribute("stock_universe");
        int pre_days = 0;
        if (!calc_date || !stock_univ || universe_node->QueryIntAttribute("pre_days", &pre_days) != tinyxml2::XML_SUCCESS) {
            spdlog::critical("Universe missing required attributes (calculate_date/stock_universe/pre_days)");
            return false;
        }
        config.calculate_date = calc_date;
        config.stock_universe = stock_univ;
        config.pre_days = pre_days;

        // 解析<Tsaigu>-><Modules>-><Module>（PDF 1.2节）
        auto* modules_node = tsaigu_node->FirstChildElement("Modules");
        if (!modules_node) {
            spdlog::critical("Config missing <Modules> node");
            return false;
        }
        for (auto* module_node = modules_node->FirstChildElement("Module"); module_node; module_node = module_node->NextSiblingElement("Module")) {
            ModuleConfig module;
            module.handler = module_node->Attribute("handler");
            module.name = module_node->Attribute("name");
            module.id = module_node->Attribute("id");
            module.path = module_node->Attribute("path");
            module.frequency = module_node->Attribute("frequency");

            // 校验Module字段
            if (module.handler.empty() || module.name.empty() || module.id.empty() || module.path.empty() || module.frequency.empty()) {
                spdlog::error("Invalid Module config (missing attributes), skipping");
                continue;
            }
            // 校验Factor频率（仅允许5min，PDF 1.2节）
//            if (module.handler == "Factor" && module.frequency != "5min") {
            if (module.handler == "Factor" && module.frequency != "5min") {
                spdlog::error("Factor {} frequency must be 5min (got {})", module.name, module.frequency);
                continue;
            }
            // 校验Indicator频率（仅允许15S/1min/5min/30min，PDF 1.2节）
            if (module.handler == "Indicator") {
                const std::vector<std::string> allowed_freq = {"15S", "1min", "5min", "30min"};
                if (std::find(allowed_freq.begin(), allowed_freq.end(), module.frequency) == allowed_freq.end()) {
                    spdlog::error("Indicator {} invalid frequency (got {})", module.name, module.frequency);
                    continue;
                }
            }
            config.modules.push_back(module);
        }

        if (config.modules.empty()) {
            spdlog::info("No valid modules loaded from config");
        }
        spdlog::info("Config loaded successfully (date: {}, universe: {}, pre_days: {})",
                     config.calculate_date, config.stock_universe, config.pre_days);
        return true;
    }
};


#endif //ALPHAFACTORFRAMEWORK_CONFIG_H
