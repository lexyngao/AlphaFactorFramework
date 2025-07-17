//
// Created by lexyn on 25-7-14.
//

#ifndef ALPHAFACTORFRAMEWORK_CONFIG_LOADER_H
#define ALPHAFACTORFRAMEWORK_CONFIG_LOADER_H

#include <string>
#include <vector>
#include <tinyxml2.h>

struct UniverseConfig {
    std::string calculate_date;  // 计算日期
    std::string stock_universe;  // 股票池
    int pre_days = 0;            // 提前加载天数
};

struct ModuleConfig {
    std::string handler;   // Indicator或Factor
    std::string name;      // 名称
    std::string id;        // 类名
    std::string path;      // 存储路径
    std::string frequency; // 频率（15S/5min等）
};

class ConfigLoader {
private:
    UniverseConfig universe;
    std::vector<ModuleConfig> modules;
public:
    bool load(const std::string& config_path);

    const UniverseConfig& get_universe() const { return universe; }
    const std::vector<ModuleConfig>& get_modules() const { return modules; }
};

#endif //ALPHAFACTORFRAMEWORK_CONFIG_LOADER_H
