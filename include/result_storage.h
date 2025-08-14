//
// Created by lexyn on 25-7-14.
//

#ifndef ALPHAFACTORFRAMEWORK_RESULT_STORAGE_H
#define ALPHAFACTORFRAMEWORK_RESULT_STORAGE_H

#include "data_structures.h"
#include "cal_engine.h"
#include "config.h"
#include "diff_indicator.h"
#include <fstream>
#include <zlib.h>
#include <filesystem>
#include <sstream>
#include "utils.h"

namespace fs = std::filesystem;

// 结果存储管理器（PDF 4节）
class ResultStorage {
public:
    // 保存单个指标模块的结果（GZ压缩，行=bar_index，列=stock_code）

    // 保存单个指标模块的结果（从Indicator自身的storage_读取数据）
    static bool save_indicator(
            const std::shared_ptr<Indicator>& indicator,  // 目标指标实例
            const ModuleConfig& module,
            const std::string& date
    ) {
        try {
            // 1. 验证参数有效性
            if (!indicator) {
                spdlog::error("保存失败：指标实例为空");
                return false;
            }
            if (module.handler != "Indicator") {
                spdlog::error("模块[{}]不是Indicator类型", module.name);
                return false;
            }
            if (module.path.empty() || module.name.empty()) {
                spdlog::error("模块[{}]路径或名称为空", module.name);
                return false;
            }
            if(indicator->is_calculated()){
                spdlog::info("指标[{}]已经保存",module.name);
                return true;
            }

            // 2. 检查是否为DiffIndicator，如果是则使用特殊保存逻辑
            if (module.id == "DiffIndicator") {
                // 尝试转换为DiffIndicator并调用其特殊保存方法
                if (auto diff_ind = std::dynamic_pointer_cast<DiffIndicator>(indicator)) {
                    return diff_ind->save_results(module, date);
                }
            }

            // 3. 创建存储目录
            fs::path base_path = fs::path(module.path) / date / module.frequency;
            if (!fs::exists(base_path) && !fs::create_directories(base_path)) {
                spdlog::error("创建目录失败: {}", base_path.string());
                return false;
            }

            // 4. 从Indicator的storage_中收集数据（核心修改）
            const auto& indicator_storage = indicator->get_storage();  // 获取指标自己的存储
            if (indicator_storage.empty()) {
                spdlog::warn("指标[{}]的storage_为空，无数据可保存", module.name);
                return true;
            }

            // 提取指标实际处理过的股票列表（仅包含有数据的股票）
            std::vector<std::string> stock_list;
            for (const auto& [stock_code, _] : indicator_storage) {
                stock_list.push_back(stock_code);
            }

            // 按bar_index分组收集数据
            std::map<int, std::map<std::string, double>> bar_data;  // bar_index -> {股票 -> 数值}
            int max_bar_index = -1;

            int bars_per_day = indicator->get_bars_per_day();
            for (const auto& [stock_code, holder_ptr] : indicator_storage) {
                if (!holder_ptr) {
                    spdlog::warn("指标[{}]的股票[{}]holder为空，跳过", module.name, stock_code);
                    continue;
                }
                
                const BarSeriesHolder* holder = holder_ptr.get();
                if (!holder) {
                    spdlog::warn("指标[{}]的股票[{}]holder指针为空，跳过", module.name, stock_code);
                    continue;
                }
                
                // 对于DiffIndicator，需要特殊处理
                std::string key_to_use = module.name;
                if (module.id == "DiffIndicator") {
                    // DiffIndicator使用第一个字段的output_key作为默认键
                    // 这里我们暂时使用"volume"作为默认键，实际应该从DiffIndicator获取
                    key_to_use = "volume";
                    spdlog::debug("DiffIndicator[{}]使用键名: {}", module.name, key_to_use);
                }
                
                // T日数据现在存储在MBarSeries中
                GSeries series = holder->get_m_bar(key_to_use);
                
                // 检查series是否有效
                if (series.get_size() == 0) {
                    spdlog::warn("指标[{}]的股票[{}]键[{}]数据为空，跳过", module.name, stock_code, key_to_use);
                    continue;
                }
                
                // 确保series长度和bars_per_day一致
                if (series.get_size() < bars_per_day) {
                    series.resize(bars_per_day);
                }
                
                for (int ti = 0; ti < bars_per_day; ++ti) {
                    double value = series.get(ti);
                    bar_data[ti][stock_code] = value;
                    if (ti > max_bar_index) max_bar_index = ti;
                }
            }

            if (bar_data.empty()) {
                spdlog::warn("指标[{}]无有效数据可保存", module.name);
                return true;
            }

            // 4. 生成GZ压缩文件（格式：指标名_日期_频率.csv.gz）
            std::string filename = fmt::format("{}_{}_{}.csv.gz",
                                               module.name, date, module.frequency);
            fs::path file_path = base_path / filename;

            // 5. 写入GZ文件
            gzFile gz_file = gzopen(file_path.string().c_str(), "wb");
            if (!gz_file) {
                spdlog::error("无法创建GZ文件: {}", file_path.string());
                return false;
            }

            // 5.1 写入表头（bar_index + 所有股票代码）
            std::string header = "bar_index";
            for (const auto& stock_code : stock_list) {
                header += "," + stock_code;
            }
            header += "\n";
            gzwrite(gz_file, header.data(), header.size());

            // 5.2 按bar_index写入每行数据（确保所有bar_index连续）
            for (int ti = 0; ti <= max_bar_index; ++ti) {
                std::string line = std::to_string(ti);  // 行首为bar_index

                // 为每个股票填充对应bar的数据
                for (const auto& stock_code : stock_list) {
                    auto bar_it = bar_data.find(ti);
                    if (bar_it != bar_data.end()) {
                        auto stock_it = bar_it->second.find(stock_code);
                        if (stock_it != bar_it->second.end()) {
                            double value = stock_it->second;
                            if (std::isnan(value)) {
                                line += ",";  // NaN值留空
                            } else {
                                line += fmt::format(",{:.6f}", value);  // 有数据
                            }
                        } else {
                            line += ",";  // 无数据（留空）
                        }
                    } else {
                        line += ",";  // 该bar_index无任何数据
                    }
                }
                line += "\n";
                gzwrite(gz_file, line.data(), line.size());
            }

            // 6. 关闭文件并输出日志
            gzclose(gz_file);
            spdlog::info("指标[{}]数据保存成功：{}（{}个时间桶，{}只股票）",
                         module.name, file_path.string(), max_bar_index + 1, stock_list.size());
            return true;

        } catch (const std::exception& e) {
            spdlog::error("保存指标[{}]失败：{}", module.name, e.what());
            return false;
        }
    }

// 加载多日指标数据（核心实现）
    static bool load_multi_day_indicators(
            const std::shared_ptr<Indicator>& indicator,
            const ModuleConfig& module,
            const GlobalConfig& global_config
    ) {
        spdlog::info("load_multi_day_indicators 开始执行");
        const std::string& T_date = global_config.calculate_date;
        const int pre_days = global_config.pre_days;
        const std::string& universe = global_config.stock_universe;
        spdlog::info("参数: T_date={}, pre_days={}, universe={}", T_date, pre_days, universe);

        // 步骤1：读取T日股票列表
        spdlog::info("步骤1：读取T日股票列表");
        std::vector<std::string> T_stock_list = load_stock_list(universe, T_date);
        spdlog::info("T日股票列表大小: {}", T_stock_list.size());
        if (T_stock_list.empty()) {
            spdlog::error("指标{}T日[{}]股票列表为空，无法继续",indicator->name()
                          , T_date);
            return false;
        }

        // 步骤2：加载T日数据
        spdlog::info("步骤2：加载T日数据");
        bool T_exists = load_single_day_indicator(
            indicator, module, T_date, T_stock_list
        );
        if (T_exists) {
            spdlog::info("指标{}T日[{}]指标已存在，直接复用",indicator->name(), T_date);
            indicator->mark_as_calculated();  // 标记为已计算
            indicator->set_frequency(module.frequency);
        } else {
            spdlog::info("指标{}T日[{}]指标不存在，将在计算阶段生成",indicator->name(), T_date);
        }

        // 步骤3：加载历史指标（T-pre_days ~ T-1日）
        spdlog::info("步骤3：开始加载历史指标，pre_days={}", pre_days);
        for (int i = 1; i <= pre_days; ++i) {
            std::string hist_date = get_prev_date(T_date, i);
            spdlog::info("开始加载历史日期[{}]的指标数据", hist_date);
            std::vector<std::string> hist_stock_list = load_stock_list(universe, hist_date);
            std::unordered_map<std::string, GSeries> hist_raw_data;
            if (!load_historical_indicator_data(indicator, module, hist_date, hist_raw_data)) {
                spdlog::warn("历史日期[{}]指标数据不存在，跳过", hist_date);
                continue;
            }
            // 步骤4：重索引并存储到indicator->get_storage()
            for (const auto& stock : T_stock_list) {
                GSeries series;
                if (hist_raw_data.count(stock)) {
                    series = hist_raw_data.at(stock);
                } else {
                    // 不存在的股票用NAN填充
                    int bar_count = 0;
                    if (!hist_stock_list.empty() && hist_raw_data.count(hist_stock_list[0])) {
                        bar_count = hist_raw_data.at(hist_stock_list[0]).get_size();
                    }
                    for (int k = 0; k < bar_count; ++k) {
                        series.push(NAN);
                    }
                }
                auto holder_it = indicator->get_storage().find(stock);
                if (holder_it != indicator->get_storage().end() && holder_it->second) {
                    // 新的索引逻辑：i=1表示往前1日，i=2表示往前2日，...，i=pre_days表示往前pre_days日
                    holder_it->second->set_his_series(module.name, i, series);
                }
            }
            spdlog::info("历史日期[{}]指标重索引完成", hist_date);
        }
        return true;
    }


// 子函数1：加载单一日指标，直接写入indicator->get_storage()
static bool load_single_day_indicator(
        const std::shared_ptr<Indicator>& indicator,
        const ModuleConfig& module,
        const std::string& date,
        const std::vector<std::string>& T_stock_list
) {
    // 更新indicator的频率为配置文件中指定的频率
    indicator->set_storage_frequency(module.frequency);
    spdlog::info("更新指标[{}]频率为: {}", module.name, module.frequency);
    
    fs::path base_path = fs::path(module.path) / date / module.frequency;
    
    // 1. 尝试加载单文件（兼容现有Indicator）
    fs::path single_file = base_path / fmt::format("{}_{}_{}.csv.gz", 
                                                   module.name, date, module.frequency);
    if (fs::exists(single_file)) {
        spdlog::info("发现单文件指标：{}", single_file.string());
        return load_single_indicator_file(indicator, module, date, T_stock_list, single_file);
    }
    
    // 2. 扫描多文件（DiffIndicator等）
    std::string pattern = fmt::format("{}_*_{}_{}.csv.gz", 
                                     module.name, date, module.frequency);
    std::vector<fs::path> files = scan_indicator_files(base_path, pattern);
    
    if (!files.empty()) {
        spdlog::info("发现多文件指标，共{}个文件", files.size());
        return load_multiple_indicator_files(indicator, module, date, T_stock_list, files);
    }
    
    spdlog::warn("未找到指标文件：{}", base_path.string());
    return false;
}

// 辅助函数：扫描匹配的文件
static std::vector<fs::path> scan_indicator_files(
        const fs::path& dir, 
        const std::string& pattern
) {
    std::vector<fs::path> files;
    if (!fs::exists(dir)) {
        return files;
    }
    
    // 简单的通配符匹配：将*替换为实际的文件名部分
    std::string prefix = pattern.substr(0, pattern.find('*'));
    std::string suffix = pattern.substr(pattern.find('*') + 1);
    
    for (const auto& entry : fs::directory_iterator(dir)) {
        if (entry.is_regular_file()) {
            std::string filename = entry.path().filename().string();
            if (filename.size() >= prefix.size() + suffix.size() &&
                filename.substr(0, prefix.size()) == prefix &&
                filename.substr(filename.size() - suffix.size()) == suffix) {
                files.push_back(entry.path());
            }
        }
    }
    
    // 按文件名排序，确保加载顺序一致
    std::sort(files.begin(), files.end());
    return files;
}

// 辅助函数：加载单个指标文件
static bool load_single_indicator_file(
        const std::shared_ptr<Indicator>& indicator,
        const ModuleConfig& module,
        const std::string& date,
        const std::vector<std::string>& T_stock_list,
        const fs::path& file_path
) {
    // 解析文件到stock->GSeries
    std::unordered_map<std::string, GSeries> stock_series;
    if (!parse_indicator_gz_to_map(file_path, module.name, date, T_stock_list, stock_series)) {
        spdlog::error("解析T日[{}]指标文件失败", date);
        return false;
    }
    // 直接写入indicator的存储结构（T日数据）
    for (const auto& stock : T_stock_list) {
        auto holder_it = indicator->get_storage().find(stock);
        if (holder_it != indicator->get_storage().end() && holder_it->second) {
            // T日数据使用offline_set_m_bar方法存储到MBarSeries中
            holder_it->second->offline_set_m_bar(module.name, stock_series[stock]);
        }
    }
    return true;
}

// 辅助函数：加载多个指标文件（解析output_key并存储）
static bool load_multiple_indicator_files(
        const std::shared_ptr<Indicator>& indicator,
        const ModuleConfig& module,
        const std::string& date,
        const std::vector<std::string>& T_stock_list,
        const std::vector<fs::path>& files
) {
    for (const auto& file_path : files) {
        std::string filename = file_path.filename().string();
        
        // 解析文件名格式：{module.name}_{output_key}_{date}_{frequency}.csv.gz
        // 例如：DiffIndicator_volume_20240701_1min.csv.gz
        std::string prefix = fmt::format("{}_", module.name);
        std::string suffix = fmt::format("_{}_{}.csv.gz", date, module.frequency);
        
        if (filename.size() <= prefix.size() + suffix.size()) {
            spdlog::warn("文件名格式错误：{}", filename);
            continue;
        }
        
        std::string output_key = filename.substr(
            prefix.size(), 
            filename.size() - prefix.size() - suffix.size()
        );
        
        spdlog::info("加载多文件指标：{} -> output_key: {}", filename, output_key);
        
        // 解析文件到stock->GSeries
        std::unordered_map<std::string, GSeries> stock_series;
        if (!parse_indicator_gz_to_map(file_path, module.name, date, T_stock_list, stock_series)) {
            spdlog::error("解析T日[{}]指标文件失败：{}", date, filename);
            continue;
        }
        
        // 直接写入indicator的存储结构（T日数据），使用output_key作为存储键
        for (const auto& stock : T_stock_list) {
            auto holder_it = indicator->get_storage().find(stock);
            if (holder_it != indicator->get_storage().end() && holder_it->second) {
                // T日数据使用offline_set_m_bar方法存储到MBarSeries中，键名为output_key
                holder_it->second->offline_set_m_bar(output_key, stock_series[stock]);
            }
        }
    }
    
    return true;
}

// 保存单个因子模块的结果（从Factor自身的factor_storage读取数据）
//TODO：暂时取1min
static bool save_factor(
        const std::shared_ptr<Factor>& factor,  // 目标因子实例
        const ModuleConfig& module,
        const std::string& date,
        const std::vector<std::string>& stock_list  // 股票列表
) {
    try {
        // 1. 验证参数有效性
        if (!factor) {
            spdlog::error("保存失败：因子实例为空");
            return false;
        }
        if (module.handler != "Factor") {
            spdlog::error("模块[{}]不是Factor类型", module.name);
            return false;
        }
        if (module.path.empty() || module.name.empty()) {
            spdlog::error("模块[{}]路径或名称为空", module.name);
            return false;
        }

        // 2. 创建存储目录
        fs::path base_path = fs::path(module.path) / date / "1min";
        if (!fs::exists(base_path) && !fs::create_directories(base_path)) {
            spdlog::error("创建目录失败: {}", base_path.string());
            return false;
        }

        // 3. 从Factor的factor_storage中收集数据
        const auto& factor_storage = factor->get_storage();
        if (factor_storage.empty()) {
            spdlog::warn("因子[{}]的factor_storage为空，无数据可保存", module.name);
            return true;
        }

        // 4. 生成GZ压缩文件（格式：因子名_日期_5min.csv.gz）
        std::string filename = fmt::format("{}_{}_1min.csv.gz", module.name, date);
        fs::path file_path = base_path / filename;

        // 5. 写入GZ文件
        gzFile gz_file = gzopen(file_path.string().c_str(), "wb");
        if (!gz_file) {
            spdlog::error("无法创建GZ文件: {}", file_path.string());
            return false;
        }

        // 5.1 写入表头（bar_index + 所有股票代码）
        std::string header = "bar_index";
        // 使用传入的股票列表

        for (const auto& stock_code : stock_list) {
            header += "," + stock_code;
        }
        header += "\n";
        gzwrite(gz_file, header.data(), header.size());

        // 5.2 按bar_index写入每行数据
        for (const auto& [bar_index, factor_data] : factor_storage) {
            std::string line = std::to_string(bar_index);  // 行首为bar_index

            // 获取该时间桶的因子数据
            auto factor_it = factor_data.find(module.name);
            if (factor_it != factor_data.end()) {
                const GSeries& series = factor_it->second;
                
                // 检查series是否有效
                if (series.get_size() == 0) {
                    spdlog::warn("因子[{}]时间桶[{}]数据为空，填充空值", module.name, bar_index);
                    // 填充空值
                    for (size_t i = 0; i < stock_list.size(); ++i) {
                        line += ",";
                    }
                } else {
                    // 为每个股票填充对应bar的数据
                    for (int i = 0; i < series.get_size() && i < static_cast<int>(stock_list.size()); ++i) {
                        double value = series.get(i);
                        if (std::isnan(value)) {
                            line += ",";  // 无数据（留空）
                        } else {
                            line += fmt::format(",{:.6f}", value);  // 有数据
                        }
                    }
                    
                    // 如果series长度小于股票列表长度，补充空值
                    for (size_t i = series.get_size(); i < stock_list.size(); ++i) {
                        line += ",";
                    }
                }
            } else {
                // 该时间桶没有该因子的数据，填充空值
                for (size_t i = 0; i < stock_list.size(); ++i) {
                    line += ",";
                }
            }
            line += "\n";
            gzwrite(gz_file, line.data(), line.size());
        }

        // 6. 关闭文件并输出日志
        gzclose(gz_file);
        spdlog::info("因子[{}]数据保存成功：{}（{}个时间桶）",
                     module.name, file_path.string(), factor_storage.size());
        return true;

    } catch (const std::exception& e) {
        spdlog::error("保存因子[{}]失败：{}", module.name, e.what());
        return false;
    }
}

private:

    // 子函数2：加载历史指标原始数据（未重索引）
    static bool load_historical_indicator_data(
            const std::shared_ptr<Indicator>& indicator,
            const ModuleConfig& module,
            const std::string& hist_date,
            std::unordered_map<std::string, GSeries>& out_raw_data
    ) {
        fs::path file_path = fs::path(module.path) / hist_date / module.frequency /
                             fmt::format("{}_{}_{}.csv.gz", module.name, hist_date, module.frequency);
        if (!fs::exists(file_path)) {
            return false;
        }

        // 解析GZ文件为原始数据（股票->GSeries）
        gzFile gz_file = gzopen(file_path.string().c_str(), "rb");
        if (!gz_file) return false;

        // 读取表头（bar_index + 股票代码）
        std::vector<std::string> stock_list;
        std::string header;
        char buffer[1024];
        while (gzgets(gz_file, buffer, sizeof(buffer)) != nullptr) {
            header = buffer;
            break;
        }
        // 解析表头股票列表（跳过"bar_index"）
        split_header(header, stock_list);

        // 读取数据行并构建GSeries
        std::map<int, std::vector<double>> bar_data;  // bar_index -> 数值列表
        int max_bar = -1;
        while (gzgets(gz_file, buffer, sizeof(buffer)) != nullptr) {
            std::string line = buffer;
            auto [bar_idx, values] = parse_data_line(line, stock_list.size());
            bar_data[bar_idx] = values;
            max_bar = std::max(max_bar, bar_idx);
        }
        gzclose(gz_file);

        // 转换为股票->GSeries映射
        for (size_t i = 0; i < stock_list.size(); ++i) {
            const std::string& stock = stock_list[i];
            GSeries series;
            for (int bar = 0; bar <= max_bar; ++bar) {
                if (bar_data.count(bar) && i < bar_data[bar].size()) {
                    series.push(bar_data[bar][i]);  // 有效数据
                } else {
                    series.push(NAN);  // 空值填充
                }
            }
            out_raw_data[stock] = series;
        }

        return true;
    }

    // 子函数2：历史数据重索引（按T日股票列表对齐）
    static void reindex_historical_data(
            const std::unordered_map<std::string, GSeries>& hist_raw_data,  // 原始历史数据
            const std::vector<std::string>& T_stock_list,  // T日股票列表（目标）
            const std::vector<std::string>& hist_stock_list,  // 历史日股票列表（原始）
            const std::string& indicator_name,
            Frequency freq,
            BaseSeriesHolder& out_holder
    ) {
        // 遍历T日股票列表，填充数据
        for (const auto& T_stock : T_stock_list) {
            // 若历史数据包含该股票，直接复用；否则用空值填充
            if (hist_raw_data.count(T_stock)) {
                const GSeries& raw_series = hist_raw_data.at(T_stock);
                out_holder.set_his_series(indicator_name, 0, raw_series);  // 存储为历史日数据
            } else {
                // 创建空序列（长度与历史数据一致）
                GSeries empty_series;
                int bar_count = 0;
                if (!hist_stock_list.empty() && hist_raw_data.count(hist_stock_list[0])) {
                    bar_count = hist_raw_data.at(hist_stock_list[0]).get_size();  // 按历史数据长度对齐
                }
                for (int i = 0; i < bar_count; ++i) {
                    empty_series.push(NAN);  // 空值填充
                }
                out_holder.set_his_series(indicator_name, 0, empty_series);
            }
        }
    }

    // 辅助函数：解析表头股票列表
    static void split_header(const std::string& header, std::vector<std::string>& stock_list) {
        std::stringstream ss(header);
        std::string token;
        while (std::getline(ss, token, ',')) {
            if (token != "bar_index") {  // 跳过首列
                stock_list.push_back(token);
            }
        }
    }

    // 辅助函数：解析数据行
    static std::pair<int, std::vector<double>> parse_data_line(
            const std::string& line, size_t expected_size
    ) {
        std::stringstream ss(line);
        std::string token;
        int bar_idx = -1;
        std::vector<double> values;

        // 解析bar_index
        std::getline(ss, token, ',');
        if (!token.empty()) bar_idx = std::stoi(token);

        // 解析数值
        while (std::getline(ss, token, ',')) {
            if (token.empty() || token == "nan" || token == "NaN" || token == "NAN") {
                values.push_back(NAN);
            } else {
                try {
                    values.push_back(std::stod(token));
                } catch (const std::exception& e) {
                    values.push_back(NAN);  // 解析失败时设为NaN
                }
            }
        }

        // 补全缺失的空值
        while (values.size() < expected_size) {
            values.push_back(NAN);
        }

        return {bar_idx, values};
    }

    // 解析函数
    static bool parse_indicator_gz_to_map(
            const fs::path& file_path,
            const std::string& indicator_name,
            const std::string& date,
            const std::vector<std::string>& T_stock_list,
            std::unordered_map<std::string, GSeries>& stock_series
    ) {
        gzFile gz_file = gzopen(file_path.string().c_str(), "rb");
        if (!gz_file) {
            spdlog::error("无法打开GZ文件: {}", file_path.string());
            return false;
        }

        // 1. 解析表头（bar_index + 股票代码）
        std::vector<std::string> stock_list;
        char buffer[4096];
        if (gzgets(gz_file, buffer, sizeof(buffer)) == nullptr) {
            spdlog::error("GZ文件为空: {}", file_path.string());
            gzclose(gz_file);
            return false;
        }
        std::string header = buffer;
        // 去除可能的换行符
        if (!header.empty() && header.back() == '\n') {
            header.pop_back();
        }
        if (!header.empty() && header.back() == '\r') {
            header.pop_back();
        }
        std::stringstream ss(header);
        std::string token;
        while (std::getline(ss, token, ',')) {
            if (token != "bar_index") {  // 跳过首列
                stock_list.push_back(token);
            }
        }

        // 2. 解析数据行，构建GSeries（按股票维度）
        int max_bar_index = -1;

        // 3. 读取每行数据（bar_index -> 各股票数值），先找到最大bar_index
        while (gzgets(gz_file, buffer, sizeof(buffer)) != nullptr) {
            std::string line = buffer;
            std::stringstream line_ss(line);

            // 解析bar_index（首列）
            std::getline(line_ss, token, ',');
            if (token.empty()) continue;
            int bar_index = std::stoi(token);
            max_bar_index = std::max(max_bar_index, bar_index);
        }

        // 重新打开文件
        gzclose(gz_file);
        gz_file = gzopen(file_path.string().c_str(), "rb");
        if (!gz_file) {
            spdlog::error("无法重新打开GZ文件: {}", file_path.string());
            return false;
        }

        // 跳过表头
        gzgets(gz_file, buffer, sizeof(buffer));

        // 4. 初始化每个股票的GSeries（先调整到正确大小）
        for (const auto& stock : T_stock_list) {
            stock_series[stock] = GSeries(max_bar_index + 1);  // 直接创建正确大小的序列
        }

        // 5. 再次读取每行数据，设置值
        while (gzgets(gz_file, buffer, sizeof(buffer)) != nullptr) {
            std::string line = buffer;
            std::stringstream line_ss(line);

            // 解析bar_index（首列）
            std::getline(line_ss, token, ',');
            if (token.empty()) continue;
            int bar_index = std::stoi(token);

                        // 解析每个股票的数值
            size_t stock_idx = 0;
            while (std::getline(line_ss, token, ',')) {
                if (stock_idx >= stock_list.size()) break;  // 改为 > ：list和stock_idx不同
                const std::string& file_stock_code = stock_list[stock_idx];
                double value;
                if (token.empty() || token == "nan" || token == "NaN" || token == "NAN") {
                    value = NAN;
                } else {
                    try {
                        value = std::stod(token);
                    } catch (const std::exception& e) {
                        value = NAN;  // 解析失败时设为NaN
                    }
                }

                // 为当前股票的GSeries设置指定bar_index的值
                // 检查这个股票是否在T_stock_list中
                if (stock_series.count(file_stock_code)) {
                    GSeries& series = stock_series[file_stock_code];
                    series.set(bar_index, value);  // 现在序列大小正确，可以设置值
                }
                stock_idx++;
            }
        }

        gzclose(gz_file);

        return true;
    }

};


#endif //ALPHAFACTORFRAMEWORK_RESULT_STORAGE_H
