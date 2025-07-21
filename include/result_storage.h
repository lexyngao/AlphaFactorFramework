//
// Created by lexyn on 25-7-14.
//

#ifndef ALPHAFACTORFRAMEWORK_RESULT_STORAGE_H
#define ALPHAFACTORFRAMEWORK_RESULT_STORAGE_H

#include "data_structures.h"
#include "cal_engine.h"
#include "config.h"
#include <fstream>
#include <zlib.h>
#include "utils.h"

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

            // 2. 创建存储目录
            fs::path base_path = fs::path(module.path) / date / module.frequency;
            if (!fs::exists(base_path) && !fs::create_directories(base_path)) {
                spdlog::error("创建目录失败: {}", base_path.string());
                return false;
            }

            // 3. 从Indicator的storage_中收集数据（核心修改）
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
                if (!holder_ptr) continue;
                const BaseSeriesHolder* holder = holder_ptr.get();
                GSeries series = holder->his_slice_bar(module.name, 5); // T日索引
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
                            line += fmt::format(",{:.6f}", stock_it->second);  // 有数据
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
        const std::string& T_date = global_config.calculate_date;
        const int pre_days = global_config.pre_days;
        const std::string& universe = global_config.stock_universe;

        // 步骤1：读取T日股票列表
        std::vector<std::string> T_stock_list = load_stock_list(universe, T_date);
        if (T_stock_list.empty()) {
            spdlog::error("T日[{}]股票列表为空，无法继续", T_date);
            return false;
        }

        // 步骤2：加载T日数据
        bool T_exists = load_single_day_indicator(
            indicator, module, T_date, T_stock_list
        );
        if (T_exists) {
            spdlog::info("T日[{}]指标已存在，直接复用", T_date);
        } else {
            spdlog::info("T日[{}]指标不存在，将在计算阶段生成", T_date);
        }

        // 步骤3：加载历史指标（T-pre_days ~ T-1日）
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
                    holder_it->second->set_his_series(module.name, pre_days - i, series);
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
    fs::path file_path = fs::path(module.path) / date / module.frequency /
                         fmt::format("{}_{}_{}.csv.gz", module.name, date, module.frequency);
    if (!fs::exists(file_path)) {
        return false;
    }
    // 解析文件到stock->GSeries
    std::unordered_map<std::string, GSeries> stock_series;
    if (!parse_indicator_gz_to_map(file_path, module.name, date, T_stock_list, stock_series)) {
        spdlog::error("解析T日[{}]指标文件失败", date);
        return false;
    }
    // 直接写入indicator的存储结构
    for (const auto& stock : T_stock_list) {
        auto holder_it = indicator->get_storage().find(stock);
        if (holder_it != indicator->get_storage().end() && holder_it->second) {
            holder_it->second->set_his_series(module.name, 5, stock_series[stock]);
        }
    }
    return true;
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
                    bar_count = hist_raw_data.at(hist_stock_list[0]).size;  // 按历史数据长度对齐
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
            if (token.empty()) {
                values.push_back(NAN);
            } else {
                values.push_back(std::stod(token));
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
        std::stringstream ss(header);
        std::string token;
        while (std::getline(ss, token, ',')) {
            if (token != "bar_index") {  // 跳过首列
                stock_list.push_back(token);
            }
        }

        // 2. 解析数据行，构建GSeries（按股票维度）
        int max_bar_index = -1;

        // 初始化每个股票的GSeries（先预留空间）
        for (const auto& stock : stock_list) {
            stock_series[stock] = GSeries();
        }

        // 3. 读取每行数据（bar_index -> 各股票数值）
        while (gzgets(gz_file, buffer, sizeof(buffer)) != nullptr) {
            std::string line = buffer;
            std::stringstream line_ss(line);
            std::vector<std::string> values;

            // 解析bar_index（首列）
            std::getline(line_ss, token, ',');
            if (token.empty()) continue;
            int bar_index = std::stoi(token);
            max_bar_index = std::max(max_bar_index, bar_index);

            // 解析每个股票的数值
            size_t stock_idx = 0;
            while (std::getline(line_ss, token, ',')) {
                if (stock_idx >= stock_list.size()) break;
                const std::string& stock_code = stock_list[stock_idx];
                double value = token.empty() ? NAN : std::stod(token);

                // 为当前股票的GSeries设置指定bar_index的值
                GSeries& series = stock_series[stock_code];
                series.set(bar_index, value);  // 使用set方法（而非push）设置指定索引的值
                stock_idx++;
            }
        }

        gzclose(gz_file);

        // 4. 调整所有GSeries的大小（确保长度一致），并通过set_his_series存入BaseSeriesHolder
        for (auto& [stock_code, series] : stock_series) {
            if (max_bar_index >= 0) {
                series.resize(max_bar_index + 1);  // 确保序列长度覆盖所有bar_index
            }
            // 调用公有方法set_his_series，避免访问私有成员HisBarSeries
            // 假设his_day_index=5对应历史数据（根据实际逻辑调整）
            // out_holder.set_his_series(indicator_name, 5, series); // This line is removed as per new_code
        }

        return true;
    }
};


#endif //ALPHAFACTORFRAMEWORK_RESULT_STORAGE_H
