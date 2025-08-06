#include "diff_indicator.h"
#include "data_structures.h"
#include <thread>
#include <sstream>
#include <fstream>
#include <zlib.h>
#include "spdlog/fmt/fmt.h"
#include <filesystem>
#include "spdlog/spdlog.h"

namespace fs = std::filesystem;

void DiffIndicator::setup_default_fields() {
    // 配置volume差分
    DiffFieldConfig volume_config;
    volume_config.field_name = "volume";
    volume_config.output_key = "volume";
    volume_config.getter = [](const TickData& tick) -> double { return tick.volume; };
    volume_config.description = "成交量差分";
    add_diff_field(volume_config);
    
    // 配置amount差分
    DiffFieldConfig amount_config;
    amount_config.field_name = "amount";
    amount_config.output_key = "amount";
    amount_config.getter = [](const TickData& tick) -> double { return tick.total_value_traded; };
    amount_config.description = "成交额差分";
    add_diff_field(amount_config);
}

void DiffIndicator::add_diff_field(const DiffFieldConfig& config) {
    diff_fields_.push_back(config);
    
    // 为每个字段初始化互斥锁
    if (cache_mutexes_.find(config.field_name) == cache_mutexes_.end()) {
        cache_mutexes_.emplace(config.field_name, std::make_unique<std::mutex>());
    }
    
    spdlog::info("[DiffIndicator] 添加差分字段: {} ({})", config.field_name, config.description);
}

void DiffIndicator::Calculate(const SyncTickData& tick_data) {
    auto thread_id = std::this_thread::get_id();
    std::ostringstream oss;
    oss << thread_id;
    std::string thread_id_str = oss.str();

    spdlog::info("[DiffCalculate-Enter] symbol={} thread_id={}", tick_data.symbol, thread_id_str);

    auto it = storage_.find(tick_data.symbol);
    if (it == storage_.end()) {
        spdlog::warn("[DiffCalculate] symbol={} not found in storage_ (thread_id={})", tick_data.symbol, thread_id_str);
        return;
    }
    BarSeriesHolder* holder = it->second.get();

    int ti = get_time_bucket_index(tick_data.tick_data.real_time);
    spdlog::debug("[DiffCalculate] symbol={} real_time={} ti={} (thread_id={})", 
                 tick_data.symbol, tick_data.tick_data.real_time, ti, thread_id_str);
    
    if (ti < 0) {
        spdlog::debug("[DiffCalculate] symbol={} invalid ti (thread_id={}) real_time={}", 
                     tick_data.symbol, thread_id_str, tick_data.tick_data.real_time);
        return;
    }
    
    int bar_index = ti;

    // 为每个配置的字段计算差分
    for (const auto& field_config : diff_fields_) {
        const std::string& field_name = field_config.field_name;
        const std::string& output_key = field_config.output_key;
        
        // 获取当前字段的值
        double current_value = field_config.getter(tick_data.tick_data);
        
        // 计算差分
        double field_diff = calculate_field_diff(field_name, tick_data.symbol, 
                                               tick_data.tick_data.real_time, current_value);
        
        // 获取或创建GSeries
        GSeries series = holder->get_m_bar(output_key);
        if (series.empty()) {
            series = GSeries();
            series.resize(get_bars_per_day());
            spdlog::debug("[DiffCalculate] symbol={} new {} GSeries allocated (thread_id={})", 
                         tick_data.symbol, output_key, thread_id_str);
        }
        
        // 在时间桶内累加差分值
        double existing_value = series.get(bar_index);
        if (!std::isnan(existing_value)) {
            field_diff += existing_value;
            spdlog::debug("[DiffCalculate] symbol={} accumulated {}: {} + {} = {}", 
                         tick_data.symbol, output_key, existing_value, field_diff - existing_value, field_diff);
        } else {
            spdlog::debug("[DiffCalculate] symbol={} first valid {} in bucket: {}", 
                         tick_data.symbol, output_key, field_diff);
        }
        
        // 保存结果
        series.set(bar_index, field_diff);
        holder->offline_set_m_bar(output_key, series);
        
        spdlog::debug("[DiffCalculate] symbol={} ti={} bar_index={} {}_diff={} (thread_id={})", 
                     tick_data.symbol, ti, bar_index, output_key, field_diff, thread_id_str);
        
        // 输出时间桶信息
        log_time_bucket_info(tick_data.symbol, bar_index, field_diff);
    }
}

double DiffIndicator::calculate_field_diff(const std::string& field_name, 
                                         const std::string& stock_code,
                                         uint64_t current_time,
                                         double current_value) {
    std::lock_guard<std::mutex> lock(*cache_mutexes_[field_name]);
    
    // 获取该股票该字段的时间序列缓存
    auto& stock_series = time_series_caches_[field_name][stock_code];
    
    // 查找前一个时间戳的累积值
    double prev_value = 0.0;
    auto it = stock_series.lower_bound(current_time);
    if (it != stock_series.begin()) {
        --it;  // 前一个时间戳
        prev_value = it->second;
        spdlog::debug("[DiffCalculate] symbol={} field={} found prev_value={} at time={}", 
                     stock_code, field_name, prev_value, it->first);
    } else {
        spdlog::debug("[DiffCalculate] symbol={} field={} first tick, setting prev_value=0", 
                     stock_code, field_name);
    }
    
    // 计算差分
    double field_diff = current_value - prev_value;
    
    // 存储当前时间戳的累积值
    stock_series[current_time] = current_value;
    
    spdlog::debug("[DiffCalculate] symbol={} field={} time={} diff: {} - {} = {}", 
                 stock_code, field_name, current_time, current_value, prev_value, field_diff);
    
    return field_diff;
}

BarSeriesHolder* DiffIndicator::get_field_bar_series_holder(const std::string& stock_code, const std::string& field_name) const {
    auto it = storage_.find(stock_code);
    if (it != storage_.end()) {
        return it->second.get();
    }
    return nullptr;
}

void DiffIndicator::reset_diff_storage() {
    for (const auto& field_config : diff_fields_) {
        const std::string& field_name = field_config.field_name;
        std::lock_guard<std::mutex> lock(*cache_mutexes_[field_name]);
        time_series_caches_[field_name].clear();
        spdlog::info("[DiffIndicator] 重置{}时间序列缓存", field_name);
    }
}

bool DiffIndicator::save_results(const ModuleConfig& module, const std::string& date) {
    try {
        // 1. 验证参数有效性
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

        // 3. 从Indicator的storage_中收集数据
        const auto& indicator_storage = get_storage();
        if (indicator_storage.empty()) {
            spdlog::warn("指标[{}]的storage_为空，无数据可保存", module.name);
            return true;
        }

        // 提取指标实际处理过的股票列表
        std::vector<std::string> stock_list;
        for (const auto& [stock_code, _] : indicator_storage) {
            stock_list.push_back(stock_code);
        }

        // 4. 为每个配置的字段分别保存数据
        for (const auto& field_config : diff_fields_) {
            const std::string& field_name = field_config.field_name;
            const std::string& output_key = field_config.output_key;
            
            // 按bar_index分组收集数据
            std::map<int, std::map<std::string, double>> bar_data;
            int max_bar_index = -1;

            int bars_per_day = get_bars_per_day();
            for (const auto& [stock_code, holder_ptr] : indicator_storage) {
                if (!holder_ptr) continue;
                const BarSeriesHolder* holder = holder_ptr.get();
                GSeries series = holder->get_m_bar(output_key);
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
                spdlog::warn("指标[{}]的{}数据为空，跳过保存", module.name, output_key);
                continue;
            }

            // 5. 生成GZ压缩文件
            std::string filename = fmt::format("{}_{}_{}_{}.csv.gz",
                                               module.name, output_key, date, module.frequency);
            fs::path file_path = base_path / filename;

            // 6. 写入GZ文件
            gzFile gz_file = gzopen(file_path.string().c_str(), "wb");
            if (!gz_file) {
                spdlog::error("无法创建GZ文件: {}", file_path.string());
                return false;
            }

            // 6.1 写入表头
            std::string header = "bar_index";
            for (const auto& stock_code : stock_list) {
                header += "," + stock_code;
            }
            header += "\n";
            gzwrite(gz_file, header.data(), header.size());

            // 6.2 按bar_index写入每行数据
            for (int ti = 0; ti <= max_bar_index; ++ti) {
                std::string line = std::to_string(ti);

                for (const auto& stock_code : stock_list) {
                    auto bar_it = bar_data.find(ti);
                    if (bar_it != bar_data.end()) {
                        auto stock_it = bar_it->second.find(stock_code);
                        if (stock_it != bar_it->second.end()) {
                            double value = stock_it->second;
                            if (std::isnan(value)) {
                                line += ",";
                            } else {
                                line += fmt::format(",{:.6f}", value);
                            }
                        } else {
                            line += ",";
                        }
                    } else {
                        line += ",";
                    }
                }
                line += "\n";
                gzwrite(gz_file, line.data(), line.size());
            }

            gzclose(gz_file);
            spdlog::info("指标[{}]的{}数据保存成功：{}（{}个时间桶，{}只股票）",
                         module.name, output_key, file_path.string(), max_bar_index + 1, stock_list.size());
        }

        return true;

    } catch (const std::exception& e) {
        spdlog::error("保存指标[{}]失败：{}", module.name, e.what());
        return false;
    }
} 