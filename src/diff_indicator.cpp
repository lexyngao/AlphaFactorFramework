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

        // 2. 检查是否需要聚合（内部15S -> 配置频率）
        if (storage_frequency_str_ != "15S") {
            spdlog::info("DiffIndicator内部15S数据聚合到{}频率进行存储", storage_frequency_str_);
            return save_results_with_frequency(module, date, storage_frequency_str_);
        }

        // 3. 如果配置就是15S，直接保存原始数据
        spdlog::info("DiffIndicator保存数据");
        fs::path base_path = fs::path(module.path) / date / storage_frequency_str_;
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
                if (!holder_ptr) {
                    spdlog::warn("DiffIndicator[{}]的股票[{}]holder为空，跳过", module.name, stock_code);
                    continue;
                }
                
                const BarSeriesHolder* holder = holder_ptr.get();
                if (!holder) {
                    spdlog::warn("DiffIndicator[{}]的股票[{}]holder指针为空，跳过", module.name, stock_code);
                    continue;
                }
                
                GSeries series = holder->get_m_bar(output_key);
                
                // 检查series是否有效
                if (series.get_size() == 0) {
                    spdlog::warn("DiffIndicator[{}]的股票[{}]键[{}]数据为空，跳过", module.name, stock_code, output_key);
                    continue;
                }
                
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
                                               module.name, output_key, date, storage_frequency_str_);
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

bool DiffIndicator::save_results_with_frequency(const ModuleConfig& module, const std::string& date, const std::string& target_frequency) {
    try {
        // 如果目标频率与基础频率相同，直接调用原始保存方法
        std::string base_freq = "15S";
        switch (get_frequency()) {
            case Frequency::F15S: base_freq = "15S"; break;
            case Frequency::F1MIN: base_freq = "1min"; break;
            case Frequency::F5MIN: base_freq = "5min"; break;
            case Frequency::F30MIN: base_freq = "30min"; break;
        }

        if (base_freq == target_frequency) {
            return save_results(module, date);
        }
        
        // 创建存储目录
        fs::path base_path = fs::path(module.path) / date / target_frequency;
        if (!fs::exists(base_path) && !fs::create_directories(base_path)) {
            spdlog::error("创建目录失败: {}", base_path.string());
            return false;
        }

        // 获取聚合比率
        int ratio = get_aggregation_ratio(base_freq, target_frequency);
        int target_bars = get_target_bars_per_day(target_frequency);
        
        spdlog::info("开始聚合：{} -> {}，聚合比率: {}，目标桶数: {}", base_freq, target_frequency, ratio, target_bars);

        // 提取股票列表
        const auto& indicator_storage = get_storage();
        std::vector<std::string> stock_list;
        for (const auto& [stock_code, _] : indicator_storage) {
            stock_list.push_back(stock_code);
        }

        // 为每个字段分别进行聚合和保存
        for (const auto& field_config : diff_fields_) {
            const std::string& output_key = field_config.output_key;
            
            // 收集聚合后的数据
            std::map<int, std::map<std::string, double>> aggregated_data;
            
            for (const auto& [stock_code, holder_ptr] : indicator_storage) {
                if (!holder_ptr) continue;
                
                const BarSeriesHolder* holder = holder_ptr.get();
                GSeries base_series = holder->get_m_bar(output_key);
                GSeries output_series(target_bars);
                
                // 处理上午时段：bucket 0-479 -> 上午桶
                int morning_base_buckets = 120 * 4;  // 480个15S桶
                int morning_target_buckets;
                if (target_frequency == "1min") morning_target_buckets = 120;
                else if (target_frequency == "5min") morning_target_buckets = 24;
                else if (target_frequency == "30min") morning_target_buckets = 4;
                
                aggregate_time_segment(base_series, output_series, 0, morning_base_buckets - 1, ratio, 0);
                
                // 处理下午时段：bucket 480-947 -> 下午桶
                int afternoon_base_start = 480;
                int afternoon_base_end = 947;
                aggregate_time_segment(base_series, output_series, afternoon_base_start, afternoon_base_end, ratio, morning_target_buckets);
                
                // 将聚合结果存储到数据结构中
                for (int ti = 0; ti < target_bars; ++ti) {
                    double value = output_series.get(ti);
                    aggregated_data[ti][stock_code] = value;
                }
            }

            // 生成GZ压缩文件
            std::string filename = fmt::format("{}_{}_{}_{}.csv.gz",
                                               module.name, output_key, date, target_frequency);
            fs::path file_path = base_path / filename;

            // 写入GZ文件
            gzFile gz_file = gzopen(file_path.string().c_str(), "wb");
            if (!gz_file) {
                spdlog::error("无法创建GZ文件: {}", file_path.string());
                return false;
            }

            // 写入表头
            std::string header = "bar_index";
            for (const auto& stock_code : stock_list) {
                header += "," + stock_code;
            }
            header += "\n";
            gzwrite(gz_file, header.data(), header.size());

            // 写入数据
            for (int ti = 0; ti < target_bars; ++ti) {
                std::string line = std::to_string(ti);


                for (const auto& stock_code : stock_list) {
                    auto bar_it = aggregated_data.find(ti);
                    if (bar_it != aggregated_data.end()) {
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
            spdlog::info("聚合数据保存成功：{}", file_path.string());
        }

        return true;

    } catch (const std::exception& e) {
        spdlog::error("聚合保存失败：{}", e.what());
        return false;
    }
}

int DiffIndicator::get_aggregation_ratio(const std::string& from_freq, const std::string& to_freq) {
    if (from_freq == "15S" && to_freq == "1min") return 4;
    if (from_freq == "15S" && to_freq == "5min") return 20;
    if (from_freq == "15S" && to_freq == "30min") return 120;
    if (from_freq == "1min" && to_freq == "5min") return 5;
    if (from_freq == "1min" && to_freq == "30min") return 30;
    if (from_freq == "5min" && to_freq == "30min") return 6;
    return 1;
}

int DiffIndicator::get_target_bars_per_day(const std::string& frequency) {
    if (frequency == "15S") return 948;
    if (frequency == "1min") return 237;
    if (frequency == "5min") return 48;
    if (frequency == "30min") return 8;
    return 237;  // 默认1min
}

void DiffIndicator::aggregate_time_segment(const GSeries& base_series, GSeries& output_series, 
                                         int base_start, int base_end, int ratio, int output_start) {
    int segment_length = base_end - base_start + 1;
    int output_buckets = segment_length / ratio;
    
    // 处理完整的聚合桶
    for (int i = 0; i < output_buckets; i++) {
        double sum = 0.0;
        int valid_count = 0;
        
        for (int j = 0; j < ratio; j++) {
            int base_idx = base_start + i * ratio + j;
            if (base_idx <= base_end && base_idx < base_series.get_size()) {
                double value = base_series.get(base_idx);
                if (!std::isnan(value)) {
                    sum += value;
                    valid_count++;
                }
            }
        }
        
        if (valid_count > 0 && (output_start + i) < output_series.get_size()) {
            output_series.set(output_start + i, sum);
        }
    }
    
    // 处理最后一个不完整的桶（如果有剩余数据）
    int remaining_length = segment_length % ratio;
    if (remaining_length > 0) {
        int last_bucket_start = base_start + output_buckets * ratio;
        double sum = 0.0;
        int valid_count = 0;
        
        for (int j = 0; j < remaining_length; j++) {
            int base_idx = last_bucket_start + j;
            if (base_idx <= base_end && base_idx < base_series.get_size()) {
                double value = base_series.get(base_idx);
                if (!std::isnan(value)) {
                    sum += value;
                    valid_count++;
                }
            }
        }
        
        if (valid_count > 0 && (output_start + output_buckets) < output_series.get_size()) {
            output_series.set(output_start + output_buckets, sum);
            spdlog::debug("处理不完整桶: 输出位置={}, 有效数据={}, 总和={}", 
                         output_start + output_buckets, valid_count, sum);
        }
    }
} 