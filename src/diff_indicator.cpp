#include "diff_indicator.h"
#include "data_structures.h"
#include "cal_engine.h"  // 新增：包含完整的CalculationEngine定义
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
//    volume_config.output_key = fmt::format("{}.{}.{}", storage_frequency_str_, "volume", pre_days_);
    volume_config.getter = [](const TickData& tick) -> double { return tick.volume; };
    volume_config.description = "成交量差分";
    add_diff_field(volume_config);
    
    // 配置amount差分
    DiffFieldConfig amount_config;
    amount_config.field_name = "amount";
    amount_config.output_key = "amount";
//    amount_config.output_key = fmt::format("{}.{}.{}", storage_frequency_str_, "amount", pre_days_);
    amount_config.getter = [](const TickData& tick) -> double { return tick.total_value_traded; };
    amount_config.description = "成交额差分";
    add_diff_field(amount_config);
}

void DiffIndicator::add_diff_field(const DiffFieldConfig& config) {
    diff_fields_.push_back(config);
    
    // 方案2：不再需要cache和mutex，因为已经移除cache依赖
    
    spdlog::info("[DiffIndicator] 添加差分字段: {} ({})", config.field_name, config.description);
}

void DiffIndicator::Calculate(const SyncTickData& tick_data) {
    auto thread_id = std::this_thread::get_id();
    std::ostringstream oss;
    oss << thread_id;
    std::string thread_id_str = oss.str();

    spdlog::info("[DiffCalculate-Enter] symbol={} thread_id={}", tick_data.symbol, thread_id_str);

    // 修改：使用新的get_stock_bar_holder方法获取对应股票的BarSeriesHolder
    BarSeriesHolder* stock_holder = get_stock_bar_holder(tick_data.symbol);
    if (!stock_holder) {
        spdlog::warn("[DiffIndicator] 无法获取股票{}的BarSeriesHolder", tick_data.symbol);
        return;
    }

    // 为每个配置的字段计算差分
    for (const auto& field_config : diff_fields_) {
        const std::string& field_name = field_config.field_name;
        const std::string& output_key = field_config.output_key;
        
        // 获取当前字段的值
        double current_value = field_config.getter(tick_data.tick_data);
        
        // 计算差分
        double field_diff = calculate_field_diff(field_name, tick_data.symbol, 
                                               tick_data.tick_data.real_time, current_value);
        
        // 获取当前频率的时间桶索引（使用对应股票的BarSeriesHolder的内部索引）
        int time_bucket_index = stock_holder->get_idx(frequency_);
        if (time_bucket_index < 0) {
            spdlog::warn("[DiffIndicator] 频率{}的索引无效: {}", 
                         static_cast<int>(frequency_), time_bucket_index);
            continue;
        }
        
        // 获取当前时间桶的累积差值并累加新的差值
        double accumulated_diff = get_accumulated_diff_by_bucket(output_key, tick_data.symbol, time_bucket_index, stock_holder);
        double new_accumulated_diff = accumulated_diff + field_diff;
        
        // 使用新的store_result_to_stock方法存储累积差值到指定股票的BarSeriesHolder
        store_result_to_stock(output_key, new_accumulated_diff, tick_data.symbol);
        
        spdlog::debug("[DiffCalculate] symbol={} bucket={} {}_diff={} accumulated_diff={} -> new_accumulated_diff={} (thread_id={})", 
                     tick_data.symbol, time_bucket_index, output_key, field_diff, accumulated_diff, new_accumulated_diff, thread_id_str);
    }
}

double DiffIndicator::calculate_field_diff(const std::string& field_name, 
                                         const std::string& stock_code,
                                         uint64_t current_time,
                                         double current_value) {
    // 方案B：从简单的成员变量获取前一个tick的TotalValueTraded，完全移除cache依赖
    
    // 获取前一个tick的TotalValueTraded
    double prev_total = get_previous_tick_total_value(field_name, stock_code);
    
    // 计算真正的diff：当前TotalValueTraded - 前一个tick的TotalValueTraded
    double field_diff = current_value - prev_total;
    
    // 更新前一个tick的值，为下一个tick做准备
    prev_tick_values_[field_name][stock_code] = current_value;
    
    spdlog::debug("[DiffCalculate] symbol={} field={} current_value={} prev_total={} diff={}", 
                 stock_code, field_name, current_value, prev_total, field_diff);
    
    return field_diff;
}

BarSeriesHolder* DiffIndicator::get_field_bar_series_holder(const std::string& stock_code, const std::string& field_name) const {
    // 现在从current_bar_holder_获取，或者返回nullptr
    // 这个方法在新的架构中可能不再需要，因为数据直接从BarSeriesHolder获取
    return current_bar_holder_;
}

BarSeriesHolder* DiffIndicator::get_stock_bar_holder(const std::string& stock_code) const {
    // 实现获取指定股票BarSeriesHolder的虚函数
    if (!calculation_engine_) {
        spdlog::warn("[DiffIndicator] calculation_engine_为空，无法获取股票{}的BarSeriesHolder", stock_code);
        return nullptr;
    }
    
    // 从CalculationEngine获取指定股票的BarSeriesHolder
    BarSeriesHolder* stock_holder = calculation_engine_->get_stock_bar_holder(stock_code);
    if (!stock_holder) {
        spdlog::warn("[DiffIndicator] 无法从CalculationEngine获取股票{}的BarSeriesHolder", stock_code);
    }
    
    return stock_holder;
}

void DiffIndicator::set_calculation_engine(std::shared_ptr<CalculationEngine> engine) {
    calculation_engine_ = engine;
    spdlog::info("[DiffIndicator] 已设置CalculationEngine引用");
}

void DiffIndicator::reset_diff_storage() {
    // 方案B：清理前一个tick的值
    prev_tick_values_.clear();
    spdlog::info("[DiffIndicator] 已清理前一个tick的值");
}

bool DiffIndicator::save_results(const ModuleConfig& module, const std::string& date, const std::shared_ptr<CalculationEngine>& cal_engine) {
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

//        // 2. 检查是否需要聚合（内部15S -> 配置频率）
//        if (storage_frequency_str_ != "15S") {
//            spdlog::info("DiffIndicator内部15S数据聚合到{}频率进行存储", storage_frequency_str_);
//            return save_results_with_frequency(module, date, storage_frequency_str_);
//        }

        // 3. 如果配置就是15S，直接保存原始数据
        spdlog::info("DiffIndicator保存数据");
        fs::path base_path = fs::path(module.path) / date / storage_frequency_str_;
        if (!fs::exists(base_path) && !fs::create_directories(base_path)) {
            spdlog::error("创建目录失败: {}", base_path.string());
            return false;
        }

        // 4. 在新的架构中，数据存储在CalculationEngine的BarSeriesHolder中
        // 从CalculationEngine获取数据并保存
        if (!cal_engine) {
            spdlog::error("CalculationEngine为空，无法获取数据");
            return false;
        }

        // 获取所有股票的BarSeriesHolder
        auto all_bar_holders = cal_engine->get_all_bar_series_holders();
        if (all_bar_holders.empty()) {
            spdlog::warn("没有找到任何BarSeriesHolder，无数据可保存");
            return true;
        }

        // 为每个字段分别进行聚合和保存
        for (const auto& field_config : diff_fields_) {
            const std::string& output_key = field_config.output_key;

            // 收集聚合后的数据
            std::map<int, std::map<std::string, double>> aggregated_data;
            
            // 从CalculationEngine获取数据并处理
            for (const auto& [stock_code, holder_ptr] : all_bar_holders) {
                if (!holder_ptr) continue;
                
                const BarSeriesHolder* holder = holder_ptr.get();
                GSeries base_series = holder->get_m_bar(fmt::format("{}.{}.{}", storage_frequency_str_, output_key, pre_days_));
                
                // 保存所有时间桶的数据
                for (int i = 0; i < base_series.get_size(); ++i) {
                    double val = base_series.get(i);
                    if (!std::isnan(val)) {
                        aggregated_data[i][stock_code] = val;
                    }
                }
            }

            // 生成GZ压缩文件
            std::string filename = fmt::format("{}_{}_{}_{}.csv.gz",
                                               module.name, output_key, date, storage_frequency_str_);
            fs::path file_path = base_path / filename;

            // 写入GZ文件
            gzFile gz_file = gzopen(file_path.string().c_str(), "wb");
            if (!gz_file) {
                spdlog::error("无法创建GZ文件: {}", file_path.string());
                return false;
            }

            // 写入表头
            std::string header = "bar_index";
            for (const auto& [stock_code, _] : all_bar_holders) {
                header += "," + stock_code;
            }
            header += "\n";
            gzwrite(gz_file, header.data(), header.size());

            // 写入数据
            // 确定有多少个时间桶
            int max_time_bucket = -1;
            for (const auto& [ti, _] : aggregated_data) {
                if (ti > max_time_bucket) max_time_bucket = ti;
            }
            
            // 写入所有时间桶的数据
            for (int ti = 0; ti <= max_time_bucket; ++ti) {
                std::string line = std::to_string(ti);

                for (const auto& [stock_code, _] : all_bar_holders) {
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
        spdlog::error("保存指标[{}]失败：{}", module.name, e.what());
        return false;
    }
}

bool DiffIndicator::aggregate(const std::string& target_frequency,std::map<int, std::map<std::string, double>> &aggregated_data){
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
            return true;
        }

        // 获取聚合比率
        int ratio = get_aggregation_ratio(base_freq, target_frequency);
        int target_bars = get_target_bars_per_day(target_frequency);

        spdlog::info("开始聚合：{} -> {}，聚合比率: {}，目标桶数: {}", base_freq, target_frequency, ratio, target_bars);

        // Clear output parameter first
        aggregated_data.clear();

        // 在新的架构中，数据存储在CalculationEngine的BarSeriesHolder中
        // 这个方法现在需要从外部传入数据，或者暂时返回false
        spdlog::warn("DiffIndicator::aggregate: 新架构中需要从外部获取数据");
        return false;

    } catch (const std::exception& e) {
        spdlog::error("聚合失败：{}", e.what());
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

        // 在新的架构中，数据存储在CalculationEngine的BarSeriesHolder中
        // 这个方法现在需要从外部传入数据，或者暂时返回false
        spdlog::warn("DiffIndicator::aggregate: 新架构中需要从外部获取数据");
        return false;

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



double DiffIndicator::get_accumulated_diff_by_bucket(const std::string& field_name, 
                                                    const std::string& stock_code,
                                                    int time_bucket_index,
                                                    BarSeriesHolder* stock_holder) {
    // 从指定股票的BarSeriesHolder获取指定时间桶的累积差值
    if (!stock_holder) {
        spdlog::warn("[DiffIndicator] stock_holder为空，无法获取累积差值");
        return 0.0;
    }
    
    // 从BarSeriesHolder获取该时间桶的当前值
    std::string series_key = fmt::format("{}.{}.{}", storage_frequency_str_, field_name, pre_days_);
    GSeries series = stock_holder->get_m_bar(series_key);
    
    if (time_bucket_index >= series.get_size()) {
        spdlog::warn("[DiffIndicator] 时间桶索引超出范围: index={}, size={}", time_bucket_index, series.get_size());
        return 0.0;
    }
    
    double current_value = series.get(time_bucket_index);
    
    // 如果当前值不是NaN，返回它；否则返回0.0
    if (std::isnan(current_value)) {
        spdlog::debug("[DiffIndicator] 时间桶{}的累积差值为NaN，返回0.0", time_bucket_index);
        return 0.0;
    }
    
    spdlog::debug("[DiffIndicator] 通过索引获取时间桶{}的累积差值: {}", time_bucket_index, current_value);
    return current_value;
}

double DiffIndicator::get_previous_tick_total_value(const std::string& field_name, 
                                                   const std::string& stock_code) {
    // 方案B：从简单的成员变量获取前一个tick的TotalValueTraded
    
    auto field_it = prev_tick_values_.find(field_name);
    if (field_it != prev_tick_values_.end()) {
        auto stock_it = field_it->second.find(stock_code);
        if (stock_it != field_it->second.end()) {
            double prev_value = stock_it->second;
            spdlog::debug("[DiffIndicator] 获取前一个tick的TotalValueTraded: field={}, stock={}, value={}", 
                         field_name, stock_code, prev_value);
            return prev_value;
        }
    }
    
    // 如果没有找到前一个tick的值，返回0（表示这是第一个tick）
    spdlog::debug("[DiffIndicator] 没有找到前一个tick的TotalValueTraded: field={}, stock={}, 返回0", 
                 field_name, stock_code);
    return 0.0;
}