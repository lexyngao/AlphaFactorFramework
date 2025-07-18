#include "config.h"
#include "data_loader.h"
#include "cal_engine.h"
#include "result_storage.h"
#include "my_indicator.h"  // 包含VolumeIndicator声明
#include "spdlog/sinks/basic_file_sink.h"
#include <chrono>
#include <stdexcept>

using namespace std::chrono;

// 时间转换函数：纳秒时间戳转北京时间字符串
std::string convert_ns_to_beijing_time(uint64_t utc_ns) {
    // 北京时间 = UTC时间 + 8小时（28800秒）
    const uint64_t BEIJING_OFFSET_NS = 8ULL * 3600 * 1000000000;
    uint64_t beijing_ns = utc_ns;
    uint64_t beijing_sec = beijing_ns / 1000000000;
    uint64_t usec = (beijing_ns % 1000000000) / 1000;  // 微秒部分

    // 提取时分秒
    int hour = (beijing_sec / 3600) % 24;
    int minute = (beijing_sec % 3600) / 60;
    int second = beijing_sec % 60;

    // 格式化输出
    char buf[32];
    snprintf(buf, sizeof(buf), "%02d:%02d:%02d.%06llu",
             hour, minute, second, (unsigned long long)usec);
    return std::string(buf);
}


int main() {
    // 初始化日志
    auto file_logger = spdlog::basic_logger_mt("framework_log", "framework_15S.log", true);
    // 设置每条日志都立即刷新到文件（关闭缓冲区）
    file_logger->set_pattern("[%Y-%m-%d %H:%M:%S.%e] [%n] [%l] %v");  // 保留日志格式
    file_logger->set_level(spdlog::level::debug);
    file_logger->flush_on(spdlog::level::debug);  // 所有debug及以上级别日志立即刷新

    spdlog::set_default_logger(file_logger);
    spdlog::set_level(spdlog::level::debug);
    spdlog::info("=== 启动高频Alpha因子框架 ===");

    try {
        // 1. 加载配置文件
        ConfigLoader config_loader;
        GlobalConfig config;
        if (!config_loader.load("config/config.xml", config)) {
            throw std::runtime_error("加载配置文件失败");
        }

        // 2. 加载股票列表
        DataLoader data_loader;
        std::vector<std::string> stock_list = data_loader.get_stock_list_from_data();
        if (stock_list.empty()) {
            throw std::runtime_error("未在数据目录找到股票数据");
        }
        spdlog::info("加载股票列表完成，共{}只股票", stock_list.size());

        // 3. 初始化计算引擎
        CalculationEngine engine(config);
        // 4.注册Indicator指标(和Factor因子)
        auto volume_indicator = std::make_shared<VolumeIndicator>(); // 15S频率
        engine.add_indicator("volume", volume_indicator);
        spdlog::info("指标注册完成");
        engine.init_indicator_storage(stock_list);
        spdlog::info("计算引擎初始化完成");



        // 5. 加载并排序当日行情数据
        std::vector<MarketAllField> all_tick_datas;
        for (const auto& stock : stock_list) {
            std::vector<MarketAllField> stock_data = data_loader.load_stock_data_to_Market(stock, config.calculate_date);
            all_tick_datas.insert(all_tick_datas.end(), stock_data.begin(), stock_data.end());
        }
        data_loader.sort_market_datas(all_tick_datas);
        spdlog::info("加载当日行情数据完成，共{}条记录", all_tick_datas.size());

        // 6. 打印尾部N条数据校验
        int N = 5;
        if (all_tick_datas.size() >= N) {
            int start_idx = all_tick_datas.size() - N;
            spdlog::info("查看尾部{}条数据（索引{}到{}）：", N, start_idx, all_tick_datas.size()-1);
            for (int i = start_idx; i < all_tick_datas.size(); ++i) {
                const auto& data = all_tick_datas[i];
                spdlog::info("索引{}: 时间={}, 股票={}",
                             i,
                             convert_ns_to_beijing_time(data.timestamp),
                             data.symbol);
            }
        }

        // 7. 处理行情数据（触发指标计算）
        for (const auto& field : all_tick_datas) {
            engine.update(field);  // 统一分发到onOrder/onTrade/onTick
        }
        spdlog::info("当日行情数据处理完成");


        // 9. 验证指标计算结果
        // 验证300693.SZ的指标数据
        if (std::find(stock_list.begin(), stock_list.end(), "300693.SZ") != stock_list.end()) {
            // 从Indicator的storage_中获取holder
            BaseSeriesHolder* holder = volume_indicator->get_bar_series_holder("300693.SZ");
            if (holder) {
                // 读取指标数据（指标名应为VolumeIndicator的name_，而非硬编码）
                GSeries volume_series = holder->his_slice_bar("volume", 5);
                if (volume_series.is_valid(0)) {  // 检查索引有效性
                    double volume_930 = volume_series.get(0);
                    spdlog::info("300693.SZ 9:30-9:35 成交量: {}", volume_930);
                } else {
                    spdlog::warn("300693.SZ 在ti=0处无有效数据");
                }
            } else {
                spdlog::warn("300693.SZ 未在VolumeIndicator中找到存储");
            }
        }

        // 10. 保存计算结果
        // 9. 保存结果（按每个模块配置处理，严格匹配ModuleConfig）
        for (const auto& module : config.modules) {
            spdlog::info("开始保存模块[{}]的计算结果（类型：{}）", module.name, module.handler);

            if (module.handler == "Indicator") {
                // 保存指标结果：使用模块配置中的路径和名称
                if (!ResultStorage::save_indicator(volume_indicator, module, config.calculate_date)) {
                    spdlog::error("模块[{}]指标结果保存失败", module.name);
                } else {
                    spdlog::info("模块[{}]指标结果已保存至: {}", module.name, module.path);
                }
            }
//            else if (module.handler == "Factor") {
//                // 保存因子结果：使用模块配置中的路径和名称
//                if (!ResultStorage::save_factor(engine, module, config.calculate_date)) {
//                    spdlog::error("模块[{}]因子结果保存失败", module.name);
//                } else {
//                    spdlog::info("模块[{}]因子结果已保存至: {}", module.name, module.path);
//                }
//            }
            else {
                spdlog::warn("忽略未知模块类型: {}", module.handler);
            }
        }

        spdlog::info("结果保存完成");

    } catch (const std::exception& e) {
        spdlog::critical("程序异常终止: {}", e.what());
        return 1;
    }

    spdlog::info("=== 框架运行完成 ===");
    return 0;
}
