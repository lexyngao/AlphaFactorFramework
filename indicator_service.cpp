#include "Framework.h"
#include "config.h"
#include "spdlog/sinks/basic_file_sink.h"
#include <chrono>
#include <stdexcept>

using namespace std::chrono;

int main() {
    // 1. 初始化日志
    auto file_logger = spdlog::basic_logger_mt("indicator_service", "indicator_service.log", true);
    file_logger->set_pattern("[%Y-%m-%d %H:%M:%S.%e] [%n] [%l] %v");
    file_logger->set_level(spdlog::level::debug);
    file_logger->flush_on(spdlog::level::debug);
    spdlog::set_default_logger(file_logger);
    spdlog::set_level(spdlog::level::debug);
    spdlog::info("=== 启动Indicator计算服务 ===");

    try {
        // 2. 加载配置
        ConfigLoader config_loader;
        GlobalConfig config;
        if (!config_loader.load("config/config.xml", config)) {
            throw std::runtime_error("加载配置文件失败");
        }

        // 3. 初始化Framework（只注册Indicator）
        Framework framework(config);
        
        // 只注册Indicator模块，不注册Factor
        std::vector<ModuleConfig> indicator_modules;
        for (const auto& module : config.modules) {
            if (module.handler == "Indicator") {
                indicator_modules.push_back(module);
            }
        }
        
        framework.register_indicators_factors(indicator_modules);
        framework.load_all_indicators();

        // 4. 加载并排序行情数据
        DataLoader data_loader;
        std::vector<MarketAllField> all_tick_datas = framework.load_and_sort_market_data(data_loader);

        // 5. 运行Indicator计算引擎（只处理行情数据，不处理时间事件）
        spdlog::info("开始运行Indicator计算引擎，数据量: {}", all_tick_datas.size());
        
        // 重置所有指标的计算状态和差分存储
        framework.get_engine().reset_diff_storage();

        // 按股票分组处理行情数据
        std::unordered_map<std::string, std::vector<MarketAllField>> stock_data_map;
        for (const auto& data : all_tick_datas) {
            stock_data_map[data.symbol].push_back(data);
        }
        
        spdlog::info("数据分组完成，共{}只股票", stock_data_map.size());
        
        // 启动Indicator线程组（按股票）
        std::vector<std::thread> indicator_threads;
        for (const auto& [stock, stock_data] : stock_data_map) {
            indicator_threads.emplace_back([&framework, stock_code = stock, data = stock_data]() {
                spdlog::info("开始处理股票{}的行情数据，共{}条", stock_code, data.size());
                for (const auto& tick_data : data) {
                    framework.get_engine().update(tick_data);
                }
                spdlog::info("股票{}行情数据处理完成", stock_code);
            });
        }

        // 等待所有Indicator线程完成
        spdlog::info("等待所有Indicator线程完成...");
        for (auto& thread : indicator_threads) {
            thread.join();
        }

        // 6. 保存Indicator结果
        spdlog::info("开始保存Indicator结果...");
        framework.save_all_results();

        spdlog::info("=== Indicator计算服务运行完成 ===");
        
    } catch (const std::exception& e) {
        spdlog::critical("Indicator服务异常终止: {}", e.what());
        return 1;
    }
    
    return 0;
}
