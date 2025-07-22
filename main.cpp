#include "Framework.h"
#include "config.h"
#include "spdlog/sinks/basic_file_sink.h"
#include <chrono>
#include <stdexcept>

using namespace std::chrono;

int main() {
    // 1. 初始化日志
    auto file_logger = spdlog::basic_logger_mt("framework_log", "framework.log", true);
    file_logger->set_pattern("[%Y-%m-%d %H:%M:%S.%e] [%n] [%l] %v");
    file_logger->set_level(spdlog::level::debug);
    file_logger->flush_on(spdlog::level::debug);
    spdlog::set_default_logger(file_logger);
    spdlog::set_level(spdlog::level::debug);
    spdlog::info("=== 启动高频Alpha因子框架 ===");

    try {
        // 2. 加载配置
        ConfigLoader config_loader;
        GlobalConfig config;
        if (!config_loader.load("config/config.xml", config)) {
            throw std::runtime_error("加载配置文件失败");
        }

        // 3. 初始化Framework
        Framework framework(config);
        framework.register_indicators_factors(config.modules);
        framework.load_all_indicators();

        // 4. 加载并排序行情数据
        DataLoader data_loader;
        std::vector<MarketAllField> all_tick_datas = framework.load_and_sort_market_data(data_loader);

        // 5. 运行引擎
        framework.run_engine(all_tick_datas);

        // 6. 保存结果
        framework.save_all_results();

        spdlog::info("结果保存完成");
    } catch (const std::exception& e) {
        spdlog::critical("程序异常终止: {}", e.what());
        return 1;
    }
    spdlog::info("=== 框架运行完成 ===");
    return 0;
}
