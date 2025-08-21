#include "Framework.h"
#include "config.h"
#include "spdlog/sinks/basic_file_sink.h"
#include <chrono>
#include <stdexcept>

using namespace std::chrono;

int main() {
    // 1. 初始化日志
    auto file_logger = spdlog::basic_logger_mt("factor_service", "factor_service.log", true);
    file_logger->set_pattern("[%Y-%m-%d %H:%M:%S.%e] [%n] [%l] %v");
    file_logger->set_level(spdlog::level::debug);
    file_logger->flush_on(spdlog::level::debug);
    spdlog::set_default_logger(file_logger);
    spdlog::set_level(spdlog::level::debug);
    spdlog::info("=== 启动Factor计算服务 ===");

    try {
        // 2. 加载配置
        ConfigLoader config_loader;
        GlobalConfig config;
        if (!config_loader.load("config/config.xml", config)) {
            throw std::runtime_error("加载配置文件失败");
        }

        // 3. 初始化Framework（只注册Factor）
        Framework framework(config);
        
        // 只注册Factor模块，不注册Indicator
        std::vector<ModuleConfig> factor_modules;
        for (const auto& module : config.modules) {
            if (module.handler == "Factor") {
                factor_modules.push_back(module);
            }
        }
        
        framework.register_indicators_factors(factor_modules);
        
        // 4. 加载已保存的Indicator数据（从文件存储中读取）
        spdlog::info("开始加载已保存的Indicator数据...");
        for (const auto& module : config.modules) {
            if (module.handler == "Indicator") {
                spdlog::info("加载指标模块: {}", module.name);
                // 这里需要从文件存储中加载indicator数据
                // 由于Framework原本设计是同时管理indicator和factor，
                // 我们需要创建一个临时的indicator实例来加载数据
                std::shared_ptr<Indicator> temp_indicator;
                
                if (module.id == "VolumeIndicator") {
                    temp_indicator = std::make_shared<VolumeIndicator>(module);
                } else if (module.id == "AmountIndicator") {
                    temp_indicator = std::make_shared<AmountIndicator>(module);
                } else if (module.id == "DiffIndicator") {
                    temp_indicator = std::make_shared<DiffIndicator>(module);
                } else {
                    spdlog::warn("未知的Indicator类型: {}, 跳过", module.id);
                    continue;
                }
                
                // 加载历史数据
                ResultStorage::load_multi_day_indicators(temp_indicator, module, config);
                
                // 将加载的数据注入到engine中，供factor使用
                framework.get_engine().add_indicator(module.name, temp_indicator);
            }
        }
        
        // 5. 设置factor依赖关系
        framework.setup_factor_dependencies();

        // 6. 生成时间事件并运行Factor计算引擎
        spdlog::info("开始运行Factor计算引擎...");
        
        // 生成时间事件（不插入数据流）
        std::vector<uint64_t> time_points = framework.generate_time_points(60, config.calculate_date);
        spdlog::info("生成了 {} 个时间事件", time_points.size());
        
        // 运行Factor计算引擎
        framework.get_engine().process_factor_time_events(time_points);
        
        // 7. 保存Factor结果
        spdlog::info("开始保存Factor结果...");
        framework.save_all_results();

        spdlog::info("=== Factor计算服务运行完成 ===");
        
    } catch (const std::exception& e) {
        spdlog::critical("Factor服务异常终止: {}", e.what());
        return 1;
    }
    
    return 0;
}
