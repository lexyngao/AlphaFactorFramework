#include "Framework.h"
#include "config.h"
#include "spdlog/sinks/basic_file_sink.h"
#include <chrono>
#include <stdexcept>
#include <thread>
#include <atomic>
#include <condition_variable>

using namespace std::chrono;

class SharedMemoryService {
private:
    GlobalConfig config_;
    Framework framework_;
    std::atomic<bool> indicator_running_{false};
    std::atomic<bool> factor_running_{false};
    
    // 共享存储：Indicator和Factor共享同一个存储空间
    std::unordered_map<std::string, std::shared_ptr<Indicator>> shared_indicators_;
    std::unordered_map<std::string, std::shared_ptr<Factor>> shared_factors_;
    
public:
    SharedMemoryService(const GlobalConfig& config) 
        : config_(config), framework_(config) {
        
        // 初始化共享存储
        initialize_shared_storage();
    }
    
    void run() {
        spdlog::info("=== 启动共享内存服务（同步运行模式） ===");
        
        try {
            // 1. 同时启动Indicator和Factor线程组
            // start_both_thread_groups();
            // 分阶段模拟同时启动Indicator和Factor线程组
            start_staged_thread_groups();
            
            // 2. 等待两个线程组完成
            wait_for_completion();
            
            // 3. 保存所有结果
            save_all_results();
            
            spdlog::info("=== 共享内存服务运行完成 ===");
            
        } catch (const std::exception& e) {
            spdlog::critical("共享内存服务异常终止: {}", e.what());
            throw;
        }
    }
    
private:
    void initialize_shared_storage() {
        spdlog::info("初始化共享存储...");
        
        // 注册所有Indicator和Factor到共享存储
        framework_.register_indicators_factors(config_.modules);
        
        // 加载历史数据到共享存储
        framework_.load_all_indicators();
        
        // 设置factor依赖关系
        framework_.setup_factor_dependencies();
        
        spdlog::info("共享存储初始化完成");
    }
    
    void start_indicator_threads() {
        spdlog::info("启动Indicator线程组...");
        
        // 加载并排序行情数据
        DataLoader data_loader;
        std::vector<MarketAllField> all_tick_datas = framework_.load_and_sort_market_data(data_loader);
        
        // 重置所有指标的计算状态和差分存储
        framework_.get_engine()->reset_diff_storage();
        
        // 按股票分组处理行情数据
        std::unordered_map<std::string, std::vector<MarketAllField>> stock_data_map;
        for (const auto& data : all_tick_datas) {
            stock_data_map[data.symbol].push_back(data);
        }
        
        spdlog::info("数据分组完成，共{}只股票", stock_data_map.size());
        
        // 启动Indicator线程组（按股票）
        std::vector<std::thread> indicator_threads;
        for (const auto& [stock, stock_data] : stock_data_map) {
            indicator_threads.emplace_back([this, stock_code = stock, data = stock_data]() {
                spdlog::info("Indicator线程开始处理股票{}的行情数据，共{}条", stock_code, data.size());
                
                for (const auto& tick_data : data) {
                    framework_.get_engine()->update(tick_data);
                }
                
                spdlog::info("Indicator线程完成股票{}的处理", stock_code);
            });
        }
        
        // 等待所有Indicator线程完成
        spdlog::info("等待所有Indicator线程完成...");
        for (auto& thread : indicator_threads) {
            thread.join();
        }
        
        indicator_running_ = false;
        spdlog::info("Indicator线程组完成");
    }

    // 新增：同时启动两个线程组（同步运行）
    void start_both_thread_groups() {
        spdlog::info("同时启动Indicator和Factor线程组...");
        
        // 设置两个线程组都在运行
        indicator_running_ = true;
        factor_running_ = true;
        
        // 启动Indicator线程组（后台运行）
        std::thread indicator_thread(&SharedMemoryService::start_indicator_threads, this);
        
        // 启动Factor线程组（后台运行）
        std::thread factor_thread(&SharedMemoryService::start_factor_threads_sync, this);
        
        // 等待两个线程组完成
        indicator_thread.join();
        factor_thread.join();
        
        spdlog::info("两个线程组都已启动完成");
    }

    void start_staged_thread_groups() {
        spdlog::info("分阶段启动线程组...");
        
        // 第一阶段：启动Indicator线程组
        indicator_running_ = true;
        std::thread indicator_thread(&SharedMemoryService::start_indicator_threads, this);
        
        // 等待Indicator运行一段时间，积累数据
        spdlog::info("等待Indicator积累数据...");
        std::this_thread::sleep_for(std::chrono::seconds(90));  // 可配置的等待时间
        
        // 第二阶段：启动Factor线程组
        factor_running_ = true;
        std::thread factor_thread(&SharedMemoryService::start_factor_threads_sync, this);
        
        // 等待两个线程组完成
        indicator_thread.join();
        factor_thread.join();
        
        spdlog::info("所有线程组完成");
    }
    
    // 新增：Factor线程组的同步运行版本
    void start_factor_threads_sync() {
        spdlog::info("启动Factor线程组（同步运行模式）...");
        
        // 不需要等待Indicator，直接开始
        // 生成时间事件
        std::vector<uint64_t> time_points = framework_.generate_time_points(300, config_.calculate_date);
        spdlog::info("生成了 {} 个时间事件", time_points.size());
        
        // 运行Factor计算引擎（持续运行，直到所有时间事件处理完）
        framework_.get_engine()->process_factor_time_events_sync(time_points);
        
        factor_running_ = false;
        spdlog::info("Factor线程组完成");
    }
    
    void wait_for_completion() {
        spdlog::info("等待所有计算任务完成...");
        
        // 等待引擎完成所有计算
        framework_.get_engine()->wait_for_completion();
        
        spdlog::info("所有计算任务已完成");
    }
    
    void save_all_results() {
        spdlog::info("开始保存所有结果...");
        framework_.save_all_results();
        spdlog::info("所有结果保存完成");
    }
};

int main() {
    // 1. 初始化日志
    auto file_logger = spdlog::basic_logger_mt("shared_memory_service", "shared_memory_service_0827_indicator_first.log", true);
    file_logger->set_pattern("[%Y-%m-%d %H:%M:%S.%e] [%n] [%l] %v");
    file_logger->set_level(spdlog::level::debug);
    file_logger->flush_on(spdlog::level::debug);
    spdlog::set_default_logger(file_logger);
    spdlog::set_level(spdlog::level::debug);
    spdlog::info("=== 启动新共享内存服务 ===");

    try {
        // 2. 加载配置
        ConfigLoader config_loader;
        GlobalConfig config;
        if (!config_loader.load("config/config.xml", config)) {
            throw std::runtime_error("加载配置文件失败");
        }

        // 3. 创建并运行共享内存服务
        SharedMemoryService service(config);
        service.run();
        
    } catch (const std::exception& e) {
        spdlog::critical("共享内存服务异常终止: {}", e.what());
        return 1;
    }
    
    return 0;
}
