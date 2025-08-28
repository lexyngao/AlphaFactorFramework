cmake_minimum_required(VERSION 3.20)
project(AlphaFactorFramework)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_TOOLCHAIN_FILE "~/vcpkg/scripts/buildsystems/vcpkg.cmake" CACHE STRING "")

# 添加头文件搜索路径
include_directories("include")  # 指定include文件夹为头文件搜索路径

# 引入依赖库
#find_package(tinyxml2 REQUIRED)  # 解析XML配置文件
find_package(ZLIB REQUIRED)      # 处理gz压缩文件
#find_package(spdlog REQUIRED)    # 日志库

# 原始的可执行文件（保持兼容性）
add_executable(AlphaFactorFramework main.cpp
        src/config_loader.cpp
        src/data_loader.cpp
        src/calculation_engine.cpp
        src/result_storage.cpp
        src/tinyxml2.cpp
        src/tz.cpp
        src/my_indicator.cpp
        src/my_factor.cpp
        src/diff_indicator.cpp
        src/gseries_impl.cpp
        src/indicator_storage_helper.cpp)

# 链接库
target_link_libraries(AlphaFactorFramework PRIVATE ZLIB::ZLIB curl)

# 启用C++17（date库需要）
set_property(TARGET AlphaFactorFramework PROPERTY CXX_STANDARD 17)

# Indicator计算服务
add_executable(IndicatorService indicator_service.cpp
        src/config_loader.cpp
        src/data_loader.cpp
        src/calculation_engine.cpp
        src/result_storage.cpp
        src/tinyxml2.cpp
        src/tz.cpp
        src/my_indicator.cpp
        src/my_factor.cpp
        src/diff_indicator.cpp
        src/gseries_impl.cpp
        src/indicator_storage_helper.cpp)

target_link_libraries(IndicatorService PRIVATE ZLIB::ZLIB curl)
set_property(TARGET IndicatorService PROPERTY CXX_STANDARD 17)

# Factor计算服务
add_executable(FactorService factor_service.cpp
        src/config_loader.cpp
        src/data_loader.cpp
        src/calculation_engine.cpp
        src/result_storage.cpp
        src/tinyxml2.cpp
        src/tz.cpp
        src/my_indicator.cpp
        src/my_factor.cpp
        src/diff_indicator.cpp
        src/gseries_impl.cpp
        src/indicator_storage_helper.cpp)

target_link_libraries(FactorService PRIVATE ZLIB::ZLIB curl)
set_property(TARGET FactorService PROPERTY CXX_STANDARD 17)

# 共享内存服务（推荐）
add_executable(SharedMemoryService shared_memory_service.cpp
        src/config_loader.cpp
        src/data_loader.cpp
        src/calculation_engine.cpp
        src/result_storage.cpp
        src/tinyxml2.cpp
        src/tz.cpp
        src/my_indicator.cpp
        src/my_factor.cpp
        src/diff_indicator.cpp
        src/gseries_impl.cpp
        src/indicator_storage_helper.cpp)

target_link_libraries(SharedMemoryService PRIVATE ZLIB::ZLIB curl)
set_property(TARGET SharedMemoryService PROPERTY CXX_STANDARD 17)

# 时间映射调试程序
add_executable(debug_time_mapping examples/debug_time_mapping.cpp
        src/config_loader.cpp
        src/data_loader.cpp
        src/calculation_engine.cpp
        src/result_storage.cpp
        src/tinyxml2.cpp
        src/tz.cpp
        src/my_indicator.cpp
        src/my_factor.cpp
        src/diff_indicator.cpp
        src/gseries_impl.cpp
        src/indicator_storage_helper.cpp)

target_link_libraries(debug_time_mapping PRIVATE ZLIB::ZLIB curl)
set_property(TARGET debug_time_mapping PROPERTY CXX_STANDARD 17)