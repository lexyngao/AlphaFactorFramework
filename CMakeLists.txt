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

add_executable(AlphaFactorFramework main.cpp
        src/config_loader.cpp
        src/data_loader.cpp
        src/calculation_engine.cpp
        src/result_storage.cpp
        src/tinyxml2.cpp
        src/tz.cpp
        src/my_indicator.cpp
        src/my_factor.cpp)

# 链接库
#target_link_libraries(AlphaFactorFramework PRIVATE tinyxml2 ZLIB::ZLIB spdlog::spdlog)
target_link_libraries(AlphaFactorFramework PRIVATE ZLIB::ZLIB )
# 添加这一行：链接curl库
target_link_libraries(AlphaFactorFramework PRIVATE curl)

# 启用C++17（date库需要）
set_property(TARGET AlphaFactorFramework PROPERTY CXX_STANDARD 17)