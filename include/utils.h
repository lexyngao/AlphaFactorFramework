//
// Created by lexyn on 25-7-17.
//

#ifndef ALPHAFACTORFRAMEWORK_UTILS_H
#define ALPHAFACTORFRAMEWORK_UTILS_H

#include <iostream>
#include <vector>
#include <string>
#include <fstream>
#include <spdlog/spdlog.h>

// 辅助函数：计算指定日期的前n天（返回"YYYYMMDD"格式）
std::string get_prev_date(const std::string& date, int n) {
    // 实现日期解析与偏移（可使用date库如HowardHinnant/date，或手动解析）
    // 简化示例：假设日期为连续交易日，实际需处理节假日
    int year = std::stoi(date.substr(0, 4));
    int month = std::stoi(date.substr(4, 2));
    int day = std::stoi(date.substr(6, 2));
    // ... 日期偏移逻辑 ...
    return fmt::format("{:04d}{:02d}{:02d}", year, month, day);
}

// 辅助函数：从股票池文件读取指定交易日的股票列表
std::vector<std::string> load_stock_list(const std::string& universe_name, const std::string& trading_day) {
    std::vector<std::string> stock_list;
    std::string filename = fmt::format("data/stock_universe/{}_{}.txt", universe_name, trading_day);
    std::ifstream file(filename);
    if (!file.is_open()) {
        spdlog::error("无法读取股票池文件: {}", filename);
        return stock_list;
    }
    std::string stock;
    while (std::getline(file, stock)) {
        if (!stock.empty()) stock_list.push_back(stock);
    }
    spdlog::info("加载股票池[{}]（{}日）：{}只股票", universe_name, trading_day, stock_list.size());
    return stock_list;
}

#endif //ALPHAFACTORFRAMEWORK_UTILS_H
