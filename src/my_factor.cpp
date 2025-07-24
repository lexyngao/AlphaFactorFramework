#include "my_factor.h"

GSeries VolumeFactor::definition(
    const std::unordered_map<std::string, BarSeriesHolder*>& barRunner,
    const std::vector<std::string>& sorted_stock_list,
    int ti
) {
    GSeries result;
    
    // 定义因子参数：读取前1天的volume数据
    const int pre_length = 1;  // 读取前1天的历史数据
    const std::string indicator_name = "volume";  // 基于volume指标
    
    for (const auto& stock : sorted_stock_list) {
        double value = NAN;
        auto it = barRunner.find(stock);
        if (it != barRunner.end() && it->second) {
            // 使用get_today_min_series获取包含历史数据的完整序列
            // pre_length=1: 读取前1天的历史数据 + T日数据
            // today_minute_index=ti: 当前时间桶索引
            GSeries full_series = it->second->get_today_min_series(indicator_name, pre_length, ti);
            
            if (!full_series.empty() && full_series.is_valid(full_series.get_size() - 1)) {
                // 计算因子值：使用最新时间点的volume值
                value = full_series.get(full_series.get_size() - 1);
            }
        }
        result.push(value);
    }
    
    spdlog::debug("VolumeFactor计算完成，时间桶: {}, 股票数量: {}, 有效数据: {}", 
                  ti, sorted_stock_list.size(), result.get_valid_num());
    
    return result;
}