#include "my_factor.h"

GSeries VolumeFactor::definition(
    const std::unordered_map<std::string, BarSeriesHolder*>& barRunner,
    const std::vector<std::string>& sorted_stock_list,
    int ti
) {
    GSeries result;
    int pre_length = 1;  // 需要历史数据的天数
    
    for (const auto& stock : sorted_stock_list) {
        double value = NAN;
        auto it = barRunner.find(stock);
        if (it != barRunner.end() && it->second) {
            // 获取融合数据
            GSeries series = it->second->get_today_min_series("volume", pre_length, ti);
            
            // 计算在融合序列中的正确索引
            // 融合序列结构：[历史数据(pre_length * 240)] + [当日数据(ti + 1)]
            // 我们要访问的是当日数据的第ti个位置
            int fusion_index = pre_length * 240 + ti;  // 历史数据长度 + 当日索引
            
            if (series.is_valid(fusion_index)) {
                value = series.get(fusion_index);
            }
        }
        result.push(value);
    }
    return result;
} 