#include "my_factor.h"

GSeries VolumeFactor::definition(
    const std::unordered_map<std::string, BaseSeriesHolder*>& barRunner,
    const std::vector<std::string>& sorted_stock_list,
    int ti
) {
    GSeries result;
    for (const auto& stock : sorted_stock_list) {
        double value = NAN;
        auto it = barRunner.find(stock);
        if (it != barRunner.end() && it->second) {
            // T日数据现在存储在MBarSeries中
            GSeries series = it->second->get_m_bar("volume");
            if (series.is_valid(ti)) value = series.get(ti);
        }
        result.push(value);
    }
    return result;
} 