////
//// Created by lexyn on 25-7-14.
////
//#include "data_loader.h"
//#include "data_structures.h"
//#include <fstream>
//#include <zlib.h>
//#include <sstream>
//#include <algorithm>
//
//// 辅助函数：解压gz文件到字符串
//std::string gz_read(const std::string& file_path) {
//    gzFile fp = gzopen(file_path.c_str(), "rb");
//    if (!fp) {
//        spdlog::error("Failed to open gz file: {}", file_path);
//        return "";
//    }
//
//    char buffer[1024];
//    std::stringstream ss;
//    int bytes_read;
//    while ((bytes_read = gzread(fp, buffer, sizeof(buffer))) > 0) {
//        ss.write(buffer, bytes_read);
//    }
//    gzclose(fp);
//    return ss.str();
//}
//
//// 加载单只股票的当日订单数据
//std::vector<OrderData> DataLoader::load_orders(const std::string& stock_code, const std::string& date) {
////    std::vector<OrderData> orders;
////    std::string file_path = "data/" + stock_code + "/order/" + date + ".gz";
////    std::string content = gz_read(file_path);
////    if (content.empty()) return orders;
////
////    // 假设文件每行是一个OrderData，按逗号分隔（需根据实际数据格式调整）
////    std::stringstream ss(content);
////    std::string line;
////    while (std::getline(ss, line)) {
////        OrderData order;
////        // 示例解析（需替换为实际数据格式）
////        sscanf(line.c_str(), "%lld,%c,%lf,%lf,%c,%d,%lld",
////               &order.order_number, &order.order_kind, &order.price, &order.volume,
////               &order.bs_flag, &order.real_time, &order.appl_seq_num);
////        orders.push_back(order);
////    }
////    return orders;
//}
//
//// 同理实现load_trades、load_ticks（结构类似，解析对应字段）
//
//
