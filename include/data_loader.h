//
// Created by lexyn on 25-7-14.
//
#ifndef ALPHAFACTORFRAMEWORK_DATA_LOADER_H
#define ALPHAFACTORFRAMEWORK_DATA_LOADER_H

#include "data_structures.h"
#include <string>
#include <vector>
#include <filesystem>
#include <zlib.h>
#include <sstream>
#include "cal_engine.h"
#include <cstdint>
#include <chrono>
#include "date/date.h"


namespace fs = std::filesystem;

inline bool endsWith(const std::string& str, const std::string& suffix) {
    return str.size() >= suffix.size() &&
           str.compare(str.size() - suffix.size(), suffix.size(), suffix) == 0;
}

// 数据加载器（处理行情数据和历史Indicator加载，PDF 2节）
class DataLoader {
private:
    // 解压gz文件到字符串（返回空字符串表示失败）
    static std::string gz_decompress(const std::string& file_path) {
        gzFile fp = gzopen(file_path.c_str(), "rb");
        if (!fp) {
            spdlog::error("Failed to open gz file: {}", file_path);
            return "";
        }

        std::stringstream ss;
        char buffer[4096];
        int bytes_read;
        while ((bytes_read = gzread(fp, buffer, sizeof(buffer))) > 0) {
            ss.write(buffer, bytes_read);
        }
        int err;
        const char* err_msg = gzerror(fp, &err);
        if (err != Z_OK && err != Z_STREAM_END) {
            spdlog::error("gzread failed for {}: {}", file_path, err_msg);
        }
        gzclose(fp);
        return ss.str();
    }

public:
    // 解析格式："YYYY-MM-DD HH:MM:SS.fffffffff"（9位小数，纳秒级）
    // 返回：从1970-01-01 00:00:00 UTC开始的总纳秒数（uint64_t）
    static uint64_t parse_datetime_ns(const std::string& datetime_str) {
        try {
            // 分割日期时间部分和纳秒部分（.后的9位）
            size_t dot_pos = datetime_str.find('.');
            if (dot_pos == std::string::npos) {
                spdlog::error("时间格式错误：缺少纳秒部分（.fffffffff），字符串：{}", datetime_str);
                return 0;
            }

            // 提取日期时间部分（"YYYY-MM-DD HH:MM:SS"）
            std::string dt_part = datetime_str.substr(0, dot_pos);
            // 提取纳秒部分（"fffffffff"，确保9位）
            std::string ns_part = datetime_str.substr(dot_pos + 1);
            // 处理小数部分：补全至9位（纳秒）
            uint64_t ns;
            if (ns_part.size() == 3) {
                // 3位→毫秒，转换为纳秒（乘以1e6）
                // 例如："123" → 123 * 1e6 = 123000000纳秒
                ns = std::stoull(ns_part) * 1000000ULL;
            } else if (ns_part.size() == 9) {
                // 9位→直接作为纳秒
                ns = std::stoull(ns_part);
            } else {
                // 其他长度：警告并尝试截断/补全（避免严格报错）
                spdlog::warn("小数部分长度为{}位（非3或9），尝试兼容处理：{}",
                             ns_part.size(), datetime_str);
                // 截断或补全至9位
                if (ns_part.size() < 9) {
                    ns_part += std::string(9 - ns_part.size(), '0');  // 补0
                } else {
                    ns_part = ns_part.substr(0, 9);  // 截断
                }
                ns = std::stoull(ns_part);
            }

            // 解析日期时间到秒级
            std::istringstream iss(dt_part);
            date::sys_time<std::chrono::seconds> tp;  // 秒级时间点
            iss >> date::parse("%Y-%m-%d %H:%M:%S", tp);
            if (iss.fail()) {
                spdlog::error("日期时间解析失败，格式应为YYYY-MM-DD HH:MM:SS，字符串：{}", dt_part);
                return 0;
            }
            
            // 解析纳秒部分（0-999999999）
            if (ns > 999999999) {
                spdlog::error("纳秒值超出范围（必须≤999999999），值：{}", ns);
                return 0;
            }
            
            // 计算总纳秒数（epoch到当前时间的纳秒）
            auto sec_since_epoch = std::chrono::duration_cast<std::chrono::seconds>(
                    tp.time_since_epoch()
            ).count();
            
            // 修复：输入的时间是北京时间，但date::parse解析为UTC时间
            // 所以我们需要将北京时间转换为UTC时间
            // 北京时间 = UTC + 8小时，所以 UTC = 北京时间 - 8小时
            sec_since_epoch -= 8 * 3600;  // 减去8小时得到UTC时间
            
            uint64_t total_ns = sec_since_epoch * 1000000000ULL + ns;  // 秒→纳秒 + 剩余纳秒

            return total_ns;

        } catch (const std::exception& e) {
            spdlog::error("解析纳秒时间戳失败：{}，错误：{}", datetime_str, e.what());
            return 0;
        }
    }

    // 解析OrderData（示例：实际需按你的gz文件格式调整）
    static std::vector<OrderData> parse_orders(const std::string& content) {
        std::vector<OrderData> orders;
        std::stringstream ss(content);
        std::string line;

        // 跳过标题行
        if (ss.good()) {
            std::getline(ss, line);
        }
        //解析数据行
        while (std::getline(ss, line)) {
            OrderData order;
            std::vector<std::string> tokens;
            std::stringstream lineStream(line);
            std::string token;

            // 使用逗号分隔字段
            while (std::getline(lineStream, token, ',')) {
                tokens.push_back(token);
            }

            // 确保有足够的字段
            if (tokens.size() < 21) {
                spdlog::warn("Error parsing datetime: "+line);
                continue;
            }

            try {
                // 解析各字段
                // 注意：需要根据OrderData结构定义调整字段映射
                order.order_number = std::stoll(tokens[9]);    // OrderIndex
                order.order_kind = tokens[10][0];             // OrderType
                order.price = std::stod(tokens[11]);          // OrderPrice
                order.volume = std::stod(tokens[12]);         // OrderQty
                order.bs_flag = tokens[13][0];                // OrderBSFlag
                order.real_time = parse_datetime_ns(tokens[1]);  // 需要实现的日期时间解析函数
                order.appl_seq_num = std::stoll(tokens[18]);  // ApplSeqNum


                // 其他可能需要的字段...
                // order.security_id = tokens[5];
                order.symbol = tokens[8];

                orders.push_back(order);
            } catch (const std::exception& e) {
                spdlog::warn("Error parsing order ");
            }
        }

        return orders;
    }

    static std::vector<TradeData> parse_trades(const std::string& content) {
        std::vector<TradeData> trades;
        std::stringstream ss(content);
        std::string line;

        // 跳过标题行（第一行是字段名，无需解析）
        if (std::getline(ss, line)) {
            spdlog::trace("Skipping trade data header: {}", line);
        }

        // 逐行解析数据
        while (std::getline(ss, line)) {
            // 按逗号分割字段
            std::vector<std::string> tokens;
            std::stringstream line_ss(line);
            std::string token;
            while (std::getline(line_ss, token, ',')) {
                tokens.push_back(token);
            }

            // 校验字段数量（实际数据有21个字段，索引0~20）
            if (tokens.size() != 21) {
                spdlog::warn("Invalid trade data line (expected 21 fields, got {}): {}", tokens.size(), line);
                continue;
            }

            try {
                TradeData trade;

                // 字段映射（根据您提供的content格式）：
                // 0:TradingDate, 1:TimeStamp, 2:ExchangeTime, ..., 8:TradeIndex, 9:TradeBuyNo, 10:TradeSellNo,
                // 11:TradeType, 12:TradeBSFlag, 13:TradePrice, 14:TradeQty, 15:TradeMoney, 16:Symbol, ..., 20:ApplSeqNum

                // 1. 买方订单号（TradeBuyNo）→ bid_no
                trade.bid_no = std::stoll(tokens[9]);  // tokens[9]是TradeBuyNo

                // 2. 卖方订单号（TradeSellNo）→ ask_no
                trade.ask_no = std::stoll(tokens[10]);  // tokens[10]是TradeSellNo

                // 3. 成交编号（TradeIndex）→ trade_no
                trade.trade_no = std::stoll(tokens[8]);  // tokens[8]是TradeIndex

                // 4. 成交方向（TradeBSFlag）→ side（0=中性，1=买，2=卖，根据实际业务调整）
                trade.side = tokens[12][0];  // tokens[12]是TradeBSFlag

                // 5. 取消标志（未在数据中明确体现，默认为'N'表示正常成交）
                trade.cancel_flag = 'N';  // 实际数据中无cancel_flag字段，暂设为正常

                // 6. 成交价格（TradePrice）→ price
                trade.price = std::stod(tokens[13]);  // tokens[13]是TradePrice

                // 7. 成交量（TradeQty）→ volume
                trade.volume = std::stod(tokens[14]);  // tokens[14]是TradeQty

                // 8. 时间戳（TimeStamp）→ real_time（转换为秒级时间戳）
                trade.real_time = parse_datetime_ns(tokens[1]);  // 复用之前的时间解析函数

                // 9. 应用序列号（ApplSeqNum）→ appl_seq_num
                trade.appl_seq_num = std::stoll(tokens[20]);  // tokens[20]是ApplSeqNum

                //其他可能指标
                trade.symbol = tokens[16];


                trades.push_back(trade);
            } catch (const std::exception& e) {
                spdlog::error("Failed to parse trade data: {} (line: {})", e.what(), line);
            }
        }

        spdlog::info("Parsed {} valid trade records", trades.size());
        return trades;
    }


    // 解析TickData（按实际格式调整）
    static std::vector<TickData> parse_ticks(const std::string& content) {
        std::vector<TickData> ticks;
        std::stringstream ss(content);
        std::string line;

        // 跳过标题行
        if (std::getline(ss, line)) {
            spdlog::trace("Skipping tick data header: {}", line);
        }

        // 逐行解析数据
        while (std::getline(ss, line)) {
            // 按逗号分割字段（共39个字段，索引0~38）
            std::vector<std::string> tokens;
            std::stringstream line_ss(line);
            std::string token;
            while (std::getline(line_ss, token, ',')) {
                tokens.push_back(token);
            }

            // 校验字段数量
            if (tokens.size() != 39) {
                spdlog::warn("Invalid tick data line (expected 39 fields, got {}): {}", tokens.size(), line);
                continue;
            }

            try {
                TickData tick;

                // 1. 成交量（Volume，索引3）
                tick.volume = std::stod(tokens[3]);

                // 2. 买盘价格（BidPrice1~BidPrice5，索引4~8）
                for (int i = 0; i < 5; ++i) {
                    tick.bid_price_v[i] = std::stod(tokens[4 + i]);  // 4=BidPrice1, 5=BidPrice2...
                }

                // 3. 卖盘价格（AskPrice1~AskPrice5，索引9~13）
                for (int i = 0; i < 5; ++i) {
                    tick.ask_price_v[i] = std::stod(tokens[9 + i]);  // 9=AskPrice1, 10=AskPrice2...
                }

                // 4. 价格信息（索引14~23）
                tick.last_price = std::stod(tokens[14]);   // 14=LastPrice
                tick.pre_close = std::stod(tokens[15]);    // 15=PreClose
                tick.limit_high = std::stod(tokens[18]);   // 18=LimitHigh
                tick.limit_low = std::stod(tokens[19]);    // 19=LimitLow
                tick.high_price = std::stod(tokens[20]);   // 20=High
                tick.low_price = std::stod(tokens[21]);    // 21=Low
                tick.open_price = std::stod(tokens[22]);   // 22=Open
                tick.close_price = std::stod(tokens[23]);  // 23=Close

                // 5. 总成交额（TotalValueTraded，索引24）
                tick.total_value_traded = std::stod(tokens[24]);

                // 6. 买盘数量（BidVol1~BidVol5，索引26~30）
                for (int i = 0; i < 5; ++i) {
                    tick.bid_volume_v[i] = std::stod(tokens[26 + i]);  // 26=BidVol1, 27=BidVol2...
                }

                // 7. 卖盘数量（AskVol1~AskVol5，索引31~35）
                for (int i = 0; i < 5; ++i) {
                    tick.ask_volume_v[i] = std::stod(tokens[31 + i]);  // 31=AskVol1, 32=AskVol2...
                }

                // 8. 股票代码（Symbol，索引36）
                tick.symbol = tokens[36];

                // 9. 时间戳（TimeStamp，索引1 → 转换为秒级）
                tick.real_time = parse_datetime_ns(tokens[1]);  // 复用之前的时间解析函数

                // 10. 序列号（数据中无直接字段，用ExchangeTime哈希生成）
                tick.appl_seq_num = std::hash<std::string>{}(tokens[2]);  // 2=ExchangeTime

                ticks.push_back(tick);
            } catch (const std::exception& e) {
                spdlog::error("Failed to parse tick data: {} (line: {})", e.what(), line);
            }
        }

        spdlog::info("Parsed {} valid tick records", ticks.size());
        return ticks;
    }



public:
    // 修正后：返回该股票的所有单条行情数据（order/trade/snapshot分别拆分为独立对象）
    std::vector<SyncTickData> load_stock_data(const std::string& stock_code, const std::string& date) {
        std::vector<SyncTickData> all_data;  // 存储该股票的所有单条行情数据

        // 1. 加载并拆分order数据（每笔订单作为独立SyncTickData）
        std::string order_path = "data/" + stock_code + "/order/" + date + ".gz";
        std::string order_content = gz_decompress(order_path);
        if (!order_content.empty()) {
            std::vector<OrderData> orders = parse_orders(order_content);
            for (const auto& order : orders) {  // 遍历每笔订单，创建独立对象
                SyncTickData sync_data;
                sync_data.symbol = stock_code;
                sync_data.orders.push_back(order);  // 仅包含当前订单
                sync_data.tick_data.real_time = order.real_time;  // 复用订单的时间戳
                sync_data.tick_data.appl_seq_num = order.appl_seq_num;  // 复用订单的序列号
                all_data.push_back(sync_data);
            }
            spdlog::info("Loaded {} orders for {}", orders.size(), stock_code);
        } else {
            spdlog::warn("No order data for {} on {}", stock_code, date);
        }

        // 2. 加载并拆分trade数据（每笔成交作为独立SyncTickData）
        std::string trade_path = "data/" + stock_code + "/trade/" + date + ".gz";
        std::string trade_content = gz_decompress(trade_path);
        if (!trade_content.empty()) {
            std::vector<TradeData> trades = parse_trades(trade_content);
            for (const auto& trade : trades) {  // 遍历每笔成交，创建独立对象
                SyncTickData sync_data;
                sync_data.symbol = stock_code;
                sync_data.trans.push_back(trade);  // 仅包含当前成交
                sync_data.tick_data.real_time = trade.real_time;  // 复用成交的时间戳
                sync_data.tick_data.appl_seq_num = trade.appl_seq_num;  // 复用成交的序列号
                all_data.push_back(sync_data);
            }
            spdlog::info("Loaded {} trades for {}", trades.size(), stock_code);
        } else {
            spdlog::warn("No trade data for {} on {}", stock_code, date);
        }

        // 3. 加载并拆分snapshot数据（每个快照作为独立SyncTickData）
        std::string snap_path = "data/" + stock_code + "/snap/" + date + ".gz";
        std::string snap_content = gz_decompress(snap_path);
        if (!snap_content.empty()) {
            std::vector<TickData> ticks = parse_ticks(snap_content);
            for (const auto& tick : ticks) {  // 遍历每个快照，创建独立对象
                SyncTickData sync_data;
                sync_data.symbol = stock_code;
                sync_data.tick_data = tick;  // 包含当前快照
                // 快照的real_time和appl_seq_num已在tick中解析
                all_data.push_back(sync_data);
            }
            spdlog::info("Loaded {} snapshots for {}", ticks.size(), stock_code);
        } else {
            spdlog::warn("No snapshot data for {} on {}", stock_code, date);
        }

        return all_data;  // 返回该股票的所有单条行情数据
    }

    // 新：直接返回该股票的所有MarketAllField（order/trade/tick统一结构）
    std::vector<MarketAllField> load_stock_data_to_Market(const std::string& stock_code, const std::string& date) {
        std::vector<MarketAllField> all_fields;  // 存储统一结构的数据

        // 1. 加载并拆分order数据（转换为MarketAllField::Order）
        std::string order_path = "data/" + stock_code + "/order/" + date + ".gz";
        std::string order_content = gz_decompress(order_path);
        if (!order_content.empty()) {
            std::vector<OrderData> orders = parse_orders(order_content);
            for (const auto& order : orders) {
                // 创建Order类型的MarketAllField
                MarketAllField field(
                        MarketBufferType::Order,
                        stock_code,
                        order.real_time,          // timestamp = 订单时间戳
                        order.appl_seq_num        // 序列号
                );
                field.order = order;  // 存储订单数据
                all_fields.push_back(field);
            }
//            spdlog::info("Loaded {} orders for {}", orders.size(), stock_code);
        } else {
            spdlog::warn("No order data for {} on {}", stock_code, date);
        }

        // 2. 加载并拆分trade数据（转换为MarketAllField::Trade）
        std::string trade_path = "data/" + stock_code + "/trade/" + date + ".gz";
        std::string trade_content = gz_decompress(trade_path);
        if (!trade_content.empty()) {
            std::vector<TradeData> trades = parse_trades(trade_content);
            for (const auto& trade : trades) {
                // 创建Trade类型的MarketAllField
                MarketAllField field(
                        MarketBufferType::Trade,
                        stock_code,
                        trade.real_time,          // timestamp = 成交时间戳
                        trade.appl_seq_num        // 序列号
                );
                field.trade = trade;  // 存储成交数据
                all_fields.push_back(field);
            }
//            spdlog::info("Loaded {} trades for {}", trades.size(), stock_code);
        } else {
            spdlog::warn("No trade data for {} on {}", stock_code, date);
        }

        // 3. 加载并拆分snapshot数据（转换为MarketAllField::Tick）
        std::string snap_path = "data/" + stock_code + "/snap/" + date + ".gz";
        std::string snap_content = gz_decompress(snap_path);
        if (!snap_content.empty()) {
            std::vector<TickData> ticks = parse_ticks(snap_content);
            for (const auto& tick : ticks) {
                // 创建Tick类型的MarketAllField
                MarketAllField field(
                        MarketBufferType::Tick,
                        stock_code,
                        tick.real_time,           // timestamp = 快照时间戳
                        tick.appl_seq_num         // 序列号
                );
                field.tick = tick;  // 存储快照数据
                all_fields.push_back(field);
            }
//            spdlog::info("Loaded {} snapshots for {}", ticks.size(), stock_code);
        } else {
            spdlog::warn("No snapshot data for {} on {}", stock_code, date);
        }

        return all_fields;  // 直接返回统一结构的vector
    }
//    std::vector<OrderData> load_orders(const std::string& stock_code, const std::string& date);

    // 按规则排序行情数据（PDF 2节）
    static void sort_tick_datas(std::vector<SyncTickData>& tick_datas) {
        std::sort(tick_datas.begin(), tick_datas.end(), [](const SyncTickData& a, const SyncTickData& b) {
            // 1. 优先比较real_time（小的在前）
            if (a.tick_data.real_time != b.tick_data.real_time) {
                return a.tick_data.real_time < b.tick_data.real_time;
            }

            // 2. 比较数据来源优先级（区分市场，PDF 2节）
            auto get_priority = [](const SyncTickData& data) {
                bool is_sh = (data.symbol.find(".SH") != std::string::npos);
                // 上交所：trade(0) < order(1) < snapshot(2)（数字越小越优先）
                // 深交所：order(0) < trade(1) < snapshot(2)
                // 此处简化为：根据是否有trade/order判断优先级
                if (is_sh) {
                    if (!data.trans.empty()) return 0;  // 有trade
                    if (!data.orders.empty()) return 1; // 有order
                    return 2;                           // 只有snapshot
                } else {
                    if (!data.orders.empty()) return 0; // 有order
                    if (!data.trans.empty()) return 1;  // 有trade
                    return 2;                           // 只有snapshot
                }
            };
            int a_prio = get_priority(a);
            int b_prio = get_priority(b);
            if (a_prio != b_prio) {
                return a_prio < b_prio;
            }

            // 3. 比较appl_seq_num（小的在前）
            return a.tick_data.appl_seq_num < b.tick_data.appl_seq_num;
        });
        spdlog::info("Sorted {} tick datas by rules", tick_datas.size());
    }

    // 针对MarketAllField的排序函数
    static void sort_market_datas(std::vector<MarketAllField>& market_datas) {
        std::sort(market_datas.begin(), market_datas.end(),
                  [](const MarketAllField& a, const MarketAllField& b) {
                      // 1. 优先比较时间戳（timestamp，小的在前）
                      if (a.timestamp != b.timestamp) {
                          return a.timestamp < b.timestamp;
                      }

                      // 2. 比较数据来源优先级（区分上交所/深交所，与原逻辑一致）
                      auto get_priority = [](const MarketAllField& data) {
                          bool is_sh = (data.symbol.find(".SH") != std::string::npos);
                          // 上交所：Trade(0) < Order(1) < Tick(2)（数字越小越优先）
                          // 深交所：Order(0) < Trade(1) < Tick(2)
                          if (is_sh) {
                              if (data.type == MarketBufferType::Trade)  return 0;
                              if (data.type == MarketBufferType::Order)  return 1;
                              return 2;  // Tick（快照）
                          } else {
                              if (data.type == MarketBufferType::Order)  return 0;
                              if (data.type == MarketBufferType::Trade)  return 1;
                              return 2;  // Tick（快照）
                          }
                      };
                      int a_prio = get_priority(a);
                      int b_prio = get_priority(b);
                      if (a_prio != b_prio) {
                          return a_prio < b_prio;
                      }

                      // 3. 比较序列号（appl_seq_num，小的在前）
                      return a.appl_seq_num < b.appl_seq_num;
                  });
        spdlog::info("Sorted {} market datas by rules", market_datas.size());
    }

    // 加载历史Indicator数据（T-pre_days到T-1，PDF 1.2节）
//    bool load_historical_indicators(
//            const std::string& date,  // T日
//            int pre_days,
//            const std::vector<std::string>& current_stock_list,
//            CalculationEngine& engine
//    ) {
//        // 1. 生成历史日期（如T=20240701，T-1=20240628，需考虑交易日历）
//        // 实际应用中需结合交易日历计算，此处简化为假设连续日期
//        std::vector<std::string> hist_dates;
//        for (int i = 1; i <= pre_days; ++i) {
//            hist_dates.push_back("202406" + std::to_string(28 - i));  // 示例日期
//        }
//
//        // 2. 加载每个历史日期的Indicator数据
//        for (int day_idx = 0; day_idx < pre_days; ++day_idx) {
//            std::string hist_date = hist_dates[day_idx];
//            int his_day_index = day_idx + 1;  // 历史日期索引（T-pre_days=0+1=1，...，T-1=pre_days）
//
//            // 3. 按当前股票列表reindex（PDF 1.2节）
//            for (const auto& stock : current_stock_list) {
//                // 假设从路径加载历史数据：/dat/indicator/hist_date/15S/indicator_name.gz
//                // 实际应用中需解析gz文件，此处简化为模拟数据
//                GSeries hist_series(std::vector<double>(10, 0.0));  // 示例：10个bar的历史数据
//                engine.get_indicator_storage().at(stock)->set_his_series("volume_indicator", his_day_index, hist_series);
//            }
//        }
//
//        spdlog::info("Loaded historical indicators for {} days (pre_days={})", pre_days, pre_days);
//        return true;
//    }

    // 遍历data目录获取股票列表（实际应用需结合stock_universe配置，PDF 1.2节）
    static std::vector<std::string> get_stock_list_from_data(const std::string& data_dir = "data") {
        std::vector<std::string> stock_list;
        if (!fs::exists(data_dir) || !fs::is_directory(data_dir)) {
            spdlog::error("Data directory {} not exists", data_dir);
            return stock_list;
        }
        for (const auto& entry : fs::directory_iterator(data_dir)) {
            if (entry.is_directory()) {
                std::string stock_code = entry.path().filename().string();
                // 过滤有效股票代码（如603103.SH、000001.SZ）
                if (stock_code.size() >= 6 && (endsWith(stock_code, ".SH") || endsWith(stock_code, ".SZ"))) {
                    stock_list.push_back(stock_code);
                }
            }
        }
        spdlog::info("Found {} stocks in data directory", stock_list.size());
        return stock_list;
    }
};



#endif //ALPHAFACTORFRAMEWORK_DATA_LOADER_H
