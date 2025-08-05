# AlphaFactorFramework

一个高频Alpha因子计算框架，用于实时处理股票行情数据并计算技术指标和因子。

## 项目结构

```
AlphaFactorFramework/
├── include/                 # 头文件目录
│   ├── Framework.h         # 主框架类
│   ├── cal_engine.h       # 计算引擎
│   ├── data_structures.h  # 核心数据结构定义
│   ├── my_indicator.h     # 具体指标类
│   ├── my_factor.h        # 具体因子类
│   ├── data_loader.h      # 数据加载器
│   ├── config.h           # 配置相关
│   ├── result_storage.h   # 结果存储
│   └── ...                # 其他工具类头文件
├── src/                   # 源文件目录
│   ├── my_indicator.cpp   # 具体指标实现
│   ├── my_factor.cpp      # 具体因子实现
│   ├── data_loader.cpp    # 数据加载实现
│   └── ...                # 其他源文件
├── config/                # 配置文件
│   └── config.xml         # 主配置文件
├── data/                  # 数据目录
│   ├── [股票代码]/        # 按股票代码组织的数据
│   │   ├── order/         # 订单数据
│   │   ├── snap/          # 快照数据
│   │   └── trade/         # 成交数据
│   ├── indicator/         # 指标计算结果
│   ├── factor/            # 因子计算结果
│   └── stock_universe/    # 股票池文件
├── main.cpp               # 主程序入口
└── CMakeLists.txt         # 构建配置
```

## 核心组件关系

### 主要运行逻辑

1. **main.cpp** - 程序入口点
   - 初始化日志系统
   - 加载配置文件
   - 创建Framework实例
   - 注册指标和因子
   - 加载历史指标数据
   - 加载并排序行情数据
   - 运行计算引擎
   - 保存计算结果

2. **Framework** - 主框架类
   - 管理整个计算流程
   - 注册指标和因子模块
   - 协调数据加载和引擎运行
   - 处理结果保存

3. **CalculationEngine** - 计算引擎
   - 核心计算组件
   - 管理指标和因子的计算
   - 处理实时数据流
   - 支持多线程并行计算
   - 处理时间触发事件

### 指标 (Indicator) 相关

指标是基础计算单元，负责从原始行情数据中提取和计算基础特征：

- **VolumeIndicator** (`my_indicator.h/cpp`)
  - 计算成交量相关指标
  - 支持15秒、1分钟、5分钟、30分钟频率
  - 成交量数据

- **AmountIndicator** (`my_indicator.h/cpp`)
  - 计算成交金额相关指标
  - 支持15秒、1分钟、5分钟、30分钟频率
  - 成交金额数据

指标特点：
- 实时计算，基于tick数据
- 支持历史数据加载
- 提供时间序列存储
- 支持差分计算和状态重置

### 因子 (Factor) 相关

因子是基于指标(Factor)计算的特征：

- **VolumeFactor** (`my_factor.h/cpp`)
  - 基于成交量指标计算的因子
 

- **PriceFactor** (`my_factor.h/cpp`)
  - 基于成交量与成交金额计算价格数据的因子


## 数据结构

### 核心数据结构

- **GSeries**: 核心数据序列类
- **SyncTickData**: 同步的tick数据结构
- **MarketAllField**: 统一的市场数据结构，包含order、snap、trade等类型
- **BarSeriesHolder**: 指标数据存储容器
- **BaseSeriesHolder**: 基础序列存储类

### 数据流

1. **原始数据** → DataLoader → MarketAllField
2. **MarketAllField** → CalculationEngine → Indicator计算
3. **Indicator结果** → BarSeriesHolder存储
4. **BarSeriesHolder** → Factor计算 → GSeries
5. **GSeries** → ResultStorage保存

## 配置系统

通过`config/config.xml`配置：
- 计算日期
- 股票池
- 指标模块（频率、路径等）
- 因子模块（频率、路径等）

## 构建和运行

```bash
# 构建项目
mkdir build && cd build
cmake ..
ninja

# 运行
./AlphaFactorFramework
```

## 扩展开发

### 添加新指标
1. 继承`Indicator`基类
2. 实现`Calculate`方法
3. 在`Framework.h`中注册新类型
4. 在配置文件中添加模块配置

### 添加新因子
1. 继承`Factor`基类
2. 实现`definition`方法
3. 在`Framework.h`中注册新类型
4. 在配置文件中添加模块配置

## 日志系统

使用spdlog进行日志记录：
- 框架运行日志：`framework.log`
- 支持不同级别的日志输出
- 包含时间戳和模块信息
