#!/bin/bash

# AlphaFactor框架服务启动脚本
# 支持独立运行Indicator和Factor服务

echo "=== AlphaFactor框架服务启动脚本 ==="

# 检查参数
if [ "$1" = "indicator" ]; then
    echo "启动Indicator计算服务..."
    ./IndicatorService
elif [ "$1" = "factor" ]; then
    echo "启动Factor计算服务..."
    ./FactorService
elif [ "$1" = "both" ]; then
    echo "启动两个服务（并行运行）..."
    
    # 启动Indicator服务（后台运行）
    echo "启动Indicator服务..."
    ./IndicatorService > indicator.log 2>&1 &
    INDICATOR_PID=$!
    echo "Indicator服务已启动，PID: $INDICATOR_PID"
    
    # 等待Indicator服务完成
    echo "等待Indicator服务完成..."
    wait $INDICATOR_PID
    INDICATOR_EXIT_CODE=$?
    
    if [ $INDICATOR_EXIT_CODE -eq 0 ]; then
        echo "Indicator服务运行成功，启动Factor服务..."
        
        # 启动Factor服务
        ./FactorService > factor.log 2>&1 &
        FACTOR_PID=$!
        echo "Factor服务已启动，PID: $FACTOR_PID"
        
        # 等待Factor服务完成
        wait $FACTOR_PID
        FACTOR_EXIT_CODE=$?
        
        if [ $FACTOR_EXIT_CODE -eq 0 ]; then
            echo "所有服务运行成功！"
            exit 0
        else
            echo "Factor服务运行失败，退出码: $FACTOR_EXIT_CODE"
            exit $FACTOR_EXIT_CODE
        fi
    else
        echo "Indicator服务运行失败，退出码: $INDICATOR_EXIT_CODE"
        exit $INDICATOR_EXIT_CODE
    fi
    
elif [ "$1" = "shared" ]; then
    echo "启动共享内存服务（推荐）..."
    ./cmake-build-debug/SharedMemoryService > shared_memory.log 2>&1
    SHARED_EXIT_CODE=$?
    
    if [ $SHARED_EXIT_CODE -eq 0 ]; then
        echo "共享内存服务运行成功！"
        exit 0
    else
        echo "共享内存服务运行失败，退出码: $SHARED_EXIT_CODE"
        exit $SHARED_EXIT_CODE
    fi
    
elif [ "$1" = "original" ]; then
    echo "启动原始的统一服务..."
    ./AlphaFactorFramework
else
    echo "用法: $0 {indicator|factor|both|shared|original}"
    echo "  indicator  - 只运行Indicator计算服务"
    echo "  factor     - 只运行Factor计算服务"
    echo "  both       - 先运行Indicator，再运行Factor"
    echo "  shared     - 运行共享内存服务（推荐）"
    echo "  original   - 运行原始的完整服务"
    echo ""
    echo "示例:"
    echo "  $0 indicator    # 只计算指标"
    echo "  $0 factor       # 只计算因子"
    echo "  $0 both         # 先指标后因子"
    echo "  $0 shared       # 共享内存服务（推荐）"
    exit 1
fi
