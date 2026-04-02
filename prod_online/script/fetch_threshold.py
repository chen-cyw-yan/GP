import akshare as ak
import pandas as pd
import numpy as np

def analyze_and_get_thresholds(stock_code, date_str):
    print(f"📊 正在分析股票: {stock_code} (日期: {date_str})...")
    
    # 1. 获取数据
    try:
        # 获取分笔数据
        df = ak.stock_zh_a_tick_tx_js(symbol=stock_code)
    except Exception as e:
        print(f"❌ 数据获取失败: {e}")
        return

    # 2. 数据清洗
    # 转换时间，处理异常
    df['成交时间'] = pd.to_datetime(df['成交时间'])
    df = df.dropna(subset=['成交时间'])
    
    # 过滤掉 09:25:00 的集合竞价数据
    # 原因：集合竞价的单子通常巨大，会严重拉高阈值，导致连续竞价期间的判断失真
    df_continuous = df[df['成交时间'].dt.hour > 9].copy()
    
    if df_continuous.empty:
        print("⚠️ 警告：没有连续竞价数据，将使用全量数据计算。")
        df_continuous = df

    # 3. 计算阈值 (核心步骤)
    # 我们定义：
    # 大单阈值 = 90% 分位点 (即只有 10% 的单子比这个大)
    # 小单阈值 = 30% 分位点 (即 30% 的单子比这个小)
    
    # --- 计算金额阈值 ---
    # quantile(0.9) 表示从小到大排列，排在 90% 位置的数值
    threshold_amt_big = df_continuous['成交金额'].quantile(0.90)
    threshold_amt_small = df_continuous['成交金额'].quantile(0.30)
    
    # --- 计算手数阈值 ---
    threshold_vol_big = df_continuous['成交量'].quantile(0.90)
    threshold_vol_small = df_continuous['成交量'].quantile(0.30)

    # 4. 输出结果 (格式化)
    print("-" * 40)
    print(f"📈 {stock_code} 基于今日数据的判定规则如下：")
    print("-" * 40)
    
    print(f"💰 [金额] 大单阈值: > {threshold_amt_big:,.0f} 元")
    print(f"💰 [金额] 小单阈值: < {threshold_amt_small:,.0f} 元")
    print(f"✋ [手数] 大单阈值: > {threshold_vol_big:,.0f} 手")
    print(f"✋ [手数] 小单阈值: < {threshold_vol_small:,.0f} 手")
    print("-" * 40)
    
    # 5. 返回结果，方便你直接复制到代码里
    return {
        "amt_big": threshold_amt_big,
        "amt_small": threshold_amt_small,
        "vol_big": threshold_vol_big,
        "vol_small": threshold_vol_small
    }

# ==========================
# 执行入口
# ==========================
if __name__ == "__main__":
    # 这里填入你想分析的股票和日期
    code = "sz002487"
    date = "20231025"  # 请确保这个日期有数据
    
    # 运行分析
    rules = analyze_and_get_thresholds(code, date)
    
    if rules:
        print("\n✅ 建议的 Python 判定代码 (可直接复制):")
        print(f"""
# 判定逻辑示例
if amount > {rules['amt_big']:.0f}:
    order_type = '大单'
elif amount < {rules['amt_small']:.0f}:
    order_type = '小单'
else:
    order_type = '中单'
        """)