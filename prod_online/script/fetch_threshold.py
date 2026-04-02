import akshare as ak
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
import warnings

# 屏蔽警告
warnings.filterwarnings('ignore')

# ==========================
# 1. 数据库配置 (请修改为你的配置)
# ==========================
DB_CONFIG = 'mysql+pymysql://user:password@localhost:3306/stock_db?charset=utf8mb4'
engine = create_engine(DB_CONFIG)

def get_tick_data(code):
    """
    获取并清洗 tick 数据
    """
    print(f"⏳ 正在获取 {code} 的 tick 数据...")
    try:
        # 获取分笔数据
        df = ak.stock_zh_a_tick_tx_js(symbol=code) # 注意：akshare接口可能需要指定日期，这里以示例为准
        
        # 基础清洗
        df['成交时间'] = pd.to_datetime(df['成交时间'])
        # df = df.dropna(subset=['成交时间']) # 丢弃时间解析失败的行
        
        # 处理性质列：09:25:00 集合竞价通常为空，标记为 '集合竞价'
        # 这一步很重要，防止后续计算 '买盘/卖盘' 逻辑时报错
        if '性质' in df.columns:
            df['性质'] = df['性质'].fillna('集合竞价')
        
        return df
    except Exception as e:
        print(f"❌ 获取数据失败: {e}")
        return None

def calculate_dynamic_thresholds(df):
    """
    计算大中小单的动态阈值
    使用分位数来定义，适应不同股价的股票
    """
    # 过滤掉集合竞价数据，仅根据连续竞价（9:30以后）的数据来制定标准
    # 这样可以避免集合竞价的超大单扭曲全天的阈值
    continuous_df = df[df['成交时间'].dt.hour > 9].copy()
    
    if len(continuous_df) == 0:
        continuous_df = df # 如果只有集合竞价数据，那就凑合用吧

    # 定义分位点
    # 95% 以上为超大单，90%-95% 为大单
    vol_quantiles = continuous_df['成交量'].quantile([0.5, 0.9, 0.95])
    amt_quantiles = continuous_df['成交金额'].quantile([0.5, 0.9, 0.95])

    thresholds = {
        'vol_mid': vol_quantiles[0.5],
        'vol_big': vol_quantiles[0.9],
        'vol_super': vol_quantiles[0.95],
        'amt_mid': amt_quantiles[0.5],
        'amt_big': amt_quantiles[0.9],
        'amt_super': amt_quantiles[0.95]
    }
    
    return thresholds

def label_orders(df, thresholds):
    """
    根据阈值给每一笔成交打标签
    逻辑：只要 量 或 额 满足条件，即视为该等级（就高不就低）
    """
    def get_order_level(row):
        vol = row['成交量']
        amt = row['成交金额']
        
        # 超大单
        if vol >= thresholds['vol_super'] or amt >= thresholds['amt_super']:
            return '超大单'
        # 大单
        elif vol >= thresholds['vol_big'] or amt >= thresholds['amt_big']:
            return '大单'
        # 中单
        elif vol >= thresholds['vol_mid'] or amt >= thresholds['amt_mid']:
            return '中单'
        # 小单
        else:
            return '小单'

    # 应用函数
    df['订单类型'] = df.apply(get_order_level, axis=1)
    
    # 计算净流入方向 (用于后续统计)
    # 逻辑：买盘=流入, 卖盘=流出, 中性盘/集合竞价=0
    def get_flow_direction(row):
        if row['性质'] == '买盘':
            return row['成交金额']
        elif row['性质'] == '卖盘':
            return -row['成交金额']
        else:
            return 0
            
    df['资金流向'] = df.apply(get_flow_direction, axis=1)
    
    return df

def save_to_db(df, code, date_str):
    """
    存储到数据库
    表结构建议包含：code, date, tick_time, price, volume, amount, order_type, flow_direction
    """
    # 准备入库数据
    save_df = df[['成交时间', '成交价格', '成交量', '成交金额', '性质', '订单类型', '资金流向']].copy()
    save_df['code'] = code
    save_df['date'] = date_str
    
    # 重命名列以匹配数据库
    save_df.rename(columns={
        '成交时间': 'tick_time',
        '成交价格': 'price',
        '成交量': 'volume',
        '成交金额': 'amount',
        '性质': 'nature',
        '订单类型': 'order_type',
        '资金流向': 'flow_val'
    }, inplace=True)
    
    try:
        # 写入数据库 (追加模式)
        save_df.to_sql('stock_tick_analysis', engine, if_exists='append', index=False)
        print(f"✅ {code} 数据入库成功，共 {len(df)} 条。")
        
        # 打印简单的统计结果
        summary = save_df.groupby('order_type')['flow_val'].sum()
        print("📊 当日资金概览 (元):")
        print(summary)
        
    except Exception as e:
        print(f"❌ 入库失败: {e}")

# ==========================
# 主程序执行
# ==========================
if __name__ == "__main__":
    target_code = "sz002487" # 示例代码
    current_date = "20231025" # 示例日期
    
    # 1. 获取数据
    df_tick = get_tick_data(target_code)
    
    if df_tick is not None and not df_tick.empty:
        # 2. 计算动态阈值
        # 这一步会分析全天的单子，算出什么是大单
        ths = calculate_dynamic_thresholds(df_tick)
        print(f"📊 阈值计算完成 -> 大单金额阈值: {ths['amt_big']:.2f}, 超大单金额阈值: {ths['amt_super']:.2f}")
        
        # 3. 打标签
        df_labeled = label_orders(df_tick, ths)
        
        # 4. 存储
        # 这里演示存入数据库，实际使用时请确保表已创建
        # save_to_db(df_labeled, target_code, current_date)
        
        # 仅展示前几行结果
        print(df_labeled[['成交时间', '成交金额', '性质', '订单类型', '资金流向']].head(10))