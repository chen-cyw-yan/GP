import pandas as pd
from sqlalchemy import create_engine
import BacktestEngine_1_0_1 as be
# ==============================
# 1️⃣ 数据库连接
# ==============================
engine = create_engine("mysql+pymysql://root:chen@127.0.0.1:3306/gp")

# ==============================
# 2️⃣ 读取数据
# ==============================
sql = """
SELECT 
    date,
    code,
    close,
    turnover,
    outstanding_share
FROM stock
"""
df = pd.read_sql(sql, engine)

# ==============================
# 3️⃣ 数据预处理
# ==============================
df['date'] = pd.to_datetime(df['date'])
df = df.sort_values(['code', 'date'])

# ==============================
# 4️⃣ 计算指标
# ==============================

# ✅ 流通市值（元）
df['float_mkt_cap'] = df['close'] * df['outstanding_share'] * 10000

# ✅ 近8天平均换手率
df['avg_turnover_8d'] = (
    df.groupby('code')['turnover']
    .rolling(8)
    .mean()
    .reset_index(level=0, drop=True)
)

# ✅ 100天前收盘价
df['close_100d_ago'] = df.groupby('code')['close'].shift(100)

# ✅ 近100天涨幅
df['pct_100d'] = (df['close'] - df['close_100d_ago']) / df['close_100d_ago']

# ==============================
# 5️⃣ 条件筛选
# ==============================
condition = (
    (df['float_mkt_cap'] > 2e9) & # 流通市值大于20亿
    (df['avg_turnover_8d'] >= 0.08) & # 近8天平均换手率大于8%
    (df['avg_turnover_8d'] <= 0.18) & # 近8天平均换手率小于18%
    (df['pct_100d'] <= 0.2) # 近100天涨幅小于20%
)

result = df.loc[condition, ['date', 'code']].copy()
# result.rename(columns={'date': 'trigger_date'}, inplace=True)

# ==============================
# 6️⃣ 输出结果
# ==============================
print(result.head())

# 如果你要保存
# result.to_csv('signal_result.csv', index=False
sell_config = {
    # 'total_profit': 0.5,          # 总盈利20%
    # 'daily_profit': 0.1,          # 当天盈利10%
    # 'sell_on_limit_up': True,     # 涨停是否卖出
    # 'avg_daily_profit': 0.07,     # 平均每天收益5%
    # 'drawdown': 0.02,              # 从最高回撤10%
    'high_to_close_drop': 0.02,   # 最高到收盘回落5%
    # 'max_hold_days': 5,           # 持股天数
    # 'daily_drop': -0.08,          # 当天跌幅
    'total_loss': -0.07,           # 总亏损
    # 'ladder': {
    #     'trigger': 0.05,          # 当天涨幅>5%
    #     'sell_pct': 0.3           # 卖出30%
    # }
}



engine = be.BacktestEngine(result,sell_config=sell_config,buy_time='close',max_daily_buys=5,buy_priority='turnover')
engine.run()

pd.DataFrame(engine.trade_log).to_excel("trades.xlsx", index=False)
engine.equity_df.to_excel("equity.xlsx", index=False)