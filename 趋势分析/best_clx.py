import logging
import time
from datetime import datetime
import akshare as ak
import pandas as pd
import numpy as np
import tqdm
# import pyecharts.options as opts
# from pyecharts.charts import Line
import pandas as pd
from sqlalchemy import create_engine
from sklearn.linear_model import LinearRegression
import matplotlib.pyplot as plt
from sqlalchemy import create_engine
import pymysql
# engine = create_engine("mysql+pymysql://root:chen@127.0.0.1:3306/gp")
conn = pymysql.connect(
            host='127.0.0.1',
            user='root',
            password='chen',
            database='gp',
            # use_unicode=args.encoding,
        )
cursor = conn.cursor()
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)
def toSql(sql: str, rows: list):
    """
        连接数据库
    """
    # print(sql,rows)
    try:

        cursor.executemany(sql, rows)
        conn.commit()
    except Exception as e:
        raise ConnectionError("[ERROR] 连接数据库失败，具体原因是：" + str(e))
def has_limit_up(df, window=15, limit_pct=9.8):#半个月内涨停
    pct = df["close"].pct_change() * 100
    return pct.tail(window).max() >= limit_pct

def has_up_gap(df, lookback=10):# 存在跳空高开
    prev_high = df["high"].shift(1)
    gap = df["low"] > prev_high
    return gap.tail(lookback).any()

def pre_rise_strong(df, days=3):# 前三天收阳
    last = df.tail(days + 1).iloc[:-1]
    body = (last["close"] - last["open"]) / last["open"]
    return (body > 0).all() and (body.mean() > 0.01)

def volume_expand(df, base_days=5, min_ratio=1.5, max_ratio=3):# 成交量放大，1.5-3倍
    vol = df["volume"]
    base_mean = vol.shift(1).rolling(base_days).mean()
    ratio = vol / base_mean
    return ratio.iloc[-1] >= min_ratio and ratio.iloc[-1] <= max_ratio

def is_valid_stock(df):
    if len(df) < 30:
        return False

    return (
        has_limit_up(df, window=15) and
        has_up_gap(df, lookback=10) and
        pre_rise_strong(df, days=3) and
        volume_expand(df, base_days=5, min_ratio=1.5, max_ratio=3)
    )


df=pd.read_sql('select * from gp.stock',con=conn)
base_df=df

result = []

for code, g in tqdm.tqdm(df.groupby("code")):
    g = g.sort_values("date").reset_index(drop=True)
    in_signal = False      # 是否处于连续触发区间
    signal_seq = 0         # 连续触发计数
    for i in range(30, len(g)):
        window = g.iloc[:i+1]
        is_signal = is_valid_stock(window)
        if is_signal:
            if not in_signal:
                # === False → True：首次触发 ===
                in_signal = True
                signal_seq = 1
            else:
                # === 连续触发 ===
                signal_seq += 1

            result.append({
                "code": code,
                "name": g.loc[i, "name"],
                "date": g.loc[i, "date"],
                "close": g.loc[i, "close"],
                "signal_seq": signal_seq
            })
        else:
            # === True → False：连续结束 ===
            in_signal = False
            signal_seq = 0
res_df = pd.DataFrame(result)
# print(res_df)
today = datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)
end_date = today.strftime("%Y%m%d")
res_df.to_excel(f"res_df_{end_date}.xlsx", index=False)