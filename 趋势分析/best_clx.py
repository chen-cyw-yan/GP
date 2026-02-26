import time
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
df=pd.read_sql('select * from gp.stock',con=conn)
base_df=df
def has_limit_up(df, window=15, limit_pct=9.8):#半个月内涨停
    pct = df["close"].pct_change() * 100
    return pct.tail(window).max() >= limit_pct

def has_up_gap(df, lookback=10):# 存在跳空高开
    prev_high = df["high"].shift(1)
    gap = df["low"] > prev_high
    return gap.tail(lookback).any()

# 前三天收阳
def pre_rise_strong(df, days=3):# 前三天收阳
    last = df.tail(days + 1).iloc[:-1]
    body = (last["close"] - last["open"]) / last["open"]
    return (body > 0).all() and (body.mean() > 0.01)

# 前三天收阳，最后一天阳线1%以上
def pre_rise_strong_and_today(df, days=3):  # 最近 days 天（含当天）全部收阳
    last = df.tail(days)
    body = (last["close"] - last["open"]) / last["open"]
    return (body > 0).all() and (body.mean() > 0.01)



# 阳线1%以上
def pre_rise_strong_bast_min(df, days=3, min_last_body=0.01):
    last = df.tail(days)
    body = (last["close"] - last["open"]) / last["open"]

    return (
        (body > 0).all() and
        body.mean() > 0.01 and
        body.iloc[-1] >= min_last_body   # 当天实体 >= 1%
    )



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
        pre_rise_strong_and_today(df, days=3) and
        volume_expand(df, base_days=5, min_ratio=1.5, max_ratio=3)
    )

result = []


def precompute_regulatory_abnormal_vectorized(df):
    close = df["close"]

    abnormal_flag = pd.Series(False, index=close.index)
    abnormal_reason = pd.Series("", dtype="object", index=close.index)

    # 计算收益率并四舍五入
    pct_3 = (close / close.shift(3) - 1).round(6)
    pct_5 = (close / close.shift(5) - 1).round(6)
    pct_10 = (close / close.shift(10) - 1).round(6)

    # 条件判断（加容差或直接比较）
    cond3 = (pct_3.abs() >= 0.20)
    cond5 = (pct_5.abs() >= 0.30) & (~cond3)
    cond10 = (pct_10.abs() >= 0.50) & (~cond3) & (~cond5)

    abnormal_flag = cond3 | cond5 | cond10

    # 格式化（此时 pct 已是干净 float）
    fmt3 = pct_3.fillna(0).map("{:.2%}".format).astype(str)
    fmt5 = pct_5.fillna(0).map("{:.2%}".format).astype(str)
    fmt10 = pct_10.fillna(0).map("{:.2%}".format).astype(str)

    if cond3.any():
        abnormal_reason.loc[cond3] = "3日涨跌幅异常(" + fmt3.loc[cond3] + ")"
    if cond5.any():
        abnormal_reason.loc[cond5] = "5日涨跌幅异常(" + fmt5.loc[cond5] + ")"
    if cond10.any():
        abnormal_reason.loc[cond10] = "10日涨跌幅异常(" + fmt10.loc[cond10] + ")"

    return abnormal_flag, abnormal_reason


df['是否触发异动'], df['异动类型'] = precompute_regulatory_abnormal_vectorized(df)

def precompute_next_day_abnormal(df):

    close = df["close"]

    # 前N日价格
    c3 = close.shift(2)   # t-2
    c5 = close.shift(4)
    c10 = close.shift(9)

    current = close

    # ===== 计算触发阈值需要的下一日涨幅 =====

    # 公式推导:
    # current*(1+x)/cN - 1 >= threshold
    # => (1+x) >= (1+threshold)*cN/current
    # => x >= (1+threshold)*cN/current - 1

    req3 = (1.20 * c3 / current - 1)
    req5 = (1.30 * c5 / current - 1)
    req10 = (1.50 * c10 / current - 1)

    # 取三种里面最小的涨幅（因为满足任意一个即可）
    required = pd.concat([req3, req5, req10], axis=1).min(axis=1)

    # 最大涨停 10%
    max_up = 0.10

    possible = (required <= max_up) & (required > 0)

    # 格式化
    required_fmt = required.map(lambda x: f"{x:.2%}" if pd.notna(x) else "")

    reason = pd.Series("", index=df.index)

    reason[possible] = "下一日若上涨 " + required_fmt[possible] + " 将触发异动"

    return possible, required


df["下一日可能触发异动"], df["下一日最小所需涨幅"] = precompute_next_day_abnormal(df)


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
                "代码": code,
                "名称": g.loc[i, "name"],
                "日期": g.loc[i, "date"],
                "收盘价": g.loc[i, "close"],
                "触发信号次数": signal_seq,
                "是否异动类型":g.loc[i,'异动类型'],
                "下一日可能触发异动":g.loc[i,"下一日可能触发异动"],
                "下一日异动最小所需涨幅":g.loc[i,"下一日最小所需涨幅"]
            })

        else:
            # === True → False：连续结束 ===
            in_signal = False
            signal_seq = 0

res_df = pd.DataFrame(result)
# print(res_df)
res_df=res_df.sort_values('日期',ascending=False)
res_df.to_excel("res_df_0212.xlsx", index=False)