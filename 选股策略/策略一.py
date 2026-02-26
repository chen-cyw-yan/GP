import time
import warnings

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
warnings.filterwarnings("ignore", category=UserWarning)
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


# 成交量放大，1.5-3倍
def volume_expand(df, base_days=5, min_ratio=1.5, max_ratio=3):
    vol = df["volume"]
    # base_mean = vol.shift(1).rolling(base_days).mean()
    base_mean = vol.rolling(base_days).mean()
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
# 首次触发信号
def get_first_signals(df):
    signals = []

    in_signal = False

    for i in range(30, len(df)):
        window = df.iloc[:i+1]
        is_signal = is_valid_stock(window)

        if is_signal and not in_signal:
            signals.append(i)
            in_signal = True
        elif not is_signal:
            in_signal = False

    return signals
# 判断涨停
def is_limit_up_today(df, idx, limit_pct=9.8):
    if idx == 0:
        return False
    pct = (df.loc[idx, "close"] / df.loc[idx - 1, "close"] - 1) * 100
    return pct >= limit_pct


# 监管异动触发判断（价格维度）
def  regulatory_abnormal_trigger(
    df,
    idx,
    is_st=False
):
    """
    A股监管异动触发判断（价格维度）

    Parameters
    ----------
    df : DataFrame（需包含 close）
    idx : 当前索引（int）
    is_st : 是否 ST 股票

    Returns
    -------
    (bool, str) : 是否触发, 触发原因
    """

    if idx < 10:
        return False, ""

    close = df["close"]

    # ===== 阈值设置 =====
    if is_st:
        rule_3d, rule_5d, rule_10d = 0.15, 0.20, 0.30
    else:
        rule_3d, rule_5d, rule_10d = 0.20, 0.30, 0.50

    # ===== 累计涨跌幅 =====
    pct_3d = close.iloc[idx] / close.iloc[idx - 3] - 1
    pct_5d = close.iloc[idx] / close.iloc[idx - 5] - 1
    pct_10d = close.iloc[idx] / close.iloc[idx - 10] - 1

    if abs(pct_3d) >= rule_3d:
        return True, f"3日累计涨跌幅异常（{pct_3d:.2%}）"

    if abs(pct_5d) >= rule_5d:
        return True, f"5日累计涨跌幅异常（{pct_5d:.2%}）"

    if abs(pct_10d) >= rule_10d:
        return True, f"10日累计涨跌幅异常（{pct_10d:.2%}）"

    return False, ""

def precompute_regulatory_abnormal(df):
    n = len(df)
    abnormal_flag = [False] * n
    abnormal_reason = [""] * n
    close = df["close"].values

    for i in range(n):
        if i < 10:
            continue

        pct_3 = close[i] / close[i - 3] - 1
        pct_5 = close[i] / close[i - 5] - 1
        pct_10 = close[i] / close[i - 10] - 1

        if abs(pct_3) >= 0.20:
            abnormal_flag[i] = True
            abnormal_reason[i] = f"3日涨跌幅异常({pct_3:.2%})"
        elif abs(pct_5) >= 0.30:
            abnormal_flag[i] = True
            abnormal_reason[i] = f"5日涨跌幅异常({pct_5:.2%})"
        elif abs(pct_10) >= 0.50:
            abnormal_flag[i] = True
            abnormal_reason[i] = f"10日涨跌幅异常({pct_10:.2%})"

    return abnormal_flag, abnormal_reason
def precompute_signals(df):
    n = len(df)
    signal = [False] * n
    for i in range(n):
        if i < 10:
            continue
        signal[i] = is_valid_stock(df.iloc[:i])
    return signal
def precompute_all(df):
    n = len(df)

    # ===== 监管异动 =====
    abnormal_flag = np.zeros(n, dtype=bool)
    abnormal_reason = [""] * n
    close = df["close"].values

    for i in tqdm.tqdm(range(10, n)):
        pct_3 = close[i] / close[i - 3] - 1
        pct_5 = close[i] / close[i - 5] - 1
        pct_10 = close[i] / close[i - 10] - 1

        if abs(pct_3) >= 0.20:
            abnormal_flag[i] = True
            abnormal_reason[i] = f"3日涨跌幅异常({pct_3:.2%})"
        elif abs(pct_5) >= 0.30:
            abnormal_flag[i] = True
            abnormal_reason[i] = f"5日涨跌幅异常({pct_5:.2%})"
        elif abs(pct_10) >= 0.50:
            abnormal_flag[i] = True
            abnormal_reason[i] = f"10日涨跌幅异常({pct_10:.2%})"

    # ===== 涨停 =====
    limit_up = np.zeros(n, dtype=bool)
    pct = np.zeros(n)
    pct[1:] = (close[1:] / close[:-1] - 1) * 100
    limit_up = pct >= 9.8

    # ===== 信号 =====
    signal = np.zeros(n, dtype=bool)
    in_signal = False
    for i in tqdm.tqdm(range(30, n)):
        if is_valid_stock(df.iloc[:i+1]):
            if not in_signal:
                signal[i] = True
                in_signal = True
        else:
            in_signal = False

    return signal, abnormal_flag, abnormal_reason, limit_up
def backtest_add_position_close_buy_fast(df, shares_per_buy=100):
    results = []
    print('计算precompute_all')
    signal, abnormal_flag, abnormal_reason, limit_up = precompute_all(df)
    print('计算precompute_all完成')
    n = len(df)
    open_p = df["open"].values
    close_p = df["close"].values

    for sig_idx in tqdm.tqdm(np.where(signal)[0]):
        buy_idx = sig_idx + 1
        if buy_idx >= n:
            continue

        # ===== 买入日监管异动：不买但记录 =====
        if abnormal_flag[buy_idx]:
            results.append({
                "股票代码": df.loc[sig_idx, "code"],
                "股票名称": df.loc[sig_idx, "name"],
                "信号日期": df.loc[sig_idx, "date"],
                "首次买入日期": df.loc[buy_idx, "date"],
                "卖出日期": df.loc[buy_idx, "date"],
                "卖出原因": f"监管异动不买入：{abnormal_reason[buy_idx]}",
                "异动原因": abnormal_reason[buy_idx],
                "持仓股数": 0,
                "成本均价": 0.0,
                "卖出价": 0.0,
                "盈亏比例(%)": 0.0,
                "加仓次数": 0,
                "持仓天数": 0
            })
            continue

        # ===== 建仓 =====
        total_shares = shares_per_buy
        total_cost = open_p[buy_idx] * shares_per_buy
        avg_price = open_p[buy_idx]
        add_times = 0

        sell_idx = n - 1
        sell_reason = "回测结束未触发卖出"
        abnormal_mark = ""

        # ===== 持仓推进 =====
        for i in range(buy_idx + 1, n):

            # 加仓
            if signal[i] and not abnormal_flag[i] and not limit_up[i]:
                total_shares += shares_per_buy
                total_cost += close_p[i] * shares_per_buy
                avg_price = total_cost / total_shares
                add_times += 1

            # 卖出条件
            if close_p[i] < open_p[i]:
                sell_idx = i
                sell_reason = "收阴卖出"
                break

            if (close_p[i] / close_p[i - 1] - 1) <= -0.02:
                sell_idx = i
                sell_reason = "单日大跌卖出"
                break

            if abnormal_flag[i]:
                sell_idx = i
                sell_reason = f"监管异动卖出：{abnormal_reason[i]}"
                abnormal_mark = abnormal_reason[i]
                break

        sell_price = close_p[sell_idx]

        results.append({
            "股票代码": df.loc[sig_idx, "code"],
            "股票名称": df.loc[sig_idx, "name"],
            "信号日期": df.loc[sig_idx, "date"],
            "首次买入日期": df.loc[buy_idx, "date"],
            "卖出日期": df.loc[sell_idx, "date"],
            "卖出原因": sell_reason,
            "异动原因": abnormal_mark,
            "持仓股数": total_shares,
            "成本均价": round(avg_price, 3),
            "卖出价": sell_price,
            "盈亏比例(%)": round((sell_price / avg_price - 1) * 100, 2),
            "加仓次数": add_times,
            "持仓天数": sell_idx - buy_idx + 1
        })

    return pd.DataFrame(results)


def backtest_add_position_close_buy(df, shares_per_buy=100):
    results = []

    signal_idx_list = get_first_signals(df)

    for sig_idx in signal_idx_list:
        buy_idx = sig_idx + 1
        if buy_idx >= len(df):
            continue

        # ===== 监管异动检查（但不跳过信号）=====
        is_abnormal, abnormal_reason = regulatory_abnormal_trigger(df, buy_idx)

        # ==============================
        # 情况一：触发监管异动 → 不买入，但记录
        # ==============================
        if is_abnormal:
            results.append({
                "股票代码": df.loc[sig_idx, "code"],
                "股票名称": df.loc[sig_idx, "name"],
                "信号日期": df.loc[sig_idx, "date"],
                "首次买入日期": df.loc[buy_idx, "date"],
                "卖出日期": df.loc[buy_idx, "date"],
                "卖出原因": f"监管异动不买入：{abnormal_reason}",
                "异动原因": abnormal_reason,
                "持仓股数": 0,
                "成本均价": 0.0,
                "卖出价": 0.0,
                "盈亏比例(%)": 0.0,
                "加仓次数": 0,
                "持仓天数": 0
            })
            continue

        # ==============================
        # 情况二：正常交易逻辑
        # ==============================
        total_shares = shares_per_buy
        buy_price = df.loc[buy_idx, "open"]
        total_cost = buy_price * shares_per_buy
        avg_price = buy_price

        add_times = 0
        sell_reason = "回测结束未触发卖出"
        abnormal_mark = ""

        # ★ 关键：提前初始化 sell_idx（兜底）
        sell_idx = buy_idx

        # ===== 持仓推进 =====
        for i in range(buy_idx + 1, len(df)):
            today_open = df.loc[i, "open"]
            today_close = df.loc[i, "close"]
            yesday_close = df.loc[i - 1, "close"]

            window = df.iloc[:i - 1]
            is_signal = is_valid_stock(window)

            is_abnormal, abnormal_reason = regulatory_abnormal_trigger(df, i)

            # ===== 加仓（仅在未触发监管异动时）=====
            if is_signal and not is_abnormal:
                if not is_limit_up_today(df, i):
                    total_shares += shares_per_buy
                    total_cost += today_close * shares_per_buy
                    avg_price = total_cost / total_shares
                    add_times += 1

            # ===== 卖出条件 =====
            if today_close < today_open:
                sell_idx = i
                sell_reason = "收阴卖出"
                break

            if (yesday_close - today_close) / yesday_close < -0.02:
                sell_idx = i
                sell_reason = "单日大跌卖出"
                break

            if is_abnormal:
                sell_idx = i
                sell_reason = f"监管异动卖出：{abnormal_reason}"
                abnormal_mark = abnormal_reason
                break

        # ===== 如果循环正常跑完（没有 break）=====
        if sell_idx == buy_idx:
            sell_idx = len(df) - 1

        sell_price = df.loc[sell_idx, "close"]

        results.append({
            "股票代码": df.loc[sig_idx, "code"],
            "股票名称": df.loc[sig_idx, "name"],
            "信号日期": df.loc[sig_idx, "date"],
            "首次买入日期": df.loc[buy_idx, "date"],
            "卖出日期": df.loc[sell_idx, "date"],
            "卖出原因": sell_reason,
            "异动原因": abnormal_mark,
            "持仓股数": total_shares,
            "成本均价": round(avg_price, 3),
            "卖出价": sell_price,
            "盈亏比例(%)": round((sell_price / avg_price - 1) * 100, 2),
            "加仓次数": add_times,
            "持仓天数": sell_idx - buy_idx + 1
        })

    return pd.DataFrame(results)

bt_df= backtest_add_position_close_buy_fast(df)
bt_df.to_excel('202060209.xlsx',index=False)