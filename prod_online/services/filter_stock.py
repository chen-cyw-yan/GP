import logging
import time
import akshare as ak
import pandas as pd
import numpy as np

from datetime import datetime, timedelta
import tqdm
# import pyecharts.options as opts
# from pyecharts.charts import Line
import pandas as pd
from sqlalchemy import create_engine
from sklearn.linear_model import LinearRegression
import matplotlib.pyplot as plt
from sqlalchemy import create_engine
import pymysql
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)
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


import pandas as pd
import numpy as np


def precompute_regulatory_abnormal_vectorized(df):
    close = df["close"]

    # 初始化
    abnormal_flag = pd.Series(False, index=close.index)
    abnormal_reason = pd.Series("", dtype="object", index=close.index)

    # 1. 计算收益率 (保留原始浮点数用于比较，避免四舍五入影响判断)
    # 注意：shift(3) 前3行会是 NaN，abs() 后也是 NaN，比较时会自动为 False
    pct_3 = close / close.shift(3) - 1
    pct_5 = close / close.shift(5) - 1
    pct_10 = close / close.shift(10) - 1

    # 2. 定义基础条件 (不再互斥，允许同时满足)
    cond3 = pct_3.abs() >= 0.20
    cond5 = pct_5.abs() >= 0.30
    cond10 = pct_10.abs() >= 0.50

    # 3. 合并标志位
    abnormal_flag = cond3 | cond5 | cond10

    # 4. 格式化字符串 (只在需要显示时格式化，提高效率)
    # 使用 mask 或 where 可以避免对全量数据进行 format，但为了代码清晰，这里直接格式化
    # 注意处理 NaN，fillna("") 或者在 map 前处理
    fmt3 = pct_3.fillna(0).map("{:.2%}".format)
    fmt5 = pct_5.fillna(0).map("{:.2%}".format)
    fmt10 = pct_10.fillna(0).map("{:.2%}".format)

    # 5. 核心修改：按“严重程度”或“优先级”赋值
    # 策略：先写3日，再写5日（覆盖3日），再写10日（覆盖5日）
    # 这样如果同时满足3日和5日，最终显示的是5日（通常长周期大涨幅更值得关注）

    # 初始化 reason 为空
    abnormal_reason[:] = ""

    # 第一层：3日
    mask3 = cond3
    abnormal_reason.loc[mask3] = "3日涨跌幅异常(" + fmt3.loc[mask3] + ")"

    # 第二层：5日 (直接覆盖，不需要 & ~cond3)
    mask5 = cond5
    abnormal_reason.loc[mask5] = "5日涨跌幅异常(" + fmt5.loc[mask5] + ")"

    # 第三层：10日 (直接覆盖)
    mask10 = cond10
    abnormal_reason.loc[mask10] = "10日涨跌幅异常(" + fmt10.loc[mask10] + ")"

    # 【可选进阶】如果你想同时显示所有触发的规则（例如："3日...; 5日..."）
    # 则需要用字符串拼接而不是覆盖，代码如下：
    # reasons = []
    # if cond3.any(): reasons.append("3日..." + fmt3) ...
    # 但通常单一最严重原因更清晰。

    return abnormal_flag, abnormal_reason


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


def precompute_regulatory_abnormal_vectorized(df):
    """
    检测当前是否触发异动 (3日/5日/10日)
    逻辑：优先级覆盖 (10日 > 5日 > 3日)
    """
    close = df["close"]

    # 初始化
    abnormal_flag = pd.Series(False, index=close.index)
    abnormal_reason = pd.Series("", dtype="object", index=close.index)

    # 1. 计算区间涨跌幅 (注意：这里用的是绝对涨幅，非偏离值)
    # 如果需符合严格监管，建议改为：(close / close.shift(N)) - 1 - (index_close / index_close.shift(N) - 1)
    pct_3 = close / close.shift(3) - 1
    pct_5 = close / close.shift(5) - 1
    pct_10 = close / close.shift(10) - 1

    # 补充：如果有30日数据，建议加上
    pct_30 = close / close.shift(30) - 1

    # 2. 定义阈值 (根据您的需求：3日20%, 5日30%, 10日50%)
    # 注意：标准监管10日通常是100%，这里沿用您代码的50%作为自定义监控
    cond3 = pct_3.abs() >= 0.20
    cond5 = pct_5.abs() >= 0.30
    cond10 = pct_10.abs() >= 0.50
    cond30 = pct_30.abs() >= 1.00  # 示例：30日翻倍

    # 3. 合并标志位
    abnormal_flag = cond3 | cond5 | cond10 | cond30

    # 4. 格式化
    fmt3 = pct_3.fillna(0).map("{:.2%}".format)
    fmt5 = pct_5.fillna(0).map("{:.2%}".format)
    fmt10 = pct_10.fillna(0).map("{:.2%}".format)
    fmt30 = pct_30.fillna(0).map("{:.2%}".format)

    # 5. 优先级赋值 (低 -> 高 覆盖)
    abnormal_reason[:] = ""

    mask3 = cond3
    abnormal_reason.loc[mask3] = "3日涨跌幅异常(" + fmt3.loc[mask3] + ")"

    mask5 = cond5
    abnormal_reason.loc[mask5] = "5日涨跌幅异常(" + fmt5.loc[mask5] + ")"

    mask10 = cond10
    abnormal_reason.loc[mask10] = "10日涨跌幅异常(" + fmt10.loc[mask10] + ")"

    mask30 = cond30
    abnormal_reason.loc[mask30] = "30日涨跌幅异常(" + fmt30.loc[mask30] + ")"

    return abnormal_flag, abnormal_reason, cond3, cond5, cond10, cond30


def precompute_next_level_gap(df, cond3, cond5, cond10, cond30):
    """
    计算距离【下一个更高级别】异动所需的明日涨幅
    逻辑：阶梯式判断。
    - 若当前无异动 -> 算距离3日还差多少
    - 若当前已3日 -> 算距离5日还差多少
    - 若当前已5日 -> 算距离10日还差多少
    - 若当前已10日 -> 算距离30日还差多少
    """
    close = df["close"]

    # 定义各级别的阈值
    THRESH = {
        '3': 0.20,
        '5': 0.30,
        '10': 0.50,
        '30': 1.00
    }

    # 准备结果列
    next_level_name = pd.Series("", index=close.index, dtype="object")
    required_pct = pd.Series(np.nan, index=close.index)
    is_possible = pd.Series(False, index=close.index)

    # 辅助函数：计算所需涨幅
    # 公式：current * (1+x) / past_close - 1 = thresh  =>  x = (1+thresh)*past_close/current - 1
    def calc_gap(past_close_series, threshold):
        return (1 + threshold) * past_close_series / close - 1

    # --- 第一阶梯：计算距离 3日异动 的差距 (针对当前无异动的股票) ---
    # 条件：非3日 且 非5日 且 非10日 且 非30日
    mask_none = ~(cond3 | cond5 | cond10 | cond30)
    if mask_none.any():
        c3 = close.shift(2)  # 过去2天
        req = calc_gap(c3, THRESH['3'])
        required_pct.loc[mask_none] = req.loc[mask_none]
        next_level_name.loc[mask_none] = "3日异动"

    # --- 第二阶梯：计算距离 5日异动 的差距 (针对当前仅触发3日的股票) ---
    # 条件：是3日 但 不是5日 (且不是更高)
    mask_3_only = cond3 & (~cond5) & (~cond10) & (~cond30)
    if mask_3_only.any():
        c5 = close.shift(4)  # 过去4天
        req = calc_gap(c5, THRESH['5'])
        required_pct.loc[mask_3_only] = req.loc[mask_3_only]
        next_level_name.loc[mask_3_only] = "5日异动"

    # --- 第三阶梯：计算距离 10日异动 的差距 (针对当前仅触发5日的股票) ---
    # 条件：是5日 但 不是10日
    mask_5_only = cond5 & (~cond10) & (~cond30)
    if mask_5_only.any():
        c10 = close.shift(9)  # 过去9天
        req = calc_gap(c10, THRESH['10'])
        required_pct.loc[mask_5_only] = req.loc[mask_5_only]
        next_level_name.loc[mask_5_only] = "10日异动"

    # --- 第四阶梯：计算距离 30日异动 的差距 (针对当前仅触发10日的股票) ---
    # 条件：是10日 但 不是30日
    mask_10_only = cond10 & (~cond30)
    if mask_10_only.any():
        c30 = close.shift(29)  # 过去29天
        req = calc_gap(c30, THRESH['30'])
        required_pct.loc[mask_10_only] = req.loc[mask_10_only]
        next_level_name.loc[mask_10_only] = "30日异动"

    # --- 第五阶梯：已达30日 ---
    mask_30_only = cond30
    if mask_30_only.any():
        next_level_name.loc[mask_30_only] = "已达最高级(30日)"
        required_pct.loc[mask_30_only] = np.nan

    # --- 判断明日是否“可能”触发 (假设涨停限制为10% 或 20%) ---
    # 这里设定阈值为 0.10 (10%)，如果是创业板/科创板可改为 0.20
    limit_up = 0.10
    # 条件：所需涨幅 > 0 (需要涨) 且 所需涨幅 <= 涨停限制
    is_possible = (required_pct > 0) & (required_pct <= limit_up)

    # 格式化输出文本
    reason_text = pd.Series("", index=close.index, dtype="object")
    mask_valid = is_possible & next_level_name.str.contains("异动")

    if mask_valid.any():
        fmt_req = required_pct.loc[mask_valid].map("{:.2%}".format)
        target_name = next_level_name.loc[mask_valid]
        reason_text.loc[mask_valid] = f"明日若涨 {{}} 将触发{{}}".format(fmt_req, target_name)
        # 上面format用法有误，修正如下：
        reason_text.loc[mask_valid] = "明日若涨 " + fmt_req + " 将触发" + target_name

    return is_possible, required_pct, next_level_name, reason_text


def filer_stock():

    # 1. 获取当前日期和时间
    today = datetime.now()

    # 2. 计算前 90 天的日期
    days_ago_90 = today - timedelta(days=90)

    # 3. 格式化输出 (例如：'2025-12-18')
    date_str = days_ago_90.strftime('%Y-%m-%d')

    print(f"当前日期: {today.strftime('%Y-%m-%d')}")
    print(f"90天前: {date_str}")

    df = pd.read_sql(f"select * from gp.stock where date>='{date_str}'", con=conn)
    base_df = df
    result = []
    # df['是否触发异动'], df['异动类型'] = precompute_regulatory_abnormal_vectorized(df)
    # df["下一日可能触发异动"], df["下一日最小所需涨幅"] = precompute_next_day_abnormal(df)

    logger.info('计算触发异动')

    flag, reason, c3, c5, c10, c30 = precompute_regulatory_abnormal_vectorized(df)

    logger.info('计算触发异动完成')
    df['是否触发异动'] = flag
    df['异动类型'] = reason
    logger.info('计算触发异动计算完成')
    # 2. 再跑下一日预测 (传入中间条件变量)
    logger.info('再跑下一日预测异动所需涨幅')
    possible, req_pct, next_lvl, next_reason = precompute_next_level_gap(df, c3, c5, c10, c30)

    df['下一日可能触发'] = possible
    df['所需最小涨幅'] = req_pct
    df['目标等级'] = next_lvl
    df['预警信息'] = next_reason

    logger.info('剔除高位票')
    # df=df.loc[~(df['异动类型'].str.contains('10日涨跌幅')|df['异动类型'].str.contains('10日涨跌幅'))]
    df=df.loc[~df['异动类型'].str.contains('10日涨跌幅')]
    logger.info('剔除高位票完成')

    for code, g in tqdm.tqdm(df.groupby("code")):
        g = g.sort_values("date").reset_index(drop=True)

        # ⭐ 只保留最近60条记录
        if len(g) > 60:
            g = g.iloc[-60:].reset_index(drop=True)

        in_signal = False
        signal_seq = 0

        for i in range(30, len(g)):
            window = g.iloc[:i + 1]
            is_signal = is_valid_stock(window)

            if is_signal:
                if not in_signal:
                    in_signal = True
                    signal_seq = 1
                else:
                    signal_seq += 1

                result.append({
                    "代码": code,
                    "名称": g.loc[i, "name"],
                    "日期": g.loc[i, "date"],
                    "收盘价": g.loc[i, "close"],
                    "触发信号次数": signal_seq,
                    "是否异动类型": g.loc[i, '异动类型'],
                    "下一日可能触发": g.loc[i, "下一日可能触发"],
                    "所需最小涨幅": g.loc[i, "所需最小涨幅"],
                    "目标等级":g.loc[i, '目标等级'],
                    "预警信息":g.loc[i, '预警信息']
                })
            else:
                in_signal = False
                signal_seq = 0

    res_df = pd.DataFrame(result)
    res_df=res_df.sort_values('日期',ascending=False)
    return res_df