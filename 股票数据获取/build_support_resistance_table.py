#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2026/2/5 10:18
# @Author : chenyanwen
# @email:1183445504@qq.com
import time
import akshare as ak
import pandas as pd
import numpy as np
import tqdm
import random
# import pyecharts.options as opts
# from pyecharts.charts import Line
import pandas as pd
from sqlalchemy import create_engine
from sklearn.linear_model import LinearRegression
import matplotlib.pyplot as plt
from datetime import datetime
from dateutil.relativedelta import relativedelta
from sqlalchemy import create_engine
import pymysql
import logging
# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)
# 获取当前日期（不含时间）
today = datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)
con = create_engine(f"mysql+pymysql://root:chen@127.0.0.1:3306/gp")
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
def get_stocks_price_item(code):
    logger.info('获取今日成交密集区')
    stock_zh_a_tick_tx_js_df = ak.stock_zh_a_tick_tx_js(symbol=code)
    return stock_zh_a_tick_tx_js_df
def need_to_get(dates):
    sqls=f'select distinct ts.symbol as code  from gp.tick_support_resistance as ts where ts.trade_date !="{dates}"'
    dfs=pd.read_sql(sql=sqls,con=con)
    if dfs.empty:
        df=pd.read_sql("select distinct code from gp.stock",con=con)
    else:
        df=dfs
    return df
def build_support_resistance_table(
    df: pd.DataFrame,
    trade_date: str,
    symbol: str,
    price_bin: float = 0.01,
    dense_ratio: float = 0.3,
    method: str = "amount"   # amount / volume
):
    data = df

    # 1. VWAP
    vwap = (data["成交价格"] * data["成交量"]).sum() / data["成交量"].sum()

    # 2. 价格分桶
    data["price_bin"] = (data["成交价格"] / price_bin).round(0) * price_bin
    data["price_bin"] = data["price_bin"].round(3)

    # 3. 选择用成交金额 or 成交量
    if method == "amount":
        profile = data.groupby("price_bin")["成交金额"].sum()
    else:
        profile = data.groupby("price_bin")["成交量"].sum()

    profile = profile.sort_values(ascending=False)

    # 4. 密集成交区
    total = profile.sum()
    dense_bins = profile[(profile.cumsum() / total) <= dense_ratio]

    dense_lower = dense_bins.index.min()
    dense_upper = dense_bins.index.max()

    # 5. 支撑 / 压力定义
    support_price = dense_lower
    resistance_price = dense_upper

    # 6. 结果表
    result = pd.DataFrame([{
        "trade_date": trade_date,
        "symbol": symbol,

        "support_price": support_price,
        "resistance_price": resistance_price,

        "vwap": round(vwap, 3),

        "dense_lower": dense_lower,
        "dense_upper": dense_upper,
        "dense_ratio": dense_ratio,

        "calc_method": f"{method}_profile",
        "price_bin": price_bin,

        "total_amount": data["成交金额"].sum(),
        "total_volume": data["成交量"].sum()
    }])

    return result
if __name__ == '__main__':
    today=today.strftime("%Y-%m-%d")
    need_df=need_to_get(today)
    print(need_df)
    df_ls=[]
    for k,v in need_df.iterrows():
        code=v['code']
        logger.info(f'读取数据{code}')
        today_df=get_stocks_price_item(code)
        print(today_df)
        support_resistance=build_support_resistance_table(today_df,trade_date=today,
        symbol=code,
        price_bin=0.01,
        dense_ratio=0.3,
        method="amount")
        df_ls.append(support_resistance)
        print(support_resistance)
    all_df=pd.concat(df_ls)
    sql = f"REPLACE INTO gp.tick_support_resistance(`{'`,`'.join(all_df.columns)}`) VALUES ({','.join(['%s' for _ in range(all_df.shape[1])])})"
    toSql(sql=sql, rows=all_df.values.tolist())