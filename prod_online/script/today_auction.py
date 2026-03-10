#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2026/2/24 10:16
# @Author : chenyanwen
# @email:1183445504@qq.com
import sys
import os
import dataframe_image as dfi
sys.path.append(
    os.path.abspath(
        os.path.join(os.path.dirname(__file__), "../../")
    )
)
import json
from prod_online.config.feishu_utils import FeishuUtils
import warnings
import prod_online.config.utils as utils
import pandas as pd
import requests
import time
import sys
import akshare as ak
import pandas as pd
import numpy as np
from concurrent.futures import ThreadPoolExecutor, as_completed
from sqlalchemy import create_engine
from datetime import datetime
from dateutil.relativedelta import relativedelta
import pymysql
import logging
# import prod_online.config.utils as utils
# ==============================
# 日志配置
# ==============================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)
from concurrent.futures import ThreadPoolExecutor, as_completed
# ==============================
# 数据库连接
# ==============================
engine = create_engine("mysql+pymysql://root:chen@127.0.0.1:3306/gp?charset=utf8mb4")

def stock_zh_a_tick_tx_js(symbol: str = "sz000001",timeout=30,page_size=-1) -> pd.DataFrame:
    """
    腾讯财经-历史分笔数据
https://gu.qq.com/sz300494/gp/detail
    :param symbol: 股票代码
    :type symbol: str
    :return: 历史分笔数据
    :rtype: pandas.DataFrame
    """
    big_df = pd.DataFrame()
    page = 0
    warnings.warn("正在下载数据，请稍等")
    if page_size == -1:
        while True:
            try:
                url = "http://stock.gtimg.cn/data/index.php"
                params = {
                    "appn": "detail",
                    "action": "data",
                    "c": symbol,
                    "p": page,
                }
                r = requests.get(url, params=params,timeout=timeout)
                text_data = r.text
                temp_df = (
                    pd.DataFrame(eval(text_data[text_data.find("[") :])[1].split("|"))
                    .iloc[:, 0]
                    .str.split("/", expand=True)
                )
                page += 1
                big_df = pd.concat([big_df, temp_df], ignore_index=True)
            except:  # noqa: E722
                break
    else:
        while page < page_size:
            try:
                url = "http://stock.gtimg.cn/data/index.php"
                params = {
                    "appn": "detail",
                    "action": "data",
                    "c": symbol,
                    "p": page,
                }
                r = requests.get(url, params=params,timeout=timeout)
                text_data = r.text
                temp_df = (
                    pd.DataFrame(eval(text_data[text_data.find("[") :])[1].split("|"))
                    .iloc[:, 0]
                    .str.split("/", expand=True)
                )
                page += 1
                big_df = pd.concat([big_df, temp_df], ignore_index=True)
            except:  # noqa: E722
                break
    if not big_df.empty:
        big_df = big_df.iloc[:, 1:].copy()
        big_df.columns = [
            "成交时间",
            "成交价格",
            "价格变动",
            "成交量",
            "成交金额",
            "性质",
        ]
        big_df.reset_index(drop=True, inplace=True)
        property_map = {
            "S": "卖盘",
            "B": "买盘",
            "M": "中性盘",
        }
        big_df["性质"] = big_df["性质"].map(property_map)
        big_df = big_df.astype(
            {
                "成交时间": str,
                "成交价格": float,
                "价格变动": float,
                "成交量": int,
                "成交金额": int,
                "性质": str,
            }
        )
    return big_df
# 竞价指标计算
def calc_auction_factors(df1, df2, df3):

    # ===== 今日竞价 =====
    row = df1.iloc[0]

    auction_price = row["成交价格"]
    auction_volume = row["成交量"]
    auction_amount = row["成交金额"]

    # ===== 昨日行情 =====
    prev_close = df3.iloc[0]["close"]
    prev_high = df3.iloc[0]["high"]
    prev_low = df3.iloc[0]["low"]
    float_shares = df3.iloc[0]["outstanding_share"]
    prev_amount = df3.iloc[0]["amount"]

    float_market_cap = prev_close * float_shares

    # ===== 历史竞价 =====
    hist_vol_mean = df2["volume"].mean()
    hist_amt_mean = df2["amount"].mean()

    # ======================
    # 1 竞价涨幅
    # ======================
    auction_ret = auction_price / prev_close - 1

    # ======================
    # 2 竞价金额强度
    # ======================
    auction_strength = auction_amount / float_market_cap

    # ======================
    # 3 竞价换手率
    # ======================
    auction_turnover = auction_volume / float_shares

    # ======================
    # 4 竞价量比
    # ======================
    auction_vol_ratio = (
        auction_volume / hist_vol_mean
        if hist_vol_mean != 0 else np.nan
    )

    # ======================
    # 5 竞价资金密度
    # ======================
    auction_density = (
        auction_amount / auction_volume
        if auction_volume != 0 else np.nan
    )

    # ======================
    # 6 竞价位置
    # ======================
    auction_pos = (
        (auction_price - prev_low) /
        (prev_high - prev_low)
        if (prev_high - prev_low) != 0 else np.nan
    )

    # ======================
    # 7 竞价金额放大倍数
    # ======================
    auction_amt_ratio = (
        auction_amount / hist_amt_mean
        if hist_amt_mean != 0 else np.nan
    )

    # ======================
    # 8 竞价资金占昨日成交比例
    # ======================
    auction_vs_yesterday = (
        auction_amount / prev_amount
        if prev_amount != 0 else np.nan
    )

    factors = {
        "auction_price": auction_price,
        "auction_volume": auction_volume,
        "auction_amount": auction_amount,

        "auction_ret": auction_ret,
        "auction_strength": auction_strength,
        "auction_turnover": auction_turnover,
        "auction_vol_ratio": auction_vol_ratio,
        "auction_density": auction_density,
        "auction_pos": auction_pos,
        "auction_amt_ratio": auction_amt_ratio,
        "auction_vs_yesterday": auction_vs_yesterday
    }

    return factors



# 竞价评分
def calc_auction_score(f):

    score = (
        0.25 * f["auction_ret"] +
        0.20 * f["auction_vol_ratio"] +
        0.15 * f["auction_turnover"] +
        0.15 * f["auction_amt_ratio"] +
        0.15 * f["auction_pos"] +
        0.10 * f["auction_vs_yesterday"]
    )

    return score

# 竞价筛选
def auction_filter(f):

    if not (-0.01 <= f["auction_ret"] <= 0.04):
        return False

    if f["auction_vol_ratio"] < 3:
        return False

    if f["auction_turnover"] < 0.002:
        return False

    if f["auction_amt_ratio"] < 2:
        return False

    if f["auction_vs_yesterday"] < 0.03:
        return False

    return True

# 竞价分析
def analyze_auction(df1, df2, df3):

    factors = calc_auction_factors(df1, df2, df3)

    score = calc_auction_score(factors)

    selected = auction_filter(factors)

    factors["score"] = score
    factors["selected"] = selected

    return factors
def analyze_stock(row, trade_date):
    try:
        ts_code = row["stock_code"]
        print(f"分析 {ts_code}")

        sql2 = f'''
        select * from gp.stock_call_auction
        where stock_code="{ts_code}" order by trade_date desc limit 20
        '''
        df2 = pd.read_sql(sql2, engine)
        df2['volume']=df2['volume']*100
        df1 = stock_zh_a_tick_tx_js(
            symbol=ts_code,
            timeout=60,
            page_size=1
        )
        df1['成交量']=df1['成交量']*100

        sql3 = f'''
        select * from gp.stock
        where code="{ts_code}"
        and date>="{trade_date}"
        '''
        df3 = pd.read_sql(sql3, engine)
        df3['volume']=df3['volume']*100
        if df1.empty or df2.empty or df3.empty:
            print(df1.empty , df2.empty , df3.empty)
            return None
        # 当天竞价成交量
        today_auction_vol = df1["成交量"].sum()

        # 当天买卖性质
        buy_vol = df1[df1["性质"]=="买盘"]["成交量"].sum()
        sell_vol = df1[df1["性质"]=="卖盘"]["成交量"].sum()

        if buy_vol > sell_vol:
            trade_side = "买盘主导"
        elif sell_vol > buy_vol:
            trade_side = "卖盘主导"
        else:
            trade_side = "买卖平衡"

        # 前三天竞价成交量
        last3_vol = df2.head(3)["volume"].sum()
        
        result = analyze_auction(df1, df2, df3)
        result["today_auction_vol"] = today_auction_vol
        result["last3_auction_vol"] = last3_vol
        result["trade_side"] = trade_side
        result["stock_code"] = ts_code
        result["stock_name"] = row["stock_name"]
        result["signal_date"] = row["trade_date"]
        return result

    except Exception as e:
        print(f"{row['stock_code']} 出错 {e}")
        return None

if __name__ == "__main__":

    today_dt = datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)
    today_str = today_dt.strftime("%Y-%m-%d")
    # today_str ='2026-03-06'
    logger.info(f"当前任务日期: {today_str}")

    if not utils.is_trading_day_ak(today_str):
        logger.warning(f"⚠️ {today_str} 不是 A 股交易日，程序安全退出。")
        sys.exit(0)
    trade_date=utils.get_prev_n_trading_days(n=3)
    trade_date=trade_date[0]
    sql = f'''
    select * from gp.stock_abnormal_monitor
    where trade_date >= "{trade_date}"
    '''

    alany_df = pd.read_sql(sql, engine)

    results = []

    max_workers = 8

    with ThreadPoolExecutor(max_workers=max_workers) as executor:

        futures = [
            executor.submit(analyze_stock, row, trade_date)
            for _, row in alany_df.iterrows()
        ]

        for future in as_completed(futures):

            try:
                res = future.result()

                if res:
                    results.append(res)

            except Exception as e:
                print("线程异常", e)

    res_df = pd.DataFrame(results)
    # print(res_df)
    factor_name_map = {
    "stock_code":'股票代码',
    'stock_name': '股票名称',
    "signal_date": "信号日期",
    "auction_price": "竞价价格",
    "auction_volume": "竞价成交量",
    "auction_amount": "竞价成交额",
    "auction_ret": "竞价涨幅",
    "auction_strength": "竞价金额强度",
    "auction_turnover": "竞价换手率",
    "auction_vol_ratio": "竞价量比",
    "auction_density": "竞价资金密度",
    "auction_pos": "竞价位置",
    "auction_amt_ratio": "竞价金额放大倍数",
    "auction_vs_yesterday": "竞价资金占昨日成交比",
    "score": "竞价评分",
    "selected": "是否入选",
    "today_auction_vol": "当天竞价成交量",
    "last3_auction_vol": "前三天竞价成交量",
    "trade_side": "当天成交买卖性质",
    }
    res_df.rename(columns=factor_name_map, inplace=True)
    res_df = res_df[
                [
                "股票代码",
                "股票名称",
                "信号日期",
                "当天竞价成交量",
                "前三天竞价成交量",
                "当天成交买卖性质",
                "竞价评分",
                "是否入选",
                "竞价涨幅",
                "竞价量比",
                "竞价换手率",
                "竞价金额放大倍数",
                "竞价资金占昨日成交比",
                "竞价位置",
                "竞价金额强度",
                "竞价资金密度"
                ]
                ]
    res_df.sort_values(by="竞价评分", ascending=False, inplace=True)
    res_df=res_df.drop_duplicates()
    image_path='prod_online/imges/auction.png'
    dfi.export(res_df, image_path, max_rows=100)
    fs_client=FeishuUtils('cli_a9256b2aef7a5cd4','t22QBXS6MVqsXC41GoCDvbxin0tpXyL3')
    message=f"""竞价强度分析，生成日期：{today_str}"""
    context={
        "text":message
    }
    fs_client.set_message_for_text('chat_id','oc_cd642a7fec1dcd847e91b2e1775809d2',json.dumps(context))
    fs_client.set_message_for_image('chat_id', 'oc_cd642a7fec1dcd847e91b2e1775809d2',
                                      image_path)
    # res_df.to_excel(r"竞价分析.xlsx", index=False)
    # print(res_df)