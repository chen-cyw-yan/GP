#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2026/2/24 10:16
# @Author : chenyanwen
# @email:1183445504@qq.com
import time
import akshare as ak
import pandas as pd
import numpy as np
import tqdm
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
from sqlalchemy import create_engine
from datetime import datetime
from dateutil.relativedelta import relativedelta
import pymysql
import logging

# ==============================
# 日志配置
# ==============================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# ==============================
# 日期范围
# ==============================
today = datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)
one_year_ago = today - relativedelta(years=1)

start_date = one_year_ago.strftime("%Y%m%d")
end_date = today.strftime("%Y%m%d")

logger.info(f"当前日期：{end_date}")
logger.info(f"近一年起始日期：{start_date}")

# ==============================
# 数据库连接
# ==============================
engine = create_engine("mysql+pymysql://root:chen@127.0.0.1:3306/gp")

conn = pymysql.connect(
    host='127.0.0.1',
    user='root',
    password='chen',
    database='gp',
    autocommit=False
)

cursor = conn.cursor()

def toSql(sql: str, rows: list):
    try:
        cursor.executemany(sql, rows)
        conn.commit()
    except Exception as e:
        conn.rollback()
        logger.error(f"数据库写入失败: {e}")
        raise


# ==============================
# 获取实时股票列表
# ==============================
def get_today_data():
    df = ak.stock_zh_a_spot()

    df["代码"] = df["代码"].astype(str)
    df["名称"] = df["名称"].astype(str)

    # 排除科创板
    df = df.loc[~df["代码"].str.startswith(("bj", "sh688", "sh689", "sz688", "sz689"))]

    # 排除ST
    df = df.loc[~df["名称"].str.contains("ST")]

    return df


# ==============================
# 单只股票数据获取（线程执行）
# ==============================
def fetch_stock_data(row, retry=3):
    symbol = row['代码']
    name = row['名称']

    for i in range(retry):
        try:
            df = ak.stock_zh_a_daily(
                symbol=symbol,
                start_date=start_date,
                end_date=end_date,
                adjust="qfq"
            )

            if df is None or df.empty:
                return None

            df['code'] = symbol
            df['name'] = name

            logger.info(f"完成: {symbol}")
            return df

        except Exception as e:
            logger.warning(f"{symbol} 第{i+1}次失败: {e}")
            time.sleep(1)

    logger.error(f"{symbol} 获取失败")
    return None


# ==============================
# 多线程主函数
# ==============================
def get_stocks_year_multithread(stocks_df, max_workers=6):
    logger.info(f"开始多线程获取，共 {len(stocks_df)} 只股票")

    all_results = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(fetch_stock_data, row)
            for _, row in stocks_df.iterrows()
        ]

        for future in as_completed(futures):
            result = future.result()
            if result is not None:
                all_results.append(result)

    if not all_results:
        logger.warning("没有获取到数据")
        return

    logger.info("数据获取完成，开始批量入库")

    # 合并成一个大 DataFrame
    big_df = pd.concat(all_results, ignore_index=True)

    sql = f"""
    REPLACE INTO gp.stock(`{'`,`'.join(big_df.columns)}`)
    VALUES ({','.join(['%s' for _ in range(big_df.shape[1])])})
    """

    toSql(sql=sql, rows=big_df.values.tolist())

    logger.info("全部入库完成")


# ==============================
# 主程序
# ==============================
if __name__ == '__main__':

    code = 'sh601398'
    basic_df = ak.stock_zh_a_daily(
        symbol=code,
        start_date=start_date,
        end_date=end_date,
        adjust="qfq"
    ).sort_values(by='date')

    first_date = basic_df.iloc[0, 0].strftime("%Y-%m-%d")
    cnt = basic_df.index.size

    logger.info(f"基准股票起始时间:{first_date}, 数据条数:{cnt}")

    sqls = f"""
    SELECT count(1) as cnt, code 
    FROM gp.stock 
    WHERE `date`>='{first_date}' 
      AND outstanding_share!=0 
    GROUP BY code  
    HAVING count(1) < {cnt}
    """

    need_get_stock = pd.read_sql(sql=sqls, con=engine)

    today_data_df = get_today_data()

    need_get_stock_df = today_data_df.loc[
        today_data_df['代码'].isin(need_get_stock['code'].to_list())
    ]

    logger.info(f"需要补充股票数量: {len(need_get_stock_df)}")

    get_stocks_year_multithread(need_get_stock_df, max_workers=6)

    cursor.close()
    conn.close()