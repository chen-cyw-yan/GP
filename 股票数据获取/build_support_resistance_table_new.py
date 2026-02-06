#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2026/2/5
# @Author : chenyanwen

import time
import random
import logging
from datetime import date
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import akshare as ak
import pymysql

# ================= 日志配置 =================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

# ================= 数据库配置 =================
DB_CONFIG = {
    "host": "127.0.0.1",
    "user": "root",
    "password": "chen",
    "database": "gp",
    "charset": "utf8mb4",
    "autocommit": True
}

# ================= DB 工具 =================
def get_db_conn():
    return pymysql.connect(**DB_CONFIG)

def batch_insert(sql, values):
    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            cur.executemany(sql, values)
    finally:
        conn.close()

# ================= 防重复计算 =================
def get_symbols_to_calc(trade_date):
    sql = f"""
    SELECT  distinct s.code
    FROM gp.stock s
    LEFT JOIN gp.tick_support_resistance t
      ON s.code = t.symbol
     AND t.trade_date = '{trade_date}'
    WHERE t.symbol IS NULL
    """
    conn = get_db_conn()
    try:
        return pd.read_sql(sql, conn)["code"].tolist()
    finally:
        conn.close()

# ================= AkShare 分笔数据 =================
def get_tick_data(symbol, retry=3):
    for i in range(retry):
        try:
            time.sleep(random.uniform(0.4, 0.9))  # ⭐ 限速防封
            df = ak.stock_zh_a_tick_tx_js(symbol=symbol)
            if df is not None and not df.empty:
                return df
        except Exception as e:
            logger.warning(f"{symbol} 第{i+1}次获取失败: {e}")
            time.sleep(1)
    return None

# ================= 计算核心 =================
def build_support_resistance(
    df,
    trade_date,
    symbol,
    price_bin=0.01,
    dense_ratio=0.3
):
    df = df.copy()

    total_volume = df["成交量"].sum()
    total_amount = df["成交金额"].sum()

    if total_volume == 0 or total_amount == 0:
        return None

    # VWAP
    vwap = total_amount / total_volume

    # 价格分桶
    df["price_bin"] = (df["成交价格"] / price_bin).round() * price_bin
    df["price_bin"] = df["price_bin"].round(3)

    profile = (
        df.groupby("price_bin")["成交金额"]
        .sum()
        .sort_values(ascending=False)
    )

    threshold = profile.sum() * dense_ratio
    dense = profile[profile.cumsum() <= threshold]

    if dense.empty:
        return None

    dense_lower = dense.index.min()
    dense_upper = dense.index.max()

    return (
        trade_date,
        symbol,
        round(dense_lower, 3),     # support_price
        round(dense_upper, 3),     # resistance_price
        round(vwap, 3),
        round(dense_lower, 3),
        round(dense_upper, 3),
        dense_ratio,
        "amount_profile",
        price_bin,
        int(total_amount),
        int(total_volume)
    )

# ================= Worker（线程安全） =================
def worker(symbol, trade_date):
    df_tick = get_tick_data(symbol)
    if df_tick is None or df_tick.empty:
        return None

    return build_support_resistance(
        df_tick,
        trade_date,
        symbol
    )

# ================= 主程序 =================
def main():
    trade_date = date.today().strftime("%Y-%m-%d")
    symbols = get_symbols_to_calc(trade_date)

    logger.info(f"待计算股票数量: {len(symbols)}")

    results = []

    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = {pool.submit(worker, s, trade_date): s for s in symbols}

        for future in as_completed(futures):
            res = future.result()
            if res:
                results.append(res)

    if not results:
        logger.info("无新增结果")
        return

    insert_sql = """
    REPLACE INTO gp.tick_support_resistance (
        trade_date,
        symbol,
        support_price,
        resistance_price,
        vwap,
        dense_lower,
        dense_upper,
        dense_ratio,
        calc_method,
        price_bin,
        total_amount,
        total_volume
    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """

    batch_insert(insert_sql, results)
    logger.info(f"成功写入 {len(results)} 条记录")

# ================= 入口 =================
if __name__ == "__main__":
    main()
