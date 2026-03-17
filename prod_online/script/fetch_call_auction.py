#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2026/2/24 10:16
# @Author : chenyanwen
# @email:1183445504@qq.com
import sys
import os

sys.path.append(
    os.path.abspath(
        os.path.join(os.path.dirname(__file__), "../../")
    )
)
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
import prod_online.config.utils as utils
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
# 数据库连接
# ==============================

#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""
Date: 2023/10/27 22:08
Desc: 腾讯-股票-实时行情-成交明细
成交明细-每个交易日 16:00 提供当日数据
港股报价延时 15 分钟
"""

import warnings

import pandas as pd
import requests


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




READ_ENGINE = create_engine("mysql+pymysql://root:chen@127.0.0.1:3306/gp?charset=utf8mb4")

DB_CONFIG = {
    'host': '127.0.0.1',
    'user': 'root',
    'password': 'chen',
    'database': 'gp',
    'charset': 'utf8mb4'
}

# ==============================
# 单条数据入库函数 (线程安全：每次调用新建连接)
# ==============================
def insert_single_record(data_dict):
    """
    为单个数据记录建立独立数据库连接并插入。
    确保多线程环境下不会冲突。
    """
    conn = None
    cursor = None
    try:
        conn = pymysql.connect(**DB_CONFIG, autocommit=False)
        cursor = conn.cursor()
        
        sql = """
        REPLACE INTO gp.stock_call_auction 
        (trade_date, stock_code, stock_name, auction_time, volume, amount, nature)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        row = (
            data_dict['trade_date'],
            data_dict['stock_code'],
            data_dict['stock_name'],
            data_dict['auction_time'],
            data_dict['volume'],
            data_dict['amount'],
            data_dict['nature']
        )
        
        cursor.execute(sql, row)
        conn.commit()
        logger.info(f"✅ [DB] 成功入库: {data_dict['stock_code']}")
        return True
        
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"❌ [DB] 入库失败 {data_dict.get('stock_code')}: {e}")
        return False
    finally:
        if cursor: cursor.close()
        if conn: conn.close()

# ==============================
# 单只股票数据获取 (含超时控制)
# ==============================
def fetch_and_save_stock(row, timeout_seconds=60):
    symbol = row['code']
    name = row['name']
    trade_date = row['trade_date']
    
    start_time = time.time()
    
    try:
        # --- 1. 获取数据 ---
        logger.info(f"🚀 开始获取: {symbol} ({name})")
        
        # 获取分笔数据
        df = stock_zh_a_tick_tx_js(symbol=symbol,timeout=120,page_size=1)
        
        if df is None or df.empty:
            logger.warning(f"⚠️ {symbol} 数据为空，跳过")
            return "EMPTY"

        # 检查列名
        if '成交时间' not in df.columns:
            logger.warning(f"⚠️ {symbol} 列名异常: {df.columns.tolist()}，跳过")
            return "ERROR_COL"

        # 筛选逻辑 (取第一行作为集合竞价结果，通常 9:25 是第一笔或包含汇总)
        # 注意：akshare 该接口返回的通常是当天的所有分笔，第一行往往是 9:25 的集合竞价撮合结果
        tick_row = df.iloc[0]
        
        # 简单校验时间是否接近 09:25 (可选，防止接口返回顺序变化)
        # time_str = str(tick_row.get('成交时间', ''))
        # if not time_str.startswith('09:25'):
        #     logger.warning(f"{symbol} 首条时间非 09:25 ({time_str})，尝试继续处理...")

        # 数据清洗
        def clean_number(val):
            if val is None or val == '': return 0
            if isinstance(val, (int, float)): return int(val)
            try: return int(float(str(val).replace(',', '')))
            except: return 0

        volume = clean_number(tick_row.get('成交量', 0))
        amount = clean_number(tick_row.get('成交金额', 0))
        nature = str(tick_row.get('性质', '')).strip()

        result = {
            'trade_date': trade_date,
            'stock_code': symbol,
            'stock_name': name,
            'auction_time': '09:25:00',
            'volume': volume,
            'amount': amount,
            'nature': nature
        }



        # --- 3. 立即入库 ---
        success = insert_single_record(result)
        
        if success:
            logger.info(f"✨ 完成: {symbol} | 量:{volume} 额:{amount} ")
            return "SUCCESS"
        else:
            return "DB_FAIL"

    except Exception as e:
        logger.error(f"💥 异常: {symbol} 发生错误: {e}")
        return "ERROR"

# ==============================
# 多线程主控 (含熔断休息机制)
# ==============================
def run_multithread_task(stocks_df, max_workers=8, timeout_limit=60, rest_time=60):
    logger.info(f"📊 任务开始：共 {len(stocks_df)} 只股票，线程数: {max_workers}")
    
    success_count = 0
    fail_count = 0
    timeout_triggered = False

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(fetch_and_save_stock, row): row['code'] 
            for _, row in stocks_df.iterrows()
        }

        for future in as_completed(futures):
            code = futures[future]
            try:
                status = future.result()
                
                if status == "SUCCESS":
                    success_count += 1
                elif status == "TIMEOUT":
                    fail_count += 1
                    timeout_triggered = True
                    logger.warning(f"🛑 触发超时熔断！当前股票 {code} 超时。")
                    # 注意：这里不能直接 break，因为其他线程可能还在跑。
                    # 但我们可以标记状态，后续逻辑处理。
                    # 如果要立即停止所有任务，需要调用 executor.shutdown(wait=False) 并取消其他 future
                    # 这里选择让当前超时的线程结束，其他正常线程继续，但主程序会在最后休息
                elif status == "EMPTY":
                    pass # 空数据不算失败，也不算成功
                else:
                    fail_count += 1
                    logger.warning(f"⚠️ 股票 {code} 处理结果为: {status}")
                    
            except Exception as e:
                fail_count += 1
                logger.error(f"线程执行异常 {code}: {e}")

    # --- 超时后的全局休息逻辑 ---
    if timeout_triggered:
        logger.warning(f"😴 检测到本次批次中有股票获取超时，程序将暂停休息 {rest_time} 秒...")
        time.sleep(rest_time)
        logger.info("💤 休息结束，程序将继续或退出。")
    
    logger.info(f"🏁 批次结束。成功: {success_count}, 失败/超时: {fail_count}")

# ==============================
# 主程序
# ==============================
if __name__ == '__main__':
    today_dt = datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)
    today_str = today_dt.strftime("%Y-%m-%d")
    
    logger.info(f"📅 当前任务日期: {today_str}")

    # 1. 交易日检查
    is_run=True

    if not utils.is_trading_day_ak(today_str) and is_run==False:
        logger.warning(f"⚠️ {today_str} 不是 A 股交易日，程序安全退出。")
        sys.exit(0)

    # 2. 查询待更新股票
    # 注意：SQL 中的日期需要加引号
    sqls = f"""
    SELECT * FROM (
        SELECT code, MAX(name) as name FROM gp.stock s GROUP BY code
    ) AS stock 
    WHERE code NOT IN (
        SELECT stock_code FROM gp.stock_call_auction WHERE trade_date = '{today_str}'
    )
    """
    # logger.debug(f"SQL: {sqls}")
    
    try:
        need_get_stock = pd.read_sql(sql=sqls, con=READ_ENGINE)
    except Exception as e:
        logger.error(f"❌ 查询待更新股票列表失败: {e}")
        sys.exit(1)

    if need_get_stock.empty:
        logger.info("✅ 所有股票今日数据已存在，无需更新。")
        sys.exit(0)

    need_get_stock['trade_date'] = today_str
    logger.info(f"🎯 需要补充股票数量: {len(need_get_stock)}")
    
    # 3. 执行获取 (每只股票独立入库，超时自动休息)
    # max_workers 建议不要太大，避免触发 IP 封禁，4-6 个比较安全
    run_multithread_task(need_get_stock, max_workers=4, timeout_limit=60, rest_time=60)

    logger.info("🎉 程序正常结束")