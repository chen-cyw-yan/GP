#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2026/2/24 10:16
# @Author : chenyanwen
# @email:1183445504@qq.com

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
engine = create_engine("mysql+pymysql://root:chen@127.0.0.1:3306/gp?charset=utf8mb4")

conn = pymysql.connect(
    host='127.0.0.1',
    user='root',
    password='chen',
    database='gp',
    autocommit=False,
    charset='utf8mb4'
)
cursor = conn.cursor()

def toSql(sql: str, rows: list):
    if not rows:
        return
    try:
        cursor.executemany(sql, rows)
        conn.commit()
        logger.info(f"成功写入 {len(rows)} 条数据")
    except Exception as e:
        conn.rollback()
        logger.error(f"数据库写入失败: {e}")
        raise

# ==============================
# 交易日历检查
# ==============================
def is_trading_day_ak(date_str):
    try:
        df = ak.tool_trade_date_hist_sina()
        trading_days = df['trade_date'].astype(str).tolist()
        target = date_str.replace('-', '')
        return target in trading_days
    except Exception as e:
        logger.error(f"检查交易日历失败: {e}")
        return False

# ==============================
# 单只股票数据获取（已修复列名问题）
# ==============================
def fetch_stock_data(row, retry=3):
    symbol = row['code']
    name = row['name']
    trade_date = row['trade_date']

    for i in range(retry):
        try:
            # 获取分笔数据
            df = ak.stock_zh_a_tick_tx_js(symbol=symbol)
            # print(df)
            if df is None or df.empty:
                logger.warning(f"{symbol} 数据为空")
                return None

            # 【调试】打印列名确认 (生产环境可注释掉)
            # logger.debug(f"{symbol} 列名: {df.columns.tolist()}")

            # 【修复】使用正确的中文列名 '成交时间'
            if '成交时间' not in df.columns:
                logger.warning(f"{symbol} 列名不包含 '成交时间', 实际列名: {df.columns.tolist()}")
                return None

            # 筛选 09:25:00 的数据
            # 注意：akshare 返回的时间格式通常是 "09:25:00" 字符串
            # df_0925 = df[df['成交时间'] == '09:25:00']
            
            # if df_0925.empty:
            #     # 尝试模糊匹配，防止有毫秒差异或格式问题 (可选优化)
            #     # 如果严格匹配不到，可以尝试包含 '09:25' 的行
            #     # df_0925 = df[df['成交时间'].str.contains('09:25')] 
            #     logger.warning(f"{symbol} 未找到精确的 '09:25:00' 数据")
            #     return None
            
            # 取该时刻的最后一行 (防止拆单)
            tick_row = df.iloc[0,]

            # 【修复】使用正确的中文列名提取数据
            # 列名映射: '成交量', '成交金额', '性质'
            vol_raw = tick_row.get('成交量', 0)
            amt_raw = tick_row.get('成交金额', 0)
            nat_raw = tick_row.get('性质', '')
            
            # 数据清洗与类型转换
            # 成交量和成交金额可能是字符串或带逗号的数字，需处理
            def clean_number(val):
                if val is None or val == '':
                    return 0
                if isinstance(val, (int, float)):
                    return int(val)
                try:
                    # 去除逗号并转换
                    return int(float(str(val).replace(',', '')))
                except:
                    return 0

            volume = clean_number(vol_raw)
            amount = clean_number(amt_raw)
            nature = str(nat_raw).strip() if nat_raw else ''

            # 构建符合数据库表结构的字典
            result = {
                'trade_date': trade_date,
                'stock_code': symbol,
                'stock_name': name,
                'auction_time': '09:25:00',
                'volume': volume,
                'amount': amount,
                'nature': nature
            }
            
            logger.info(f"完成: {symbol} ({name}) | 量:{volume} 额:{amount} 性质:{nature}")
            return result

        except Exception as e:
            logger.warning(f"{symbol} 第{i+1}次失败: {e}")
            time.sleep(60)

    logger.error(f"{symbol} 获取失败，已放弃")
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
        logger.warning("没有获取到任何有效数据")
        return

    logger.info(f"数据获取完成，共 {len(all_results)} 条，开始批量入库")

    # 定义固定的列顺序，必须与数据库表结构一致
    columns = ['trade_date', 'stock_code', 'stock_name', 'auction_time', 'volume', 'amount', 'nature']
    
    rows_to_insert = [
        [item[col] for col in columns] 
        for item in all_results
    ]

    sql = f"""
    REPLACE INTO gp.stock_call_auction ({','.join(columns)})
    VALUES ({','.join(['%s'] * len(columns))})
    """

    toSql(sql=sql, rows=rows_to_insert)
    logger.info("全部入库完成")

# ==============================
# 主程序
# ==============================
if __name__ == '__main__':
    today_dt = datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)
    today_str = today_dt.strftime("%Y-%m-%d")
    today_str ='2026-03-06'
    logger.info(f"当前任务日期: {today_str}")

    # if not is_trading_day_ak(today_str):
    #     logger.warning(f"⚠️ {today_str} 不是 A 股交易日，程序安全退出。")
    #     sys.exit(0)

    sqls = f"""
select * from (
select code,max(name) as name  from gp.stock s group by code
) as stock where code not in (
select stock_code from gp.stock_call_auction where trade_date={today_str}
)
    """
    print(sqls)
    # sqls_formatted = sqls % f"'{today_str}'"
    
    try:
        need_get_stock = pd.read_sql(sql=sqls, con=engine)
    except Exception as e:
        logger.error(f"查询待更新股票列表失败: {e}")
        sys.exit(1)

    # 测试用：如果需要强制测试，取消下面注释
    # need_get_stock = pd.DataFrame([{'code':'sh600021','name':'上海电力'}])

    if need_get_stock.empty:
        logger.info("✅ 所有股票今日数据已存在，无需更新。")
        sys.exit(0)

    need_get_stock['trade_date'] = today_str
    
    logger.info(f"需要补充股票数量: {len(need_get_stock)}")
    
    get_stocks_year_multithread(need_get_stock, max_workers=4)

    cursor.close()
    conn.close()
    logger.info("程序正常结束")