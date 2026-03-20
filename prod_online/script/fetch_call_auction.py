#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2026/03/20
# @Author : chenyanwen
# @Email : 1183445504@qq.com
# @Description : 腾讯财经集合竞价数据抓取与入库 (重构版)

import sys
import os
import time
import logging
import warnings
import pandas as pd
import numpy as np
import requests
import pymysql
from concurrent.futures import ThreadPoolExecutor, as_completed
from sqlalchemy import create_engine
from datetime import datetime
from typing import List, Dict, Optional, Tuple

# 添加项目路径
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))

# 引入工具类
try:
    import prod_online.config.utils as utils
except ImportError:
    utils = None
    logging.warning("未找到 utils 模块，交易日检查将跳过或默认通过。")

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
# 全局配置
# ==============================
DB_CONFIG = {
    'host': '127.0.0.1',
    'user': 'root',
    'password': 'chen',
    'database': 'gp',
    'charset': 'utf8mb4',
    'autocommit': False
}

READ_ENGINE_URL = "mysql+pymysql://root:chen@127.0.0.1:3306/gp?charset=utf8mb4"

# 腾讯接口配置
TX_TIMEOUT = 30  # 单次请求超时秒数
MAX_WORKERS = 6  # 并发线程数 (建议 4-8，过大易被封 IP)
REST_TIME_ON_TIMEOUT = 60  # 触发超时后的休息时间

# ==============================
# 1. 数据抓取模块 (纯网络 IO)
# ==============================
def stock_zh_a_tick_tx_js(symbol: str, page_size: int = 1) -> Optional[pd.DataFrame]:
    """
    腾讯财经 - 历史分笔数据 (仅获取第一页，通常包含集合竞价)
    """
    big_df = pd.DataFrame()
    page = 0
    
    try:
        while page < page_size:
            url = "http://stock.gtimg.cn/data/index.php"
            params = {
                "appn": "detail",
                "action": "data",
                "c": symbol,
                "p": page,
            }
            r = requests.get(url, params=params, timeout=TX_TIMEOUT)
            if r.status_code != 200:
                break
                
            text_data = r.text
            # 解析腾讯特有的格式
            if "[" not in text_data:
                break
                
            start_idx = text_data.find("[")
            data_list = eval(text_data[start_idx:])
            
            if len(data_list) < 2:
                break
                
            # 分割数据
            temp_df = (
                pd.DataFrame(data_list[1].split("|"))
                .iloc[:, 0]
                .str.split("/", expand=True)
            )
            
            if temp_df.empty:
                break
                
            big_df = pd.concat([big_df, temp_df], ignore_index=True)
            page += 1
            
    except Exception as e:
        logger.debug(f"抓取 {symbol} 网络异常: {e}")
        return None

    if big_df.empty:
        return None

    # 整理列名
    big_df = big_df.iloc[:, 1:].copy()
    if len(big_df.columns) >= 6:
        big_df.columns = ["成交时间", "成交价格", "价格变动", "成交量", "成交金额", "性质"]
        
        # 映射性质
        property_map = {"S": "卖盘", "B": "买盘", "M": "中性盘"}
        big_df["性质"] = big_df["性质"].map(property_map).fillna("未知")
        
        # 类型转换
        try:
            big_df["成交价格"] = big_df["成交价格"].astype(float)
            big_df["成交量"] = pd.to_numeric(big_df["成交量"], errors='coerce').fillna(0).astype(int)
            big_df["成交金额"] = pd.to_numeric(big_df["成交金额"], errors='coerce').fillna(0).astype(int)
            big_df["成交时间"] = big_df["成交时间"].astype(str)
        except Exception as e:
            logger.warning(f"{symbol} 数据类型转换失败: {e}")
            
        return big_df
    else:
        return None

def fetch_single_stock_task(row: pd.Series) -> Dict:
    """
    单个股票的抓取任务 (在线程中运行)
    返回：字典 (包含状态和数据)，如果失败则 status 为 ERROR
    """
    symbol = row['code']
    name = row['name']
    trade_date = row['trade_date']
    
    result = {
        'status': 'ERROR',
        'data': None,
        'symbol': symbol,
        'msg': ''
    }
    
    try:
        # 1. 抓取数据
        df = stock_zh_a_tick_tx_js(symbol=symbol, page_size=1)
        
        if df is None or df.empty:
            result['status'] = 'EMPTY'
            result['msg'] = '数据为空'
            return result
            
        if '成交时间' not in df.columns:
            result['status'] = 'COL_ERR'
            result['msg'] = '列名缺失'
            return result
            
        # 2. 提取集合竞价数据 (取第一行)
        tick_row = df.iloc[0]
        
        volume = int(tick_row.get('成交量', 0))
        amount = int(tick_row.get('成交金额', 0))
        nature = str(tick_row.get('性质', '')).strip()
        auction_time = str(tick_row.get('成交时间', '09:25:00'))
        
        # 简单的时间格式修正 (如果只有 HH:MM，补全秒)
        if len(auction_time) == 5:
            auction_time += ":00"
            
        result['data'] = {
            'trade_date': trade_date,
            'stock_code': symbol,
            'stock_name': name,
            'auction_time': auction_time,
            'volume': volume,
            'amount': amount,
            'nature': nature
        }
        result['status'] = 'SUCCESS'
        
    except Exception as e:
        result['msg'] = str(e)
        logger.debug(f"抓取 {symbol} 处理异常: {e}")
        
    return result

# ==============================
# 2. 数据库操作模块 (批量处理)
# ==============================
def batch_insert_to_db(records: List[Dict], conn: pymysql.Connection):
    """
    批量插入数据到数据库
    """
    if not records:
        return 0, 0
        
    cursor = conn.cursor()
    sql = """
    REPLACE INTO gp.stock_call_auction 
    (trade_date, stock_code, stock_name, auction_time, volume, amount, nature)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    
    success_count = 0
    fail_count = 0
    
    # 准备数据元组列表
    values_list = []
    for rec in records:
        try:
            row = (
                rec['trade_date'],
                rec['stock_code'],
                rec['stock_name'],
                rec['auction_time'],
                rec['volume'],
                rec['amount'],
                rec['nature']
            )
            values_list.append(row)
        except KeyError as e:
            logger.error(f"数据字段缺失: {e}, 数据: {rec}")
            fail_count += 1

    if not values_list:
        return 0, fail_count

    try:
        # 批量执行
        cursor.executemany(sql, values_list)
        conn.commit()
        success_count = len(values_list)
        logger.info(f"✅ [DB] 批量入库成功: {success_count} 条记录")
    except Exception as e:
        conn.rollback()
        logger.error(f"❌ [DB] 批量入库失败: {e}")
        fail_count = len(values_list)
    finally:
        cursor.close()
        
    return success_count, fail_count

# ==============================
# 3. 主控流程 (多线程调度)
# ==============================
def run_multithread_task(stocks_df: pd.DataFrame):
    logger.info(f"📊 任务启动：共 {len(stocks_df)} 只股票，并发线程: {MAX_WORKERS}")
    
    all_results = []
    timeout_triggered = False
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # 提交任务
        futures = {
            executor.submit(fetch_single_stock_task, row): row['code'] 
            for _, row in stocks_df.iterrows()
        }
        
        # 收集结果
        for future in as_completed(futures):
            code = futures[future]
            try:
                res = future.result()
                all_results.append(res)
                
                if res['status'] == 'SUCCESS':
                    logger.info(f"✨ [{code}] 成功 | 量:{res['data']['volume']} 额:{res['data']['amount']}")
                elif res['status'] == 'EMPTY':
                    logger.debug(f"⚠️ [{code}] 无数据")
                else:
                    logger.warning(f"⚠️ [{code}] 失败: {res['msg']}")
                    
            except Exception as e:
                logger.error(f"💥 [{code}] 线程崩溃: {e}")
                all_results.append({'status': 'ERROR', 'symbol': code, 'msg': str(e)})

    # 检查是否有超时或特定错误触发休息 (这里简化逻辑：如果有大量失败或特定错误则休息)
    # 原逻辑是检测 TIMEOUT，现在我们在 fetch 中没做显式 timeout 抛出，而是依赖 requests timeout
    # 如果需要严格的熔断休息，可以统计失败率
    error_count = sum(1 for r in all_results if r['status'] not in ['SUCCESS', 'EMPTY'])
    if error_count > len(stocks_df) * 0.3: # 超过 30% 失败
        logger.warning(f"😴 失败率过高 ({error_count}/{len(stocks_df)})，触发保护性休息 {REST_TIME_ON_TIMEOUT} 秒...")
        time.sleep(REST_TIME_ON_TIMEOUT)
        timeout_triggered = True

    # --- 批量入库 ---
    success_records = [r['data'] for r in all_results if r['status'] == 'SUCCESS' and r['data']]
    
    if success_records:
        logger.info(f"📝 开始批量入库 {len(success_records)} 条有效数据...")
        conn = None
        try:
            conn = pymysql.connect(**DB_CONFIG)
            s_cnt, f_cnt = batch_insert_to_db(success_records, conn)
            logger.info(f"🏁 入库完成。成功: {s_cnt}, 失败: {f_cnt}")
        except Exception as e:
            logger.error(f"数据库连接失败: {e}")
        finally:
            if conn: conn.close()
    else:
        logger.warning("没有成功抓取到任何数据，跳过入库。")

    return timeout_triggered

# ==============================
# 主程序入口
# ==============================
if __name__ == '__main__':
    today_dt = datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)
    today_str = today_dt.strftime("%Y-%m-%d")
    
    logger.info(f"📅 当前任务日期: {today_str}")

    # 1. 交易日检查
    is_trading_day = True
    if utils and hasattr(utils, 'is_trading_day_ak'):
        if not utils.is_trading_day_ak(today_str):
            logger.warning(f"⚠️ {today_str} 不是 A 股交易日，程序安全退出。")
            sys.exit(0)
    else:
        logger.info("跳过交易日检查 (utils 不可用或未配置)")

    # 2. 查询待更新股票
    READ_ENGINE = create_engine(READ_ENGINE_URL)
    
    sql = f"""
    SELECT * FROM (
        SELECT code, MAX(name) as name FROM gp.stock s GROUP BY code
    ) AS stock 
    WHERE code NOT IN (
        SELECT stock_code FROM gp.stock_call_auction WHERE trade_date = '{today_str}'
    )
    """
    
    try:
        need_get_stock = pd.read_sql(sql=sql, con=READ_ENGINE)
    except Exception as e:
        logger.error(f"❌ 查询待更新股票列表失败: {e}")
        sys.exit(1)

    if need_get_stock.empty:
        logger.info("✅ 所有股票今日数据已存在，无需更新。")
        sys.exit(0)

    need_get_stock['trade_date'] = today_str
    logger.info(f"🎯 需要补充股票数量: {len(need_get_stock)}")
    
    # 3. 执行任务
    try:
        run_multithread_task(need_get_stock)
        logger.info("🎉 程序正常结束")
    except KeyboardInterrupt:
        logger.warning("用户中断程序")
    except Exception as e:
        logger.error(f"程序运行严重错误: {e}", exc_info=True)