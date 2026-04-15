#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2026/03/20
# @Author : chenyanwen
# @Email : 1183445504@qq.com
# @Description : 股票板块共振分析自动化脚本 (重构优化版)

import sys
import os
import json
import time
import logging
import numpy as np
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine
from typing import Tuple, List, Optional
import pymysql
# 添加项目根目录到路径 (保持原有逻辑)
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))

# 引入飞书工具类 (确保路径正确)
try:
    from prod_online.config.feishu_utils import FeishuUtils
except ImportError:
    logger = logging.getLogger(__name__)
    logger.warning("未找到 FeishuUtils，飞书通知功能将不可用。")
    FeishuUtils = None
# 日志配置
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)
DB_URL = "mysql+pymysql://root:chen@127.0.0.1:3306/gp"
def sync_overwrite_to_ths(trade_date,ths_blk_path):
    """
    同步数据到同花顺数据库
    清空板块后写入
    :param trade_date: 交易日期
    :param ths_blk_path: 同花顺板块文件路径
    :return:
    """
    stock_part = []
    market_part = []
    engine = create_engine(DB_URL)
    sql_analy = f"SELECT * FROM gp.stock_analysis WHERE need_to_analysis = 1 AND trade_date='{trade_date}'"
    df=pd.read_sql(sql_analy,engine)
    if df.empty:
        logger.info(f"没有需要同步的数据")
        return
    stock_list = df['stock_code'].tolist()
    seen = set()
    for stock in stock_list:
        stock = str(stock).strip()
        # 去重
        if stock in seen:
            continue
        stock=stock[2:]
        seen.add(stock)
        # 校验
        if len(stock) != 6 or not stock.isdigit():
            continue
        
        # 市场识别
        if stock.startswith("6"):
            market = "17"
        elif stock.startswith(("0", "3")):
            market = "33"
        else:
            continue
        
        stock_part.append(stock)
        market_part.append(market)
    
    if not stock_part:
        return ""
    with open(ths_blk_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    
    # ---------- 3. 只修改 context ----------
    data["context"] = "|".join(stock_part) + "|," + "|".join(market_part) + "|"
    
    # ⚠️ ln 和 xn 完全不动
    
    # ---------- 4. 写回 ----------
    with open(ths_blk_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)
    pass
def sync_append_to_ths(trade_date,ths_blk_path):
    """
    同步数据到同花顺数据库
    清空板块后写入
    :param trade_date: 交易日期
    :param ths_blk_path: 同花顺板块文件路径
    :return:
    """
    stock_part = []
    market_part = []
    engine = create_engine(DB_URL)
    sql_analy = f"SELECT * FROM gp.stock_analysis WHERE need_to_analysis = 1 AND trade_date='{trade_date}'"
    df=pd.read_sql(sql_analy,engine)
    if df.empty:
        logger.info(f"没有需要同步的数据")
        return
    stock_list = df['stock_code'].tolist()
    seen = set()
    for stock in stock_list:
        stock = str(stock).strip()
        # 去重
        if stock in seen:
            continue
        stock=stock[2:]
        seen.add(stock)
        # 校验
        if len(stock) != 6 or not stock.isdigit():
            continue
        
        # 市场识别
        if stock.startswith("6"):
            market = "17"
        elif stock.startswith(("0", "3")):
            market = "33"
        else:
            continue
        
        stock_part.append(stock)
        market_part.append(market)
    
    if not stock_part:
        return ""
    
    return "|".join(stock_part) + "|," + "|".join(market_part) + "|"

if __name__ == '__main__':
    sync_overwrite_to_ths("2026-04-15","C:/App/同花顺/mx_705691517/custom_block/35")