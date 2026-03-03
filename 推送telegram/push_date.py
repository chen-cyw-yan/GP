#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2026/2/28 11:35
# @Author : chenyanwen
# @email:1183445504@qq.com
import dataframe_image as dfi
from telegram import Bot
from telegram.utils.request import Request
import time
import akshare as ak
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from sqlalchemy import create_engine
from datetime import datetime
from dateutil.relativedelta import relativedelta
import pymysql
import logging
import update_stock_date
import filter_stock
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
engine = create_engine("mysql+pymysql://root:chen@127.0.0.1:3306/gp")

conn = pymysql.connect(
    host='127.0.0.1',
    user='root',
    password='chen',
    database='gp',
    autocommit=False
)

cursor = conn.cursor()


# ==============================
# 电报机器人连接
# ==============================
TOKEN = "8760053592:AAGt8DcQ9_5Gu1OhwWYWtYz1IkHYHFXxL20"
CHAT_ID = "-1003787641029"
PROXY = {
    "proxy_type": "socks5",
    "addr": "127.0.0.1",
    "port": 7891,
}
proxy_url = "http://127.0.0.1:7890"
request = Request(
    proxy_url=proxy_url,
    connect_timeout=10,
    read_timeout=10
)
bot = Bot(TOKEN,request=request)

logger.info('更新数据')
# update_stock_date.update_date()
logger.info('更新数据完成....')
logger.info('运行策列....')
df=filter_stock.filer_stock()
    # ==============================
    # 主程序
    # ==============================

image_path = "table.png"


# ===== 生成时间 =====
now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# ===== 统计结果 =====
if df is None or df.empty:
    message = f"""
📊 策略扫描结果

🕒 生成时间：{now}
📌 策略名称：启动策略

⚠ 最近10个交易日无触发信号
"""
    bot.send_message(chat_id=CHAT_ID, text=message)

else:
    count = len(df)

    message = f"""
📊 策略扫描结果

🕒 生成时间：{now}
📌 策略名称：启动策略
🎯 本次筛选结果：{count} 只

"""
    # 先导出图片
    # ===== 只取前50行生成图片 =====
df_show = df.iloc[0:30]
logger.info(f"{df_show.index.size}")

dfi.export(df_show, image_path, max_rows=30)

    # 发送图片
with open(image_path, "rb") as f:
    bot.send_photo(
            chat_id=CHAT_ID,
            photo=f,
            caption=message
        )

    # ===== 同时发送完整Excel =====
excel_path = "result.xlsx"
df.to_excel(excel_path, index=False)

with open(excel_path, "rb") as f:
    bot.send_document(
            chat_id=CHAT_ID,
            document=f
        )