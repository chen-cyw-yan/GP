#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2026/2/28 11:35
# @Author : chenyanwen
# @email:1183445504@qq.com
import sys
import os

sys.path.append(
    os.path.abspath(
        os.path.join(os.path.dirname(__file__), "../../")
    )
)
import json
import dataframe_image as dfi
from telegram import Bot
from telegram.utils.request import Request
import time
import numpy as np
import akshare as ak
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from sqlalchemy import create_engine
from datetime import datetime
from dateutil.relativedelta import relativedelta
import pymysql
import logging
import prod_online.services.update_stock_date as update_stock_date
import prod_online.services.filter_stock as filter_stock
import prod_online.config.utils as utils
from prod_online.config.feishu_utils import FeishuUtils
# ==============================
# 日志配置
# ==============================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)
conn = pymysql.connect(
            host='127.0.0.1',
            user='root',
            password='chen',
            database='gp',
            # use_unicode=args.encoding,
        )
today_dt = datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)
today_str = today_dt.strftime("%Y-%m-%d")
    # today_str ='2026-03-06'
logger.info(f"当前任务日期: {today_str}")

if not utils.is_trading_day_ak(today_str):
    logger.warning(f"⚠️ {today_str} 不是 A 股交易日，程序安全退出。")
    sys.exit(0)
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
CHAT_ID = "-5191129435"
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


logger.info('运行策列完成....')
logger.info('存储策列数据....')
df_save=df
df_save=df_save.rename(columns={"代码":'stock_code',
"名称":'stock_name' ,
"日期":'trade_date' ,
"收盘价":'close_price' ,
"触发信号次数":'trigger_count',
"是否异动类型":'is_abnormal_type' ,
"下一日可能触发":'next_day_may_trigger' ,
"所需最小涨幅":'min_required_change' ,
"目标等级":'target_level',
"预警信息":'warning_info'})
df_tmp = df_save.replace('', np.nan)
df_tmp = df_tmp.astype(object).where(pd.notnull(df_tmp), None)

rows_data = df_tmp.values.tolist()
sql = f"""
    REPLACE INTO gp.stock_abnormal_monitor(`{'`,`'.join(df_save.columns)}`)
    VALUES ({','.join(['%s' for _ in range(df_save.shape[1])])})
    """
filter_stock.toSql(sql=sql, rows=rows_data)
logger.info('存储策列完成....')


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
df = df.iloc[0:100]
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
excel_path = "prod_online/imges/result.xlsx"
df.to_excel(excel_path, index=False)

with open(excel_path, "rb") as f:
    bot.send_document(
            chat_id=CHAT_ID,
            document=f
        )
fs_client=FeishuUtils('cli_a9256b2aef7a5cd4','t22QBXS6MVqsXC41GoCDvbxin0tpXyL3')
context={
        "text":message
    }
fs_client.set_message_for_text('chat_id','oc_cd642a7fec1dcd847e91b2e1775809d2',json.dumps(context))
fs_client.set_message_for_image('chat_id', 'oc_cd642a7fec1dcd847e91b2e1775809d2',
                                      image_path)
fs_client.set_message_for_file('chat_id', 'oc_cd642a7fec1dcd847e91b2e1775809d2',excel_path,'result.xlsx')
