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

# 获取当前跟踪最强板块
def get_best_block(x):
    stock_code = x['stock_code'][2:]
    trade_date = x['trade_date']
    
    sql = f"""
        select * from gp.tdx_block_daily as datail
        join (
            select block_code, block_name, block_type
            from gp.tdx_block_stocks as rlx
            where rlx.stock_code = '{stock_code}'
        ) as rlx 
        on datail.code = rlx.block_code
        where datail.create_date = '{trade_date}'
    """
    block_detail_df = pd.read_sql(sql=sql, con=engine)
    block_detail_df = block_detail_df.loc[
        block_detail_df['block_type'].isin(['地区板块', '概念板块', '行业板块'])
    ]

    block_dq_df = block_detail_df.loc[block_detail_df['block_type']=='地区板块'].sort_values('strength', ascending=False).reset_index(drop=True)
    block_hy_df = block_detail_df.loc[block_detail_df['block_type']=='行业板块'].sort_values('strength', ascending=False).reset_index(drop=True)
    block_gn_df = block_detail_df.loc[block_detail_df['block_type']=='概念板块'].sort_values('strength', ascending=False).reset_index(drop=True)
    # print(stock_code,trade_date,block_detail_df)
    # 如果为空，返回空字符串
    top_2_gn=block_gn_df.loc[0:2]
    gn_list = [f"{row['name']}(强度:{row['strength']})" for _, row in top_2_gn.iterrows()]
    
    # 用 "|" 拼接
    gn = "|".join(gn_list)


    dq = f"{block_dq_df.loc[0,'name']}(强度:{block_dq_df.loc[0,'strength']})" if not block_dq_df.empty else ""
    hy = f"{block_hy_df.loc[0,'name']}(强度:{block_hy_df.loc[0,'strength']})" if not block_hy_df.empty else ""
    
    return dq, hy, gn

def get_best_block(x):
    stock_code = x['stock_code'][2:]
    trade_date = x['trade_date']
    sql = f"""
        select * from gp.tdx_block_daily as datail
        join (
            select block_code, block_name, block_type
            from gp.tdx_block_stocks as rlx
            where rlx.stock_code = '{stock_code}'
        ) as rlx 
        on datail.code = rlx.block_code
        where datail.create_date = '{trade_date}'
    """
    block_detail_df = pd.read_sql(sql=sql, con=engine)
    block_detail_df = block_detail_df.loc[
        block_detail_df['block_type'].isin(['地区板块', '概念板块', '行业板块'])
    ]

    block_dq_df = block_detail_df.loc[block_detail_df['block_type']=='地区板块'].sort_values('strength', ascending=False).reset_index(drop=True)
    block_hy_df = block_detail_df.loc[block_detail_df['block_type']=='行业板块'].sort_values('strength', ascending=False).reset_index(drop=True)
    block_gn_df = block_detail_df.loc[block_detail_df['block_type']=='概念板块'].sort_values('strength', ascending=False).reset_index(drop=True)
    # print(stock_code,trade_date,block_detail_df)
    # 如果为空，返回空字符串
    top_2_gn=block_gn_df.loc[0:2]
    gn_list = [f"{row['name']}(强度:{row['strength']})" for _, row in top_2_gn.iterrows()]
    
    # 用 "|" 拼接
    gn = "，".join(gn_list)
    dq = f"{block_dq_df.loc[0,'name']}(强度:{block_dq_df.loc[0,'strength']})" if not block_dq_df.empty else ""
    hy = f"{block_hy_df.loc[0,'name']}(强度:{block_hy_df.loc[0,'strength']})" if not block_hy_df.empty else ""
    
    return dq, hy, gn

def main():
    logger.info('运行策列....')
    df=filter_stock.filer_stock()
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
    df_tmp['trade_date']=df_tmp['trade_date'].astype(str)
    df_tmp.head()
    df_tmp[['region_block','industry_block','concept_block']]=df_tmp.apply(get_best_block, axis=1, result_type='expand')


    rows_data = df_tmp.values.tolist()
    sql = f"""
        REPLACE INTO gp.stock_abnormal_monitor(`{'`,`'.join(df_tmp.columns)}`)
        VALUES ({','.join(['%s' for _ in range(df_tmp.shape[1])])})
        """
    filter_stock.toSql(sql=sql, rows=rows_data)

    last_day=max(df_tmp['trade_date'].to_list())
    
    today_df=df_tmp.loc[df_tmp['trade_date'].astype(str)==last_day]
    today_df=today_df[['stock_code','stock_name','trigger_count','is_abnormal_type','warning_info','region_block','industry_block','concept_block']]
    today_rows_data = today_df.values.tolist()
    print('xxxxx',today_df)
    sql = f"""
        REPLACE INTO gp.stock_analysis(`{'`,`'.join(today_df.columns)}`)
        VALUES ({','.join(['%s' for _ in range(today_df.shape[1])])})
        """
    filter_stock.toSql(sql=sql, rows=today_rows_data)


    logger.info('存储策列完成....')

    # exit(0)


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
    df_show = df_save.iloc[0:30]
    df = df.iloc[0:100]
    logger.info(f"{df_show.index.size}")


    # dfi.export(df_show, image_path, max_rows=30)

        # 发送图片
    # with open(image_path, "rb") as f:
    #     bot.send_photo(
    #             chat_id=CHAT_ID,
    #             photo=f,
    #             caption=message
    #         )

        # ===== 同时发送完整Excel =====
    excel_path = "prod_online/imges/result.xlsx"
    df.to_excel(excel_path, index=False)
    fs_client=FeishuUtils('cli_a9256b2aef7a5cd4','t22QBXS6MVqsXC41GoCDvbxin0tpXyL3')
    context={
            "text":message
        }
    fs_client.set_message_for_text('chat_id','oc_cd642a7fec1dcd847e91b2e1775809d2',json.dumps(context))
    fs_client.set_message_for_file('chat_id', 'oc_cd642a7fec1dcd847e91b2e1775809d2',excel_path,'result.xlsx')
    logging.info(f'发送飞书完成')
    with open(excel_path, "rb") as f:
        bot.send_document(
                chat_id=CHAT_ID,
                document=f
            )
        
    logging.info(f'发送telegram完成')

    
if __name__ == '__main__':
    main()