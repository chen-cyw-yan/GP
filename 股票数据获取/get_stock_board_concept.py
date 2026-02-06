import time
import akshare as ak
import pandas as pd
import numpy as np
import tqdm
import random
# import pyecharts.options as opts
# from pyecharts.charts import Line
import pandas as pd
from sqlalchemy import create_engine
from sklearn.linear_model import LinearRegression
import matplotlib.pyplot as plt
from datetime import datetime
from dateutil.relativedelta import relativedelta
from sqlalchemy import create_engine
import pymysql
import logging
# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)
# 获取当前日期（不含时间）
today = datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)

# 计算一年前的日期（精确回退12个月）
one_year_ago = today - relativedelta(years=1)
start_date = one_year_ago.strftime("%Y%m%d")
end_date = today.strftime("%Y%m%d")
con = create_engine(f"mysql+pymysql://root:chen@127.0.0.1:3306/gp")
conn = pymysql.connect(
            host='127.0.0.1',
            user='root',
            password='chen',
            database='gp',
            # use_unicode=args.encoding,
        )
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

board_df=ak.stock_board_concept_name_ths()
for k,v in board_df.iterrows():
    board=v['name']
    stock_board_concept_index_ths_df = ak.stock_board_concept_index_ths(symbol=board, start_date=start_date, end_date=end_date)
    stock_board_concept_index_ths_df['code']=v['code']
    stock_board_concept_index_ths_df['name']=v['name']
    stock_board_concept_index_ths_df=stock_board_concept_index_ths_df.rename(columns={

    })

    # sql = f"REPLACE INTO gp.stock(`{'`,`'.join(stock_zh_a_daily_qfq_df.columns)}`) VALUES ({','.join(['%s' for _ in range(stock_zh_a_daily_qfq_df.shape[1])])})"
    # toSql(sql=sql, rows=stock_zh_a_daily_qfq_df.values.tolist())