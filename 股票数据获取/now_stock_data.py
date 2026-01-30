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
# 获取当前日期（不含时间）
today = datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)

# 计算一年前的日期（精确回退12个月）
one_year_ago = today - relativedelta(years=1)

# 格式化为 "YYYYMMDD"
start_date = one_year_ago.strftime("%Y%m%d")
end_date = today.strftime("%Y%m%d")

print("当前日期:", end_date)      # 如: 20260120
print("近一年起始日期（一年前）:", start_date)  # 如: 20250120

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
# print(stock_zh_a_spot_em_df)


def get_today_data():
    stock_zh_a_spot_em_df = ak.stock_zh_a_spot()
    df=stock_zh_a_spot_em_df
    # 确保是字符串
    df["代码"] = df["代码"].astype(str)
    df["名称"] = df["名称"].astype(str)
    # ① 排除科创板（688 / 689）
    df = df.loc[~df["代码"].str.startswith(("bj", "sh688",'sh689','sz688','sz689'))]
    # ③ 排除 ST / *ST
    df = df.loc[~df["名称"].str.contains("ST")]
    return df

if __name__ == '__main__':
    today_data_df=get_today_data()
    today_data_df=today_data_df[["代码","名称","最新价","今开","最高","最低","成交量","成交额"]]
    today_data_df=today_data_df.rename(columns={"代码":"code","名称":"name", "最新价":"close" ,"今开":"open","最高":"high", "最低":"low", "成交量":"volume", "成交额":"amount"})
    today_data_df['date']= today.strftime("%Y-%m-%d")
    today_data_df['outstanding_share']=0
    today_data_df['turnover']=0
    print(today_data_df)
    sql = f"REPLACE INTO gp.stock(`{'`,`'.join(today_data_df.columns)}`) VALUES ({','.join(['%s' for _ in range(today_data_df.shape[1])])})"
    toSql(sql=sql, rows=today_data_df.values.tolist())