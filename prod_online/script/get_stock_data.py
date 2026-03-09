import time
import akshare as ak
import pandas as pd
import numpy as np
import tqdm
import random
# import pyecharts.options as opts
# from pyecharts.charts import Line
import sys
import os

sys.path.append(
    os.path.abspath(
        os.path.join(os.path.dirname(__file__), "../../")
    )
)
import pandas as pd
from sqlalchemy import create_engine
from sklearn.linear_model import LinearRegression
import matplotlib.pyplot as plt
from datetime import datetime
from dateutil.relativedelta import relativedelta
from sqlalchemy import create_engine
import pymysql
import logging
import prod_online.config.utils as utils
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

# 格式化为 "YYYYMMDD"
start_date = one_year_ago.strftime("%Y%m%d")
end_date = today.strftime("%Y%m%d")

logger.info(f"当前日期：{end_date}")
logger.info(f"近一年起始日期（一年前）：{start_date}")

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
def get_basic_data(symbol='sh601398'):
    stock_zh_a_daily_one = ak.stock_zh_a_daily(symbol=symbol, start_date=start_date, end_date=end_date, adjust="qfq")
    return stock_zh_a_daily_one

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

def get_stocks_year(stocks):
    logger.info('start get stock data')
    sizes=stocks.index.size
    a=0
    # print()
    logger.info(f"需要获取数量{sizes}")
    for k,v in stocks.iterrows():
        logger.info(f"获取{a}/{sizes}:代码,{v['代码']};名称,{v['名称']}")
        # print(f"{a}/{sizes}:",v['代码'],v['名称'])
        symbol = v['代码']
        stock_zh_a_daily_qfq_df = ak.stock_zh_a_daily(symbol=symbol, start_date=start_date, end_date=end_date, adjust="qfq")
        stock_zh_a_daily_qfq_df['code']=v['代码']
        stock_zh_a_daily_qfq_df['name']=v['名称']
        sql = f"REPLACE INTO gp.stock(`{'`,`'.join(stock_zh_a_daily_qfq_df.columns)}`) VALUES ({','.join(['%s' for _ in range(stock_zh_a_daily_qfq_df.shape[1])])})"
        toSql(sql=sql, rows=stock_zh_a_daily_qfq_df.values.tolist())
        # time.sleep(random.random())
        a+=1
        # break
import pandas as pd


def build_support_resistance_table(
    df: pd.DataFrame,
    trade_date: str,
    symbol: str,
    price_bin: float = 0.01,
    dense_ratio: float = 0.3,
    method: str = "amount"   # amount / volume
):
    data = df.copy()

    # 1. VWAP
    vwap = (data["成交价格"] * data["成交量"]).sum() / data["成交量"].sum()

    # 2. 价格分桶
    data["price_bin"] = (data["成交价格"] / price_bin).round(0) * price_bin
    data["price_bin"] = data["price_bin"].round(3)

    # 3. 选择用成交金额 or 成交量
    if method == "amount":
        profile = data.groupby("price_bin")["成交金额"].sum()
    else:
        profile = data.groupby("price_bin")["成交量"].sum()

    profile = profile.sort_values(ascending=False)

    # 4. 密集成交区
    total = profile.sum()
    dense_bins = profile[(profile.cumsum() / total) <= dense_ratio]

    dense_lower = dense_bins.index.min()
    dense_upper = dense_bins.index.max()

    # 5. 支撑 / 压力定义
    support_price = dense_lower
    resistance_price = dense_upper

    # 6. 结果表
    result = pd.DataFrame([{
        "trade_date": trade_date,
        "symbol": symbol,

        "support_price": support_price,
        "resistance_price": resistance_price,

        "vwap": round(vwap, 3),

        "dense_lower": dense_lower,
        "dense_upper": dense_upper,
        "dense_ratio": dense_ratio,

        "calc_method": f"{method}_profile",
        "price_bin": price_bin,

        "total_amount": data["成交金额"].sum(),
        "total_volume": data["成交量"].sum()
    }])

    return result







if __name__ == '__main__':
    today_dt = datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)
    today_str = today_dt.strftime("%Y-%m-%d")
    # today_str ='2026-03-06'
    logger.info(f"当前任务日期: {today_str}")

    if not utils.is_trading_day_ak(today_str):
        logger.warning(f"⚠️ {today_str} 不是 A 股交易日，程序安全退出。")
        sys.exit(0)
    today_dt = datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)
    today_str = today_dt.strftime("%Y-%m-%d")
    # today_str ='2026-03-06'
    logger.info(f"当前任务日期: {today_str}")

    if not utils.is_trading_day_ak(today_str):
        logger.warning(f"⚠️ {today_str} 不是 A 股交易日，程序安全退出。")
        sys.exit(0)
    code='sh601398'
    get_basic_df=get_basic_data(code)
    get_basic_df=get_basic_df.sort_values(by='date')
    first_date = get_basic_df.iloc[0, 0].strftime("%Y-%m-%d")
    cnt=get_basic_df.index.size
    logger.info(f"起始时间:{first_date},应有数据：{cnt}")
    sqls=f"""select count(1) as cnt,code from gp.stock where `date`>='{first_date}' and turnover!=0 group by code  having count(1) <{cnt}"""
    need_get_stock=pd.read_sql(sql=sqls,con=con)
    today_data_df=get_today_data()
    need_get_stock_df=today_data_df.loc[today_data_df['代码'].isin(need_get_stock['code'].to_list())]
    get_stocks_year(need_get_stock_df)
