import time

import akshare as ak
import pandas as pd
import tqdm
import pyecharts.options as opts
from pyecharts.charts import Line
import pandas as pd

import time
import random
# from datetime import time
from datetime import datetime, timedelta
# engine = create_engine("mysql+pymysql://root:chen@127.0.0.1:3306/gp")
# 设置显示所有列
pd.set_option('display.max_columns', 100)

# 可选：设置显示所有行（如有需要）
pd.set_option('display.max_rows', 100)

# 可选：调整列宽（防止内容过长被截断）
pd.set_option('display.max_colwidth', None)

# 检查设置是否生效
print(pd.get_option('display.max_columns'))

def calculate_rsi(data, columns, window=14):
    # 计算每日
    delta = data[columns].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=window).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=window).mean()

    # 计算 RS 和 RSI
    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))

    return round(rsi, 2)


# 示例数据
# data = pd.DataFrame({
#     'Close': [142, 144, 143, 145, 146, 144, 147, 149, 148, 150, 151, 153, 152, 151, 150, 149, 151, 154]
# })

# data['RSI'] = calculate_rsi(data)
# print(data[['Close', 'RSI']])


def calculate_kdj(data, n=9):
    # 计算 RSV
    low_n = data["Low"].rolling(window=n, min_periods=1).min()
    high_n = data["High"].rolling(window=n, min_periods=1).max()
    rsv = (data["Close"] - low_n) / (high_n - low_n) * 100

    # 计算 K 值
    data["K"] = rsv.ewm(alpha=1 / 3, adjust=False).mean()

    # 计算 D 值
    data["D"] = data["K"].ewm(alpha=1 / 3, adjust=False).mean()

    # 计算 J 值
    data["J"] = 3 * data["K"] - 2 * data["D"]

    return data[["K", "D", "J"]]


# # 示例数据
# data = pd.DataFrame({
#     'Close': [100, 102, 101, 104, 103, 105, 106, 108, 107, 109],
#     'Low': [99, 100, 99, 101, 102, 103, 104, 106, 105, 107],
#     'High': [101, 103, 102, 105, 104, 106, 107, 109, 108, 110]
# })

# kdj_values = calculate_kdj(data)
# print(kdj_values)


def stochastic_oscillator(data, n=14, d_window=3):
    # 计算 %K
    low_n = data["Low"].rolling(window=n, min_periods=1).min()
    high_n = data["High"].rolling(window=n, min_periods=1).max()
    data["%K"] = (data["Close"] - low_n) / (high_n - low_n) * 100

    # 计算 %D
    data["%D"] = data["%K"].rolling(window=d_window, min_periods=1).mean()

    return data[["%K", "%D"]]



def calculate_macd(data, short_window=12, long_window=26, signal_window=9):
    # 计算短期和长期 EMA
    short_ema = data["Close"].ewm(span=short_window, adjust=False).mean()
    long_ema = data["Close"].ewm(span=long_window, adjust=False).mean()

    # 计算 DIF (快线)
    data["DIF"] = short_ema - long_ema

    # 计算 DEA (慢线)
    data["DEA"] = data["DIF"].ewm(span=signal_window, adjust=False).mean()

    # 计算 MACD 柱状图
    data["Histogram"] = data["DIF"] - data["DEA"]

    return data[["DIF", "DEA", "Histogram"]]


# 示例数据
# data = pd.DataFrame({
#     'Close': [100, 102, 101, 104, 103, 105, 106, 108, 107, 109]
# })

# macd_values = calculate_macd(data)
# print(macd_values)
from sqlalchemy import create_engine
import pymysql
# engine = create_engine("mysql+pymysql://root:chen@127.0.0.1:3306/gp")
conn = pymysql.connect(
            host='127.0.0.1',
            user='root',
            password='chen',
            database='gp',
            # use_unicode=args.encoding,
        )
cursor = conn.cursor()
cursor.execute("truncate gp.stock_data")
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
        
now = datetime.now()

# conn.commit()
# cursors.close()
# 定义一个当天15:00的时间对象
mid_time = now.replace(hour=15, minute=0, second=0, microsecond=0)
end_date=''
# 判断当前时间是否大于15:00
if now > mid_time:
    # 如果大于15:00，打印当前日期
    end_date=now
    print("爬取截至时间:", now.strftime('%Y-%m-%d'))
else:
    # 否则打印前一天的日期
    yesterday = now - timedelta(days=1)
    end_date=yesterday
    print("爬取截至时间:", yesterday.strftime('%Y-%m-%d'))
start_date=now - timedelta(days=365)
print(start_date)
start_date=start_date.strftime("%Y%m%d")
end_date=end_date.strftime("%Y%m%d")

stock_sh_a_spot_em_dfs=pd.read_sql("select * from now_gp_price",con=conn)
stock_sh_a_spot_em_dfs['代码']=stock_sh_a_spot_em_dfs['代码'].str.replace('"','')
stock_sh_a_spot_em_df = stock_sh_a_spot_em_dfs
a=0
print(stock_sh_a_spot_em_df.index.size)
for index, row in stock_sh_a_spot_em_df.iterrows():
    sleeps=random.random()
    print(index,sleeps)
    if a==1500:
        print("sleep 60")
        time.sleep(60)
        a=a+1
    else:
        time.sleep(sleeps)
    stock_zh_a_hist_df = ak.stock_zh_a_hist(
        symbol=row["代码"],
        period="daily",
        start_date=start_date,
        end_date=end_date,
        adjust="",
    )
    stock_zh_a_hist_df = stock_zh_a_hist_df.rename(
        columns={
            "日期": "Date",
            "开盘": "Open",
            "收盘": "Close",
            "最高": "High",
            "最低": "Low",
            "成交量": "Volume",
            "成交额": "Amount",
            "振幅": "Amplitude",
            "涨跌幅": "Pct Change",
            "涨跌额": "Price Change",
            "换手率": "Turnover Rate",
        }
    )
    if stock_zh_a_hist_df.empty:
        print("stock_zh_a_hist_df.empty",stock_zh_a_hist_df.empty)
        continue
    try:
        stock_zh_a_hist_df["RSI"] = calculate_rsi(stock_zh_a_hist_df, columns="Close")
        stock_zh_a_hist_df[["K", "D", "J"]] = calculate_kdj(stock_zh_a_hist_df)
        stock_zh_a_hist_df[["%K", "%D"]] = stochastic_oscillator(stock_zh_a_hist_df)
        stock_zh_a_hist_df[["DIF", "DEA", "Histogram"]] = calculate_macd(
            stock_zh_a_hist_df
        )
        stock_zh_a_hist_df["code"] = row["代码"]
        stock_zh_a_hist_df["name"] = row["名称"]
        stock_zh_a_hist_df = stock_zh_a_hist_df[['Date','code', 'name', 'Open', 'Close', 'High', 'Low', 'Volume', 'Amount',
       'Amplitude', 'Pct Change', 'Price Change', 'Turnover Rate', 'RSI', 'K',
       'D', 'J', '%K', '%D', 'DIF', 'DEA', 'Histogram']]
        stock_zh_a_hist_df=stock_zh_a_hist_df.rename(columns={"Pct Change":'Pct_Change', 
                                                             'Price Change':'Price_Change',
                                                             "Turnover Rate":'Turnover_Rate',
                                                             '%K':'Percent_K',
                                                             '%D':'Percent_D'})
        stock_zh_a_hist_df=stock_zh_a_hist_df.fillna(0)
        sql = f"REPLACE INTO gp.stock_data(`{'`,`'.join(stock_zh_a_hist_df.columns)}`) VALUES ({','.join(['%s' for _ in range(stock_zh_a_hist_df.shape[1])])})"
        toSql(sql=sql,rows=stock_zh_a_hist_df.values.tolist())
    except Exception as e:
        print(e)
        exit()
# cursor.close()
        # print(row["代码"], row["名称"])
        # continue