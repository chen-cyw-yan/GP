import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
from datetime import datetime, time, timedelta
import pandas as pd
import numpy as np
from datetime import time, timedelta, datetime
import logging

import numpy as np
import pandas as pd
import akshare as ak

import warnings
warnings.filterwarnings("ignore", category=UserWarning)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

def plot_trade_density(df, bins=50):

    df = df.copy()
    
    price = df["成交价格"]
    amount = df["成交金额"]

    # 加权直方图（用成交金额加权）
    plt.figure(figsize=(8,6))
    plt.hist(price, bins=bins, weights=amount)

    plt.xlabel("Price")
    plt.ylabel("Weighted Amount")
    plt.title("Trade Density Curve (Weighted by Amount)")
    plt.grid(True)
    plt.show()



def plot_intraday_fund_flow(df):

    df = df.copy()
    df["成交时间"] = pd.to_datetime(df["成交时间"])

    # 主动买卖
    df["buy_amt"] = np.where(df["性质"] == "买盘", df["成交金额"], 0)
    df["sell_amt"] = np.where(df["性质"] == "卖盘", df["成交金额"], 0)

    df["net_amt"] = df["buy_amt"] - df["sell_amt"]

    # 按时间排序
    df = df.sort_values("成交时间")

    # 累计净资金
    df["cum_net"] = df["net_amt"].cumsum()

    # ===== 画图 =====
    fig, ax1 = plt.subplots(figsize=(12,6))

    # 价格曲线
    ax1.plot(df["成交时间"], df["成交价格"])
    ax1.set_xlabel("Time")
    ax1.set_ylabel("Price")

    # 第二轴：资金曲线
    ax2 = ax1.twinx()
    ax2.plot(df["成交时间"], df["cum_net"])
    ax2.set_ylabel("Cumulative Net Fund")

    plt.title("Intraday Price & Fund Flow")
    plt.show()


if __name__ == "__main__":
    # 构造更丰富的数据以测试“回撤修复”和“时间窗口”
    df = ak.stock_zh_a_tick_tx_js(symbol='sh603949')
    
    # plot_trade_density(df=df)
    plot_intraday_fund_flow(df=df)