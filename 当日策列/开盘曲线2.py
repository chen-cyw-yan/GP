import pandas as pd
import numpy as np
import akshare as ak
import matplotlib.pyplot as plt
from sklearn.linear_model import LinearRegression, LogisticRegression
import threading
import time
import os
import os
import shutil

plt.rcParams['font.sans-serif'] = ['SimHei']
plt.rcParams['axes.unicode_minus'] = False


class StockAnalyzer:

    def __init__(self, stock, name):

        self.stock = stock
        self.name = name


    def get_data(self):

        df = ak.stock_zh_a_tick_tx_js(symbol=self.stock)

        df['成交时间'] = pd.to_datetime(df['成交时间'])
        df = df.sort_values('成交时间')
        df = df.set_index('成交时间')

        df['type_code'] = 0
        df.loc[df['性质'].str.contains('买', na=False), 'type_code'] = 1
        df.loc[df['性质'].str.contains('卖', na=False), 'type_code'] = -1

        return df


    def plot_all(self, df):

        df = df.copy()

        df["t"] = np.arange(len(df))

        df["buy_vol"] = np.where(df["type_code"]==1, df["成交量"], 0)
        df["sell_vol"] = np.where(df["type_code"]==-1, df["成交量"], 0)

        X = df[["t"]]

        # 线性回归
        model_buy = LinearRegression().fit(X, df["buy_vol"])
        model_sell = LinearRegression().fit(X, df["sell_vol"])

        buy_pred = model_buy.predict(X)
        sell_pred = model_sell.predict(X)

        # 逻辑回归
        y = np.where(df["type_code"]==1,1,0)
        log_model = LogisticRegression()
        log_model.fit(X,y)

        prob = log_model.predict_proba(X)

        buy_prob = prob[:,1]
        sell_prob = prob[:,0]

        # 资金差
        delta = df["buy_vol"] - df["sell_vol"]
        cvd = delta.cumsum()

        plt.figure(figsize=(12,8))

        # 成交量
        plt.subplot(3,1,1)
        plt.plot(df.index,buy_pred,label="买盘趋势")
        plt.plot(df.index,sell_pred,label="卖盘趋势")
        plt.legend()
        plt.title(f"{self.name} {self.stock} 成交量趋势")

        # 概率
        plt.subplot(3,1,2)
        plt.plot(df.index,buy_prob,label="买盘概率")
        plt.plot(df.index,sell_prob,label="卖盘概率")
        plt.legend()
        plt.title("买卖概率")

        # 资金流
        plt.subplot(3,1,3)
        plt.plot(df.index,cvd,label="资金流(CVD)")
        plt.legend()

        plt.tight_layout()

        if not os.path.exists("charts"):
            os.mkdir("charts")

        plt.savefig(f"charts/{self.stock}.png")

        plt.close()


def monitor_stock(stock,name):

    analyzer = StockAnalyzer(stock,name)

    while True:

        try:

            print("更新:",name,stock)

            df = analyzer.get_data()

            analyzer.plot_all(df)

        except Exception as e:

            print(stock,"错误:",e)

        time.sleep(60)


def run(stocks):

    threads = []

    for code,name in stocks:

        t = threading.Thread(
            target=monitor_stock,
            args=(code,name)
        )
        t.start()

        threads.append(t)

        time.sleep(2)  # 防止API同时请求


def clear_folder(folder_path):
    if not os.path.exists(folder_path):
        print(f"文件夹 {folder_path} 不存在")
        return

    # 遍历文件夹中的所有内容
    for filename in os.listdir(folder_path):
        file_path = os.path.join(folder_path, filename)
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)  # 删除文件
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)  # 删除目录
            # print(f"已删除: {file_path}")
        except Exception as e:
            print(f"删除 {file_path} 时出错: {e}")
    
    print(f"✅ 文件夹 {folder_path} 已清空")

# 使用方法：替换为你

if __name__ == "__main__":
    clear_folder('E:\stock\GP\charts')
    stocks = [
    # ('sz002339','积成电子'),
    # ('sh600468','百利电气'),
    # ('sz000815','美利云'),
    # ('sh600590','泰豪科技'),
    # ('sz003036','泰坦股份'),
    ('sh603316','诚邦股份'),
    # ('sh601868','智微智能'),
    ('sh603949','雪龙集团')

    ]

    run(stocks)