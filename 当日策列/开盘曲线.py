import pandas as pd
import numpy as np
import akshare as ak
import matplotlib.pyplot as plt
from datetime import datetime, time
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from sklearn.linear_model import LinearRegression
plt.rcParams['font.sans-serif'] = ['SimHei']
plt.rcParams['axes.unicode_minus'] = False


class EarlySessionAnalyzer:

    def __init__(self, df):

        self.df = df.copy()

        self.df['成交时间'] = pd.to_datetime(self.df['成交时间'])
        self.df = self.df.sort_values('成交时间')
        self.df = self.df.set_index('成交时间')

        # 买卖方向
        self.df['type_code'] = 0
        self.df.loc[self.df['性质'].str.contains('买', na=False), 'type_code'] = 1
        self.df.loc[self.df['性质'].str.contains('卖', na=False), 'type_code'] = -1


    def get_period_data(self, minutes):

        start = datetime.combine(self.df.index[0].date(), time(9,30))
        end = start + pd.Timedelta(minutes=minutes)

        return self.df[(self.df.index >= start) & (self.df.index <= end)]


    def analyze_period(self, minutes):

        data = self.get_period_data(minutes)

        if data.empty:
            print("无数据")
            return None

        buy_vol = data.loc[data['type_code']==1,'成交量'].sum()
        sell_vol = data.loc[data['type_code']==-1,'成交量'].sum()

        total = buy_vol + sell_vol

        print(f"\n===== 前{minutes}分钟 =====")

        print("主动买入量:", buy_vol)
        print("主动卖出量:", sell_vol)

        print("买入占比:", round(buy_vol/total,3) if total>0 else 0)
        print("卖出占比:", round(sell_vol/total,3) if total>0 else 0)

        print("买卖量比:", round(buy_vol/sell_vol,2) if sell_vol>0 else "inf")

        return data


    def plot_volume_curve(self, data, minutes):

        df = data.copy()

        df['buy_vol'] = np.where(df['type_code']==1, df['成交量'], np.nan)
        df['sell_vol'] = np.where(df['type_code']==-1, df['成交量'], np.nan)

        # 只保留买盘点
        buy_df = df.dropna(subset=['buy_vol'])

        # 只保留卖盘点
        sell_df = df.dropna(subset=['sell_vol'])

        plt.figure(figsize=(12,6))

        plt.plot(
            buy_df.index,
            buy_df['buy_vol'],
            label='主动买入量',
            linewidth=2
        )

        plt.plot(
            sell_df.index,
            sell_df['sell_vol'],
            label='主动卖出量',
            linewidth=2
        )

        plt.title(f"前{minutes}分钟 买卖成交量曲线(平滑)")
        plt.xlabel("时间")
        plt.ylabel("成交量(手)")

        plt.legend()
        plt.grid(True)

        plt.show()
    def plot_regression_curve(self, df):

        df = df.copy()

        # 时间转序号
        df["t"] = np.arange(len(df))

        # 买卖量
        df["buy_vol"] = np.where(df["type_code"]==1, df["成交量"], 0)
        df["sell_vol"] = np.where(df["type_code"]==-1, df["成交量"], 0)

        X = df[["t"]]

        # 买盘回归
        model_buy = LinearRegression()
        model_buy.fit(X, df["buy_vol"])
        buy_pred = model_buy.predict(X)

        # 卖盘回归
        model_sell = LinearRegression()
        model_sell.fit(X, df["sell_vol"])
        sell_pred = model_sell.predict(X)

        plt.figure(figsize=(12,6))

        # 原始数据
        plt.plot(df.index, df["buy_vol"], alpha=0.3, label="买盘原始")
        plt.plot(df.index, df["sell_vol"], alpha=0.3, label="卖盘原始")

        # 回归趋势
        plt.plot(df.index, buy_pred, linewidth=3, label="买盘趋势线")
        plt.plot(df.index, sell_pred, linewidth=3, label="卖盘趋势线")

        plt.legend()
        plt.title("买卖盘趋势回归")
        plt.xlabel("时间")
        plt.ylabel("成交量")

        plt.grid(True)
        plt.show()

def run(stock):

    df = ak.stock_zh_a_tick_tx_js(symbol=stock)

    analyzer = EarlySessionAnalyzer(df)

    for m in [5,10,15]:

        data = analyzer.analyze_period(m)

    if data is not None:
        analyzer.plot_regression_curve(data)


if __name__ == "__main__":

    run("sh601868")