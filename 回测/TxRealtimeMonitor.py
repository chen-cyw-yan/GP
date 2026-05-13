import requests
import pandas as pd
import threading
import time
from collections import deque


class TxRealtimeMonitor:

    def __init__(self):

        self.running = True

        self.session = requests.Session()

        # 股票状态
        self.state_map = {}

    def get_page(self, symbol, page):

        url = "http://stock.gtimg.cn/data/index.php"

        params = {
            "appn": "detail",
            "action": "data",
            "c": symbol,
            "p": page,
        }

        r = self.session.get(url, params=params, timeout=10)

        text_data = r.text

        temp_df = (
            pd.DataFrame(
                eval(text_data[text_data.find("["):])[1].split("|")
            )
            .iloc[:, 0]
            .str.split("/", expand=True)
        )

        if temp_df.empty:
            return pd.DataFrame()

        temp_df = temp_df.iloc[:, 1:].copy()

        temp_df.columns = [
            "成交时间",
            "成交价格",
            "价格变动",
            "成交量",
            "成交金额",
            "性质",
        ]

        return temp_df

    def make_key(self, row):

        return f"{row['成交时间']}_{row['成交价格']}_{row['成交量']}"

    def init_symbol(self, symbol):

        if symbol not in self.state_map:

            self.state_map[symbol] = {

                # 当前读取页
                "page": 0,

                # 去重缓存
                "seen_keys": deque(maxlen=5000),
            }

    def monitor_symbol(self, symbol, sleep_sec):

        print(f"{symbol} 开始监听")

        self.init_symbol(symbol)

        while self.running:

            try:

                state = self.state_map[symbol]

                current_page = state["page"]

                while True:

                    df = self.get_page(symbol, current_page)

                    # 没数据
                    if df.empty:
                        break

                    new_rows = []

                    for _, row in df.iterrows():

                        key = self.make_key(row)

                        # 去重
                        if key in state["seen_keys"]:
                            continue

                        state["seen_keys"].append(key)

                        new_rows.append(row)

                    if new_rows:

                        new_df = pd.DataFrame(new_rows)

                        print(
                            f"\n{symbol} 第{current_page}页 "
                            f"新增{len(new_df)}条"
                        )

                        print(new_df.tail())

                    # 下一页
                    current_page += 1

                # 更新page
                state["page"] = current_page

            except Exception as e:

                print(symbol, "异常:", e)

            # 统一休眠
            time.sleep(sleep_sec)

    def start(self, stock_df, sleep_sec=1):

        for symbol in stock_df["代码"].unique():

            t = threading.Thread(
                target=self.monitor_symbol,
                args=(symbol, sleep_sec),
                daemon=True
            )

            t.start()


if __name__ == "__main__":

    stock_df = pd.DataFrame({
        "代码": [
            "sz000001",
            "sz300750",
            "sh600519"
        ]
    })

    monitor = TxRealtimeMonitor()

    # 全部统一1秒轮询
    monitor.start(stock_df, sleep_sec=1)

    while True:
        time.sleep(100)