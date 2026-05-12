from typing import Any

import pandas as pd
import requests
import warnings
from sqlalchemy import create_engine
import logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
def stock_zh_a_tick_tx_js(symbol: str = "sz000001",timeout=30,page_size=-1,page=0) -> pd.DataFrame:
    """
    腾讯财经-历史分笔数据
    https://gu.qq.com/sz300494/gp/detail
    :param symbol: 股票代码
    :type symbol: str
    :return: 历史分笔数据
    :rtype: pandas.DataFrame
    """
    big_df = pd.DataFrame()
    # page = 0
    warnings.warn("正在下载数据，请稍等")
    if page_size == -1:
        while True:
            try:
                url = "http://stock.gtimg.cn/data/index.php"
                params = {
                    "appn": "detail",
                    "action": "data",
                    "c": symbol,
                    "p": page,
                }
                r = requests.get(url, params=params,timeout=timeout)
                text_data = r.text
                temp_df = (
                    pd.DataFrame(eval(text_data[text_data.find("[") :])[1].split("|"))
                    .iloc[:, 0]
                    .str.split("/", expand=True)
                )
                page += 1
                big_df = pd.concat([big_df, temp_df], ignore_index=True)
            except:  # noqa: E722
                break
    else:
        try:
            url = "http://stock.gtimg.cn/data/index.php"
            params = {
                    "appn": "detail",
                    "action": "data",
                    "c": symbol,
                    "p": page,
                }
            r = requests.get(url, params=params,timeout=timeout)
            text_data = r.text
            temp_df = (
                    pd.DataFrame(eval(text_data[text_data.find("[") :])[1].split("|"))
                    .iloc[:, 0]
                    .str.split("/", expand=True)
                )
            big_df = pd.concat([big_df, temp_df], ignore_index=True)
        except:  # noqa: E722
            print("获取数据失败")
    if not big_df.empty:
        big_df = big_df.iloc[:, 1:].copy()
        big_df.columns = [
            "成交时间",
            "成交价格",
            "价格变动",
            "成交量",
            "成交金额",
            "性质",
        ]
        big_df.reset_index(drop=True, inplace=True)
        property_map = {
            "S": "卖盘",
            "B": "买盘",
            "M": "中性盘",
        }
        big_df["性质"] = big_df["性质"].map(property_map)
        big_df = big_df.astype(
            {
                "成交时间": str,
                "成交价格": float,
                "价格变动": float,
                "成交量": int,
                "成交金额": int,
                "性质": str,
            }
        )
    return big_df

def load_data(engine):
    sql = """
    SELECT *
    FROM stock
    WHERE volume IS NOT NULL
    ORDER BY code, date
    """
    df = pd.read_sql(sql, engine)
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values(['code', 'date']).reset_index(drop=True)
    return df

def build_features(df):

    df['ret'] = df.groupby('code')['close'].pct_change()
    df['ret_oc'] = df['close'] / df['open'] - 1

    df['vol_ma5'] = df.groupby('code')['volume'].transform(lambda x: x.rolling(5).mean())
    df['vol_ma10'] = df.groupby('code')['volume'].transform(lambda x: x.rolling(10).mean())

    df['price_ma20'] = df.groupby('code')['close'].transform(lambda x: x.rolling(20).mean())

    df['range'] = (df['high'] - df['low']) / df['close']
    df['range_ma3'] = df.groupby('code')['range'].transform(lambda x: x.rolling(3).mean())

    # === 均线 ===
    df['ma5'] = df.groupby('code')['close'].transform(lambda x: x.rolling(5).mean())
    df['ma10'] = df.groupby('code')['close'].transform(lambda x: x.rolling(10).mean())
    df['ma20'] = df.groupby('code')['close'].transform(lambda x: x.rolling(20).mean())

    return df


# ==========================
# 3️⃣ 信号生成
# ==========================
def generate_signals(df):

    # === 试盘日 ===
    df['up_move'] = (df['high'] - df['open']) / df['open']
    df['pullback'] = (df['high'] - df['close']) / (df['high'] - df['open'] + 1e-9)

    df['is_spike_day'] = (
        (df['up_move'] > 0.03) &
        (df['pullback'] > 0.4) &
        (df['close'] > df['open'] * 0.98)
    )

    df['spike_low'] = df['low'].where(df['is_spike_day'])
    df['spike_high'] = df['high'].where(df['is_spike_day'])

    df['spike_low'] = df.groupby('code')['spike_low'].ffill()
    df['spike_high'] = df.groupby('code')['spike_high'].ffill()

    # === 缩量整理 ===
    df['vol_shrink'] = df['volume'] < df['vol_ma5'] * 0.8
    df['no_break'] = df['low'] > df['spike_low']
    df['range_shrink'] = df['range'] < df['range_ma3']

    df['is_consolidation'] = (
        df['vol_shrink'] &
        df['no_break'] &
        df['range_shrink']
    )

    # === 买点 ===

    df['trend_ok'] = df['close'] > df['price_ma20']

    df['buy_signal'] = (
        df['is_consolidation'].shift(1) &
        # df['vol_expand'] &
        # df['breakout'] &
        df['trend_ok']
    )
    df['vol_expand'] = (
        (df['volume'] > df['volume'].shift(1) * 1.1) &
        (df['volume'] < df['volume'].shift(1) * 1.5)
    )

    df['breakout'] = df['close'] > df['high'].shift(1)



    df['min_sell_price'] = df['spike_low'] * 0.98


    return df


def get_future_single_stock():
    engine = create_engine("mysql+pymysql://root:chen@127.0.0.1:3306/gp")
    logger.info("="*30 + " 加载数据 " + "="*30)
    df = load_data(engine)
    logger.info("="*30 + " 计算因子 " + "="*30)
    df = build_features(df)
    logger.info("="*30 + " 计算信号 " + "="*30)
    df = generate_signals(df)



if __name__ == "__main__":
    df = stock_zh_a_tick_tx_js("sz002560",timeout=30,page_size=100,page=1)
    print(df)
    dt=get_future_single_stock()
    print(dt)