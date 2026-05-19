from typing import Dict
import sys
import os
import json
import time
import logging
import numpy as np
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine
from typing import Tuple, List, Optional
import pymysql
import pandas as pd
import numpy as np
from eltdx import TdxClient
import tqdm
from sklearn.preprocessing import MinMaxScaler
from sklearn.linear_model import LinearRegression

# result 表头中英文对照
RESULT_COLUMNS_CN = {
    "code": "股票代码",
    "name": "股票名称",
    "first_price": "竞价首价",
    "last_price": "竞价末价",
    "auction_intraday_rate": "竞价日内涨幅",
    "price_slope": "价格斜率",
    "match_slope": "撮合量斜率",
    "auction_volume_lots": "竞价成交量(手)",
    "auction_volume_shares": "竞价成交量(股)",
    "buy_unmatched": "买未匹配量",
    "sell_unmatched": "卖未匹配量",
    "bid_ask_ratio": "买卖未匹配比",
    "stability": "撤单稳定性",
    "auction_score": "竞价综合分",
    "prev_close": "昨收",
    "outstanding_share": "流通股本(万股)",
    "auction_change_rate": "竞价涨幅",
    "auction_vol_of_share": "成交量占股本(%)",
    "auction_turnover": "竞价换手率(%)",
}


def fetch_stock_context(engine, before_date: str) -> pd.DataFrame:
    """
    读取股票上下文：上一交易日行情 + 最近一条非零流通股本（万股）。

    参数:
        engine: SQLAlchemy 引擎
        before_date: 基准日期（不含），通常为当日交易日 YYYY-MM-DD

    返回:
        以 code 为索引的 DataFrame，含 prev_close、outstanding_share 等字段
    """
    sql_prev = """
    SELECT s.code, s.date AS prev_date, s.close AS prev_close,
           s.open AS prev_open, s.high AS prev_high, s.low AS prev_low,
           s.volume AS prev_volume, s.turnover AS prev_turnover, s.name AS prev_name
    FROM gp.stock s
    INNER JOIN (
        SELECT code, MAX(date) AS max_d
        FROM gp.stock
        WHERE date < %(before_date)s
        GROUP BY code
    ) t ON s.code = t.code AND s.date = t.max_d
    """
    sql_share = """
    SELECT s.code, s.outstanding_share, s.date AS share_date
    FROM gp.stock s
    INNER JOIN (
        SELECT code, MAX(date) AS max_d
        FROM gp.stock
        WHERE outstanding_share IS NOT NULL
          AND outstanding_share != 0
        GROUP BY code
    ) t ON s.code = t.code AND s.date = t.max_d
    """
    prev_df = pd.read_sql(sql_prev, con=engine, params={"before_date": before_date})
    share_df = pd.read_sql(sql_share, con=engine)
    ctx = prev_df.merge(share_df, on="code", how="left")
    return ctx.set_index("code")
def calc_auc_features(
    df: pd.DataFrame,
    prev_close: Optional[float] = None,
    outstanding_share: Optional[float] = None,
    stock_name: Optional[str] = None,
) -> Dict:
    """
    计算单只股票竞价强度特征

    注意：
    1. 撤单分析：
       使用 09:15~09:25 全量数据

    2. 趋势分析（价格斜率/撮合量斜率）：
       仅使用 09:20~09:25 数据
       因为该阶段不可撤单，更真实

    参数:
    df:
        code
        time
        price
        match（手）
        unmatched
        flag

    prev_close:
        昨收

    outstanding_share:
        流通股本（万股）

    """

    df = df.copy()

    # =========================================
    # 时间排序
    # =========================================
    df = df.sort_values("time").reset_index(drop=True)

    # =========================================
    # 基础过滤
    # =========================================
    if len(df) < 5:
        return None

    # =========================================
    # 全量数据（用于撤单分析）
    # =========================================
    df_all = df.copy()

    # =========================================
    # 仅保留 09:20 以后（用于趋势分析）
    # =========================================
    auction_start = pd.to_datetime("09:20:00").time()

    df_real = df.loc[
        df["time"].dt.time >= auction_start
    ].copy()

    # 趋势数据太少
    if len(df_real) < 3:
        return None

    # =========================================
    # 时间转秒
    # =========================================
    start_time = df_real["time"].min()

    df_real["sec"] = (
        (df_real["time"] - start_time)
        .dt.total_seconds()
    )

    # =========================================
    # 价格趋势斜率
    # 仅使用09:20后数据
    # =========================================
    x = df_real["sec"].values.reshape(-1, 1)

    y = df_real["price"].values

    model = LinearRegression()

    model.fit(x, y)

    # 原始斜率
    price_slope_raw = model.coef_[0]

    # 放大方便观察
    price_slope = price_slope_raw * 1000

    # =========================================
    # 竞价日内涨幅
    # 首价 -> 末价
    # =========================================
    first_price = df_real["price"].iloc[0]

    last_price = df_real["price"].iloc[-1]

    auction_intraday_rate = (
        (last_price - first_price)
        / first_price
    )

    # =========================================
    # 撮合量趋势斜率
    # 仅使用09:20后数据
    # =========================================
    y_match = df_real["match"].values

    model_match = LinearRegression()

    model_match.fit(x, y_match)

    match_slope = model_match.coef_[0]

    # =========================================
    # 买卖未匹配量
    # 使用全量数据
    # =========================================
    buy_df = df_all[df_all["flag"] == 1]

    sell_df = df_all[df_all["flag"] == -1]

    buy_unmatched = buy_df["unmatched"].sum()

    sell_unmatched = sell_df["unmatched"].sum()

    # 买卖未匹配比
    if sell_unmatched == 0:

        bid_ask_ratio = 999

    else:

        bid_ask_ratio = (
            buy_unmatched / sell_unmatched
        )

    # =========================================
    # 撤单稳定性
    # 使用全量数据
    # =========================================
    buy_series = (
        buy_df.groupby("time")["unmatched"]
        .sum()
        .sort_index()
    )

    if len(buy_series) >= 2:

        diff = buy_series.diff()

        # 买单减少 -> 认为撤单
        cancel_count = (diff < 0).sum()

        # 稳定性
        stability = 1 / (1 + cancel_count)

    else:

        stability = 0

    # =========================================
    # 竞价成交量
    # 通达信 match 单位是 手
    # =========================================
    auction_volume_lots = float(
        df_real["match"].iloc[-1]
    )

    # 股
    auction_volume_shares = (
        auction_volume_lots * 100
    )

    # =========================================
    # 基础结果
    # =========================================
    result = {

        "code": df["code"].iloc[0],

        "name": stock_name,

        "first_price": round(first_price, 4),

        "last_price": round(last_price, 4),

        "auction_intraday_rate": round(
            auction_intraday_rate,
            4
        ),

        "price_slope": round(
            price_slope,
            6
        ),

        "match_slope": round(
            match_slope,
            2
        ),

        "auction_volume_lots": round(
            auction_volume_lots,
            2
        ),

        "auction_volume_shares": int(
            auction_volume_shares
        ),

        "buy_unmatched": int(
            buy_unmatched
        ),

        "sell_unmatched": int(
            sell_unmatched
        ),

        "bid_ask_ratio": round(
            bid_ask_ratio,
            2
        ),

        "stability": round(
            stability,
            4
        ),
    }

    # =========================================
    # 计算竞价涨幅/换手率
    # =========================================
    if (
        prev_close is not None
        and prev_close > 0
        and outstanding_share is not None
        and outstanding_share > 0
    ):

        # -------------------------------------
        # 竞价涨幅
        # 昨收 -> 竞价末价
        # -------------------------------------
        auction_change_rate = (
            last_price / prev_close - 1
        )

        # -------------------------------------
        # 流通股本（股）
        # outstanding_share 单位：万股
        # -------------------------------------
        float_shares = (
            outstanding_share * 10000
        )

        # -------------------------------------
        # 成交量占股本
        # -------------------------------------
        auction_vol_of_share = (
            auction_volume_shares
            / float_shares
        ) * 100

        # -------------------------------------
        # 竞价换手率(%)
        # -------------------------------------
        auction_turnover = (
            auction_volume_shares
            / float_shares
        ) * 100

        result.update({

            "prev_close": round(
                float(prev_close),
                4
            ),

            "outstanding_share": round(
                float(outstanding_share),
                4
            ),

            "auction_change_rate": round(
                auction_change_rate,
                4
            ),

            "auction_vol_of_share": round(
                auction_vol_of_share,
                6
            ),

            "auction_turnover": round(
                auction_turnover,
                6
            ),
        })

    else:

        result.update({

            "prev_close": None,

            "outstanding_share": None,

            "auction_change_rate": None,

            "auction_vol_of_share": None,

            "auction_turnover": None,
        })

    return result


def rank_auction(
    df: pd.DataFrame,
    stock_context: Optional[pd.DataFrame] = None,
    code_name_map: Optional[dict] = None,
) -> pd.DataFrame:

    result = []

    for code, g in df.groupby("code"):

        try:

            prev_close = None
            outstanding_share = None
            stock_name = None

            if code_name_map and code in code_name_map:
                stock_name = code_name_map.get(code)

            if stock_context is not None and code in stock_context.index:

                row = stock_context.loc[code]

                prev_close = row.get("prev_close")
                outstanding_share = row.get("outstanding_share")
                if not stock_name:
                    stock_name = row.get("prev_name")

            features = calc_auc_features(
                g,
                prev_close=prev_close,
                outstanding_share=outstanding_share,
                stock_name=stock_name,
            )

            if features is not None:
                result.append(features)

        except Exception as e:

            print(code, e)

    result_df = pd.DataFrame(result)

    # ====================================
    # 过滤
    # ====================================

    result_df = result_df.loc[
        result_df["price_slope"] > 0
    ]

    result_df = result_df.loc[
        result_df["auction_turnover"] > 0.002
    ]

    # ====================================
    # 极端值截断
    # ====================================

    clip_cols = [

        "bid_ask_ratio",
        "match_slope",
        "price_slope",
        "auction_turnover",
        "auction_change_rate",

    ]

    for col in clip_cols:

        if col in result_df.columns:

            low = result_df[col].quantile(0.01)
            high = result_df[col].quantile(0.99)

            result_df[col] = result_df[col].clip(
                lower=low,
                upper=high
            )

    # ====================================
    # 标准化
    # ====================================
    # ====================================
# 空结果保护
# ====================================

    if result_df.empty:

        print("过滤后无有效竞价数据")

        return pd.DataFrame()
    factor_cols = [

        "price_slope",
        "bid_ask_ratio",
        "stability",
        "auction_change_rate",
        "auction_turnover",

    ]

    scaler = MinMaxScaler()

    scaled_values = scaler.fit_transform(
        result_df[factor_cols]
    )

    scaled_df = pd.DataFrame(
        scaled_values,
        columns=factor_cols,
        index=result_df.index
    )

    # ====================================
    # 综合评分
    # ====================================

    result_df["auction_score"] = (

        0.30 * scaled_df["price_slope"] +

        0.25 * scaled_df["bid_ask_ratio"] +

        0.20 * scaled_df["auction_turnover"] +

        0.15 * scaled_df["stability"] +

        0.10 * scaled_df["auction_change_rate"]

    )

    # ====================================
    # 排序
    # ====================================

    result_df = result_df.sort_values(
        "auction_score",
        ascending=False
    )

    # 股票代码、名称放前两列
    lead_cols = ["code", "name"]
    other_cols = [c for c in result_df.columns if c not in lead_cols]
    result_df = result_df[lead_cols + other_cols]

    result_df = result_df.rename(
        columns=RESULT_COLUMNS_CN
    )

    return result_df


def get_data(engine, is_all: bool = False) -> pd.DataFrame:
    """
    获取待计算股票列表。

    is_all=False: stock_analysis 中 need_to_analysis=1 的股票
    is_all=True:  gp.stock 表全部股票（每只股票取最新交易日一条）
    """
    if is_all:
        sql = """
        SELECT s.code, s.name, s.date
        FROM gp.stock s
        INNER JOIN (
            SELECT code, MAX(date) AS max_date
            FROM gp.stock
            GROUP BY code
        ) t ON s.code = t.code AND s.date = t.max_date
        """
        df_analy = pd.read_sql(sql, con=engine)
        df_analy = df_analy.rename(
            columns={"code": "stock_code", "name": "stock_name", "date": "trade_date"}
        )
    else:
        sql_analy = (
            "SELECT * FROM gp.stock_analysis WHERE need_to_analysis = 1"
        )
        df_analy = pd.read_sql(sql_analy, con=engine)
    return df_analy


def get_auction_data(df: pd.DataFrame) -> pd.DataFrame:
    with TdxClient() as client:
        dfs_ls = []
        for _, v in tqdm.tqdm(df.iterrows(), total=len(df)):
            code = v["stock_code"]
            # date = v['date']
            # df_auction = client.get_auction_data(code, date)
            auction =client.get_call_auction(code,include_raw=True)
            rows=[]
            for i in auction.items:
                # print(i.time, i.price, i.flag,i.match,i.unmatched)
                rows.append({'time':i.time,'price':i.price,'match':i.match,'unmatched':i.unmatched,'flag':i.flag})
            if not rows:
                continue
            dfs = pd.DataFrame(rows)
            dfs["code"] = code
            dfs_ls.append(dfs)
        if not dfs_ls:
            return pd.DataFrame()
        return pd.concat(dfs_ls, ignore_index=True)


def main():
    DB_URL = "mysql+pymysql://root:chen@127.0.0.1:3306/gp"
    engine = create_engine(DB_URL)
    # True: 计算 gp.stock 全部股票；False: 仅 stock_analysis 待分析股票
    use_all_stocks = True
    df_analy = get_data(engine, is_all=use_all_stocks)
    # 以任务列表最近交易日为基准，查询其上一交易日行情
    trade_date = pd.to_datetime(df_analy["trade_date"].max()).strftime("%Y-%m-%d")
    stock_context = fetch_stock_context(engine, before_date=trade_date)
    code_name_map = (
        df_analy.drop_duplicates("stock_code")
        .set_index("stock_code")["stock_name"]
        .to_dict()
        if "stock_name" in df_analy.columns
        else None
    )
    df_auction = get_auction_data(df_analy)
    result = rank_auction(
        df_auction,
        stock_context=stock_context,
        code_name_map=code_name_map,
    )
    print(result.columns)
    result = result.loc[result["价格斜率"] > 0]
    result['竞价日内涨幅']=result['竞价日内涨幅'].round(4)
    result['价格斜率']=result['价格斜率'].round(6)
    result['竞价综合分']=result['竞价综合分'].round(4)
    result['竞价涨幅']=result['竞价涨幅'].round(4)
    # result['成交量占股本(%)']=result['成交量占股本'].round(6)
    # result['竞价换手率(%)']=result['竞价换手率'].round(6)




    print(result)
    result.to_csv(r"C:\Users\cyw\Desktop\jupyternotebook\git-python\GP\回测\result.csv",index=False)

if __name__ == "__main__":
    main()