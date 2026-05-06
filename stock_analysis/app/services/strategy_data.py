"""
试盘策略数据：与「回测/试盘策列.py」一致的加载、因子与信号逻辑，
输出供前端 ECharts 使用的 JSON 结构。
"""
from __future__ import annotations

import logging
import threading
from typing import Any

import numpy as np
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

logger = logging.getLogger(__name__)

_load_lock = threading.Lock()
_df_cache: pd.DataFrame | None = None
_engine_uri_cache: str | None = None


def load_data(engine) -> pd.DataFrame:
    sql = """
    SELECT *
    FROM stock
    WHERE volume IS NOT NULL
    ORDER BY code, date
    """
    df = pd.read_sql(sql, engine)
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values(["code", "date"]).reset_index(drop=True)
    return df


def build_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["ret"] = df.groupby("code")["close"].pct_change()
    df["ret_oc"] = df["close"] / df["open"] - 1

    df["vol_ma5"] = df.groupby("code")["volume"].transform(lambda x: x.rolling(5).mean())
    df["vol_ma10"] = df.groupby("code")["volume"].transform(lambda x: x.rolling(10).mean())

    df["price_ma20"] = df.groupby("code")["close"].transform(lambda x: x.rolling(20).mean())

    df["range"] = (df["high"] - df["low"]) / df["close"]
    df["range_ma3"] = df.groupby("code")["range"].transform(lambda x: x.rolling(3).mean())

    df["ma5"] = df.groupby("code")["close"].transform(lambda x: x.rolling(5).mean())
    df["ma10"] = df.groupby("code")["close"].transform(lambda x: x.rolling(10).mean())
    df["ma20"] = df.groupby("code")["close"].transform(lambda x: x.rolling(20).mean())
    df["ma60"] = df.groupby("code")["close"].transform(lambda x: x.rolling(60).mean())

    def _rsi(series: pd.Series, period: int = 14) -> pd.Series:
        delta = series.diff()
        gain = delta.clip(lower=0.0)
        loss = (-delta.clip(upper=0.0))
        avg_gain = gain.rolling(period).mean()
        avg_loss = loss.rolling(period).mean()
        rs = avg_gain / (avg_loss + 1e-12)
        return 100 - (100 / (1 + rs))

    df["rsi14"] = df.groupby("code", group_keys=False)["close"].transform(lambda s: _rsi(s, 14))

    return df


def generate_signals(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df["up_move"] = (df["high"] - df["open"]) / df["open"]
    df["pullback"] = (df["high"] - df["close"]) / (df["high"] - df["open"] + 1e-9)

    df["is_spike_day"] = (
        (df["up_move"] > 0.03)
        & (df["pullback"] > 0.4)
        & (df["close"] > df["open"] * 0.98)
    )

    df["spike_low"] = df["low"].where(df["is_spike_day"])
    df["spike_high"] = df["high"].where(df["is_spike_day"])

    df["spike_low"] = df.groupby("code")["spike_low"].ffill()
    df["spike_high"] = df.groupby("code")["spike_high"].ffill()

    df["vol_shrink"] = df["volume"] < df["vol_ma5"] * 0.8
    df["no_break"] = df["low"] > df["spike_low"]
    df["range_shrink"] = df["range"] < df["range_ma3"]

    df["is_consolidation"] = df["vol_shrink"] & df["no_break"] & df["range_shrink"]

    vol_prev = df.groupby("code")["volume"].shift(1)
    df["vol_expand"] = (df["volume"] > vol_prev * 1.1) & (df["volume"] < vol_prev * 1.5)

    prev_high = df.groupby("code")["high"].shift(1)
    df["breakout"] = df["close"] > prev_high
    df["trend_ok"] = df["close"] > df["price_ma20"]

    df["buy_signal"] = (
        df.groupby("code")["is_consolidation"].shift(1).fillna(False)
        & df["vol_expand"]
        & df["breakout"]
        & df["trend_ok"]
    )

    df["min_sell_price"] = df["spike_low"] * 0.98

    df["vol_max5"] = df.groupby("code")["volume"].transform(lambda x: x.rolling(5).max())
    df["vol_ratio_v2"] = df["volume"] / df["vol_max5"]

    df["signal_low_vol_drop"] = (df["ret_oc"] < -0.04) & (df["vol_ratio_v2"] < 0.8)
    df["signal_low_vol_rise"] = (df["ret_oc"] > 0.04) & (df["vol_ratio_v2"] < 0.8)
    df["signal_high_vol_flat"] = (df["ret_oc"].abs() < 0.02) & (df["vol_ratio_v2"] > 1.5)

    df["vol_pric_err"] = "无信号"
    df.loc[df["signal_low_vol_drop"], "vol_pric_err"] = "缩量下跌"
    df.loc[df["signal_low_vol_rise"], "vol_pric_err"] = "缩量大涨"
    df.loc[df["signal_high_vol_flat"], "vol_pric_err"] = "放量横盘"

    return df


def normalize_turnover_to_percent(s: pd.Series) -> pd.Series:
    """
    库内 turnover 注释为「%」，但有些流水线写入为小数（如 0.05 表示 5%）。
    用于列表筛选时统一成「百分数」尺度（5 表示 5%）。
    """
    v = pd.to_numeric(s, errors="coerce").fillna(0.0)
    pos = v[v > 0]
    if len(pos) == 0:
        return v
    q95 = float(pos.quantile(0.95))
    if q95 <= 1.5:
        return v * 100.0
    return v


def _filter_main_board(df: pd.DataFrame) -> pd.DataFrame:
    is_main_board = ~df["code"].str[:5].isin(["sh688", "sz301", "sz300"])
    return df.loc[is_main_board].copy()


def _process_raw(df: pd.DataFrame) -> pd.DataFrame:
    df = _filter_main_board(df)
    df = build_features(df)
    df = generate_signals(df)
    return df


def invalidate_strategy_cache() -> None:
    global _df_cache, _engine_uri_cache
    with _load_lock:
        _df_cache = None
        _engine_uri_cache = None


def get_strategy_frame(engine_uri: str) -> pd.DataFrame:
    global _df_cache, _engine_uri_cache
    with _load_lock:
        if _df_cache is not None and _engine_uri_cache == engine_uri:
            return _df_cache

    logger.info("加载行情并计算试盘策略因子（首次或缓存失效）…")
    try:
        engine = create_engine(engine_uri)
        raw = load_data(engine)
    except SQLAlchemyError as e:
        logger.exception("数据库连接或查询失败")
        raise RuntimeError(f"数据库不可用: {e}") from e

    processed = _process_raw(raw)
    with _load_lock:
        _df_cache = processed
        _engine_uri_cache = engine_uri
    return processed


def _num_series_to_json(s: pd.Series) -> list[Any]:
    out = []
    for v in s.tolist():
        if v is None or (isinstance(v, float) and np.isnan(v)):
            out.append(None)
        else:
            out.append(round(float(v), 4) if isinstance(v, (float, np.floating)) else v)
    return out


def build_kline_payload(df: pd.DataFrame, code: str, tail: int = 600) -> dict[str, Any]:
    plot_df = df.loc[df["code"] == code].copy()
    if plot_df.empty:
        return {"error": f"未找到股票 {code}"}

    plot_df = plot_df.sort_values("date").reset_index(drop=True).tail(int(tail)).reset_index(drop=True)
    name = str(plot_df.iloc[0].get("name", "") or code)

    dates = plot_df["date"].dt.strftime("%Y-%m-%d").tolist()
    kline = plot_df[["open", "close", "low", "high"]].astype(float).round(4).values.tolist()
    volume = _num_series_to_json(plot_df["volume"])

    turnover_col = plot_df["turnover"] if "turnover" in plot_df.columns else pd.Series([None] * len(plot_df))
    turnover = _num_series_to_json(turnover_col.astype(float))

    mas = {}
    for ma in ("ma5", "ma10", "ma20", "ma60"):
        if ma in plot_df.columns:
            mas[ma] = _num_series_to_json(plot_df[ma])

    rsi = _num_series_to_json(plot_df["rsi14"]) if "rsi14" in plot_df.columns else []


    def mark_points(flag_col: str, price_fn):
        pts = []
        if flag_col not in plot_df.columns:
            return pts
        for i in range(len(plot_df)):
            row = plot_df.iloc[i]
            flag = row[flag_col]
            if pd.isna(flag) or not bool(flag):
                continue
            pts.append(
                {
                    "date": dates[i],
                    "value": round(float(price_fn(row)), 4),
                }
            )
        return pts

    marks = {
        "buy_signal": mark_points("buy_signal", lambda r: r["low"] * 0.99),
        "is_spike_day": mark_points("is_spike_day", lambda r: r["high"] * 1.01),
        "breakout": mark_points("breakout", lambda r: r["high"] * 1.03),
        "vol_expand": mark_points("vol_expand", lambda r: r["high"] * 1.02),
    }

    err_marks = []
    if "vol_pric_err" in plot_df.columns:
        for i, row in plot_df.iterrows():
            label = row["vol_pric_err"]
            if label == "无信号":
                continue
            price = row["close"]
            if label == "缩量下跌":
                price = row["low"] * 0.97
            elif label == "缩量大涨":
                price = row["high"] * 1.07
            err_marks.append({"date": dates[i], "value": round(float(price), 4), "label": str(label)})

    marks["vol_price_err"] = err_marks

    pct_chg = []
    close_vals = plot_df["close"].astype(float)
    prev = close_vals.shift(1)
    chg = (close_vals - prev) / prev.replace(0, np.nan) * 100
    pct_chg = _num_series_to_json(chg)

    return {
        "code": code,
        "name": name,
        "dates": dates,
        "kline": kline,
        "volume": volume,
        "turnover": turnover,
        "pct_chg": pct_chg,
        "ma": mas,
        "rsi14": rsi,
        "marks": marks,
    }


def list_buy_signals_recent(
    df: pd.DataFrame,
    turnover_min: float = 0.0,
    recent_trading_days: int = 120,
    query: str | None = None,
    page: int = 1,
    page_size: int = 20,
) -> tuple[list[dict[str, Any]], int]:
    udates = sorted(df["date"].unique())
    if not len(udates):
        return [], 0
    tail_dates = set(udates[-max(1, int(recent_trading_days)) :])
    buy_ok = df["buy_signal"].fillna(False).astype(bool)
    mask_date = df["date"].isin(tail_dates)
    if turnover_min is not None and float(turnover_min) > 0:
        t_eff = normalize_turnover_to_percent(df["turnover"])
        mask_turn = t_eff >= float(turnover_min)
    else:
        mask_turn = pd.Series(True, index=df.index)
    sub = df[mask_date & buy_ok & mask_turn].copy()
    if sub.empty:
        return [], 0

    sub = sub.sort_values("date").groupby("code", as_index=False).tail(1)

    if query:
        q = query.strip().lower()
        mask = sub["code"].str.lower().str.contains(q, na=False)
        if "name" in sub.columns:
            mask = mask | sub["name"].astype(str).str.lower().str.contains(q, na=False)
        plain = sub["code"].str.lower().str.replace(r"^(sh|sz|bj)", "", regex=True)
        mask = mask | plain.str.contains(q, na=False)
        sub = sub.loc[mask]

    sub["_t_sort"] = normalize_turnover_to_percent(sub["turnover"])
    sub = sub.sort_values(["date", "_t_sort"], ascending=[False, False])
    t_disp = normalize_turnover_to_percent(sub["turnover"])
    rows = []
    for idx, row in sub.iterrows():
        rows.append(
            {
                "code": row["code"],
                "name": str(row.get("name", "") or row["code"]),
                "date": row["date"].strftime("%Y-%m-%d"),
                "turnover": float(row["turnover"]) if pd.notna(row.get("turnover")) else None,
                "turnover_pct_display": float(t_disp.loc[idx])
                if pd.notna(row.get("turnover"))
                else None,
                "close": float(row["close"]) if pd.notna(row.get("close")) else None,
            }
        )
    total = len(rows)
    p = max(1, int(page))
    ps = max(1, min(int(page_size), 100))
    start = (p - 1) * ps
    return rows[start : start + ps], total


def strategy_search_stocks(
    df: pd.DataFrame,
    query: str,
    page: int = 1,
    page_size: int = 20,
) -> tuple[list[dict[str, str]], int]:
    q = (query or "").strip()
    if len(q) < 1:
        return [], 0

    latest = df.sort_values("date").groupby("code", as_index=False).tail(1)
    ql = q.lower()
    code_lower = latest["code"].str.lower()
    mask = code_lower.str.contains(ql, na=False)
    plain_code = code_lower.str.replace(r"^(sh|sz|bj)", "", regex=True)
    mask = mask | plain_code.str.contains(ql, na=False)
    if "name" in latest.columns:
        mask = mask | latest["name"].astype(str).str.lower().str.contains(ql, na=False)
    hit = latest.loc[mask].sort_values("code")
    total = int(len(hit))
    p = max(1, int(page))
    ps = max(1, min(int(page_size), 100))
    start = (p - 1) * ps
    chunk = hit.iloc[start : start + ps]
    items = [
        {"code": r["code"], "name": str(r.get("name", "") or r["code"])}
        for _, r in chunk.iterrows()
    ]
    return items, total
