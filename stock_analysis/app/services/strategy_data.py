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

# 侧栏市值：原始推算值（元）与展示口径的整体比例（数据源合并口径校准）
MARKET_CAP_YUAN_DISPLAY_SCALE = 100.0

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


def _turnover_scalar_to_percent(raw: Any) -> float | None:
    """单行 turnover 转为百分数尺度（与 normalize_turnover_to_percent 一致）。"""
    if raw is None or (isinstance(raw, float) and np.isnan(raw)):
        return None
    try:
        v = float(raw)
    except (TypeError, ValueError):
        return None
    if v <= 0:
        return None
    s = normalize_turnover_to_percent(pd.Series([v]))
    out = float(s.iloc[0])
    return out if out > 0 else None


def _circ_mv_yuan_from_row(row: pd.Series) -> float | None:
    """
    流通市值（元）。
    优先用 volume + turnover 反推股本（与 prod_online 中 turnover≈volume/outstanding_share、万股口径一致），
    避免 outstanding_share 实际按「股」入库时多乘 10000。
    """
    close = row.get("close")
    if close is None or pd.isna(close):
        return None
    c = float(close)
    if c <= 0:
        return None

    vol = row.get("volume")
    turnover = row.get("turnover")
    if vol is not None and pd.notna(vol) and turnover is not None and pd.notna(turnover):
        vol_f = float(vol)
        t_eff = _turnover_scalar_to_percent(turnover)
        if vol_f > 0 and t_eff is not None:
            # turnover 百分数尺度下：流通股数(股) ≈ volume(手) × 10000 / turnover
            shares = vol_f * 10000.0 / t_eff
            if shares > 0 and np.isfinite(shares):
                return c * shares

    sh = row.get("outstanding_share")
    if sh is None or pd.isna(sh):
        return None
    s = float(sh)
    if s <= 0:
        return None
    # 字段注释多为「万股」；若数值已达「股」量级（如误按股入库），不再 ×10000
    if s >= 1e8:
        return c * s
    return c * s * 10000.0


def _format_mv_yuan(yuan: float) -> str:
    if yuan >= 1e8:
        return f"{yuan / 1e8:.2f}亿"
    if yuan >= 1e4:
        return f"{yuan / 1e4:.2f}万"
    return f"{yuan:.0f}元"


def market_cap_display_from_row(row: pd.Series) -> str | None:
    """
    侧栏展示用市值文案：优先使用表中已有市值字段（元），否则用成交量/换手率或股本估算。
    结果在格式化前整体除以 MARKET_CAP_YUAN_DISPLAY_SCALE，与当前数据源量级对齐。
    """
    yuan = None
    for col in ("total_mv", "total_market_value", "circ_mv", "float_mv"):
        if col in row.index:
            raw = row.get(col)
            if raw is not None and pd.notna(raw):
                try:
                    yuan = float(raw)
                    break
                except (TypeError, ValueError):
                    pass
    if yuan is None:
        yuan = _circ_mv_yuan_from_row(row)
    if yuan is None:
        return None
    return _format_mv_yuan(yuan / MARKET_CAP_YUAN_DISPLAY_SCALE)


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
                "market_cap_display": market_cap_display_from_row(row),
            }
        )
    total = len(rows)
    p = max(1, int(page))
    ps = max(1, min(int(page_size), 100))
    start = (p - 1) * ps
    return rows[start : start + ps], total


def list_recent_spike_multi(
    df: pd.DataFrame,
    recent_trading_days: int = 7,
    min_spike_count: int = 2,
    page: int = 1,
    page_size: int = 20,
) -> tuple[list[dict[str, Any]], int]:
    """
    近期试盘：在最近 ``recent_trading_days`` 个交易日内，``is_spike_day``（试盘日）出现次数 ≥ ``min_spike_count`` 的股票。
    与 K 线图上「试盘日」标记同一套规则。
    """
    udates = sorted(df["date"].unique())
    if not len(udates):
        return [], 0
    tail_dates = set(udates[-max(1, int(recent_trading_days)) :])
    spike_ok = df["is_spike_day"].fillna(False).astype(bool)
    win = df[spike_ok & df["date"].isin(tail_dates)].copy()
    if win.empty:
        return [], 0

    agg = win.groupby("code", as_index=False).agg(
        spike_count=("date", "count"),
        last_spike=("date", "max"),
    )
    need = int(min_spike_count)
    agg = agg[agg["spike_count"] >= need]
    if agg.empty:
        return [], 0

    rep = win.sort_values("date").groupby("code", as_index=False).tail(1)
    merged = agg.merge(rep, on="code", how="inner")
    merged = merged.sort_values(["spike_count", "last_spike"], ascending=[False, False])

    t_disp = normalize_turnover_to_percent(merged["turnover"])
    rows: list[dict[str, Any]] = []
    for idx, row in merged.iterrows():
        ld = row["last_spike"]
        date_str = ld.strftime("%Y-%m-%d") if hasattr(ld, "strftime") else str(ld)[:10]
        rows.append(
            {
                "code": row["code"],
                "name": str(row.get("name", "") or row["code"]),
                "date": date_str,
                "spike_count": int(row["spike_count"]),
                "recent_window_days": int(recent_trading_days),
                "turnover": float(row["turnover"]) if pd.notna(row.get("turnover")) else None,
                "turnover_pct_display": float(t_disp.loc[idx])
                if pd.notna(row.get("turnover"))
                else None,
                "close": float(row["close"]) if pd.notna(row.get("close")) else None,
                "market_cap_display": market_cap_display_from_row(row),
            }
        )
    total = len(rows)
    p = max(1, int(page))
    ps = max(1, min(int(page_size), 100))
    start = (p - 1) * ps
    return rows[start : start + ps], total


def list_stock_analysis_startup(
    engine_uri: str,
    page: int = 1,
    page_size: int = 20,
    strategy_df: pd.DataFrame | None = None,
) -> tuple[list[dict[str, Any]], int]:
    """
    读取 gp.stock_analysis 中 need_to_analysis='1' 的记录（启动策列）。
    若传入 strategy_df（与 get_strategy_frame 一致），则按 code 匹配最新一日行情并填充 market_cap_display。
    """
    p = max(1, int(page))
    ps = max(1, min(int(page_size), 100))
    offset = (p - 1) * ps
    try:
        engine = create_engine(engine_uri)
    except SQLAlchemyError as e:
        logger.exception("创建数据库引擎失败（启动策列）")
        raise RuntimeError(f"数据库不可用: {e}") from e

    count_sql = (
        "SELECT COUNT(*) AS c FROM stock_analysis WHERE need_to_analysis = '1'"
    )
    page_sql = """
    SELECT stock_code, stock_name, trade_date, trigger_count,
           is_abnormal_type, warning_info, industry_block, concept_block,
           update_time
    FROM stock_analysis
    WHERE need_to_analysis = '1'
    ORDER BY (trade_date IS NULL) ASC, trade_date DESC, update_time DESC
    LIMIT {lim} OFFSET {off}
    """.format(
        lim=int(ps),
        off=int(offset),
    )

    try:
        total_df = pd.read_sql(count_sql, engine)
        total = int(total_df.iloc[0]["c"]) if len(total_df) else 0
        if total == 0:
            return [], 0
        df = pd.read_sql(page_sql, engine)
    except SQLAlchemyError as e:
        logger.exception("查询 stock_analysis 失败")
        raise RuntimeError(f"读取启动策列失败: {e}") from e

    latest_by_code: dict[str, pd.Series] = {}
    if strategy_df is not None and len(strategy_df.index):
        tail = strategy_df.sort_values("date").groupby("code", as_index=False).tail(1)
        for _, srow in tail.iterrows():
            latest_by_code[str(srow["code"])] = srow

    rows: list[dict[str, Any]] = []
    for _, row in df.iterrows():
        td = row.get("trade_date")
        date_str = None
        if td is not None and not pd.isna(td):
            if hasattr(td, "strftime"):
                date_str = td.strftime("%Y-%m-%d")
            else:
                date_str = str(td)[:10]
        code = str(row["stock_code"])
        s_latest = latest_by_code.get(code)
        rows.append(
            {
                "code": code,
                "name": str(row.get("stock_name") or "") or code,
                "date": date_str,
                "trigger_count": int(row["trigger_count"])
                if pd.notna(row.get("trigger_count"))
                else None,
                "is_abnormal_type": str(row["is_abnormal_type"])
                if pd.notna(row.get("is_abnormal_type")) and row.get("is_abnormal_type")
                else None,
                "warning_info": str(row["warning_info"])
                if pd.notna(row.get("warning_info")) and row.get("warning_info")
                else None,
                "industry_block": str(row["industry_block"])
                if pd.notna(row.get("industry_block")) and row.get("industry_block")
                else None,
                "market_cap_display": market_cap_display_from_row(s_latest)
                if s_latest is not None
                else None,
            }
        )
    return rows, total


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
        {
            "code": r["code"],
            "name": str(r.get("name", "") or r["code"]),
            "market_cap_display": market_cap_display_from_row(r),
        }
        for _, r in chunk.iterrows()
    ]
    return items, total
