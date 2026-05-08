"""
个股关联概念板块：共振指数与排行榜。
共振计算与 prod_online/script/block_analysis.py 一致（norm_score + market_factor + calc_resonance_score）。
"""
from __future__ import annotations

import logging
import re
from typing import Any

import numpy as np
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

logger = logging.getLogger(__name__)

CONCEPT_BLOCK_TYPE = "概念板块"

SHORT_TREND_LABELS: dict[int, str] = {
    1: "∧顶反转",
    2: "∨底反转",
    3: "W双底",
    4: "M双顶",
    5: "横盘",
    6: "横盘后升",
    7: "横盘后降",
    8: "上升",
    9: "下降",
    10: "升后短回",
    11: "降后短回",
    12: "升后横盘",
    13: "降后横盘",
    14: "不确定",
}


def _short_trend_label(v: Any) -> str | None:
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return None
    try:
        i = int(v)
    except (TypeError, ValueError):
        return str(v)
    return SHORT_TREND_LABELS.get(i, f"形态{i}")


def process_market_factors(df_detail: pd.DataFrame) -> tuple[pd.DataFrame, float]:
    """与 block_analysis.process_market_factors 一致。"""
    strength = pd.to_numeric(df_detail["strength"], errors="coerce")
    mean_val = strength.mean()
    std_val = strength.std()
    if std_val == 0 or std_val is None or (isinstance(std_val, float) and np.isnan(std_val)):
        zscore = pd.Series(0.0, index=df_detail.index)
    else:
        zscore = (strength - mean_val) / std_val
    zscore = zscore.replace([np.inf, -np.inf], np.nan).fillna(0.0)
    df_detail = df_detail.copy()
    df_detail["norm_score"] = 1.0 / (1.0 + np.exp(-zscore))
    market_mean = float(df_detail["norm_score"].mean())
    market_factor = (market_mean - 0.5) * 2.0
    return df_detail, market_factor


def calc_resonance_score(scores: np.ndarray, market_factor: float) -> float:
    """与 block_analysis.calc_resonance_score 一致。"""
    if len(scores) == 0:
        return 0.0
    scores = np.array(scores, dtype=float)
    strength = float(np.mean(scores))
    consistency = 1.0 / (1.0 + np.std(scores))
    power = float(np.mean(np.square(scores)))
    base_score = 0.4 * strength + 0.3 * consistency + 0.3 * power
    return float(base_score * (1.0 + market_factor))


def normalize_plain_stock_code(stock_code_raw: str) -> str | None:
    """侧栏 sh600000 / sz000001 → 6 位数字，与 tdx_block_stocks.stock_code 对齐。"""
    s = (stock_code_raw or "").strip().lower()
    if not s:
        return None
    s = re.sub(r"^(sh|sz|bj)", "", s)
    if len(s) == 6 and s.isdigit():
        return s
    return None


def list_stock_linked_concept_blocks(
    engine_uri: str,
    stock_code_raw: str,
    page: int = 1,
    page_size: int = 10,
) -> dict[str, Any]:
    """
    仅该股在 tdx_block_stocks 中映射的概念板块；按强弱度降序分页。
    共振指数：该股各关联板块 norm_score 代入 calc_resonance_score（全市场算出的 market_factor）。
    """
    p = max(1, int(page))
    ps = max(1, min(int(page_size), 100))
    plain = normalize_plain_stock_code(stock_code_raw)

    base_empty: dict[str, Any] = {
        "trade_date": None,
        "resonance_index": None,
        "market_factor": None,
        "items": [],
        "total": 0,
        "page": p,
        "page_size": ps,
        "stock_code_plain": plain,
    }

    if plain is None:
        return {**base_empty, "hint": "请先选择左侧个股（有效代码如 sh600000）"}

    try:
        engine = create_engine(engine_uri)
    except SQLAlchemyError as e:
        logger.exception("创建数据库引擎失败（概念板块）")
        raise RuntimeError(f"数据库不可用: {e}") from e

    try:
        df_date = pd.read_sql(
            "SELECT MAX(create_date) AS last_date FROM tdx_block_daily",
            con=engine,
        )
    except SQLAlchemyError as e:
        logger.exception("查询 tdx_block_daily 最新日期失败")
        raise RuntimeError(f"读取板块日期失败: {e}") from e

    if df_date.empty or pd.isna(df_date["last_date"].iloc[0]):
        return base_empty

    last_date = df_date["last_date"].iloc[0]
    if hasattr(last_date, "strftime"):
        trade_date_str = last_date.strftime("%Y-%m-%d")
        sql_date = trade_date_str
    else:
        trade_date_str = str(last_date)[:10]
        sql_date = trade_date_str

    sql_daily = f"""
    SELECT code, name, change_pct, up_down_count, turnover, main_net_ratio,
           total_volume, total_amount, amplitude, strength, short_trend
    FROM tdx_block_daily
    WHERE create_date = '{sql_date}'
    """
    sql_stock_blocks = f"""
    SELECT DISTINCT TRIM(block_code) AS block_code
    FROM tdx_block_stocks
    WHERE block_type = '{CONCEPT_BLOCK_TYPE}'
      AND TRIM(stock_code) = '{plain}'
    """

    try:
        df_all = pd.read_sql(sql_daily, con=engine)
        df_links = pd.read_sql(sql_stock_blocks, con=engine)
    except SQLAlchemyError as e:
        logger.exception("查询板块行情或个股板块映射失败")
        raise RuntimeError(f"查询概念板块数据失败: {e}") from e

    if df_all.empty:
        return {
            **base_empty,
            "trade_date": trade_date_str,
            "stock_code_plain": plain,
        }

    df_all["code"] = df_all["code"].astype(str).str.strip()
    df_all, market_factor = process_market_factors(df_all)

    if df_links.empty:
        return {
            "trade_date": trade_date_str,
            "resonance_index": 0.0,
            "market_factor": round(float(market_factor), 4),
            "items": [],
            "total": 0,
            "page": p,
            "page_size": ps,
            "stock_code_plain": plain,
            "hint": "该股在 tdx_block_stocks 中无概念板块映射",
        }

    linked_codes = set(df_links["block_code"].astype(str).str.strip())
    df_linked = df_all[df_all["code"].isin(linked_codes)].copy()

    if df_linked.empty:
        return {
            "trade_date": trade_date_str,
            "resonance_index": 0.0,
            "market_factor": round(float(market_factor), 4),
            "items": [],
            "total": 0,
            "page": p,
            "page_size": ps,
            "stock_code_plain": plain,
            "hint": "关联板块代码在当日 tdx_block_daily 中无行情",
        }

    scores = df_linked["norm_score"].dropna().values
    resonance = calc_resonance_score(scores, float(market_factor))

    strength_num = pd.to_numeric(df_linked["strength"], errors="coerce")
    df_linked = df_linked.assign(_strength_sort=strength_num)
    df_linked = df_linked.sort_values(
        ["_strength_sort", "change_pct"],
        ascending=[False, False],
        na_position="last",
    )

    total = int(len(df_linked))
    offset = (p - 1) * ps
    page_df = df_linked.iloc[offset : offset + ps]

    items: list[dict[str, Any]] = []
    for _, row in page_df.iterrows():
        st = row.get("short_trend")
        items.append(
            {
                "code": str(row["code"]),
                "name": str(row.get("name") or row["code"]),
                "strength": float(row["strength"]) if pd.notna(row.get("strength")) else None,
                "change_pct": float(row["change_pct"]) if pd.notna(row.get("change_pct")) else None,
                "up_down_count": str(row["up_down_count"]) if pd.notna(row.get("up_down_count")) else None,
                "turnover": float(row["turnover"]) if pd.notna(row.get("turnover")) else None,
                "main_net_ratio": float(row["main_net_ratio"]) if pd.notna(row.get("main_net_ratio")) else None,
                "total_volume": int(row["total_volume"]) if pd.notna(row.get("total_volume")) else None,
                "total_amount": float(row["total_amount"]) if pd.notna(row.get("total_amount")) else None,
                "amplitude": float(row["amplitude"]) if pd.notna(row.get("amplitude")) else None,
                "short_trend": int(st) if pd.notna(st) and st is not None else None,
                "short_trend_label": _short_trend_label(st),
                "norm_score": round(float(row["norm_score"]), 4) if pd.notna(row.get("norm_score")) else None,
            }
        )

    return {
        "trade_date": trade_date_str,
        "resonance_index": round(resonance, 4),
        "market_factor": round(float(market_factor), 4),
        "items": items,
        "total": total,
        "page": p,
        "page_size": ps,
        "stock_code_plain": plain,
    }
