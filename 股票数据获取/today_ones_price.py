import pandas as pd
import numpy as np

def calc_main_force_score_detail(df):
    df = df.copy()

    # ========= 基础处理 =========
    df["成交时间"] = pd.to_datetime(df["成交时间"])
    df = df.sort_values("成交时间").reset_index(drop=True)

    total_amount = df["成交金额"].sum()
    if total_amount == 0 or len(df) < 10:
        return None

    # ========= 大单定义 =========
    BIG_RATIO = 0.005   # 单笔 >= 当日成交额 0.5%
    big_threshold = total_amount * BIG_RATIO
    df["is_big"] = df["成交金额"] >= big_threshold
    big_df = df[df["is_big"]]

    if big_df.empty:
        return None

    # =====================================================
    # 1️⃣ 主动买大单占比（40 分）
    # =====================================================
    buy_big_amount = big_df.loc[big_df["性质"] == "买盘", "成交金额"].sum()
    buy_big_ratio = buy_big_amount / total_amount
    buy_big_score = min(buy_big_ratio / 0.2, 1) * 40

    # =====================================================
    # 2️⃣ 大单推价能力（30 分）
    # =====================================================
    def calc_push_ratio(df, look_ahead=3):
        success, total = 0, 0
        for idx in df[df["is_big"]].index:
            base_price = df.loc[idx, "成交价格"]
            future = df.loc[idx + 1: idx + look_ahead, "成交价格"]
            if len(future) == 0:
                continue
            total += 1
            if future.mean() > base_price:
                success += 1
        return success / total if total > 0 else 0

    push_ratio = calc_push_ratio(df)
    push_score = push_ratio * 30

    # =====================================================
    # 3️⃣ 大单集中度（20 分）
    # =====================================================
    big_amount_ratio = big_df["成交金额"].sum() / total_amount
    concentration_score = min(big_amount_ratio / 0.4, 1) * 20

    # =====================================================
    # 4️⃣ 中性盘对倒识别（只惩罚异常行为）
    # =====================================================
    df["is_suspect_duidui"] = False

    neutral = df[df["性质"] == "中性盘"].copy()
    if not neutral.empty and len(neutral) >= 3:

        neutral["time_diff"] = neutral["成交时间"].diff().dt.total_seconds()
        neutral["price_diff"] = neutral["成交价格"].diff()
        neutral["amount_diff_ratio"] = (
            neutral["成交金额"].diff().abs() / neutral["成交金额"]
        )

        suspect_cond = (
            (neutral["time_diff"] == 0) &              # 同一秒
            (neutral["price_diff"] == 0) &             # 同价
            (neutral["amount_diff_ratio"] < 0.1)       # 金额相近
        )

        suspect_idx = neutral[suspect_cond].index
        df.loc[suspect_idx, "is_suspect_duidui"] = True

    suspect_duidui_ratio = df["is_suspect_duidui"].mean()

    # ≥10% 可疑中性盘 → 扣满 20 分
    duidui_penalty = min(suspect_duidui_ratio / 0.1, 1) * 20

    # =====================================================
    # 总分 & 评级
    # =====================================================
    total_score = (
        buy_big_score +
        push_score +
        concentration_score -
        duidui_penalty
    )

    total_score = max(0, min(100, round(total_score, 2)))

    if total_score >= 70:
        level = "强主力主导"
    elif total_score >= 55:
        level = "疑似主力（可博弈）"
    elif total_score >= 40:
        level = "资金一般"
    else:
        level = "散户盘 / 诱多"

    return {
        "total_score": total_score,
        "level": level,
        "is_tradeable": total_score >= 60,

        "detail": {
            "大单主买占比（buy_big_ratio）": round(buy_big_ratio, 4),
            "大单主买评分（满分40）（buy_big_score）": round(buy_big_score, 2),

            "大单推价成功率（push_ratio）": round(push_ratio, 4),
            "推价能力换算成分数（满分30）push_score": round(push_score, 2),

            "大单成交额占比（big_amount_ratio）": round(big_amount_ratio, 4),
            "大单成交额占比评分（concentration_score）": round(concentration_score, 2),

            "对倒比例（duidui_ratio）": round(suspect_duidui_ratio, 4),
            "对倒比例评分（duidui_penalty）": round(duidui_penalty, 2)
        }
    }
