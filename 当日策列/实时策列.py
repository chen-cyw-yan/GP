import logging
import time
from collections import deque
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

import numpy as np
import pandas as pd
import akshare as ak
import warnings

warnings.filterwarnings("ignore", category=UserWarning)
# 显示最多 100 行、50 列，列宽不限
pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 50)
pd.set_option('display.max_colwidth', None)
pd.set_option('display.width', 200)  # 终端宽度
# =========================
# 日志配置
# =========================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


# =====================================================
# 实时进场评分模型
# =====================================================
def realtime_entry_score(df):
    score_detail = {}
    total_score = 0

    price = df["成交价格"]
    amount = df["成交金额"]

    price_range = (price.max() - price.min()) / price.min() * 100

    buy_amt = df[df["性质"] == "买盘"]["成交金额"].sum()
    sell_amt = df[df["性质"] == "卖盘"]["成交金额"].sum()
    neutral_amt = df[df["性质"] == "中性盘"]["成交金额"].sum()
    total_amt = amount.sum() + 1e-6

    # =========================
    # ① 方向一致性（30）
    # =========================
    direction_ratio = buy_amt / (buy_amt + sell_amt + 1e-6)
    direction_score = min(direction_ratio * 30, 30)

    score_detail["方向一致性"] = {
        "direction_ratio": round(direction_ratio, 3),
        "score": round(direction_score, 2)
    }
    total_score += direction_score

    # =========================
    # ② 推价有效性（25）
    # =========================
    price_diff = price.diff()
    volume = df["成交量"]

    # ===== 1️⃣ 成交量加权方向性 =====
    up_vol = volume[price_diff > 0].sum()
    down_vol = volume[price_diff < 0].sum()

    push_ratio = up_vol / (up_vol + down_vol + 1e-6)
    direction_score = min(push_ratio * 20, 20)

    # ===== 2️⃣ 连续推价奖励 =====
    sign = np.sign(price_diff.dropna())
    up_flag = (sign == 1).astype(int)

    # 计算最长连续上涨段
    streak_len = up_flag.groupby(
        (up_flag != up_flag.shift()).cumsum()
    ).sum()

    max_up_streak = streak_len.max() if not streak_len.empty else 0
    streak_bonus = min(max_up_streak / 10, 1) * 5

    # ===== 3️⃣ 合成推价有效性 =====
    push_score = round(direction_score + streak_bonus, 2)

    score_detail["推价有效性"] = {
        "push_ratio": round(push_ratio, 3),
        "max_up_streak": int(max_up_streak),
        "score": push_score
    }

    total_score += push_score

    # =========================
    # ③ 成交压缩（20）
    # =========================
    big_trade_ratio = (
        (amount > amount.mean() * 3)
        .rolling(30)
        .mean()
        .iloc[-1]
    )

    compression_score = max((0.2 - big_trade_ratio) / 0.2 * 20, 0)

    score_detail["成交压缩"] = {
        "big_amount_ratio": round(big_trade_ratio, 3),
        "score": round(compression_score, 2)
    }
    total_score += compression_score

    # =========================
    # ④ 中性盘地基（15）
    # =========================
    neutral_ratio = neutral_amt / total_amt

    if price_range < 1.5:
        neutral_score = min(neutral_ratio * 40, 15)
    else:
        neutral_score = 0

    score_detail["中性盘地基"] = {
        "neutral_ratio": round(neutral_ratio, 3),
        "score": round(neutral_score, 2)
    }
    total_score += neutral_score

    # =========================
    # ⑤ 风险扣分（-30 ~ 0）
    # =========================
    risk_penalty = 0

    if price_range > 3 and big_trade_ratio > 0.3:
        risk_penalty -= 15

    if neutral_ratio > 0.4 and push_ratio < 0.5:
        risk_penalty -= 15

    score_detail["风险扣分"] = {
        "penalty": risk_penalty
    }
    total_score += risk_penalty

    # =========================
    # 状态判定
    # =========================
    if total_score >= 75:
        state = "🟢 主升确认"
    elif total_score >= 58:
        state = "🟡 可进场"
    elif total_score >= 45:
        state = "👀 观察"
    else:
        state = "❌ 弱"

    return {
        "total_score": round(total_score, 2),
        "state": state,
        "detail": score_detail
    }


# =====================================================
# 实时 Scanner（多线程）
# =====================================================
class AkshareRealtimeScanner:

    def __init__(self, codes, code_name_map=None, window_ticks=300, min_ticks=30):
        self.codes = codes
        self.code_name_map = code_name_map or {}
        self.window_ticks = window_ticks
        self.min_ticks = min_ticks

        self.buffers = {code: deque() for code in codes}
        self.last_time = {code: None for code in codes}

    # =========================
    # 拉取分笔
    # =========================
    def update_ticks(self, code):
        try:
            df = ak.stock_zh_a_tick_tx_js(symbol=code)
            if df is None or df.empty:
                return

            df["成交时间"] = pd.to_datetime(df["成交时间"])

            last_t = self.last_time.get(code)
            if last_t is not None:
                df = df[df["成交时间"] > last_t]

            if df.empty:
                return

            self.last_time[code] = df["成交时间"].max()

            for _, row in df.iterrows():
                self.buffers[code].append(row)

            while len(self.buffers[code]) > self.window_ticks:
                self.buffers[code].popleft()

        except Exception as e:
            logger.error(f"{code} 更新失败: {e}")

    # =========================
    # 评分
    # =========================
    def evaluate(self, code):
        buf = self.buffers[code]
        if len(buf) < self.min_ticks:
            return None

        df = pd.DataFrame(buf)

        res = realtime_entry_score(df)
        res["latest_time"] = df["成交时间"].iloc[-1]
        res["latest_price"] = df["成交价格"].iloc[-1]

        return res

    # =========================
    # 扫描（并行更新）
    # =========================
    def scan(self):
        results = []

        with ThreadPoolExecutor(max_workers=min(8, len(self.codes))) as executor:
            futures = [executor.submit(self.update_ticks, code) for code in self.codes]
            for _ in as_completed(futures):
                pass

        for code in self.codes:
            res = self.evaluate(code)
            if res:
                results.append((code, res))

        return results


# =====================================================
# 打印结果
# =====================================================
def print_realtime_result(code, name, res):
    print("=" * 70)
    print(f"股票: {code} | 名称: {name}")
    print(f"时间: {res['latest_time']}")
    print(f"最新价: {res['latest_price']:.2f}")
    print(f"状态: {res['state']}   总分: {res['total_score']}")
    print("-" * 70)

    for k, v in res["detail"].items():
        score = v.get("score", "")
        extra = {kk: vv for kk, vv in v.items() if kk not in ("score", "penalty")}
        penalty = v.get("penalty", "")
        if score != "":
            print(f"{k:<10} | 分数: {score:<6} | 细项: {extra}")
        elif penalty != "":
            print(f"{k:<10} | 扣分: {penalty}")

    print("=" * 70 + "\n")


# =====================================================
# 主程序
# =====================================================
if __name__ == "__main__":

    watch_list = [
        "sz002358",
    ]

    code_name_map = {
        "sz002358": "森源电气",
    }

    scanner = AkshareRealtimeScanner(
        watch_list,
        code_name_map=code_name_map
    )

    while True:
        logger.info("⏱ 开始扫描")

        results = scanner.scan()
        rows = []

        for code, res in results:
            name = code_name_map.get(code, "未知")

            rows.append({
                "股票": code,
                "名称": name,
                "时间": res["latest_time"],
                "最新价": res["latest_price"],
                "方向一致性": res["detail"]["方向一致性"]["score"],
                "推价有效性": res["detail"]["推价有效性"]["score"],
                "成交压缩": res["detail"]["成交压缩"]["score"],
                "中性盘地基": res["detail"]["中性盘地基"]["score"],
                "风险扣分": res["detail"]["风险扣分"]["penalty"],
                "状态": res["state"],
                "总分": res["total_score"]
            })

            print_realtime_result(code, name, res)

        df = pd.DataFrame(rows)
        print(df.sort_values('总分',ascending=False).to_string(index=False,justify='center'))

        logger.info("分析完成\n")
        time.sleep(30)
