import logging

import numpy as np
import pandas as pd
import akshare as ak

import warnings
warnings.filterwarnings("ignore", category=UserWarning)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)
def get_tail_df(df, tail_start="14:30"):
    df = df.copy()
    df["成交时间"] = pd.to_datetime(df["成交时间"])
    return df[df["成交时间"].dt.strftime("%H:%M") >= tail_start]
def tail_grab_score(df):
    score = 0
    detail = {}

    tail_df = get_tail_df(df,tail_start='14:50')

    if len(tail_df) < 20:
        return 0, {"reason": "尾盘成交过少"}

    # ===== 1️⃣ 尾盘成交集中度（8分）=====
    tail_amt = tail_df["成交金额"].sum()
    total_amt = df["成交金额"].sum()
    amt_ratio = tail_amt / total_amt

    amt_score = min(amt_ratio / 0.3 * 8, 8)
    score += amt_score

    detail["尾盘成交占比"] = round(amt_ratio, 3)

    # ===== 2️⃣ 尾盘方向一致性（6分）=====
    buy_amt = tail_df[tail_df["性质"] == "买盘"]["成交金额"].sum()
    sell_amt = tail_df[tail_df["性质"] == "卖盘"]["成交金额"].sum()
    direction_ratio = buy_amt / (buy_amt + sell_amt + 1e-6)

    dir_score = min(direction_ratio / 0.6 * 6, 6)
    score += dir_score

    detail["尾盘买盘占比"] = round(direction_ratio, 3)

    # ===== 3️⃣ 尾盘抬价效果（6分）=====
    price = tail_df["成交价格"]
    price_lift = (price.iloc[-1] - price.min()) / price.min() * 100

    lift_score = min(price_lift / 1.5 * 6, 6)
    score += lift_score

    detail["尾盘抬价幅度"] = round(price_lift, 2)

    return round(score, 2), detail

def realtime_entry_score(df, big_amt_quantile=0.9):
    score_detail = {}
    total_score = 0

    # =========================
    # 基础统计
    # =========================
    price = df["成交价格"]
    amount = df["成交金额"]

    price_range = (price.max() - price.min()) / price.min() * 100

    buy_amt = df[df["性质"] == "买盘"]["成交金额"].sum()
    sell_amt = df[df["性质"] == "卖盘"]["成交金额"].sum()
    neutral_amt = df[df["性质"] == "中性盘"]["成交金额"].sum()
    total_amt = amount.sum()

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
    push_up = (price_diff > 0).sum()
    push_down = (price_diff < 0).sum()

    push_ratio = push_up / (push_up + push_down + 1e-6)
    push_score = min(push_ratio * 25, 25)

    score_detail["推价有效性"] = {
        "push_ratio": round(push_ratio, 3),
        "score": round(push_score, 2)
    }
    total_score += push_score

    # =========================
    # ③ 成交压缩度（20）
    # =========================
    # big_threshold = amount.quantile(big_amt_quantile)
    big_amt_ratio = (amount > amount.mean() * 3).mean()
    compression_score = max((0.2 - big_amt_ratio) / 0.2 * 20, 0)

    score_detail["成交压缩"] = {
        "big_amount_ratio": round(big_amt_ratio, 3),
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

    # 高位放量
    if price_range > 3 and big_amt_ratio > 0.3:
        risk_penalty -= 15

    # 对倒但不涨
    if neutral_ratio > 0.4 and push_ratio < 0.5:
        risk_penalty -= 15

    score_detail["风险扣分"] = {
        "penalty": risk_penalty
    }
    total_score += risk_penalty

    # =========================
    # ⑥ 尾盘抢筹（20）
    # =========================
    tail_score, tail_detail = tail_grab_score(df)

    score_detail["尾盘抢筹"] = {
        "score": tail_score,
        **tail_detail
    }

    total_score += tail_score


    # =========================
    # 状态判定
    # =========================
    if total_score >= 80:
        state = "已拉升"
    elif total_score >= 60:
        state = "可进"
    else:
        state = "观察"

    if tail_score>=12:
        tail_state='尾盘抢筹'
    else:
        tail_state='无动作'
    return {
        "total_score": round(total_score, 2),
        "state": state,
        'tail_score':tail_score,
        'tail_state':tail_state,
        "detail": score_detail
    }
def print_realtime_result(code, res):
    print("=" * 70)
    print(f"股票: {code}")
    print(f"时间: {res['latest_time']}")
    print(f"最新价: {res['latest_price']:.2f}")
    print(f"状态: {res['state']}  总分: {res['total_score']} ,尾盘抢筹: {res['tail_state']} ，抢筹分: {res['tail_score']}")
    print("-" * 70)
    print(res["detail"])
    for k, v in res["detail"].items():
        if isinstance(v, dict):
            score = v.get("score", "")
            extra = {kk: vv for kk, vv in v.items() if kk != "score"}
            print(f"{k:<10} | 分数: {score:<6} | 细项: {extra}")

    print("=" * 70 + "\n")



if __name__ == '__main__':
    logger.info('⏱ 开始扫描')
    watch_list = [
        "sz000815",
        "sz002358",
        "sz002491",
        "sz002606",
        "sz002780",
        "sz300265"
    ]
    res_df = []
    for code in watch_list:
        logger.info(f"获取{code}，交易数据..")
        df = ak.stock_zh_a_tick_tx_js(symbol=code)
        res = realtime_entry_score(df)
        res["latest_time"] = df["成交时间"].iloc[-1]
        res["latest_price"] = df["成交价格"].iloc[-1]
        print_realtime_result(code, res)
        dt = {
            '股票':code,
                "时间": res['latest_time'],
                "最新价": res['latest_price'],
                '方向一致性':res['detail'].get('方向一致性').get('score'),
                '推价有效性':res['detail'].get('推价有效性').get('score'),
                '成交压缩':res['detail'].get('推价有效性').get('score'),
                '中性盘地基': res['detail'].get('中性盘地基').get('score'),
                '风险扣分': res['detail'].get('风险扣分').get('score'),
                "尾盘抢筹": res['tail_state'],
                '抢筹分': res['tail_score'],
                "状态": res['state'],
                "总分": res['total_score']
        }
        res_df.append(dt)
        print_realtime_result(code, res)
    dfs = pd.DataFrame(res_df)
    print(dfs)
    dfs.to_excel('jrgx.xlsx',index=False)
    logger.info('分析完成')
