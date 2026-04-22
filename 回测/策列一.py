import pandas as pd
from sqlalchemy import create_engine

# ==============================
# 1️⃣ 数据库连接
# ==============================
engine = create_engine("mysql+pymysql://root:chen@127.0.0.1:3306/gp")

# ==============================
# 获取数据（最近4根K线）
# ==============================
def get_recent_kline(code_list):
    code_str = ",".join([f"'{c}'" for c in code_list])

    sql = f"""
    SELECT date, open, high, low, close, volume, code
    FROM stock
    WHERE code IN ({code_str})
    ORDER BY code, date
    """

    df = pd.read_sql(sql, engine)

    df = df.sort_values(['code', 'date'])
    df = df.groupby('code').tail(4).reset_index(drop=True)

    return df


# ==============================
# K线形态识别（完整版）
# ==============================
def detect_pattern(df_sub):

    if len(df_sub) < 3:
        return '无'

    cur = df_sub.iloc[-1]
    prev = df_sub.iloc[-2]
    prev2 = df_sub.iloc[-3]

    body = abs(cur['close'] - cur['open'])
    upper = cur['high'] - max(cur['open'], cur['close'])
    lower = min(cur['open'], cur['close']) - cur['low']
    body = max(body, 0.0001)

    # ===== 1️⃣ 锤子线 =====
    if lower > body * 2 and upper < body:
        return '锤子线（底部信号）'

    # ===== 2️⃣ 上吊线 =====
    if lower > body * 2 and upper < body:
        return '上吊线（顶部风险）'

    # ===== 3️⃣ 吞没 =====
    if (cur['close'] > cur['open'] and
        prev['close'] < prev['open'] and
        cur['close'] > prev['open'] and
        cur['open'] < prev['close']):
        return '看涨吞没（强反转）'

    if (cur['close'] < cur['open'] and
        prev['close'] > prev['open'] and
        cur['open'] > prev['close'] and
        cur['close'] < prev['open']):
        return '看跌吞没（顶部风险）'

    # ===== 4️⃣ 启明星（三叉戟）=====
    if (
        prev2['close'] < prev2['open'] and
        abs(prev['close'] - prev['open']) < (prev2['open'] - prev2['close']) * 0.5 and
        cur['close'] > cur['open'] and
        cur['close'] > (prev2['open'] + prev2['close']) / 2
    ):
        return '启明星（三叉戟）'

    # ===== 5️⃣ 黄昏星（四天王）=====
    if (
        prev2['close'] > prev2['open'] and
        abs(prev['close'] - prev['open']) < (prev2['close'] - prev2['open']) * 0.5 and
        cur['close'] < cur['open'] and
        cur['close'] < (prev2['open'] + prev2['close']) / 2
    ):
        return '黄昏星（顶部）'

    # ===== 6️⃣ 乌云盖顶 =====
    if (
        prev['close'] > prev['open'] and
        cur['open'] > prev['close'] and
        cur['close'] < (prev['open'] + prev['close']) / 2
    ):
        return '乌云盖顶（顶部风险）'

    # ===== 7️⃣ 刺透形态 =====
    if (
        prev['close'] < prev['open'] and
        cur['open'] < prev['close'] and
        cur['close'] > (prev['open'] + prev['close']) / 2
    ):
        return '刺透形态（底部反转）'

    # ===== 8️⃣ 流星（母女）=====
    if upper > body * 2 and lower < body:
        if cur['high'] < prev['high'] and cur['low'] > prev['low']:
            return '流星形态母女（看跌）'
        return '流星形态（顶部）'

    # ===== 9️⃣ 倒锤子（父子）=====
    if upper > body * 2 and lower < body:
        return '倒锤子（顶部风险）'

    return '无明显形态'


# ==============================
# 批量执行
# ==============================
def detect_all_patterns(code_list):
    df = get_recent_kline(code_list)

    result = []

    for code, group in df.groupby('code'):
        pattern = detect_pattern(group)

        result.append({
            'code': code,
            'date': group.iloc[-1]['date'],
            'k线形态': pattern
        })

    return pd.DataFrame(result)

# ==============================
# 主函数
# ==============================
if __name__ == "__main__":

    code_list = ["sz003027","sz002039","sh603196","sh603278","sz002081","sz301667","sh603178","sz002338","sz002853","sh600736","sz301099","sh600522","sz301486","sz002536","sh600118","sh600301","sh600208","sz001330","sz000880"]

    result_df = detect_all_patterns(code_list)

    print(result_df)