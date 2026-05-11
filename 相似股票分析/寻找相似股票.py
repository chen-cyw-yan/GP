import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from sklearn.preprocessing import StandardScaler
from fastdtw import fastdtw
from scipy.spatial.distance import euclidean
from tqdm import tqdm


# =========================
# 数据库连接
# =========================
engine = create_engine(
    "mysql+pymysql://root:chen@127.0.0.1:3306/gp?charset=utf8mb4"
)


# =========================
# 参数
# =========================
TARGET_CODE = 'sz000815'
WINDOW = 30
TOP_N = 10


# =========================
# 获取目标股票
# =========================
target_sql = f"""
SELECT *
FROM stock
WHERE code = '{TARGET_CODE}'
ORDER BY date
"""

target_df = pd.read_sql(target_sql, engine)

if len(target_df) < WINDOW:
    raise Exception("目标股票数据不足")


# =========================
# 特征工程
# =========================
def build_feature(df):

    df = df.copy()

    # ======================
    # 替换inf
    # ======================
    df = df.replace([np.inf, -np.inf], np.nan)

    # ======================
    # 去除异常数据
    # ======================
    df = df[
        (df['close'] > 0) &
        (df['open'] > 0) &
        (df['high'] > 0) &
        (df['low'] > 0)
    ]

    if len(df) < WINDOW + 10:
        return None

    # ======================
    # 收益率
    # ======================
    df['ret'] = df['close'].pct_change()

    # ======================
    # 量比
    # ======================
    vol_mean = df['volume'].rolling(5).mean()

    vol_mean = vol_mean.replace(0, np.nan)

    df['vol_ratio'] = df['volume'] / vol_mean

    # ======================
    # 振幅
    # ======================
    df['amplitude'] = (
        (df['high'] - df['low']) /
        df['close']
    )

    # ======================
    # K线实体
    # ======================
    df['body'] = (
        (df['close'] - df['open']) /
        df['open']
    )

    # ======================
    # 上影线
    # ======================
    df['upper_shadow'] = (
        df['high'] -
        df[['open', 'close']].max(axis=1)
    ) / df['close']

    # ======================
    # 下影线
    # ======================
    df['lower_shadow'] = (
        df[['open', 'close']].min(axis=1) -
        df['low']
    ) / df['close']

    # ======================
    # 波动率
    # ======================
    df['volatility'] = (
        df['ret'].rolling(5).std()
    )

    # ======================
    # 删除异常
    # ======================
    df = df.replace([np.inf, -np.inf], np.nan)

    df = df.dropna()

    if len(df) < WINDOW:
        return None

    # ======================
    # 取最后窗口
    # ======================
    df = df.tail(WINDOW)

    # ======================
    # 多维特征
    # ======================
    features = df[
        [
            'ret',
            'vol_ratio',
            'amplitude',
            'body',
            'upper_shadow',
            'lower_shadow',
            'volatility'
        ]
    ].values

    # ======================
    # 检查非法值
    # ======================
    if np.isnan(features).any():
        return None

    if np.isinf(features).any():
        return None

    # ======================
    # 标准化
    # ======================
    scaler = StandardScaler()

    try:
        features = scaler.fit_transform(features)
    except:
        return None

    return features



target_feature = build_feature(target_df)

if target_feature is None:
    raise Exception("目标股票特征不足")


# =========================
# 获取全部股票代码
# =========================
code_sql = """
SELECT DISTINCT code, name
FROM stock
"""

code_df = pd.read_sql(code_sql, engine)


# =========================
# 开始计算DTW
# =========================
results = []

for _, row in tqdm(code_df.iterrows(), total=len(code_df)):

    code = row['code']
    name = row['name']

    # 跳过自己
    if code == TARGET_CODE:
        continue

    try:

        sql = f"""
        SELECT *
        FROM stock
        WHERE code = '{code}'
        ORDER BY date
        """

        df = pd.read_sql(sql, engine)

        if len(df) < WINDOW:
            continue

        feature = build_feature(df)

        if feature is None:
            continue

        # DTW距离
        distance, _ = fastdtw(
            target_feature,
            feature,
            dist=euclidean
        )

        results.append({
            'code': code,
            'name': name,
            'distance': distance
        })

    except Exception as e:
        print(code, e)


# =========================
# 输出结果
# =========================
result_df = pd.DataFrame(results)

result_df = result_df.sort_values(
    by='distance',
    ascending=True
)

print("\n最相似股票：")
print(result_df.head(TOP_N))