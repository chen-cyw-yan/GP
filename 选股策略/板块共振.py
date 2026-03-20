import pandas as pd
import numpy as np

# =============================
# 1️⃣ 模拟板块数据（替换为你的SQL）
# =============================
df_block = pd.DataFrame({
    'code': ['B1','B2','B3','B4','B5','B6'],
    'name': ['AI','算力','芯片','新能源','光伏','消费'],
    'strength': [1.2, 0.8, 1.5, -0.6, 0.3, -0.2]   # 可正可负
})

# =============================
# 2️⃣ 全市场归一化（核心🔥）
# =============================

# Z-score（相对市场）
mean = df_block['strength'].mean()
std = df_block['strength'].std()

df_block['zscore'] = (df_block['strength'] - mean) / std

# Sigmoid映射到 0~1（解决负值问题）
df_block['norm_score'] = 1 / (1 + np.exp(-df_block['zscore']))

print("=== 板块归一化结果 ===")
print(df_block[['name','strength','zscore','norm_score']])

# =============================
# 3️⃣ 股票-板块映射（示例）
# =============================
stock_block_map = {
    'A股票': ['AI', '算力', '芯片'],
    'B股票': ['新能源', '光伏'],
    'C股票': ['消费'],
    'D股票': ['AI', '消费'],
}

# 建立映射
block_score_dict = dict(zip(df_block['name'], df_block['norm_score']))
print("映射关系",block_score_dict)


# =============================
# 4️⃣ 计算市场环境因子（关键🔥）
# =============================
market_mean = df_block['norm_score'].mean()

# 市场因子：>0.5 偏强，<0.5 偏弱
market_factor = (market_mean - 0.5) * 2   # 映射到 [-1,1]

print("\n市场环境因子:", round(market_factor, 4))


# =============================
# 5️⃣ 共振模型（完整版本🔥）
# =============================
def calc_resonance(scores, market_factor):

    scores = np.array(scores)

    # 过滤无效
    if len(scores) == 0:
        return 0

    # 1️⃣ 强度
    strength = scores.mean()

    # 2️⃣ 一致性（防分化）
    consistency = 1 / (1 + scores.std())

    # 3️⃣ 强者放大
    power = np.mean(scores ** 2)

    # 4️⃣ 数量因子（递减）
    count_bonus = np.log(len(scores) + 1)

    # 5️⃣ 综合
    base_score = 0.4 * strength + 0.3 * consistency + 0.3 * power

    # 6️⃣ 市场环境修正
    final_score = base_score * (1 + market_factor)

    return final_score


# =============================
# 6️⃣ 计算股票共振
# =============================
results = []

for stock, blocks in stock_block_map.items():

    scores = []

    for b in blocks:
        if b in block_score_dict:
            scores.append(block_score_dict[b])

    resonance_score = calc_resonance(scores, market_factor)

    results.append({
        'stock': stock,
        'blocks': ",".join(blocks),
        'block_scores': scores,
        'resonance_score': resonance_score
    })

df_result = pd.DataFrame(results)

# 排序
df_result = df_result.sort_values('resonance_score', ascending=False)

print("\n=== 股票共振强度排名 ===")
print(df_result)

