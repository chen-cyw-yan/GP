import pymysql
import pandas as pd
from mootdx.quotes import Quotes
import tqdm
# ================= 配置区域 =================
# 数据库连接配置
DB_CONFIG = {
    'host': 'localhost',          # 你的MySQL服务器地址
    'user': 'root',               # 数据库用户名
    'password': 'chen',  # 数据库密码
    'database': 'gp',   # 数据库名称
    'charset': 'utf8mb4'
}
TARGET_BLOCK_NAME = '半导体'    # 你想计算走势的板块名称
# ===========================================

# 1. 从 MySQL 数据库中获取目标板块的成分股列表
print(f"正在从数据库读取【{TARGET_BLOCK_NAME}】的成分股...")
conn = pymysql.connect(**DB_CONFIG)
sql = f"SELECT stock_code FROM tdx_block_stocks WHERE block_name = '{TARGET_BLOCK_NAME}'"
# 注意：确保你的 stock_code 在数据库中是6位纯数字格式（如 000032）
stock_list_df = pd.read_sql(sql, conn)
conn.close()

if stock_list_df.empty:
    print(f"未在数据库中找到【{TARGET_BLOCK_NAME}】的成分股，请检查板块名称或数据库表。")
    exit()

# 提取股票代码列表
target_stocks = stock_list_df['stock_code'].tolist()
print(f"成功获取成分股，共 {len(target_stocks)} 只。")

# 2. 批量获取成分股的分时数据
client = Quotes.factory(market='std', bestip=True)
all_minute_data = []

print("正在批量拉取成分股分时数据（这可能需要几十秒，请耐心等待）...")
for stock in tqdm.tqdm(target_stocks):
    try:
        # 使用 mootdx 获取单只股票的当日分时数据
        minute_df = client.minute(symbol=stock)
        if not minute_df.empty:
            minute_df['code'] = stock  # 加上股票代码列，方便后续追踪
            minute_df['time'] = minute_df.index
            all_minute_data.append(minute_df)
    except Exception as e:
        # 遇到个别股票获取失败直接跳过，不影响整体
        pass

if not all_minute_data:
    print("未获取到任何有效分时数据，程序退出。")
    exit()

# 3. 数据合并与成交量加权计算板块分时走势
print("正在合并数据并计算板块成交量加权均价（VWAP）...")
# 将所有股票的分时数据拼成一张大表
combined_df = pd.concat(all_minute_data, ignore_index=True)

# 定义成交量加权平均价（VWAP）计算函数
# 公式：VWAP = Σ(价格 * 成交量) / Σ(成交量)
# 成交量越大的时刻，其价格对板块整体走势的影响权重越大
def vwap(group):
    total_volume = group['volume'].sum()
    if total_volume == 0:
        return 0
    return (group['price'] * group['volume']).sum() / total_volume
print(combined_df)
# 按时间点（time）分组，计算整个板块在每个时间点的加权均价
sector_trend = combined_df.groupby('time').apply(vwap).reset_index()
sector_trend.columns = ['time', 'vwap']

# 顺便计算一下板块整体的成交量总和（用于观察板块当天的热度变化）
sector_volume = combined_df.groupby('time')['volume'].sum().reset_index()

# 合并最终的板块分时数据
final_sector_minute = pd.merge(sector_trend, sector_volume, on='time')
final_sector_minute = final_sector_minute.sort_values('time').reset_index(drop=True)

print("\n板块分时走势计算完成！最近5个时间点的板块数据如下：")
print(final_sector_minute.tail())

# 如果需要保存为CSV方便后续用Excel或绘图工具查看
# final_sector_minute.to_csv(f"{TARGET_BLOCK_NAME}_板块分时走势.csv", index=False, encoding='utf-8-sig')