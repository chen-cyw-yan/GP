import sys
import os
import json
import logging
from datetime import datetime
from dateutil.relativedelta import relativedelta
import warnings
import requests
# 全局忽略所有警告
warnings.filterwarnings('ignore')
import pandas as pd
import numpy as np
import tqdm
import akshare as ak
import pymysql
from sqlalchemy import create_engine

# --- 配置部分 ---
# 设置路径
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# 数据库配置 (建议移至配置文件或环境变量)
DB_CONFIG = {
    'host': '127.0.0.1',
    'user': 'root',
    'password': 'chen',
    'database': 'gp'
}
DB_URL = f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:3306/{DB_CONFIG['database']}"
def stock_zh_a_tick_tx_js(symbol: str, page_size: int = 1000) :
    """
    腾讯财经 - 历史分笔数据 (仅获取第一页，通常包含集合竞价)
    """
    big_df = pd.DataFrame()
    page = 0
    TX_TIMEOUT = 30  # 单次请求超时秒数
    try:
        while page < page_size:
            url = "http://stock.gtimg.cn/data/index.php"
            params = {
                "appn": "detail",
                "action": "data",
                "c": symbol,
                "p": page,
            }


            
            r = requests.get(url, params=params, timeout=TX_TIMEOUT)
            if r.status_code != 200:
                break
                
            text_data = r.text
            # 解析腾讯特有的格式
            if "[" not in text_data:
                break
                
            start_idx = text_data.find("[")
            data_list = eval(text_data[start_idx:])
            
            if len(data_list) < 2:
                break
                
            temp_df = (
                pd.DataFrame(data_list[1].split("|"))
                .iloc[:, 0]
                .str.split("/", expand=True)
            )
            if temp_df.empty:
                break

            val = temp_df.iloc[0, 1] 
            current_time = pd.to_datetime(val, format='%H:%M:%S', errors='coerce')

            # 2. 检查是否为有效时间（排除 None 或 NaN 的情况）
            if pd.isna(current_time):
                # 解析失败或为空，根据需求选择跳过或继续
                # print("时间解析为空，跳过...")
                pass 
            else:
                # 3. 提取时间部分进行比较
                # 也可以直接比较 pd.Timestamp，这里为了配合你的逻辑提取 .time()
                first_time = current_time.time()
                cutoff_time = pd.to_datetime("09:45:00", format="%H:%M:%S").time()
                
                # print('first_time', first_time, cutoff_time)
                
                if first_time > cutoff_time:
                    # print("超过 9:45，执行 break")
                    break 
                
            big_df = pd.concat([big_df, temp_df], ignore_index=True)
            page += 1
            
    except Exception as e:
        logger.debug(f"抓取 {symbol} 网络异常: {e}")
        return None

    if big_df.empty:
        return None

    # 整理列名
    big_df = big_df.iloc[:, 1:].copy()
    if len(big_df.columns) >= 6:
        big_df.columns = ["成交时间", "成交价格", "价格变动", "成交量", "成交金额", "性质"]
        
        # 映射性质
        property_map = {"S": "卖盘", "B": "买盘", "M": "中性盘"}
        big_df["性质"] = big_df["性质"].map(property_map).fillna("未知")
        
        # 类型转换
        try:
            big_df["成交价格"] = big_df["成交价格"].astype(float)
            big_df["成交量"] = pd.to_numeric(big_df["成交量"], errors='coerce').fillna(0).astype(int)
            big_df["成交金额"] = pd.to_numeric(big_df["成交金额"], errors='coerce').fillna(0).astype(int)
            big_df["成交时间"] = big_df["成交时间"].astype(str)
        except Exception as e:
            logger.warning(f"{symbol} 数据类型转换失败: {e}")
            
        return big_df
    else:
        return None
def get_db_connection():
    """获取 pymysql 连接和 SQLAlchemy engine"""
    conn = pymysql.connect(
        host=DB_CONFIG['host'],
        user=DB_CONFIG['user'],
        password=DB_CONFIG['password'],
        database=DB_CONFIG['database'],
        charset='utf8mb4'
    )
    engine = create_engine(DB_URL)
    return conn, engine
def analyze_and_get_thresholds(stock_code):
    # logger.info(f"📊 正在分析股票: {stock_code}...")
    
    # 1. 获取数据
    try:
        # 获取分笔数据
        # df = ak.stock_zh_a_tick_tx_js(symbol=stock_code)
        df = ak.stock_zh_a_tick_tx_js(symbol=stock_code)
    except Exception as e:
        logger(f"❌ 数据获取失败: {e}")
        return

    # 2. 数据清洗
    # 转换时间，处理异常
    df['成交时间'] = pd.to_datetime(df['成交时间'])
    df = df.dropna(subset=['成交时间'])
    
    # 过滤掉 09:25:00 的集合竞价数据
    # 原因：集合竞价的单子通常巨大，会严重拉高阈值，导致连续竞价期间的判断失真
    # df_continuous = df[(df['成交时间'].dt.hour <= 9)&(df['成交时间'].dt.minute <= 45)&(df['成交时间'].dt.minute > 30)].copy()
    df_continuous = df[(df['成交时间'].dt.hour > 9)].copy()
    
    # print(df)
    if df_continuous.empty:
        logger("⚠️ 警告：没有连续竞价数据，将使用全量数据计算。")
        df_continuous = df

    # 3. 计算阈值 (核心步骤)
    # 我们定义：
    # 大单阈值 = 90% 分位点 (即只有 10% 的单子比这个大)
    # 小单阈值 = 30% 分位点 (即 30% 的单子比这个小)
    
    # --- 计算金额阈值 ---
    # quantile(0.9) 表示从小到大排列，排在 90% 位置的数值
    threshold_amt_big = df_continuous['成交金额'].quantile(0.90)
    threshold_amt_small = df_continuous['成交金额'].quantile(0.40)
    
    # --- 计算手数阈值 ---
    threshold_vol_big = df_continuous['成交量'].quantile(0.90)
    threshold_vol_small = df_continuous['成交量'].quantile(0.40)

    
    # 5. 返回结果，方便你直接复制到代码里
    return {
        "amt_big": threshold_amt_big,
        "amt_small": threshold_amt_small,
        "vol_big": threshold_vol_big,
        "vol_small": threshold_vol_small,
        "stock_code":stock_code
    }

def toSql(sql: str, rows: list,conn):
    """
        连接数据库
    """
    # print(sql,rows)
    try:
        cursor = conn.cursor()
        cursor.executemany(sql, rows)
        conn.commit()
    except Exception as e:
        raise ConnectionError("[ERROR] 连接数据库失败，具体原因是：" + str(e))

def main():
    sqls="select * from gp.stock_analysis where need_to_analysis=1"
    conn, engine = get_db_connection()
    df_analysis=pd.read_sql(sql=sqls,con=engine)
    rules_ls=[]
    for index,row in tqdm.tqdm(df_analysis.iterrows()):
        # print(row)
        code=row['stock_code']
        rules = analyze_and_get_thresholds(code)
        rules_ls.append(rules)
    rules_df=pd.DataFrame(rules_ls)
    result_df=pd.merge(df_analysis,rules_df,how='left',on='stock_code')
    print(result_df)
    result_df.rename(columns={
        'amt_big_y':'amt_big',
        'amt_small_y':'amt_small',
        'vol_big_y':'vol_big',
        'vol_small_y':'vol_small'
    },inplace=True)
    result_df=result_df[[
            'stock_code',
            'stock_name',
            'need_to_analysis',
            'trigger_count',
            'is_abnormal_type',
            'warning_info',
            'industry_block',
            'concept_block',
            'region_block',
            'concept_block_resonance',
            'create_time',
            'update_time',
            'max_buy_ratio',
            'min_buy_ratio',
            'max_zb',
            'min_zb',
            'trade_date',
            'amt_big',
            'amt_small',
            'vol_big',
            'vol_small']]
    sql = f"REPLACE INTO gp.stock_analysis(`{'`,`'.join(result_df.columns)}`) VALUES ({','.join(['%s' for _ in range(result_df.shape[1])])})"
    toSql(sql=sql, rows=result_df.values.tolist(),conn=conn)
        # df.to_csv("thshy_industries_gn.csv", index=False, encoding='utf-8-sig')
    logger.info(f"✅ 数据已保存至 thshy_industries.csv，共 {len(result_df)} 条记录")

# ==========================
# 执行入口
# ==========================
if __name__ == "__main__":
    # 这里填入你想分析的股票和日期
    main()