import sys
import os
import json
import logging
from datetime import datetime
from dateutil.relativedelta import relativedelta

import pandas as pd
import numpy as np
import tqdm
import baostock as bs
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

def fetch_analysis_candidates(engine):
    """
    从数据库获取需要分析的股票列表及股本信息
    """
    sql = """
    SELECT aly.*, stock.outstanding_share 
    FROM gp.stock_analysis as aly
    JOIN (
        SELECT code, max(outstanding_share) as outstanding_share 
        FROM gp.stock s 
        WHERE outstanding_share != 0 
        GROUP BY code
    ) as stock ON stock.code = aly.stock_code 
    WHERE aly.need_to_analysis = '1'
    """
    logger.info("正在获取待分析股票列表...")
    df = pd.read_sql(sql=sql, con=engine)
    if df.empty:
        logger.warning("未找到需要分析的股票 (need_to_analysis = 1)")
    return df

def fetch_baostock_data(codes_shares_map, start_date, end_date):
    """
    使用 Baostock 获取历史分钟线数据
    codes_shares_map: {code: outstanding_share}
    """
    logger.info(f"正在从 Baostock 下载数据 ({start_date} 至 {end_date})...")
    lg = bs.login()
    if lg.error_code != '0':
        logger.error(f"Baostock 登录失败: {lg.error_msg}")
        return pd.DataFrame()

    res_df_list = []
    
    # 转换格式：数据库中的 'sh600000' -> Baostock 需要的 'sh.600000'
    # 注意：原代码逻辑是 v['stock_code'][0:2]+'.'+v['stock_code'][2:]
    # 假设 stock_code 格式为 'sh600000' 或 'sz000001'
    
    for code, ltgb in tqdm.tqdm(codes_shares_map.items(), desc="下载个股数据"):
        # 格式化代码用于 baostock (例如 sh.600000)
        if len(code) >= 2:
            bs_code = f"{code[:2]}.{code[2:]}"
        else:
            continue
            
        rs = bs.query_history_k_data_plus(
            bs_code,
            "date,time,code,open,high,low,close,volume,amount,adjustflag",
            start_date=start_date, 
            end_date=end_date,
            frequency="5", 
            adjustflag="3"
        )

        if rs.error_code != '0':
            logger.warning(f"获取 {code} 数据失败: {rs.error_msg}")
            continue

        data_list = []
        while rs.next():
            data_list.append(rs.get_row_data())
        
        if data_list:
            results = pd.DataFrame(data_list, columns=rs.fields)
            results['code'] = code # 存回原始代码格式
            results['ltgb'] = float(ltgb)
            res_df_list.append(results)

    bs.logout()
    
    if not res_df_list:
        return pd.DataFrame()
        
    return pd.concat(res_df_list, ignore_index=True)

def process_daily_indicators(df_raw):
    """
    清洗数据并计算每日基础指标 (开盘, 收盘, 涨幅等)
    只保留每天前 3 根 5 分钟线 (约 15 分钟)
    """
    if df_raw.empty:
        return pd.DataFrame()

    logger.info("正在计算每日基础指标...")
    df = df_raw.copy()
    
    # 时间处理
    df['time'] = pd.to_datetime(df['time'], format="%Y%m%d%H%M%S%f")
    df['hour'] = df['time'].dt.hour
    df['minute'] = df['time'].dt.minute
    
    # 计算昨收 (分组内移位)
    # 注意：这里需要按 code 分组移位，否则不同股票间会串
    df['pre_close'] = df.groupby('code')['close'].shift(1)
    
    # 去除起始日期的第一条数据 (因为没有昨收)
    # 原逻辑：df=df.loc[df['date']!=start_date] 
    # 更严谨的做法是过滤掉 pre_close 为空的行
    df = df.dropna(subset=['pre_close'])

    res_df = []
    
    # 按股票和日期分组处理
    grouped = df.groupby(['code', 'date'])
    
    for (code, date), v in tqdm.tqdm(grouped, total=len(grouped), desc="计算日线指标"):
        try:
            # 1. 当天最高价
            today_high = v['high'].max()
            
            # 2. 提取特定时间点数据 (09:35 开盘, 15:00 收盘)
            # 开盘 (09:35)
            open_mask = (v['hour'] == 9) & (v['minute'] == 35)
            if not open_mask.any(): continue
            today_open = v.loc[open_mask, 'open'].iloc[0]
            
            # 收盘 (15:00)
            close_mask = (v['hour'] == 15) & (v['minute'] == 0)
            if not close_mask.any(): continue
            today_close = v.loc[close_mask, 'close'].iloc[0]
            
            # 昨收 (09:35 的 pre_close)
            yes_close = v.loc[open_mask, 'pre_close'].iloc[0]
            
            # 类型转换防错
            today_open = float(today_open)
            today_close = float(today_close)
            yes_close = float(yes_close)
            
            # 计算涨幅
            zf = round((today_close - yes_close) / yes_close, 4) if yes_close != 0 else 0.0
            sjzf = round((today_close - today_open) / today_open, 4) if today_open != 0 else 0.0
            
            # 构建新列
            v = v.copy() # 避免 SettingWithCopyWarning
            v['today_high'] = today_high
            v['today_open'] = today_open
            v['today_close'] = today_close
            v['yes_close'] = yes_close
            v['zf'] = zf
            v['sjzf'] = sjzf
            
            # 只取前 3 根 K 线 (约 15 分钟)
            v_short = v.iloc[:3]
            res_df.append(v_short)
            
        except Exception as e:
            logger.warning(f"处理 {code} {date} 时出错: {e}")
            continue

    if not res_df:
        return pd.DataFrame()
        
    return pd.concat(res_df, ignore_index=False)

def calculate_volume_estimation(df):
    """
    估算买卖量并计算每日汇总指标
    """
    if df.empty:
        return pd.DataFrame()

    logger.info("正在估算成交量分布...")
    dfs = df.copy()
    
    # 确保数值类型
    for col in ['high', 'low', 'close', 'volume']:
        dfs[col] = pd.to_numeric(dfs[col], errors='coerce')

    # 定义估算函数
    def estimate_bar_buy_ratio(row):
        high, low, close = row['high'], row['low'], row['close']
        if pd.isna(high) or pd.isna(low) or pd.isna(close):
            return np.nan
        range_val = high - low
        if range_val == 0:
            return 0.5
        return round((close - low) / range_val, 4)

    dfs['buy_ratio'] = dfs.apply(estimate_bar_buy_ratio, axis=1)
    dfs['est_buy_vol'] = dfs['volume'] * dfs['buy_ratio']
    dfs['est_sell_vol'] = dfs['volume'] * (1 - dfs['buy_ratio'])

    min15_list = []
    
    # 按日汇总
    grouped = dfs.groupby(['code', 'date'])
    for (code, date), v in tqdm.tqdm(grouped, total=len(grouped), desc="汇总每日数据"):
        buy_volume = v['est_buy_vol'].sum()
        sell_volume = v['est_sell_vol'].sum()
        
        if buy_volume == 0 or sell_volume == 0:
            continue
            
        # 防止除零，添加微小值
        ratio = (buy_volume + 1e-8) / (sell_volume + 1e-8)
        
        all_volume = buy_volume + sell_volume
        
        # 获取流通股本 (取第一行即可)
        v_reset = v.reset_index(drop=True)
        all_lt_volume = v_reset.loc[0, 'ltgb']
        
        if all_lt_volume == 0:
            continue
            
        zb = round(all_volume / all_lt_volume, 6) # 占比通常很小，多留几位小数
        
        dt = {
            "code": code,
            "date": date,
            "buy_volume": buy_volume,
            "sell_volume": sell_volume,
            "buy_ratio": ratio,
            "all_volume": all_volume,
            "zb": zb,
            "zf": v_reset.loc[0, 'zf'],
            "sjzf": v_reset.loc[0, 'sjzf']
        }
        min15_list.append(dt)

    if not min15_list:
        return pd.DataFrame()
        
    result_df = pd.DataFrame(min15_list)
    # 可选：计算次日涨幅 (需要后续数据支持，此处仅做结构保留，实际可能全为 NaN 如果是最后一天)
    # result_df['next_day_zf'] = result_df.sort_values(['code', 'date']).groupby('code')['zf'].shift(-1)
    
    return result_df

def aggregate_yearly_stats(daily_stats_df, candidates_df, engine, conn):
    """
    按股票代码统计近一年的最大/最小值，并回写数据库
    """
    if daily_stats_df.empty:
        logger.warning("无每日统计数据可聚合")
        return

    logger.info("正在聚合年度统计指标并更新数据库...")
    
    # 确保代码列类型一致
    daily_stats_df['code'] = daily_stats_df['code'].astype(str)
    candidates_df['stock_code'] = candidates_df['stock_code'].astype(str)
    
    # 分组计算极值
    # stats = daily_stats_df.groupby('code').agg(
    #     min_buy_ratio=('buy_ratio', 'min'),
    #     max_buy_ratio=('buy_ratio', 'max'),
    #     min_zb=('zb', 'min'),
    #     max_zb=('zb', 'max')
    # ).reset_index()
    stats = daily_stats_df.groupby('code').agg(
        low_buy_ratio=('buy_ratio', lambda x: x.quantile(0.05)),
        high_buy_ratio=('buy_ratio', lambda x: x.quantile(0.95)),
        low_zb=('zb', lambda x: x.quantile(0.05)),
        high_zb=('zb', lambda x: x.quantile(0.95))
    ).reset_index()

    
    # 合并到候选表 (左连接，保留所有需要分析的股票，没有数据的保持 NULL)
    # 注意：pandas merge 后列名可能需要调整以匹配数据库
    update_df = candidates_df.merge(stats, left_on='stock_code', right_on='code', how='left')
    
    # 填充需要的列，如果 merge 产生重复列，选择正确的
    # 这里的逻辑是：用计算出的 stats 更新 update_df 中的对应列
    for col in ['min_buy_ratio', 'max_buy_ratio', 'min_zb', 'max_zb']:
        if col in stats.columns:
            # 将计算结果映射回原表
            mapping = stats.set_index('code')[col].to_dict()
            update_df[col] = update_df['stock_code'].map(mapping)

    # 准备写入数据库的列
    target_cols = [
        'stock_code', 'stock_name', 'need_to_analysis', 'trigger_count',
        'is_abnormal_type', 'warning_info', 'industry_block', 'concept_block',
        'region_block', 'concept_block_resonance',
        'min_buy_ratio', 'max_buy_ratio', 'min_zb', 'max_zb'
    ]
    
    # 确保所有目标列都存在
    final_df = update_df[[c for c in target_cols if c in update_df.columns]]
    
    # 替换 NaN 为 None (SQL NULL)
    final_df = final_df.where(pd.notnull(final_df), None)
    
    rows_data = final_df.values.tolist()
    columns_str = ','.join([f"`{c}`" for c in final_df.columns])
    placeholders = ','.join(['%s'] * len(final_df.columns))
    
    sql = f"REPLACE INTO gp.stock_analysis ({columns_str}) VALUES ({placeholders})"
    
    try:
        cursor=conn.cursor()
        cursor.executemany(sql, rows_data)
        engine.dispose() # 提交事务由 pymysql cursor 控制，但 dispose 释放连接池
        conn.commit() # 提交
        logger.info(f"成功更新 {len(rows_data)} 条记录到数据库")
    except Exception as e:
        logger.error(f"数据库更新失败: {e}")
        raise

def main():
    """主程序入口"""
    logger.info("="*30 + " 开始执行股票分析任务 " + "="*30)
    
    # 1. 初始化连接
    try:
        conn, engine = get_db_connection()
        # cursor = conn.cursor()
    except Exception as e:
        logger.error(f"数据库连接失败: {e}")
        return

    try:
        # 2. 获取待分析股票
        candidates_df = fetch_analysis_candidates(engine)
        if candidates_df.empty:
            return

        # 3. 设定时间范围 (近一年)
        today_dt = datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)
        one_year_ago = today_dt - relativedelta(years=1)
        start_date = one_year_ago.strftime("%Y-%m-%d")
        end_date = today_dt.strftime("%Y-%m-%d")

        # 4. 下载数据
        # 构建 {code: ltgb} 字典
        code_share_map = candidates_df.set_index('stock_code')['outstanding_share'].to_dict()
        raw_data_df = fetch_baostock_data(code_share_map, start_date, end_date)
        
        if raw_data_df.empty:
            logger.warning("未下载到任何数据")
            return

        # 5. 数据处理流水线
        daily_indicators_df = process_daily_indicators(raw_data_df)
        if daily_indicators_df.empty:
            logger.warning("处理后无有效日数据")
            return
            
        daily_stats_df = calculate_volume_estimation(daily_indicators_df)
        if daily_stats_df.empty:
            logger.warning("无有效成交量统计数据")
            return

        # 6. 聚合统计并回写数据库
        aggregate_yearly_stats(daily_stats_df, candidates_df, engine, conn)

        logger.info("任务执行完毕！")

    except Exception as e:
        logger.error(f"任务执行过程中发生严重错误: {e}", exc_info=True)
    
    finally:
        # 清理资源
        try:
            cursor.close()
            conn.close()
            logger.info("数据库连接已关闭")
        except:
            pass

if __name__ == "__main__":
    main()