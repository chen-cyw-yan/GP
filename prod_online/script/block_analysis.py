#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2026/03/20
# @Author : chenyanwen
# @Email : 1183445504@qq.com
# @Description : 股票板块共振分析自动化脚本 (重构优化版)

import sys
import os
import json
import time
import logging
import numpy as np
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine
from typing import Tuple, List, Optional
import pymysql
# 添加项目根目录到路径 (保持原有逻辑)
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))

# 引入飞书工具类 (确保路径正确)
try:
    from prod_online.config.feishu_utils import FeishuUtils
except ImportError:
    logger = logging.getLogger(__name__)
    logger.warning("未找到 FeishuUtils，飞书通知功能将不可用。")
    FeishuUtils = None

# ==============================
# 全局配置
# ==============================
DB_URL = "mysql+pymysql://root:chen@127.0.0.1:3306/gp"
OUTPUT_EXCEL_PATH = r'prod_online\imges\analy.xlsx'

# 飞书配置
FEISHU_APP_ID = 'cli_a9256b2aef7a5cd4'
FEISHU_APP_SECRET = 't22QBXS6MVqsXC41GoCDvbxin0tpXyL3'
FEISHU_CHAT_ID = 'oc_cd642a7fec1dcd847e91b2e1775809d2'

# 日志配置
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# ==============================
# 1. 数据加载模块
# ==============================
def load_data_from_db(engine) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, str]:
    """加载所有必要的数据"""
    logger.info("正在加载数据库数据...")
    
    # 1. 获取最新交易日期
    df_date = pd.read_sql("SELECT MAX(create_date) as last_date FROM gp.tdx_block_daily", con=engine)
    if df_date.empty or pd.isna(df_date['last_date'].iloc[0]):
        raise ValueError("无法获取最新的板块行情日期，表 gp.tdx_block_daily 可能为空。")
    
    last_date = df_date['last_date'].iloc[0]
    # 如果是 datetime 对象，转为字符串
    if isinstance(last_date, datetime):
        last_date = last_date.strftime('%Y-%m-%d')
    
    logger.info(f"最新交易日期: {last_date}")

    # 2. 加载待分析股票
    sql_analy = "SELECT * FROM gp.stock_analysis WHERE need_to_analysis = 1"
    df_analy = pd.read_sql(sql_analy, con=engine)
    if df_analy.empty:
        logger.warning("没有需要分析的股票 (need_to_analysis=1)。")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), ""
    
    # 清洗股票代码：去除前缀，统一为字符串
    df_analy['code_clean'] = df_analy['stock_code'].str[2:].astype(str)

    # 3. 加载板块关系
    sql_relx = "SELECT * FROM gp.tdx_block_stocks"
    df_relx = pd.read_sql(sql_relx, con=engine)
    if df_relx.empty:
        raise ValueError("板块关系表 gp.tdx_block_stocks 为空。")
    df_relx['stock_code'] = df_relx['stock_code'].astype(str) # 确保匹配类型一致

    # 4. 加载板块详情
    sql_detail = f"SELECT * FROM gp.tdx_block_daily WHERE create_date = '{last_date}'"
    df_detail = pd.read_sql(sql_detail, con=engine)
    if df_detail.empty:
        raise ValueError(f"未找到日期 {last_date} 的板块行情数据。")
    df_detail['code'] = df_detail['code'].astype(str)

    logger.info(f"数据加载完成：股票{len(df_analy)}只，关系{len(df_relx)}条，详情{len(df_detail)}条。")
    return df_analy, df_relx, df_detail, last_date

# ==============================
# 2. 核心计算模块
# ==============================
def calc_resonance_score(scores: np.ndarray, market_factor: float) -> float:
    """计算单只股票的共振分"""
    if len(scores) == 0:
        return 0.0
    
    scores = np.array(scores)
    strength = np.mean(scores)
    consistency = 1.0 / (1.0 + np.std(scores))
    power = np.mean(np.square(scores))
    
    base_score = 0.4 * strength + 0.3 * consistency + 0.3 * power
    return base_score * (1.0 + market_factor)

def process_market_factors(df_detail: pd.DataFrame) -> Tuple[pd.DataFrame, float]:
    """计算市场因子并归一化"""
    mean_val = df_detail['strength'].mean()
    std_val = df_detail['strength'].std()
    
    if std_val == 0 or np.isnan(std_val):
        df_detail['zscore'] = 0.0
    else:
        df_detail['zscore'] = (df_detail['strength'] - mean_val) / std_val
    
    df_detail['norm_score'] = 1.0 / (1.0 + np.exp(-df_detail['zscore']))
    
    market_mean = df_detail['norm_score'].mean()
    market_factor = (market_mean - 0.5) * 2.0
    
    logger.info(f"市场环境因子: {market_factor:.4f} (基准均值: {market_mean:.4f})")
    return df_detail, market_factor

def calculate_all_resonance(df_analy: pd.DataFrame, df_relx: pd.DataFrame, 
                            df_detail: pd.DataFrame, market_factor: float) -> pd.DataFrame:
    """
    批量计算所有股票的共振分
    使用 vectorized groupby 替代循环
    """
    logger.info("正在计算全量共振分数...")
    
    # 1. 筛选概念板块
    df_concept_relx = df_relx[df_relx['block_type'] == '概念板块']
    
    # 2. 合并：股票 -> 概念关系 -> 板块分数
    df_merge = pd.merge(
        df_analy[['stock_code', 'code_clean']],
        df_concept_relx[['stock_code', 'block_code']],
        left_on='code_clean', right_on='stock_code',
        how='left'
    )
    
    df_merge = pd.merge(
        df_merge,
        df_detail[['code', 'norm_score']],
        left_on='block_code', right_on='code',
        how='left'
    )
    
    # 3. 分组应用计算函数
    def group_calc(group):
        scores = group['norm_score'].dropna().values
        score = calc_resonance_score(scores, market_factor)
        return pd.Series({'resonance_score': round(score, 2)})
    
    df_scores = df_merge.groupby('stock_code_x').apply(group_calc).reset_index()
    df_scores.rename(columns={'stock_code_x': 'stock_code'}, inplace=True)
    
    return df_scores

def extract_top_blocks_vectorized(df_relx: pd.DataFrame, df_detail: pd.DataFrame) -> pd.DataFrame:
    """
    高性能提取 Top 板块信息 (地区Top1, 行业Top1, 概念Top2)
    完全向量化，无循环
    """
    logger.info("正在提取顶部板块信息 (向量化加速)...")
    
    # 1. 预合并获取名称和强度
    df_full = pd.merge(
        df_relx,
        df_detail[['code', 'strength', 'name']],
        left_on='block_code', right_on='code',
        how='left'
    )
    
    # 2. 排序：股票 -> 类型 -> 强度(降序)
    df_sorted = df_full.sort_values(
        ['stock_code', 'block_type', 'strength'], 
        ascending=[True, True, False]
    )
    
    # 3. 打标签：组内排名
    df_sorted['rank'] = df_sorted.groupby(['stock_code', 'block_type']).cumcount() + 1
    
    results = []
    
    # --- 地区板块 (Rank=1) ---
    mask_dq = (df_sorted['block_type'] == '地区板块') & (df_sorted['rank'] == 1)
    if mask_dq.any():
        df_dq = df_sorted.loc[mask_dq, ['stock_code', 'name', 'strength']].copy()
        df_dq['region_block'] = df_dq.apply(lambda x: f"{x['name']}({x['strength']:.2f})", axis=1)
        results.append(df_dq[['stock_code', 'region_block']])
    
    # --- 行业板块 (Rank=1) ---
    mask_hy = (df_sorted['block_type'] == '行业板块') & (df_sorted['rank'] == 1)
    if mask_hy.any():
        df_hy = df_sorted.loc[mask_hy, ['stock_code', 'name', 'strength']].copy()
        df_hy['industry_block'] = df_hy.apply(lambda x: f"{x['name']}({x['strength']:.2f})", axis=1)
        results.append(df_hy[['stock_code', 'industry_block']])
    
    # --- 概念板块 (Rank<=2, 拼接) ---
    mask_gn = (df_sorted['block_type'] == '概念板块') & (df_sorted['rank'] <= 3)
    if mask_gn.any():
        df_gn_raw = df_sorted.loc[mask_gn, ['stock_code', 'name', 'strength']].copy()
        df_gn_raw['item'] = df_gn_raw.apply(lambda x: f"{x['name']}({x['strength']:.2f})", axis=1)
        df_gn = df_gn_raw.groupby('stock_code')['item'].apply(lambda x: "，".join(x)).reset_index()
        df_gn.rename(columns={'item': 'concept_block'}, inplace=True)
        results.append(df_gn[['stock_code', 'concept_block']])
    
    if not results:
        return pd.DataFrame(columns=['stock_code', 'region_block', 'industry_block', 'concept_block'])
    
    # 合并所有结果
    df_final = results[0]
    for i in range(1, len(results)):
        df_final = pd.merge(df_final, results[i], on='stock_code', how='outer')
        
    return df_final

# ==============================
# 3. 数据库写入与通知模块
# ==============================
def save_to_db_and_notify(df_final: pd.DataFrame, engine, conn):
    """保存结果到数据库并发送飞书通知"""
    if df_final.empty:
        logger.warning("结果为空，跳过保存和通知。")
        return

    logger.info("正在保存结果到数据库...")
    
    # 准备 REPLACE INTO 语句
    columns = df_final.columns.tolist()
    # 过滤掉可能引起冲突的非数据库列（如果有），这里假设所有列都在表中
    # 注意：create_time 和 update_time 如果数据库有默认值或触发器，可能需要特殊处理
    # 这里假设直接覆盖
    
    col_str = ','.join([f"`{c}`" for c in columns])
    placeholders = ','.join(['%s'] * len(columns))
    sql = f"REPLACE INTO gp.stock_analysis ({col_str}) VALUES ({placeholders})"
    print(sql)
    try:
        cursor = conn.cursor()
        # 转换数据为列表，处理 NaN
        data_rows = df_final.where(pd.notnull(df_final), None).values.tolist()
        cursor.executemany(sql, data_rows)
        conn.commit()
        logger.info(f"成功更新 {len(data_rows)} 条记录到数据库。")
    except Exception as e:
        conn.rollback()
        logger.error(f"数据库写入失败: {e}")
        raise

    # 生成 Excel 并发送飞书
    logger.info("正在生成 Excel 并发送飞书通知...")
    try:
        # 准备导出列
        export_cols = [
            'stock_code', 'stock_name', 'trigger_count','is_abnormal_type','warning_info',
            'industry_block', 'region_block', 'concept_block',
            'concept_block_resonance', 'create_time', 'update_time','trade_date'
        ]
        # 确保列存在
        available_export_cols = [c for c in export_cols if c in df_final.columns]
        df_export = df_final[available_export_cols].copy()
        
        # 重命名中文表头
        rename_map = {
            'stock_code': '股票代码', 'stock_name': '股票名称', 'trigger_count': '触发次数'
            ,'is_abnormal_type':'是否异动类型','warning_info':'下一日可能触发',
            'industry_block': '行业板块', 'region_block': '地区板块', 'concept_block': '概念板块',
            'concept_block_resonance': '概念共振得分', 'create_time': '创建时间', 'update_time': '更新时间','trade_date':'最新添加时间'
        }
        df_export = df_export.rename(columns={k: v for k, v in rename_map.items() if k in df_export.columns})
        
        # 确保目录存在
        os.makedirs(os.path.dirname(OUTPUT_EXCEL_PATH), exist_ok=True)
        df_export.to_excel(OUTPUT_EXCEL_PATH, index=False)
        
        # 发送飞书
        if FeishuUtils:
            fs_client = FeishuUtils(FEISHU_APP_ID, FEISHU_APP_SECRET)
            
            # 发送文本消息
            context = {"text": f"📊 板块共振分析完成\n日期: {datetime.now().strftime('%Y-%m-%d')}\n共分析 {len(df_export)} 只股票"}
            fs_client.set_message_for_text('chat_id', FEISHU_CHAT_ID, json.dumps(context))
            
            # 发送文件
            fs_client.set_message_for_file('chat_id', FEISHU_CHAT_ID, OUTPUT_EXCEL_PATH, 'analy.xlsx')
            logger.info("飞书通知发送成功。")
        else:
            logger.warning("FeishuUtils 未导入，跳过通知。")
            
    except Exception as e:
        logger.error(f"Excel 生成或飞书通知失败: {e}")
def add_into_fs_table(df_to_fs_wiki,last_date,engine):
    code_list_str='","'.join(df_to_fs_wiki['stock_code'].to_list())
    print(code_list_str)
    sqls=f"""
        select date,code,`close` from gp.stock where `date`='{last_date}' and code in ("{code_list_str}")
    """
    today_stock=pd.read_sql(sql=sqls,con=engine)
    df_to_fs_wiki=pd.merge(df_to_fs_wiki,today_stock,how='inner',left_on=['stock_code','trade_date'],right_on=['code','date'])
    df_to_fs_wiki=df_to_fs_wiki[['stock_code',
        'stock_name',
        'trigger_count',
        'is_abnormal_type',
        'warning_info',
        'concept_block_resonance',
        'trade_date',
        'industry_block',
        'concept_block',
        'close']]
    cols={'stock_code':'代码',
        'stock_name':'名称',
        'trigger_count':'触动次数',
        'is_abnormal_type':'异动类型',
        'warning_info':'预警详情',
        'concept_block_resonance':'板块共振得分',
        'trade_date':'触发日期',
        'industry_block':'行业板块',
        'concept_block':'概念板块',
        'close':'收盘价'
    }
    df_to_fs_wiki=df_to_fs_wiki.rename(columns=cols)
    print(df_to_fs_wiki)
    fs_client = FeishuUtils(FEISHU_APP_ID, FEISHU_APP_SECRET)
    for index, row in df_to_fs_wiki.iterrows():
        # 1. 处理日期 -> 毫秒时间戳
        # 先确保是 datetime 对象，再转时间戳 * 1000
        date_val = row['触发日期']
        if isinstance(date_val, str):
            # 如果是字符串，先转 datetime
            date_obj = pd.to_datetime(date_val)
        else:
            date_obj = date_val
        
        # 转换为毫秒时间戳 (int)
        timestamp_ms = int(time.mktime(date_obj.timetuple()) * 1000)

        # 2. 构建字典
        dt = {
            # --- 转为字符串 (str) ---
            "名称": str(row['名称']) if pd.notna(row['名称']) else "",
            "代码": str(row['代码']) if pd.notna(row['代码']) else "",
            "异动类型": str(row['异动类型']) if pd.notna(row['异动类型']) else "",
            "行业板块": str(row['行业板块']) if pd.notna(row['行业板块']) else "",
            "概念板块": str(row['概念板块']) if pd.notna(row['概念板块']) else "",
            "预警详情": str(row['预警详情']) if pd.notna(row['预警详情']) else "",
            
            # --- 转为数字 (int/float) ---
            # 使用 pd.to_numeric 可以安全地将字符串或数字转为数值类型，errors='coerce' 会将无法转换的变为 NaN
            "收盘价": pd.to_numeric(row['收盘价'], errors='coerce'),
            "触动次数": pd.to_numeric(row['触动次数'], errors='coerce'),
            "板块共振得分": pd.to_numeric(row['板块共振得分'], errors='coerce'),
            # --- 日期戳 ---
            "触发日期": timestamp_ms
        }
        # print(dt)
        # exit(0)
        # 可选：将数字类型的 NaN 填充为 0，防止后续 JSON 序列化报错
        dt["收盘价"] = 0 if pd.isna(dt["收盘价"]) else dt["收盘价"]
        dt["触动次数"] = 0 if pd.isna(dt["触动次数"]) else dt["触动次数"]
        dt["板块共振得分"] = 0 if pd.isna(dt["板块共振得分"]) else dt["板块共振得分"]
        rs=fs_client.insert_table_record(app_token='FFewwkxf2izEVxkyA7Yc821GnXe',table_id='tbliSMaFdxeKSM8y',line=dt)
        logging.info(f'插入飞书文档结果：{rs}！！')
    # print(1)

# ==============================
# 主流程
# ==============================
def main():
    start_time = time.time()
    logger.info("=== 开始股票板块共振分析 ===")
    
    engine = None
    conn = None
    
    try:
        # 初始化连接
        engine = create_engine(DB_URL)
        conn = pymysql.connect(
            host='127.0.0.1', user='root', password='chen', 
            database='gp', autocommit=False
        )
        
        # 1. 加载数据
        df_analy, df_relx, df_detail, trade_date = load_data_from_db(engine)
        if df_analy.empty:
            return

        # 2. 计算市场因子
        df_detail, market_factor = process_market_factors(df_detail)

        # 3. 计算共振分
        df_scores = calculate_all_resonance(df_analy, df_relx, df_detail, market_factor)
        
        # 合并分数
        df_analy = pd.merge(df_analy, df_scores, on='stock_code', how='left')
        df_analy['resonance_score'] = df_analy['resonance_score'].fillna(0.0)

        # 4. 提取顶部板块 (向量化)
        df_blocks = extract_top_blocks_vectorized(df_relx, df_detail)
        
        # 合并板块信息 (使用 code_clean 匹配)
        df_analy = pd.merge(df_analy, df_blocks, left_on='code_clean', right_on='stock_code', how='left')
        cols = df_analy.columns.tolist()
        if 'stock_code_x' in cols and 'stock_code_y' in cols:
            # 删除右侧合并进来的纯净码列 (stock_code_y)
            df_analy.drop(columns=['stock_code_y','industry_block_x', 'concept_block_x', 'region_block_x'], inplace=True)
            # 将左侧的 stock_code_x 重命名回 stock_code
            df_analy.rename(columns={'stock_code_x': 'stock_code',
                                     'industry_block_y':'industry_block', 
                                     'concept_block_y':'concept_block', 
                                     'region_block_y':'region_block'
                                     }, inplace=True)
            logger.info("已修复 merge 导致的 stock_code 列名冲突。")
        elif 'stock_code' not in cols:
            # 极端情况：如果 stock_code 完全丢失，尝试从 code_clean 恢复（加回前缀逻辑需根据实际数据调整，这里假设必须有）
            # 通常上面的逻辑能解决，这里做个保底检查
            logger.error("致命错误：处理后仍然缺失 stock_code 列！")
            logger.error(f"当前列列表: {cols}")
            return
        # 填充空值
        for col in ['region_block', 'industry_block', 'concept_block']:
            if col in df_analy.columns:
                df_analy[col] = df_analy[col].fillna("")
            else:
                df_analy[col] = ""
        # print(df_analy.columns)
        last_day=max(df_analy['trade_date'].dropna().to_list())
        # print(last_day)
        df_to_fs_wiki=df_analy.loc[df_analy['trade_date']==last_day]
        # print(df_to_fs_wiki)
        add_into_fs_table(df_to_fs_wiki,last_date=last_day,engine=engine)
        # 5. 整理最终列
        final_columns = [
            'stock_code', 'stock_name', 'need_to_analysis', 'trigger_count', 'is_abnormal_type', 'warning_info',
            'industry_block', 'concept_block', 'region_block',
            'create_time', 'update_time', 'resonance_score','trade_date'
        ]
        available_cols = [c for c in final_columns if c in df_analy.columns]
        df_final = df_analy[available_cols].copy()
        # 重命名
        if 'resonance_score' in df_final.columns:
            df_final.rename(columns={'resonance_score': 'concept_block_resonance'}, inplace=True)
        
        # 排序
        if 'concept_block_resonance' in df_final.columns:
            df_final = df_final.sort_values('concept_block_resonance', ascending=False)

        # 6. 保存与通知
        save_to_db_and_notify(df_final, engine, conn)
        
        elapsed = time.time() - start_time
        logger.info(f"=== 分析完成，总耗时: {elapsed:.2f} 秒 ===")

    except Exception as e:
        logger.error(f"程序运行严重错误: {e}", exc_info=True)
    finally:
        if conn:
            conn.close()
        if engine:
            engine.dispose()

if __name__ == "__main__":
    main()