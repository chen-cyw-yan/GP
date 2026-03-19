import os
import re
import pandas as pd
import pymysql
from pathlib import Path
from datetime import datetime
def read_tdx_file(file_path):
    """
    自动识别通达信导出文件格式
    """
    try:
        # 尝试 HTML（最常见）
        return pd.read_html(file_path, encoding='gbk')[0]
    except:
        pass

    try:
        # 尝试 Excel
        return pd.read_excel(file_path, engine='xlrd', dtype=str)
    except:
        pass

    try:
        # 尝试 CSV
        return pd.read_csv(file_path, encoding='gbk')
    except:
        pass

    raise ValueError(f"无法解析文件: {file_path}")
# === 配置 ===
# DATA_DIR = r"E:\new_tdx\T0002\export\block_data" # 替换为你的 .xls 文件目录
DATA_DIR = r"C:\new_tdx\T0002\export"

DB_CONFIG = {
    'host': '127.0.0.1',
    'user': 'root',
    'password': 'chen',
    'database': 'gp',
    'charset': 'utf8mb4'
}

# 字段映射：Excel 列名 → 数据库字段名
COLUMN_MAP = {
    '代码': 'code',
    '名称': 'name',
    '涨幅%': 'change_pct',
    '现价': 'price',
    '涨跌': 'change_amt',
    '涨速%': 'speed_pct',
    '量比': 'volume_ratio',
    '涨跌数': 'up_down_count',
    '涨停数': 'limit_up_count',
    '跌停数': 'limit_down_count',
    '3日涨幅%': 'chg_3d',
    '总金额': 'total_amount',
    '开盘金额': 'open_amount',
    '换手%': 'turnover',
    '换手Z': 'turnover_z',
    '量涨速%': 'vol_speed_pct',
    '短换手%': 'short_turnover',
    '2分钟金额': 'amount_2min',
    '主力净额': 'main_net_amount',
    '主力净比%': 'main_net_ratio',
    '开盘换手Z': 'open_turnover_z',
    '开盘昨比%': 'open_vs_yest_pct',
    '连涨天': 'consecutive_up_days',
    '昨涨幅%': 'yest_change_pct',
    '5日涨幅%': 'chg_5d',
    '10日涨幅%': 'chg_10d',
    '20日涨幅%': 'chg_20d',
    '60日涨幅%': 'chg_60d',
    '一年涨幅%': 'chg_1y',
    '月初至今%': 'chg_mtd',
    '年初至今%': 'chg_ytd',
    '强弱度%': 'strength',
    '总量': 'total_volume',
    '市盈率': 'pe',
    '市净率': 'pb',
    '振幅%': 'amplitude',
    '昨收': 'prev_close',
    '今开': 'open_price',
    '最高': 'high_price',
    '最低': 'low_price',
    '均价': 'avg_price',
    '开盘%': 'open_pct',
    '回头波%': 'pullback_wave',
    '攻击波%': 'attack_wave',
    '现均差%': 'price_avg_diff_pct',
    '创建日期': 'dummy_create_date',  # 忽略，用文件名日期
    '流通市值': 'circ_mv',
    'AB股总市值': 'total_mv_ab',
    '流通股本Z': 'circ_shares_z',
    '流通股(亿)': 'circ_shares',
    '总股本(亿)': 'total_shares',
    '短期形态': 'short_trend',
    '中期形态': 'mid_trend',
    '长期形态': 'long_trend'
}

def parse_date_from_filename(filename: str):
    """从 '板块指数20260318.xls' 提取 2026-03-18"""
    match = re.search(r'(\d{8})', filename)
    if match:
        date_str = match.group(1)
        return datetime.strptime(date_str, '%Y%m%d').date()
    else:
        raise ValueError(f"无法从文件名提取日期: {filename}")

def clean_value(val):
    if pd.isna(val):
        return None

    s = str(val).strip()

    # ❗处理各种脏值
    if s in ['--', '-', '', 'None']:
        return None

    # 去掉百分号
    if s.endswith('%'):
        s = s[:-1]

    # 去掉逗号
    s = s.replace(',', '')

    # 处理“亿”
    if s.endswith('亿'):
        try:
            return float(s[:-1]) * 1e4  # 转万元
        except:
            return None

    # 转数字
    try:
        return float(s)
    except:
        return None

def connect_db():
    return pymysql.connect(**DB_CONFIG)
def clean_value(val):
    if pd.isna(val):
        return None

    s = str(val).strip()

    # ❗处理各种脏值
    if s in ['--', '-', '', 'None']:
        return None


    # 处理“亿”
    if s.endswith('亿'):
        try:
            return float(s[:-1]) * 1e4  # 转万元
        except:
            return None

    # # 转数字
    # try:
    #     return float(s)
    # except:
    #     return None
    return s

def process_xls_files(data_dir):
    conn = connect_db()
    cursor = conn.cursor()
    
    try:
        for file_path in Path(data_dir).glob("板块指数*.xls"):
            print(f"\n📁 处理文件: {file_path.name}")
            trade_date = parse_date_from_filename(file_path.name)
            print(f"  📅 交易日: {trade_date}")

            # 读取 Excel
            # df = pd.read_excel(file_path, dtype=str)  # 先全读为字符串，避免科学计数法
            # df=read_tdx_file(file_path)
            df = pd.read_csv(file_path, sep='\t', encoding='gbk')
            # print(df.columns.tolist())

            # 重命名列
            df.rename(columns=COLUMN_MAP, inplace=True)
            df=df.loc[~df['code'].str.contains('数据来源')]
            df['code']=df['code'].str.replace('\=|\"+','',regex=True)  
            # 添加 create_date 列
            df['create_date'] = trade_date
            # df = df.where(pd.notnull(df), None)
            
            # 找出哪些列还存在 NaN
            
            # 清理数据
            for col in df.columns:
                if col in COLUMN_MAP.values() and col != 'create_date':
                    df[col] = df[col].apply(clean_value)
            df = df.fillna(0)
            # 构造插入数据
            records = []
            for _, row in df.iterrows():
                record = tuple(row[col] for col in [
                    'code', 'name', 'change_pct', 'price', 'change_amt', 'speed_pct',
                    'volume_ratio', 'up_down_count', 'limit_up_count', 'limit_down_count',
                    'chg_3d', 'total_amount', 'open_amount', 'turnover', 'turnover_z',
                    'vol_speed_pct', 'short_turnover', 'amount_2min', 'main_net_amount',
                    'main_net_ratio', 'open_turnover_z', 'open_vs_yest_pct',
                    'consecutive_up_days', 'yest_change_pct', 'chg_5d', 'chg_10d',
                    'chg_20d', 'chg_60d', 'chg_1y', 'chg_mtd', 'chg_ytd', 'strength',
                    'total_volume', 'pe', 'pb', 'amplitude', 'prev_close', 'open_price',
                    'high_price', 'low_price', 'avg_price', 'open_pct', 'pullback_wave',
                    'attack_wave', 'price_avg_diff_pct', 'create_date',
                    'circ_mv', 'total_mv_ab', 'circ_shares_z', 'circ_shares',
                    'total_shares', 'short_trend', 'mid_trend', 'long_trend'
                ])
                records.append(record)
            
            # 批量插入
            if records:
                placeholders = ','.join(['%s'] * len(records[0]))
                sql = f"""
                INSERT INTO tdx_block_daily VALUES ({placeholders})
                ON DUPLICATE KEY UPDATE
                    name=VALUES(name), price=VALUES(price), change_pct=VALUES(change_pct)
                """
                cursor.executemany(sql, records)
                conn.commit()
                print(f"  ✅ 插入 {len(records)} 条记录")
    
    except Exception as e:
        print(f"❌ 错误: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    process_xls_files(DATA_DIR)