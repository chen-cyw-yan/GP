import os
import re
import pandas as pd
import pymysql
from datetime import datetime
import sys

# 添加项目根目录到路径
sys.path.append(
    os.path.abspath(
        os.path.join(os.path.dirname(__file__), "../../")
    )
)

# ✅ 修复1: 使用原始字符串或正斜杠避免转义问题
DATA_DIR = r"E:\stock\GP\prod_online\imges\limit_up_data"  # 或 "prod_online/imges/limit_up_data"

DB_CONFIG = {
    "host": "127.0.0.1",
    "user": "root",
    "password": "chen",
    "database": "gp",
    "charset": "utf8mb4"
}


def safe_str(s):
    """安全地将任意对象转为字符串，避免 NaN / None 导致正则失败"""
    if pd.isna(s):
        return ""
    return str(s).strip()


def normalize_columns(df):
    new_cols = {}
    for col in df.columns:
        col_str = safe_str(col)
        # 移除日期部分
        base = re.sub(r'\d{4}\.\d{2}\.\d{2}', '', col_str)
        base = base.strip()
        new_cols[col] = base if base else str(col)  # 防止列名变空
    df.rename(columns=new_cols, inplace=True)
    return df


def extract_date(df):
    for col in df.columns:
        col_str = safe_str(col)
        m = re.search(r'\d{4}\.\d{2}\.\d{2}', col_str)
        if m:
            try:
                return datetime.strptime(m.group(), "%Y.%m.%d").date()
            except ValueError:
                continue
    return None


def insert_mysql(df, trade_date):
    conn = pymysql.connect(**DB_CONFIG)
    cursor = conn.cursor()

    sql = """
    INSERT INTO stock_limit_up (
        trade_date,
        stock_code,
        stock_name,
        price,
        pct_change,
        is_limit_up,
        limit_up_type,
        board_count,
        first_limit_time,
        final_limit_time,
        limit_detail,
        consecutive_limit_days,
        limit_reason,
        seal_volume,
        seal_amount,
        seal_ratio,
        seal_flow_ratio,
        open_board_count,
        float_market_value
    )
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON DUPLICATE KEY UPDATE
        price=VALUES(price),
        pct_change=VALUES(pct_change),
        limit_up_type=VALUES(limit_up_type)
    """

    for _, row in df.iterrows():
        # 使用 .get() 并设置默认值 None，防止 KeyError
        clean_row = row.where(pd.notna(row), None)
        data = (
            trade_date,
            clean_row.get("股票代码"),
            clean_row.get("股票简称"),
            clean_row.get("现价(元)"),
            clean_row.get("涨跌幅(%)"),
            clean_row.get("涨停"),
            clean_row.get("涨停类型"),
            clean_row.get("几天几板"),
            clean_row.get("首次涨停时间"),
            clean_row.get("最终涨停时间"),
            clean_row.get("涨停明细数据"),
            clean_row.get("连续涨停天数(天)"),
            clean_row.get("涨停原因类别"),
            clean_row.get("涨停封单量(股)"),
            clean_row.get("涨停封单额(元)"),
            clean_row.get("涨停封成比(%)"),
            clean_row.get("涨停封流比(%)"),
            clean_row.get("涨停开板次数(次)"),
            clean_row.get("a股流通市值 (元)")
        )
        cursor.execute(sql, data)

    conn.commit()
    cursor.close()
    conn.close()


def main():
    if not os.path.exists(DATA_DIR):
        print(f"目录不存在: {DATA_DIR}")
        return

    files = [
        f for f in os.listdir(DATA_DIR)
        if f.endswith(".xls") or f.endswith(".xlsx")
    ]
    print(files)
    files.sort(reverse=True)
    files = files[:5]

    for file in files:
        path = os.path.join(DATA_DIR, file)
        print("处理:", file)

        try:
            # ✅ 使用 read_html 兼容 HTML 格式的“假Excel”
            tables = pd.read_html(path, encoding='utf-8')
            if not tables:
                print(f"  警告: 文件 {file} 中未找到表格")
                continue
            df = tables[0]
            df.columns = df.iloc[0]          # 将第0行（第一行）设为列名
            df = df.drop(df.index[0])        # 删除原来的第0行（现在它已是列名）
            df = df.reset_index(drop=True)   # 重置索引（可选，让行号从0开始）
            # ✅ 关键：清理列名，确保可处理
            df.columns = [safe_str(col) for col in df.columns]

            trade_date = extract_date(df)
            print(df)
            if trade_date is None:
                print(f"  未找到日期: {file}")
                continue

            df = normalize_columns(df)
            insert_mysql(df, trade_date)
            print("  完成:", trade_date)

        except Exception as e:
            print(f"  处理失败 {file}: {e}")
            continue


if __name__ == "__main__":
    main()