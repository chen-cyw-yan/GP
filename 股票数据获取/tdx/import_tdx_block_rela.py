import pymysql
from pathlib import Path

# === 配置区 ===
DATA_DIR = r"C:\new_tdx\T0002\export"

DB_CONFIG = {
    'host': '127.0.0.1',
    'user': 'root',
    'password': 'chen',
    'database': 'gp',
    'charset': 'utf8mb4'
}

BATCH_SIZE = 1000  # 批量提交大小


# === 工具函数 ===

def connect_db():
    return pymysql.connect(**DB_CONFIG)


def read_file_safely(file_path):
    """
    自动识别编码读取文件（解决通达信乱码问题）
    """
    encodings = ['gbk', 'utf-8', 'utf-8-sig', 'gb18030']

    for enc in encodings:
        try:
            with open(file_path, 'r', encoding=enc) as f:
                return f.readlines()
        except UnicodeDecodeError:
            continue

    # 最后一招：忽略错误字符
    print(f"⚠️ 编码异常，使用 ignore 模式: {file_path.name}")
    with open(file_path, 'r', encoding='gb18030', errors='ignore') as f:
        return f.readlines()


def clean_text(s):
    """
    清洗脏字符
    """
    return s.replace('\x00', '').replace('\ufeff', '').strip()


# === 主逻辑 ===

def process_txt_files(data_dir):
    conn = connect_db()
    cursor = conn.cursor()

    total_inserted = 0

    try:
        # 🔥 清空表
        print("🔄 正在清空表 tdx_block_stocks...")
        cursor.execute("TRUNCATE TABLE tdx_block_stocks;")
        conn.commit()
        print("✅ 表已清空")

        # 遍历文件
        for file_path in Path(data_dir).glob("*.txt"):
            filename_no_ext = file_path.stem

            print(f"\n📂 正在处理文件: {file_path.name}")

            try:
                lines = read_file_safely(file_path)
                batch = []
                file_count = 0

                for line_num, line in enumerate(lines, 1):
                    line = line.strip()
                    if not line:
                        continue

                    parts = line.split(',')

                    if len(parts) < 4:
                        print(f"  ⚠️ 跳过无效行 {line_num}: {line}")
                        continue

                    block_code = clean_text(parts[0])
                    block_name = clean_text(parts[1])
                    stock_code = clean_text(parts[2])
                    stock_name = clean_text(parts[3])

                    batch.append((
                        block_code,
                        block_name,
                        stock_code,
                        stock_name,
                        filename_no_ext
                    ))

                    # 🚀 批量提交
                    if len(batch) >= BATCH_SIZE:
                        insert_batch(cursor, batch)
                        conn.commit()
                        file_count += len(batch)
                        total_inserted += len(batch)
                        batch.clear()

                # 最后一批
                if batch:
                    insert_batch(cursor, batch) 
                    conn.commit()
                    file_count += len(batch)
                    total_inserted += len(batch)

                print(f"  ✅ {file_path.name} 插入 {file_count} 条")

            except Exception as e:
                print(f"  ❌ 文件处理失败: {file_path.name} -> {e}")
                conn.rollback()
                continue

        print("\n🎉 全部完成！")
        print(f"📊 总插入条数: {total_inserted}")

    except Exception as e:
        print(f"❌ 全局错误: {e}")
        conn.rollback()

    finally:
        cursor.close()
        conn.close()


def insert_batch(cursor, batch):
    """
    批量插入（防重复可选）
    """
    sql = """
    INSERT INTO tdx_block_stocks
    (block_code, block_name, stock_code, stock_name, block_type)
    VALUES (%s, %s, %s, %s, %s)
    """
    cursor.executemany(sql, batch)


# === 启动 ===

if __name__ == "__main__":
    process_txt_files(DATA_DIR)