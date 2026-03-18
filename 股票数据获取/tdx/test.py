import os
import pandas as pd

def load_tdx_blocks(block_type='gn', tdx_path=r"C:\new_tdx"):
    """
    读取通达信本地板块数据
    :param block_type: 'gn'=概念, 'hy'=行业, 'dy'=地域, 'zs'=指数/自定义
    :param tdx_path: 通达信安装目录
    :return: DataFrame with columns ['block_name', 'stock_code']
    """
    file_map = {
        'gn': 'gn.cfg',
        'hy': 'hy.cfg',
        'dy': 'dy.cfg',
        'zs': 'zs.cfg'
    }
    file_name = file_map.get(block_type, 'gn.cfg')
    file_path = os.path.join(tdx_path, "T0002", "blocknew", file_name)
    
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"未找到板块文件: {file_path}")
    
    blocks = []
    with open(file_path, 'r', encoding='gbk') as f:
        for line in f:
            line = line.strip()
            if not line or ',' not in line:
                continue
            parts = line.split(',')
            block_name = parts[0]
            stocks = [s.strip() for s in parts[1:] if s.strip().isdigit() and len(s.strip()) == 6]
            for stock in stocks:
                blocks.append({'block_name': block_name, 'stock_code': stock})
    
    return pd.DataFrame(blocks)

# 使用示例
concept_df = load_tdx_blocks('gn', tdx_path=r"D:\tdx")  # 替换为你的路径
print(concept_df.head())