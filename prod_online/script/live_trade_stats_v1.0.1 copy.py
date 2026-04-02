from concurrent.futures import ThreadPoolExecutor, as_completed
from flask_cors import CORS
import requests
from sqlalchemy import create_engine
import pandas as pd
import akshare as ak
import logging
import datetime
# ==============================
# 日志配置
# ==============================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

engine = create_engine("mysql+pymysql://root:chen@127.0.0.1:3306/gp")
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


def fetch_one_stock(v):
    try:
        stock_code = v['stock_code']
        
        # 读取配置参数 (来自 stock_analysis 表)
        amt_big_threshold = v.get('amt_big', 100_0000)  # 默认 100万
        amt_small_threshold = v.get('amt_small', 10_0000) # 默认 10万
        vol_big_threshold = v.get('vol_big', 1000)         # 默认 1000手
        vol_small_threshold = v.get('vol_small', 100)      # 默认 100手

        # 1. 获取原始分笔数据
        dft_raw = stock_zh_a_tick_tx_js(symbol=stock_code)
        if dft_raw is None or dft_raw.empty:
            return None

        # 2. 数据清洗与预处理
        # 腾讯数据原始列：0=未知, 1=成交时间, 2=成交价格, 3=成交量(手), 4=成交金额(元), 5=性质
        # 我们需要映射列名以便处理
        dft = dft_raw.copy()
        dft.columns = ["idx", "成交时间", "成交价格", "成交量", "成交金额", "性质_raw"]
        
        # 转换时间
        dft['成交时间'] = pd.to_datetime(dft['成交时间'], errors='coerce')
        dft = dft.dropna(subset=['成交时间'])
        
        # 提取时分
        dft['hour'] = dft['成交时间'].dt.hour
        dft['minute'] = dft['成交时间'].dt.minute # 修正拼写错误 (mintue -> minute)
        
        # 过滤 9:45 之前的集合竞价数据
        dft = dft[(dft['hour'] == 9) & (dft['minute'] <= 45)]
        if dft.empty:
            return None

        # 转换数据类型
        dft['成交价格'] = pd.to_numeric(dft['成交价格'], errors='coerce')
        dft['成交量'] = pd.to_numeric(dft['成交量'], errors='coerce').fillna(0).astype(int)
        dft['成交额计算'] = dft['成交价格'] * dft['成交量'] # 计算单笔成交总额(元)

        # 3. 资金流向分类 (核心逻辑)
        # 初始化分类列
        dft['资金类型'] = '中单' # 默认中单
        dft['方向'] = dft['性质_raw'].map({'B': '流入', 'S': '流出', 'M': '中性'}).fillna('未知')

        # --- 大单判定 (满足任一条件即可) ---
        # 条件1: 成交额 > amt_big
        # 条件2: 成交量 > vol_big
        is_big = (dft['成交额计算'] > amt_big_threshold) | (dft['成交量'] > vol_big_threshold)
        dft.loc[is_big, '资金类型'] = '大单'

        # --- 小单判定 (满足任一条件即可) ---
        # 条件1: 成交额 < amt_small
        # 条件2: 成交量 < vol_small
        # 注意: 这里的逻辑是, 如果是大单, 就不会被判定为小单 (因为上面已经赋值了)
        is_small = (dft['成交额计算'] < amt_small_threshold) | (dft['成交量'] < vol_small_threshold)
        dft.loc[is_small, '资金类型'] = '小单'

        # 4. 构建复合标签 (如: 大单流入, 小单流出)
        dft['分类标签'] = dft['资金类型'] + '_' + dft['方向']

        # 5. 聚合统计 (按分钟聚合)
        # 我们需要统计每种类型的成交量(手)
        pivot_df = pd.pivot_table(
            dft,
            index='minute',
            columns='分类标签',
            values='成交量', # 统计成交量(手)
            aggfunc='sum',
            fill_value=0
        )

        # 6. 计算累计值和比率 (根据你的原需求保留)
        # --- 保留原有的 buy_ratio 计算逻辑 (基于原始买盘/卖盘) ---
        # 如果你需要基于新分类计算比率，需要修改这里的逻辑
        dft['性质'] = dft['性质_raw'].map({'B': '买盘', 'S': '卖盘', 'M': '中性盘'}).fillna('未知')
        
        pivot_original = pd.pivot_table(
            dft,
            index='minute',
            columns='性质',
            values='成交量',
            aggfunc='sum',
            fill_value=0
        )
        
        pivot_df['累计_买盘'] = pivot_original.get('买盘', 0).cumsum()
        pivot_df['累计_卖盘'] = pivot_original.get('卖盘', 0).cumsum()
        
        # 防止除零错误
        pivot_df['buy_ratio'] = pivot_df['累计_买盘'] / (pivot_df['累计_卖盘'] + 1e-6)
        
        # --- 新增: 计算大中小单净流入 (流入 - 流出) ---
        # 假设列名为: 大单_流入, 大单_流出
        pivot_df['大单净流入'] = (
            pivot_df.get('大单_流入', 0) - pivot_df.get('大单_流出', 0)
        ).cumsum()
        
        pivot_df['小单净流出'] = (
            pivot_df.get('小单_流出', 0) - pivot_df.get('小单_流入', 0)
        ).cumsum() # 小单流出通常被视为散户离场

        # 7. 填充股票基本信息
        pivot_df['stock_name'] = v['stock_name']
        pivot_df['stock_code'] = stock_code
        
        # 8. 换手率计算 (zb)
        # 注意: 这里假设 outstanding_share 是总股本(手)
        # 如果 outstanding_share 是股，需要除以 100
        pivot_df['zb'] = (
            (pivot_df['累计_买盘'] + pivot_df['累计_卖盘']) * 100 / v['outstanding_share']
        )

        return pivot_df.reset_index()

    except Exception as e:
        logger.error(f"❌ {v.get('stock_code', '未知')} error:", exc_info=True)
        return None
    
def get_all_data(df_anal):
    results = []

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(fetch_one_stock, v) for _, v in df_anal.iterrows()]

        for future in as_completed(futures):
            res = future.result()
            if res is not None:
                results.append(res)

    if results:
        return pd.concat(results)
    else:
        return pd.DataFrame()


from flask import Flask, jsonify
import threading
import time

app = Flask(__name__)

global_data = []
CORS(app)
# 后台线程：持续更新数据
def data_updater():
    global global_data
    df_anal = pd.read_sql(
            """select need.*,stock.outstanding_share  from gp.stock_analysis as need
join (select s.code,max(s.outstanding_share) as outstanding_share  from gp.stock s group by s.`code` ) as stock
on need.stock_code=stock.code 
where need_to_analysis=1""",
            con=engine
        )

    df_rate=pd.read_sql("select * from gp.stock_strategy_data_15minute where code in ('"+df_anal['stock_code'].str.cat(sep="','")+"')",con=engine)
    # print(df_all)

    while True:
        print("🔄 更新数据中...")

        # ⭐ 每次循环重新读取（关键）
        df_all = get_all_data(df_anal)
        # 防御性判断：抓取失败时 df_all 可能为空或缺少 stock_name 列
        if df_all.empty or 'stock_name' not in df_all.columns:
            print("⚠️ 本次未获取到有效数据，跳过。")
            global_data = []
            time.sleep(10)
            continue

        # 只返回：按“最后一条分钟记录的 buy_ratio”从高到低的前 10 只股票
        tmp_by_stock = {}
        last_buy_by_stock = {}

        for stock in df_all['stock_name'].unique():
            tmp = df_all[df_all['stock_name'] == stock]
            tmp = tmp.loc[tmp['mintue'] != 25]

            if tmp.empty:
                continue

            tmp['buy_ratio'] = tmp['buy_ratio'].round(2)
            # tmp['buy_ratio_norm'] = tmp['buy_ratio_norm'].round(3)
            tmp['zb'] = tmp['zb'].round(5)
            tmp = tmp.sort_values('mintue')
            tmp = tmp.iloc[1:]  # 跟你原逻辑一致：去掉第一行

            if tmp.empty:
                continue

            last_buy = tmp.iloc[-1]['buy_ratio']
            if last_buy is None:
                continue
            try:
                last_buy = float(last_buy)
            except Exception:
                continue

            # 跳过 NaN
            if pd.isna(last_buy):
                continue

            tmp_by_stock[stock] = tmp[['mintue', 'buy_ratio', 'zb']].copy()
            last_buy_by_stock[stock] = last_buy

        top10_stocks = sorted(last_buy_by_stock.items(), key=lambda x: x[1], reverse=True)[:10]
        # print(top10_stocks)
        # for item in top10_stocks:
        #     name=item[0]
        #     last_df = tmp_by_stock[name]
        #     last_ratio = last_df.iloc[-1]['buy_ratio']
        #     last_zb = last_df.iloc[-1]['zb']
        #     code=df_all.loc[df_all['stock_name']==name,'stock_code'].to_list()[0]
        #     df_one_rate=df_rate.loc[(df_rate['code']==code)&(df_rate['buy_ratio'] >= last_ratio) & (df_rate['zb'] >= last_zb)]
        #     tmp_by_stock[stock]['win_rate']=round((df_one_rate['mrzf_and_next_sum']>0).mean(),2)
        for item in top10_stocks:
            name = item[0]

            last_df = tmp_by_stock[name]

            last_ratio = last_df.iloc[-1]['buy_ratio']
            last_zb = last_df.iloc[-1]['zb']

            code = df_all.loc[df_all['stock_name'] == name, 'stock_code'].iloc[0]

            df_one_rate = df_rate[
                (df_rate['code'] == code) &
                (df_rate['buy_ratio'] >= last_ratio) &
                (df_rate['zb'] >= last_zb)
            ]

            if df_one_rate.empty:
                win_rate = None
            else:
                win_rate = round((df_one_rate['mrzf_and_next_sum'] > 0).mean(), 2)
                all_cnt=(df_one_rate['mrzf_and_next_sum']).count()
                win_cnt=(df_one_rate['mrzf_and_next_sum'] > 0).sum()
            print(all_cnt,win_cnt)
            # ⭐ 给整列赋值（不是只一行）
            tmp_by_stock[name]['win_rate'] = win_rate
            tmp_by_stock[name]['cnt'] = all_cnt
            tmp_by_stock[name]['win_cnt'] = win_cnt

        # print(tmp_by_stock)
        global_data = [
            {
                "name": stock,
                "data": tmp_by_stock[stock][['mintue', 'buy_ratio', 'zb', 'win_rate','cnt','win_cnt']].values.tolist()
            }
            for stock, _ in top10_stocks
            if stock in tmp_by_stock
        ]
        time.sleep(15)


@app.route("/data")
def get_data():
    return jsonify(global_data)


if __name__ == "__main__":
    t = threading.Thread(target=data_updater)
    t.daemon = True
    t.start()

    app.run(debug=True)