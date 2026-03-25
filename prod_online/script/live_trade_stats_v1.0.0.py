from concurrent.futures import ThreadPoolExecutor, as_completed


from sqlalchemy import create_engine
import pandas as pd
import akshare as ak
engine = create_engine("mysql+pymysql://root:chen@127.0.0.1:3306/gp")


def fetch_one_stock(v):
    try:
        stock_code = v['stock_code']
        dft = ak.stock_zh_a_tick_tx_js(symbol=stock_code)

        dft['成交时间'] = pd.to_datetime(dft['成交时间'])
        dft['hour'] = dft['成交时间'].dt.hour
        dft['mintue'] = dft['成交时间'].dt.minute

        dft = dft[(dft['hour'] == 9) & (dft['mintue'] <= 45)]

        pivot_df = pd.pivot_table(
            dft,
            index='mintue',
            columns='性质',
            values='成交量',
            aggfunc='sum',
            fill_value=0
        )

        pivot_df['累计_买盘'] = pivot_df.get('买盘', 0).cumsum()
        pivot_df['累计_卖盘'] = pivot_df.get('卖盘', 0).cumsum()

        pivot_df['buy_ratio'] = pivot_df['累计_买盘'] / (pivot_df['累计_卖盘'] + 1e-6)

        pivot_df['buy_ratio_norm'] = (
            (pivot_df['buy_ratio'] - v['min_buy_ratio']) /
            (v['max_buy_ratio'] - v['min_buy_ratio'] + 1e-6)
        )

        pivot_df['stock_name'] = v['stock_name']
        pivot_df['stock_code'] =stock_code

        return pivot_df.reset_index()

    except Exception as e:
        print(f"❌ {v['stock_code']} error:", e)
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

# 后台线程：持续更新数据
def data_updater():
    global global_data

    while True:
        print("🔄 更新数据中...")

        # ⭐ 每次循环重新读取（关键）
        df_anal = pd.read_sql(
            "select * from gp.stock_analysis where need_to_analysis=1",
            con=engine
        )

        df_all = get_all_data(df_anal)
        print(df_all)
        data = []
        for stock in df_all['stock_name'].unique():
            tmp = df_all[df_all['stock_name'] == stock]

            data.append({
                "name": stock,
                "data": tmp[['mintue', 'buy_ratio_norm']].values.tolist()
            })

        global_data = data

        time.sleep(10)


@app.route("/data")
def get_data():
    print(global_data)
    return jsonify(global_data)


if __name__ == "__main__":
    t = threading.Thread(target=data_updater)
    t.daemon = True
    t.start()

    app.run(debug=True)