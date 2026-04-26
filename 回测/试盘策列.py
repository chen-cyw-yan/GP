import time
import akshare as ak
import pandas as pd
import numpy as np
import tqdm
# import pyecharts.options as opts
# from pyecharts.charts import Line
import tqdm
import pandas as pd
from sqlalchemy import create_engine
from sklearn.linear_model import LinearRegression
import matplotlib.pyplot as plt
from sqlalchemy import create_engine
import BacktestEngine_1_0_2 as bk
import pymysql
from pyecharts.charts import Kline, Scatter, Grid
from pyecharts import options as opts
import os
engine = create_engine("mysql+pymysql://root:chen@127.0.0.1:3306/gp")
conn = pymysql.connect(
            host='127.0.0.1',
            user='root',
            password='chen',
            database='gp',
            # use_unicode=args.encoding,
        )
cursor = conn.cursor()
def toSql(sql: str, rows: list):
    """
        连接数据库
    """
    # print(sql,rows)
    try:

        cursor.executemany(sql, rows)
        conn.commit()
    except Exception as e:
        raise ConnectionError("[ERROR] 连接数据库失败，具体原因是：" + str(e))







def plot_html(code,df,date):
    # === pyecharts 交互K线图（导出HTML）===

    plot_df = df.loc[df['code'] == code].copy()
    plot_df['date'] = pd.to_datetime(plot_df['date'])
    plot_df = plot_df.sort_values('date').reset_index(drop=True)

    if plot_df.empty:
        print(f'未找到股票 {code} 的数据')
    else:
        x_data = plot_df['date'].dt.strftime('%Y-%m-%d').tolist()
        k_data = plot_df[['open', 'close', 'low', 'high']].round(3).values.tolist()

        kline = (
            Kline(init_opts=opts.InitOpts(width='100%', height='900px', bg_color='#ffffff'))
            .add_xaxis(xaxis_data=x_data)
            .add_yaxis(
                series_name=code,
                y_axis=k_data,
                itemstyle_opts=opts.ItemStyleOpts(color='#ef232a', color0='#14b143', border_color='#ef232a', border_color0='#14b143')
            )
            .set_global_opts(
                title_opts=opts.TitleOpts(title=f'{code} K线图（信号标记）'),
                datazoom_opts=[opts.DataZoomOpts(type_='inside'), opts.DataZoomOpts(type_='slider')],
                xaxis_opts=opts.AxisOpts(type_='category'),
                yaxis_opts=opts.AxisOpts(is_scale=True),
                tooltip_opts=opts.TooltipOpts(trigger='axis', axis_pointer_type='cross'),
                legend_opts=opts.LegendOpts(pos_top='2%')
            )
        )

        # 标记点（使用 scatter 叠加）
        spike_idx = plot_df.index[plot_df['is_spike_day'].fillna(False)]
        vol_idx = plot_df.index[plot_df['vol_expand'].fillna(False)]
        breakout_idx = plot_df.index[plot_df['breakout'].fillna(False)]
        buy_idx = plot_df.index[plot_df['buy_signal'].fillna(False)]

        if len(spike_idx) > 0:
            spike_scatter = (
                Scatter()
                .add_xaxis([x_data[i] for i in spike_idx])
                .add_yaxis(
                    '试盘日',
                    [float(plot_df.loc[i, 'high'] * 1.01) for i in spike_idx],
                    symbol='triangle',
                    symbol_size=11,
                    itemstyle_opts=opts.ItemStyleOpts(color='orange'),
                    label_opts=opts.LabelOpts(is_show=False)
                )
            )
            kline.overlap(spike_scatter)

        if len(vol_idx) > 0:
            vol_scatter = (
                Scatter()
                .add_xaxis([x_data[i] for i in vol_idx])
                .add_yaxis(
                    '放量扩增',
                    [float(plot_df.loc[i, 'high'] * 1.03) for i in vol_idx],
                    symbol='rect',
                    symbol_size=9,
                    itemstyle_opts=opts.ItemStyleOpts(color='blue'),
                    label_opts=opts.LabelOpts(is_show=False)
                )
            )
            kline.overlap(vol_scatter)

        if len(breakout_idx) > 0:
            breakout_scatter = (
                Scatter()
                .add_xaxis([x_data[i] for i in breakout_idx])
                .add_yaxis(
                    '突破信号',
                    [float(plot_df.loc[i, 'high'] * 1.05) for i in breakout_idx],
                    symbol='pin',
                    symbol_size=13,
                    itemstyle_opts=opts.ItemStyleOpts(color='#00B894'),
                    label_opts=opts.LabelOpts(is_show=False)
                )
            )
            kline.overlap(breakout_scatter)

        if len(buy_idx) > 0:
            buy_scatter = (
                Scatter()
                .add_xaxis([x_data[i] for i in buy_idx])
                .add_yaxis(
                    '买入信号',
                    [float(plot_df.loc[i, 'low'] * 0.99) for i in buy_idx],
                    symbol='diamond',
                    symbol_size=12,
                    itemstyle_opts=opts.ItemStyleOpts(color='purple'),
                    label_opts=opts.LabelOpts(is_show=False)
                )
            )
            kline.overlap(buy_scatter)

        grid = Grid(init_opts=opts.InitOpts(width='100%', height='900px', page_title=f'{code} K线图'))
        grid.add(kline, grid_opts=opts.GridOpts(pos_left='4%', pos_right='2%', pos_top='8%', pos_bottom='8%'))

        out_dir = r'C:\Users\cyw\Desktop\jupyternotebook\git-python\GP\回测\html'
        os.makedirs(out_dir, exist_ok=True)
        out_file = os.path.join(out_dir, f'{code}_kline_signals_pyecharts.html')
        grid.render(out_file)
        print(f'HTML已生成: {out_file}')



def clear_folder(folder_path):
    # 检查文件夹是否存在
    if not os.path.exists(folder_path):
        print(f"文件夹不存在: {folder_path}")
        return

    # 遍历文件夹下的所有内容
    for item in os.listdir(folder_path):
        item_path = os.path.join(folder_path, item)
        
        try:
            if os.path.isfile(item_path) or os.path.islink(item_path):
                os.unlink(item_path)  # 删除文件或链接
            elif os.path.isdir(item_path):
                shutil.rmtree(item_path)  # 删除子文件夹及其内容
        except Exception as e:
            print(f'删除失败 {item_path}. 原因: {e}')




def main():
    sql = """
    SELECT *
    FROM stock
    WHERE volume IS NOT NULL
    ORDER BY code, date
    """
    df = pd.read_sql(sql, engine)



    # ==========================
    # 2️⃣ 预处理
    # ==========================
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values(['code', 'date']).reset_index(drop=True)

    # ==========================
    # 3️⃣ 基础指标
    # ==========================
    df['ret'] = df.groupby('code')['close'].pct_change()

    df['vol_ma5'] = df.groupby('code')['volume'].transform(lambda x: x.rolling(5).mean())
    df['vol_ma10'] = df.groupby('code')['volume'].transform(lambda x: x.rolling(10).mean())

    df['price_ma20'] = df.groupby('code')['close'].transform(lambda x: x.rolling(20).mean())

    # 波动收敛
    df['range'] = (df['high'] - df['low']) / df['close']
    df['range_ma3'] = df.groupby('code')['range'].transform(lambda x: x.rolling(3).mean())

    # ==========================
    # 4️⃣ 冲高回落（试盘日）
    # ==========================
    df['up_move'] = (df['high'] - df['open']) / df['open']
    df['pullback'] = (df['high'] - df['close']) / (df['high'] - df['open'] + 1e-9)

    df['is_spike_day'] = (
        (df['up_move'] > 0.03) &                 # 冲高3%
        (df['pullback'] > 0.4) &                 # 有回落
        (df['close'] > df['open'] * 0.98)        # 不太弱
    )

    # 记录试盘关键位
    df['spike_low'] = df['low'].where(df['is_spike_day'])
    df['spike_high'] = df['high'].where(df['is_spike_day'])

    # 向后填充（每个股票内部）
    df['spike_low'] = df.groupby('code')['spike_low'].ffill()
    df['spike_high'] = df.groupby('code')['spike_high'].ffill()

    # ==========================
    # 5️⃣ 缩量整理
    # ==========================
    df['vol_shrink'] = df['volume'] < df['vol_ma5'] * 0.8

    # 不破试盘低点（核心）
    df['no_break'] = df['low'] > df['spike_low']

    # 波动收敛
    df['range_shrink'] = df['range'] < df['range_ma3']

    df['is_consolidation'] = (
        df['vol_shrink'] &
        df['no_break'] &
        df['range_shrink']
    )

    # ==========================
    # 6️⃣ 买点触发（放量 + 突破）
    # ==========================
    df['vol_expand'] = (
        (df['volume'] > df['volume'].shift(1) * 1.1) &
        (df['volume'] < df['volume'].shift(1) * 1.5)
    )

    # 突破前一日高点
    df['breakout'] = df['close'] > df['high'].shift(1)

    # 趋势过滤（避免震荡股）
    df['trend_ok'] = df['close'] > df['price_ma20']

    # 最终信号
    df['buy_signal'] = (
        df['is_consolidation'].shift(1) &
        df['vol_expand'] &
        df['breakout'] &
        df['trend_ok']
    )
    df['min_sell_price'] = df['spike_low']*0.98
    # 使用示例
    folder_dir = 'prod_online\imges\deep_img'
    clear_folder(folder_dir)