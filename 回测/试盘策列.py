import os
import shutil
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import tqdm
from pyecharts.charts import Kline, Scatter, Grid, Line
from pyecharts import options as opts
import logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)
# ==========================
# 1️⃣ 读取数据
# ==========================
def load_data(engine):
    sql = """
    SELECT *
    FROM stock
    WHERE volume IS NOT NULL
    ORDER BY code, date
    """
    df = pd.read_sql(sql, engine)
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values(['code', 'date']).reset_index(drop=True)
    return df


# ==========================
# 2️⃣ 构建特征
# ==========================
def build_features(df):

    df['ret'] = df.groupby('code')['close'].pct_change()
    df['ret_oc'] = df['close'] / df['open'] - 1

    df['vol_ma5'] = df.groupby('code')['volume'].transform(lambda x: x.rolling(5).mean())
    df['vol_ma10'] = df.groupby('code')['volume'].transform(lambda x: x.rolling(10).mean())

    df['price_ma20'] = df.groupby('code')['close'].transform(lambda x: x.rolling(20).mean())

    df['range'] = (df['high'] - df['low']) / df['close']
    df['range_ma3'] = df.groupby('code')['range'].transform(lambda x: x.rolling(3).mean())

    # === 均线 ===
    df['ma5'] = df.groupby('code')['close'].transform(lambda x: x.rolling(5).mean())
    df['ma10'] = df.groupby('code')['close'].transform(lambda x: x.rolling(10).mean())
    df['ma20'] = df.groupby('code')['close'].transform(lambda x: x.rolling(20).mean())

    return df


# ==========================
# 3️⃣ 信号生成
# ==========================
def generate_signals(df):

    # === 试盘日 ===
    df['up_move'] = (df['high'] - df['open']) / df['open']
    df['pullback'] = (df['high'] - df['close']) / (df['high'] - df['open'] + 1e-9)

    df['is_spike_day'] = (
        (df['up_move'] > 0.03) &
        (df['pullback'] > 0.4) &
        (df['close'] > df['open'] * 0.98)
    )

    df['spike_low'] = df['low'].where(df['is_spike_day'])
    df['spike_high'] = df['high'].where(df['is_spike_day'])

    df['spike_low'] = df.groupby('code')['spike_low'].ffill()
    df['spike_high'] = df.groupby('code')['spike_high'].ffill()

    # === 缩量整理 ===
    df['vol_shrink'] = df['volume'] < df['vol_ma5'] * 0.8
    df['no_break'] = df['low'] > df['spike_low']
    df['range_shrink'] = df['range'] < df['range_ma3']

    df['is_consolidation'] = (
        df['vol_shrink'] &
        df['no_break'] &
        df['range_shrink']
    )

    # === 买点 ===
    df['vol_expand'] = (
        (df['volume'] > df['volume'].shift(1) * 1.1) &
        (df['volume'] < df['volume'].shift(1) * 1.5)
    )

    df['breakout'] = df['close'] > df['high'].shift(1)
    df['trend_ok'] = df['close'] > df['price_ma20']

    df['buy_signal'] = (
        df['is_consolidation'].shift(1) &
        df['vol_expand'] &
        df['breakout'] &
        df['trend_ok']
    )

    df['min_sell_price'] = df['spike_low'] * 0.98

    # === 量价异常 ===
    df['vol_max5'] = df.groupby('code')['volume'].transform(lambda x: x.rolling(5).max())
    df['vol_ratio_v2'] = df['volume'] / df['vol_max5']

    df['signal_low_vol_drop'] = (df['ret_oc'] < -0.04) & (df['vol_ratio_v2'] < 0.8)
    df['signal_low_vol_rise'] = (df['ret_oc'] > 0.04) & (df['vol_ratio_v2'] < 0.8)
    df['signal_high_vol_flat'] = (df['ret_oc'].abs() < 0.02) & (df['vol_ratio_v2'] > 1.5)

    df['vol_pric_err'] = '无信号'
    df.loc[df['signal_low_vol_drop'], 'vol_pric_err'] = '缩量下跌'
    df.loc[df['signal_low_vol_rise'], 'vol_pric_err'] = '缩量大涨'
    df.loc[df['signal_high_vol_flat'], 'vol_pric_err'] = '放量横盘'

    return df


# ==========================
# 4️⃣ 清空目录
# ==========================
def clear_folder(folder_path):
    if not os.path.exists(folder_path):
        return

    for item in os.listdir(folder_path):
        item_path = os.path.join(folder_path, item)
        if os.path.isfile(item_path):
            os.unlink(item_path)
        elif os.path.isdir(item_path):
            shutil.rmtree(item_path)


# ==========================
# 5️⃣ K线图
# ==========================

def plot_html(code, df, saving_dir, date='2026-04-24'):
    plot_df = df.loc[df['code'] == code].copy()
    plot_df['date'] = pd.to_datetime(plot_df['date'])
    plot_df = plot_df.sort_values('date').reset_index(drop=True)
    name=plot_df.loc[0,'name']

    if plot_df.empty:
        print(f'未找到股票 {code} 的数据')
        return

    x_data = plot_df['date'].dt.strftime('%Y-%m-%d').tolist()
    k_data = plot_df[['open', 'close', 'low', 'high']].round(3).values.tolist()

    # === K线 ===
    kline = (
        Kline()
        .add_xaxis(x_data)
        .add_yaxis(
            series_name=code,
            y_axis=k_data,
            itemstyle_opts=opts.ItemStyleOpts(
                color='#ef232a', color0='#14b143',
                border_color='#ef232a', border_color0='#14b143'
            )
        )
    )

    # === 均线 ===
    line = Line().add_xaxis(x_data)

    for ma, color in zip(['ma5', 'ma10', 'ma20'], ['#f1c40f', '#3498db', '#9b59b6']):
        if ma in plot_df.columns:
            line.add_yaxis(
                ma.upper(),
                plot_df[ma].tolist(),
                is_smooth=True,
                linestyle_opts=opts.LineStyleOpts(width=1.5, color=color),
                label_opts=opts.LabelOpts(is_show=False)
            )

    kline.overlap(line)

    # === 原有信号 ===
    spike_idx = plot_df.index[plot_df['is_spike_day'].fillna(False)]
    vol_idx = plot_df.index[plot_df['vol_expand'].fillna(False)]
    breakout_idx = plot_df.index[plot_df['breakout'].fillna(False)]
    buy_idx = plot_df.index[plot_df['buy_signal'].fillna(False)]

    def add_scatter(name, idx, y_func, color, symbol, size):
        if len(idx) == 0:
            return None
        return (
            Scatter()
            .add_xaxis([x_data[i] for i in idx])
            .add_yaxis(
                name,
                [y_func(i) for i in idx],
                symbol=symbol,
                symbol_size=size,
                itemstyle_opts=opts.ItemStyleOpts(color=color),
                label_opts=opts.LabelOpts(is_show=False)
            )
        )

    for scatter in [
        add_scatter('试盘日', spike_idx, lambda i: plot_df.loc[i, 'high'] * 1.01, 'orange', 'triangle', 11),
        add_scatter('放量扩增', vol_idx, lambda i: plot_df.loc[i, 'high'] * 1.03, 'blue', 'rect', 9),
        add_scatter('突破信号', breakout_idx, lambda i: plot_df.loc[i, 'high'] * 1.05, '#00B894', 'pin', 13),
        add_scatter('买入信号', buy_idx, lambda i: plot_df.loc[i, 'low'] * 0.99, 'purple', 'diamond', 12),
    ]:
        if scatter:
            kline.overlap(scatter)

    # === 新增：量价错误信号 ===
    err_col = 'vol_pric_err'
    if err_col in plot_df.columns:

        drop_idx = plot_df.index[plot_df[err_col] == '缩量下跌']
        rise_idx = plot_df.index[plot_df[err_col] == '缩量大涨']
        flat_idx = plot_df.index[plot_df[err_col] == '放量横盘']

        err_scatters = [
            add_scatter('缩量下跌', drop_idx, lambda i: plot_df.loc[i, 'low'] * 0.97, 'green', 'triangle', 10),
            add_scatter('缩量大涨', rise_idx, lambda i: plot_df.loc[i, 'high'] * 1.07, 'red', 'triangle', 10),
            add_scatter('放量横盘', flat_idx, lambda i: plot_df.loc[i, 'close'], 'black', 'circle', 8),
        ]

        for scatter in err_scatters:
            if scatter:
                kline.overlap(scatter)

    # === 全局配置 ===
    kline.set_global_opts(
        title_opts=opts.TitleOpts(title=f'{name}({code}) K线图（含均线+信号）',pos_left='left'),
        datazoom_opts=[opts.DataZoomOpts(type_='inside'), opts.DataZoomOpts(type_='slider')],
        xaxis_opts=opts.AxisOpts(type_='category'),
        yaxis_opts=opts.AxisOpts(is_scale=True),
        tooltip_opts=opts.TooltipOpts(trigger='axis'),
        legend_opts=opts.LegendOpts(pos_top='2%')
    )

    grid = Grid(init_opts=opts.InitOpts(width='100%', height='900px'))
    grid.add(kline, grid_opts=opts.GridOpts(pos_left='4%', pos_right='2%', pos_top='8%', pos_bottom='8%'))

    os.makedirs(saving_dir, exist_ok=True)
    out_file = os.path.join(saving_dir, f'{name}_{code}_kline_signals.html')
    grid.render(out_file)

    # print(f'HTML已生成: {out_file}')

# ==========================
# 6️⃣ 主流程
# ==========================
def main(add_codes=[],turnover_min=5.0,date='2026-04-29'):

    engine = create_engine("mysql+pymysql://root:chen@127.0.0.1:3306/gp")
    logger.info("="*30 + " 加载数据 " + "="*30)
    df = load_data(engine)
    logger.info("="*30 + " 计算因子 " + "="*30)
    df = build_features(df)
    logger.info("="*30 + " 计算信号 " + "="*30)
    df = generate_signals(df)

    
    logger.info("="*30 + " 过滤科创板 " + "="*30)
    is_main_board = ~df['code'].str[:5].isin(['sh688', 'sz301', 'sz300'])
    df=df.loc[is_main_board]

    code_ls=[
            "sh600338",
            "sh603931",
            "sz300582",
            "sz300283",
            "sh603066",
            "sh603338",
            "sz300290"
            ]


    date = pd.to_datetime(date)
    # turnover_min = 5.0
    day_df = df.loc[
       (df['date'] == date) &
       (df['turnover'] >= turnover_min) &
        ((df['buy_signal'])  |
          (df['code'].isin(code_ls)))
    ]

    logger.info("="*30 + " 清除文件 " + "="*30)
    output_dir = r'C:\Users\cyw\Desktop\jupyternotebook\git-python\GP\回测\html'
    clear_folder(output_dir)

    logger.info("="*30 + " 生成K线图 " + "="*30)
    for _, row in tqdm.tqdm(day_df.iterrows()):
        plot_html(row['code'], df, output_dir)

    print(f"完成，共生成 {len(day_df)} 个图")


# ==========================
# 启动
# ==========================
if __name__ == "__main__":
    main()