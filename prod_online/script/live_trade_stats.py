import baostock as bs
import pandas as pd
import tqdm
from datetime import datetime
from dateutil.relativedelta import relativedelta
from sqlalchemy import create_engine
from pyecharts.charts import Line
from pyecharts import options as opts
import pandas as pd
import numpy as np
engine = create_engine("mysql+pymysql://root:chen@127.0.0.1:3306/gp")
import akshare as ak

sql='select * from gp.stock_analysis where need_to_analysis=1'
df_anal=pd.read_sql(sql=sql,con=engine)
ls=[]
for k,v in df_anal.iterrows():
    stock_code=v['stock_code']
    dft = ak.stock_zh_a_tick_tx_js(symbol=stock_code)
    dft['成交时间']=pd.to_datetime(dft['成交时间'])
    dft['hour']=dft['成交时间'].dt.hour
    dft['mintue']=dft['成交时间'].dt.minute

    if '成交时间' in dft.columns:
        dft['成交时间'] = pd.to_datetime(dft['成交时间'])
        dft.set_index('成交时间', inplace=True)
        dft.sort_index(inplace=True)

    dft['raw_type'] = dft['性质'].astype(str)
    dft['type_code'] = 0
    dft.loc[dft['raw_type'].str.contains('买', na=False), 'type_code'] = 1
    dft.loc[dft['raw_type'].str.contains('卖', na=False), 'type_code'] = -1

    if 'price_shift' not in dft.columns:
        dft['price_shift'] = dft['成交价格'].shift(-1) - dft['成交价格']
    neutral_mask = (dft['type_code'] == 0)
    dft.loc[neutral_mask & (dft['price_shift'] > 0), 'type_code'] = 1
    dft.loc[neutral_mask & (dft['price_shift'] < 0), 'type_code'] = -1
    dft=dft.loc[(dft['hour']==9)&(dft['mintue']>=0)&(dft['mintue']<=45)]

    pivot_df=pd.pivot_table(
        dft,
        index='mintue',
        columns='性质',
        values='成交量',
        aggfunc='sum',
        fill_value=0
    )


    pivot_df['总计']=pivot_df['中性盘']+pivot_df['买盘']+pivot_df['卖盘']


    # 3. 计算累计值 (核心步骤)
    # cumsum() 会沿着索引顺序（这里是分钟从小到大）进行累加
    pivot_df['累计_买盘'] = pivot_df['买盘'].cumsum()
    pivot_df['累计_卖盘'] = pivot_df['卖盘'].cumsum()
    pivot_df['累计_总计'] = pivot_df['总计'].cumsum()\

    pivot_df['max_buy_ratio']=v['max_buy_ratio']
    pivot_df['min_buy_ratio']=v['min_buy_ratio']
    pivot_df['max_zb']=v['max_zb']
    pivot_df['min_zb']=v['min_zb']
    pivot_df['buy_ratio']=pivot_df['累计_买盘']/pivot_df['累计_卖盘']
    pivot_df['buy_ratio_clip'] = pivot_df['buy_ratio'].clip(pivot_df['min_buy_ratio'], pivot_df['max_buy_ratio'])
    # pivot_df['buy_ratio_norm'] = (pivot_df['buy_ratio'] - pivot_df['min_buy_ratio']) / (pivot_df['max_buy_ratio'] - pivot_df['min_buy_ratio'])
    pivot_df['buy_ratio_norm'] = (
        (pivot_df['buy_ratio_clip'] - pivot_df['min_buy_ratio']) /
        (pivot_df['max_buy_ratio'] - pivot_df['min_buy_ratio'])
    )
    
    pivot_df['stock_code']=v['stock_code']
    pivot_df['stock_name']=v['stock_name']
    ls.append(pivot_df)

df_all=pd.concat(ls)


def plot_buy_ratio_line(df):
    # ========= 1. 数据清洗 =========
    df = df.copy()
    if 'mintue' not in df.columns:
        df = df.reset_index() 
    # 处理 inf
    df['buy_ratio_norm'] = np.where(
        np.isfinite(df['buy_ratio_norm']),
        df['buy_ratio_norm'],
        0
    )

    # minute 转 int（避免排序问题）
    df['mintue'] = df['mintue'].astype(int)

    # ========= 2. 获取所有股票 =========
    stock_list = df['stock_name'].unique().tolist()

    # ========= 3. 初始化图 =========
    line = Line()

    # X轴统一（取全集）
    x_axis = sorted(df['mintue'].astype(str).unique().tolist())
    line.add_xaxis(x_axis)
    # print(x_axis)
    # ========= 4. 循环每个股票 =========
    for stock in stock_list:
        tmp = df[df['stock_name'] == stock].sort_values('mintue')
        
        # 对齐分钟（防止缺失）
        y_axis=tmp['buy_ratio_norm'].round(3).tolist()
        
        y_axis=tmp['buy_ratio'].round(3).tolist()
        line.add_yaxis(
            series_name=stock,
            y_axis=y_axis,
            is_smooth=True,
            label_opts=opts.LabelOpts(is_show=False),
        )

    # ========= 5. 全局配置 =========
    line.set_global_opts(
        title_opts=opts.TitleOpts(title="分时买卖强度 (buy_ratio_norm)"),
        tooltip_opts=opts.TooltipOpts(trigger="axis"),
        xaxis_opts=opts.AxisOpts(name="分钟"),
        # yaxis_opts=opts.AxisOpts(name="买卖比(归一化)",min_=0,max_=0.02),
        legend_opts=opts.LegendOpts(pos_top="5%"),
        datazoom_opts=[opts.DataZoomOpts(), opts.DataZoomOpts(type_="inside")]
    )

    return line

chart = plot_buy_ratio_line(df_all)
chart.render(r"C:\Users\cyw\Desktop\jupyternotebook\git-python\GP\当日策列\static\multi_stock_monitor_fixed.html")
