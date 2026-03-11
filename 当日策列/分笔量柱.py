import akshare as ak
import pandas as pd
from pyecharts.charts import Bar
from pyecharts import options as opts


def draw_tick_volume(code="000001"):
    # 获取分笔
    df = ak.stock_zh_a_tick_tx_js(symbol=code)

    df = df.sort_values("成交时间").reset_index(drop=True)

    # 三类量
    buy_vol = []
    sell_vol = []
    neutral_vol = []

    for _, row in df.iterrows():
        if row["性质"] == "买盘":
            buy_vol.append(row["成交量"])
            sell_vol.append(0)
            neutral_vol.append(0)

        elif row["性质"] == "卖盘":
            buy_vol.append(0)
            sell_vol.append(row["成交量"])
            neutral_vol.append(0)

        else:
            buy_vol.append(0)
            sell_vol.append(0)
            neutral_vol.append(row["成交量"])

    times = df["成交时间"].tolist()

    bar = (
        Bar()
        .add_xaxis(times)

        .add_yaxis(
            "买盘",
            buy_vol,
            itemstyle_opts=opts.ItemStyleOpts(color="#ff4d4f"),
            stack="stack",
            label_opts=opts.LabelOpts(is_show=False),
        )

        .add_yaxis(
            "卖盘",
            sell_vol,
            itemstyle_opts=opts.ItemStyleOpts(color="#00a65a"),
            stack="stack",
            label_opts=opts.LabelOpts(is_show=False),
        )

        .add_yaxis(
            "中性盘",
            neutral_vol,
            itemstyle_opts=opts.ItemStyleOpts(color="#808080"),
            stack="stack",
            label_opts=opts.LabelOpts(is_show=False),
        )

        .set_global_opts(
            title_opts=opts.TitleOpts(title=f"{code} 分笔成交量"),
            xaxis_opts=opts.AxisOpts(
                type_="category",
                axislabel_opts=opts.LabelOpts(rotate=45),
            ),
            yaxis_opts=opts.AxisOpts(name="成交量"),
            datazoom_opts=[opts.DataZoomOpts()],
            tooltip_opts=opts.TooltipOpts(trigger="axis"),
        )
    )

    bar.render(f"E:/stock/GP/当日策列/static/{code}_tick_volume.html")
    print("生成完成")


if __name__ == "__main__":
    draw_tick_volume("sz002335")