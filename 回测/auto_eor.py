import os
import sys
import time
import logging
from collections import deque
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from sqlalchemy import create_engine

# 项目根目录（GP），便于导入 prod_online
_GP_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _GP_ROOT not in sys.path:
    sys.path.insert(0, _GP_ROOT)

try:
    from prod_online.config.feishu_utils import FeishuUtils
except ImportError:
    FeishuUtils = None  # type: ignore

# 飞书机器人（与 prod_online 脚本一致，可用环境变量覆盖）
FEISHU_APP_ID = os.environ.get("FEISHU_APP_ID", "cli_a9256b2aef7a5cd4")
FEISHU_APP_SECRET = os.environ.get(
    "FEISHU_APP_SECRET",
    "t22QBXS6MVqsXC41GoCDvbxin0tpXyL3",
)
FEISHU_CHAT_ID = os.environ.get(
    "FEISHU_CHAT_ID",
    # "oc_cd642a7fec1dcd847e91b2e1775809d2",
    "oc_3f5f526dfb0c056b8ca2a996c6baff0b"
)

# ======================================================
# 日志
# ======================================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

logger = logging.getLogger(__name__)

# ======================================================
# 实时监控类
# ======================================================

class TxRealtimeMonitor:

    def __init__(self):

        self.running = True

        # 股票状态
        self.state_map = {}

        # 策略参数
        self.signal_map = {}

        self._feishu_client = None
        if FeishuUtils is not None:
            try:
                self._feishu_client = FeishuUtils(
                    FEISHU_APP_ID,
                    FEISHU_APP_SECRET,
                )
            except Exception as exc:
                logger.warning("飞书客户端初始化失败，将仅写日志: %s", exc)
        else:
            logger.warning("未导入 FeishuUtils，飞书通知已关闭（请检查项目路径与 lark_oapi）。")

        # Session
        self.session = requests.Session()

        # 扩大连接池
        adapter = HTTPAdapter(
            pool_connections=100,
            pool_maxsize=100
        )

        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

    def _send_feishu_signal_text(self, text: str) -> None:
        """将信号文案发到飞书群/会话（与 feishu_utils.set_message_for_text 一致）。"""
        if self._feishu_client is None:
            return
        body = text.strip()
        if not body:
            return
        try:
            self._feishu_client.set_message_for_text(
                "chat_id",
                FEISHU_CHAT_ID,
                body,
            )
        except Exception:
            logger.exception("飞书发送失败")

    # ======================================================
    # 获取分页tick
    # ======================================================

    def get_page(self, symbol, page):

        url = "http://stock.gtimg.cn/data/index.php"

        params = {
            "appn": "detail",
            "action": "data",
            "c": symbol,
            "p": page,
        }

        r = self.session.get(
            url,
            params=params,
            timeout=5
        )

        text_data = r.text

        # 无数据
        if "[" not in text_data:
            return pd.DataFrame()

        try:

            temp_df = (
                pd.DataFrame(
                    eval(
                        text_data[text_data.find("["):]
                    )[1].split("|")
                )
                .iloc[:, 0]
                .str.split("/", expand=True)
            )

        except:
            return pd.DataFrame()

        if temp_df.empty:
            return pd.DataFrame()

        temp_df = temp_df.iloc[:, 1:].copy()

        temp_df.columns = [
            "成交时间",
            "成交价格",
            "价格变动",
            "成交量",
            "成交金额",
            "性质",
        ]

        property_map = {
            "S": "卖盘",
            "B": "买盘",
            "M": "中性盘",
        }

        temp_df["性质"] = temp_df["性质"].map(
            property_map
        )

        temp_df = temp_df.astype(
            {
                "成交时间": str,
                "成交价格": float,
                "价格变动": float,
                "成交量": int,
                "成交金额": int,
            }
        )

        return temp_df

    # ======================================================
    # 唯一key
    # ======================================================

    def make_key(self, row):

        return (
            f"{row['成交时间']}_"
            f"{row['成交价格']}_"
            f"{row['成交量']}"
        )

    # ======================================================
    # 初始化股票
    # ======================================================

    def init_symbol(self, symbol):

        if symbol not in self.state_map:

            self.state_map[symbol] = {

                # 当前页
                "page": 0,

                # 去重缓存
                "seen_keys": deque(maxlen=5000),

                # 盘中最高价
                "high": 0,

                # 盘中累计成交量(股)
                "volume": 0,

                # 是否已经触发
                "triggered": False,
            }

    # ======================================================
    # 扫描单只股票
    # ======================================================

    def scan_symbol(self, symbol,name,min_sell_price):

        try:

            self.init_symbol(symbol)

            signal = self.signal_map[symbol]

            need_vol = signal["need_vol"]

            breakout_price = signal["breakout_price"]

            state = self.state_map[symbol]

            current_page = state["page"]

            # ======================================================
            # 当前页
            # ======================================================

            df = self.get_page(
                symbol,
                current_page
            )

            if df.empty:
                return

            new_rows = []

            for _, row in df.iterrows():

                key = self.make_key(row)

                # 去重
                if key in state["seen_keys"]:
                    continue

                state["seen_keys"].append(key)

                new_rows.append(row)

            # ======================================================
            # 有新增数据
            # ======================================================

            if new_rows:

                new_df = pd.DataFrame(new_rows)

                # 当前新增tick最高价
                tick_high = new_df[
                    "成交价格"
                ].max()

                # 腾讯单位: 手
                # 转换成股
                tick_volume = (
                    new_df["成交量"].sum() * 100
                )

                # 更新盘中最高价
                state["high"] = max(
                    state["high"],
                    tick_high
                )

                # 更新盘中累计量
                state["volume"] += tick_volume

                # logger.info(
                #     f"{symbol} "
                #     f"最高={state['high']:.2f} "
                #     f"量={state['volume']:.0f}"
                # )

                # ======================================================
                # 信号触发
                # ======================================================

                if (
                    not state["triggered"]
                    and
                    state["high"] > breakout_price
                    and
                    state["volume"] > need_vol
                ):

                    state["triggered"] = True

                    signal_text = f"""
触发信号

股票:{symbol}
股票名称:{name}

突破价格:{breakout_price:.2f}
当前最高:{state['high']:.2f}

需求量: {need_vol:.0f}
当前量: {state['volume']:.0f}

止损价:{min_sell_price:.2f}
"""
                    logger.warning(signal_text)
                    self._send_feishu_signal_text(signal_text)

            # ======================================================
            # 检查下一页是否存在
            # ======================================================

            next_df = self.get_page(
                symbol,
                current_page + 1
            )

            if not next_df.empty:

                state["page"] += 1

        except Exception:

            logger.exception(symbol)

    # ======================================================
    # 启动
    # ======================================================

    def start(
        self,
        stock_df,
        sleep_sec=1,
        max_workers=50
    ):

        # 保存策略参数
        for _, row in stock_df.iterrows():

            self.signal_map[
                row["code"]
            ] = row.to_dict()

        logger.info(
            f"""
==================================================
启动监控

股票数量:
{len(stock_df)}

线程池:
{max_workers}

轮询间隔:
{sleep_sec}s
==================================================
"""
        )

        executor = ThreadPoolExecutor(
            max_workers=max_workers
        )

        while self.running:

            try:

                symbols = list(
                    self.signal_map.keys()
                )

                executor.map(
                    self.scan_symbol,
                    symbols,
                    [self.signal_map[symbol]['name'] for symbol in symbols],
                    [self.signal_map[symbol]['min_sell_price'] for symbol in symbols]
                )

            except Exception:

                logger.exception("主循环异常")

            time.sleep(sleep_sec)

# ======================================================
# 加载数据
# ======================================================

def load_data(engine):

    sql = """
    SELECT *
    FROM stock
    WHERE volume IS NOT NULL
    ORDER BY code, date
    """

    df = pd.read_sql(sql, engine)

    df['date'] = pd.to_datetime(df['date'])

    df = df.sort_values(
        ['code', 'date']
    ).reset_index(drop=True)

    return df

# ======================================================
# 构建特征
# ======================================================

def build_features(df):

    df['ret'] = df.groupby(
        'code'
    )['close'].pct_change()

    df['ret_oc'] = (
        df['close'] / df['open'] - 1
    )

    df['vol_ma5'] = df.groupby(
        'code'
    )['volume'].transform(
        lambda x: x.rolling(5).mean()
    )

    df['vol_ma10'] = df.groupby(
        'code'
    )['volume'].transform(
        lambda x: x.rolling(10).mean()
    )

    df['price_ma20'] = df.groupby(
        'code'
    )['close'].transform(
        lambda x: x.rolling(20).mean()
    )

    df['range'] = (
        (df['high'] - df['low'])
        / df['close']
    )

    df['range_ma3'] = df.groupby(
        'code'
    )['range'].transform(
        lambda x: x.rolling(3).mean()
    )

    return df

# ======================================================
# 生成信号
# ======================================================

def generate_signals(df):

    # ======================
    # 试盘日
    # ======================

    df['up_move'] = (
        (df['high'] - df['open'])
        / df['open']
    )

    df['pullback'] = (
        (df['high'] - df['close'])
        /
        (df['high'] - df['open'] + 1e-9)
    )

    df['is_spike_day'] = (
        (df['up_move'] > 0.03)
        &
        (df['pullback'] > 0.4)
        &
        (df['close'] > df['open'] * 0.98)
    )

    df['spike_low'] = df['low'].where(
        df['is_spike_day']
    )

    df['spike_high'] = df['high'].where(
        df['is_spike_day']
    )

    df['spike_low'] = df.groupby(
        'code'
    )['spike_low'].ffill()

    df['spike_high'] = df.groupby(
        'code'
    )['spike_high'].ffill()

    # ======================
    # 缩量整理
    # ======================

    df['vol_shrink'] = (
        df['volume']
        <
        df['vol_ma5'] * 0.8
    )

    df['no_break'] = (
        df['low']
        >
        df['spike_low']
    )

    df['range_shrink'] = (
        df['range']
        <
        df['range_ma3']
    )

    df['is_consolidation'] = (
        df['vol_shrink']
        &
        df['no_break']
        &
        df['range_shrink']
    )

    # ======================
    # 趋势
    # ======================

    df['trend_ok'] = (
        df['close']
        >
        df['price_ma20']
    )

    # ======================
    # 买点
    # ======================

    df['buy_signal'] = (
        df['is_consolidation'].shift(1)
        &
        df['trend_ok']
    )

    # ======================
    # 需求量
    # ======================

    df['need_vol'] = (
        df.groupby('code')['volume']
        .shift(1) * 1.1
    )

    # ======================
    # 突破价格
    # ======================

    df['breakout_price'] = (
        df.groupby('code')['high']
        .shift(1)
    )

    df['min_sell_price'] = df['spike_low'] * 0.98

    return df

# ======================================================
# 获取候选股
# ======================================================

def get_future_single_stock():

    engine = create_engine(
        "mysql+pymysql://root:chen@127.0.0.1:3306/gp"
    )

    logger.info("=" * 30 + " 加载数据 " + "=" * 30)

    df = load_data(engine)

    logger.info("=" * 30 + " 计算因子 " + "=" * 30)

    df = build_features(df)

    logger.info("=" * 30 + " 计算信号 " + "=" * 30)

    df = generate_signals(df)

    start_date = (
        df['date'].max()
        -
        pd.Timedelta(days=15)
    )

    # 最近15天
    df_buy = df.loc[
        df['date'] >= start_date
    ]

    # 试盘日
    df_buy = df_buy.loc[
        df_buy['is_spike_day'] == True
    ]

    # 最新交易日
    df_buy = df_buy.loc[
        df_buy['date']
        ==
        df_buy['date'].max()
    ]

    out_cols = [
        'code',
        'name',
        'date',
        'close',
        'need_vol',
        'breakout_price',
        'min_sell_price',
    ]
    if 'name' not in df_buy.columns:
        df_buy = df_buy.copy()
        df_buy['name'] = df_buy['code'].astype(str)

    df_buy = df_buy[out_cols]

    return df_buy

# ======================================================
# 主程序
# ======================================================

if __name__ == "__main__":

    dt = get_future_single_stock()

    # 排除创业板 科创板
    is_main_board = ~dt['code'].str[:5].isin(
        ['sh688', 'sz301', 'sz300']
    )

    dt = dt.loc[is_main_board]

    logger.info(
        f"候选股票数量: {len(dt)}"
    )

    # print(dt.head())

    # 启动实时监控
    monitor = TxRealtimeMonitor()

    monitor.start(
        dt,

        # 全市场建议别低于10秒
        sleep_sec=10,

        # 线程池
        max_workers=50
    )