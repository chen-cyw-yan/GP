import pandas as pd                      # 数据处理库
import numpy as np                       # 数值计算库
from sqlalchemy import create_engine     # 数据库连接

class BacktestEngine:

    def __init__(self,
                 signal_df,              # 触发信号 DataFrame（必须包含 date, code）
                 buy_mode='open',        # 买入方式：open=次日开盘，close=次日收盘
                 buy_priority='turnover',# 买入优先级：turnover/amount/market_cap
                 sell_config=None,       # 卖出规则（字典）
                 init_cash=1000000,      # 初始资金
                 max_positions=5):       # 最大持仓数量

        self.signal_df = signal_df       # 保存信号数据
        self.buy_mode = buy_mode         # 保存买入方式
        self.buy_priority = buy_priority # 保存优先级规则
        self.sell_config = sell_config or {}  # 卖出规则（为空则给默认空字典）

        self.cash = init_cash            # 当前现金
        self.init_cash = init_cash       # 初始资金（用于统计收益）
        self.max_positions = max_positions  # 最大持仓数量

        self.positions = {}              # 当前持仓（字典：code -> 持仓信息）
        self.trade_log = []              # 交易记录

        # 创建数据库连接
        self.engine = create_engine(
            "mysql+pymysql://用户名:密码@localhost:3306/gp?charset=utf8mb4"
        )

        self.load_data()                 # 初始化时加载行情数据

    # ==========================
    # 加载数据库日线数据
    # ==========================
    def load_data(self):
        sql = """
        SELECT date, code, open, high, low, close,
               volume, amount, turnover, outstanding_share
        FROM stock
        ORDER BY code, date
        """
        self.df = pd.read_sql(sql, self.engine)   # 读取数据
        self.df = self.df.sort_values(['code', 'date'])  # 排序（非常重要）

    # ==========================
    # 获取某只股票某天数据
    # ==========================
    def get_row(self, code, date):
        df = self.df[(self.df['code'] == code) & (self.df['date'] == date)]  # 筛选
        if df.empty:                   # 如果没有数据（停牌等）
            return None
        return df.iloc[0]              # 返回这一行

    # ==========================
    # 获取下一交易日数据
    # ==========================
    def get_next_day(self, code, date):
        df = self.df[self.df['code'] == code]  # 取该股票全部数据

        idx = df[df['date'] == date].index     # 找当前日期索引
        if len(idx) == 0:                      # 没找到
            return None

        idx = idx[0]                           # 取位置
        if idx + 1 >= len(df):                 # 如果已经最后一天
            return None

        return df.iloc[idx + 1]                # 返回下一天

    # ==========================
    # 买入逻辑
    # ==========================
    def try_buy(self, date):

        # 找当天触发信号的股票
        today_signal = self.signal_df[self.signal_df['date'] == date]

        if today_signal.empty:                 # 如果没有信号
            return

        # 计算剩余可买仓位数量
        available_slots = self.max_positions - len(self.positions)

        if available_slots <= 0:               # 如果仓位满了
            return

        candidates = []                       # 候选股票列表

        # 遍历信号股票
        for _, row in today_signal.iterrows():
            code = row['code']                # 股票代码

            next_day = self.get_next_day(code, date)  # 获取次日数据

            if next_day is None:              # 没数据跳过
                continue

            candidates.append(next_day)       # 加入候选池

        if not candidates:                    # 如果没有候选
            return

        candidates_df = pd.DataFrame(candidates)  # 转DataFrame

        # ===== 根据优先级排序 =====
        if self.buy_priority == 'turnover':
            candidates_df = candidates_df.sort_values('turnover', ascending=False)

        elif self.buy_priority == 'amount':
            candidates_df = candidates_df.sort_values('amount', ascending=False)

        elif self.buy_priority == 'market_cap':
            # 计算流通市值
            candidates_df['market_cap'] = candidates_df['close'] * candidates_df['outstanding_share']
            candidates_df = candidates_df.sort_values('market_cap', ascending=False)

        # 只保留最多可买数量
        candidates_df = candidates_df.head(available_slots)

        # 遍历买入
        for _, row in candidates_df.iterrows():

            code = row['code']               # 股票代码

            # 买入价格（根据模式）
            price = row['open'] if self.buy_mode == 'open' else row['close']

            # 每只股票分配资金（平均分）
            cash_per_stock = self.cash / available_slots if available_slots > 0 else 0

            shares = cash_per_stock // price  # 可买股数（向下取整）

            if shares <= 0:                  # 买不起
                continue

            cost = shares * price           # 实际花费

            self.cash -= cost               # 扣除现金

            # 记录持仓
            self.positions[code] = {
                'buy_price': price,         # 买入价格
                'shares': shares,           # 股数
                'buy_date': row['date'],    # 买入日期
                'max_profit': 0             # 历史最大收益
            }

            # 记录交易
            self.trade_log.append({
                'date': row['date'],
                'code': code,
                'action': 'buy',
                'price': price,
                'shares': shares
            })

    # ==========================
    # 卖出逻辑
    # ==========================
    def try_sell(self, date):

        remove_list = []                    # 待删除持仓

        for code, pos in self.positions.items():

            row = self.get_row(code, date) # 当前行情
            if row is None:
                continue

            price = row['close']           # 用收盘价卖出

            # 当前收益率
            ret = (price - pos['buy_price']) / pos['buy_price']

            # 更新历史最大收益
            pos['max_profit'] = max(pos['max_profit'], ret)

            sell = False                  # 是否卖出

            # ===== 持仓天数止盈 =====
            if 'max_hold_days' in self.sell_config:
                days = (date - pos['buy_date']).days
                if days >= self.sell_config['max_hold_days']:
                    sell = True

            # ===== 收益止盈 =====
            if 'take_profit' in self.sell_config:
                if ret >= self.sell_config['take_profit']:
                    sell = True

            # ===== 止损 =====
            if 'stop_loss' in self.sell_config:
                if ret <= -self.sell_config['stop_loss']:
                    sell = True

            # ===== 回撤止损 =====
            if 'drawdown' in self.sell_config:
                if pos['max_profit'] - ret >= self.sell_config['drawdown']:
                    sell = True

            # ===== 执行卖出 =====
            if sell:
                self.cash += pos['shares'] * price  # 回收资金

                self.trade_log.append({
                    'date': date,
                    'code': code,
                    'action': 'sell',
                    'price': price,
                    'shares': pos['shares'],
                    'return': ret
                })

                remove_list.append(code)

        # 删除已卖出的持仓
        for code in remove_list:
            del self.positions[code]

    # ==========================
    # 主回测流程
    # ==========================
    def run(self):

        dates = sorted(self.df['date'].unique())  # 所有交易日

        equity_curve = []                         # 净值曲线

        for date in dates:

            self.try_sell(date)                   # 先卖（重要）
            self.try_buy(date)                    # 再买

            total_value = self.cash               # 现金

            # 加上持仓市值
            for code, pos in self.positions.items():
                row = self.get_row(code, date)
                if row is not None:
                    total_value += pos['shares'] * row['close']

            # 记录净值
            equity_curve.append({
                'date': date,
                'equity': total_value
            })

        self.equity_df = pd.DataFrame(equity_curve)  # 转DataFrame

        return self.equity_df                         # 返回结果
if __name__ == '__main__':
    signal_df = pd.DataFrame({
        'date': [...],
        'code': [...]
    })

    engine = BacktestEngine(
        signal_df=signal_df,
        buy_mode='open',
        buy_priority='turnover',
        sell_config={
            'max_hold_days': 10,
            'take_profit': 0.1,
            'stop_loss': 0.05,
            'drawdown': 0.05
        },
        init_cash=1000000,
        max_positions=5
    )
    result = engine.run()
    print(result)
    pass