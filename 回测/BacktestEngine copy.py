import pandas as pd                      # 数据处理库
import numpy as np                       # 数值计算库
from sqlalchemy import create_engine     # 数据库连接
# import 

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
            "mysql+pymysql://root:chen@127.0.0.1:3306/gp"
        )

        self.load_data()                 # 初始化时加载行情数据
        self.df['date'] = pd.to_datetime(self.df['date'])
        self.signal_df['date'] = pd.to_datetime(self.signal_df['date'])
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

        df = self.df[self.df['code'] == code]

        # 找未来的第一天
        df = df[df['date'] > date]

        if df.empty:
            return None

        return df.iloc[0]

    # ==========================
    # 买入逻辑
    # ==========================
    def try_buy(self, date):

        today_signal = self.signal_df[self.signal_df['date'] == date]

        print(f"\n📅 {date} | 信号数: {len(today_signal)} | 当前持仓: {len(self.positions)} | 现金: {self.cash:.2f}")

        if today_signal.empty:
            print("❌ 无信号")
            return

        available_slots = self.max_positions - len(self.positions)

        if available_slots <= 0:
            print("⚠️ 仓位已满")
            return

        candidates = []

        for _, row in today_signal.iterrows():
            code = row['code']

            next_day = self.get_next_day(code, date)

            if next_day is None:
                print(f"❌ {code} 次日无数据（可能停牌/日期不匹配）")
                continue

            # print(f"✅ {code} 找到次日: {next_day['date']}")

            candidates.append(next_day)

        if not candidates:
            print("❌ 没有可买候选")
            return

        candidates_df = pd.DataFrame(candidates)

        # 排序
        if self.buy_priority == 'turnover':
            candidates_df = candidates_df.sort_values('turnover', ascending=False)
        elif self.buy_priority == 'amount':
            candidates_df = candidates_df.sort_values('amount', ascending=False)
        elif self.buy_priority == 'market_cap':
            candidates_df['market_cap'] = candidates_df['close'] * candidates_df['outstanding_share']
            candidates_df = candidates_df.sort_values('market_cap', ascending=False)

        candidates_df = candidates_df.head(available_slots)

        print(f"🎯 候选股票: {list(candidates_df['code'])}")

        for _, row in candidates_df.iterrows():

            code = row['code']
            price = row['open'] if self.buy_mode == 'open' else row['close']

            cash_per_stock = self.cash / available_slots if available_slots > 0 else 0
            shares = cash_per_stock // price

            if shares <= 0:
                print(f"❌ {code} 买不起 | price={price:.2f}, cash_per_stock={cash_per_stock:.2f}")
                continue

            cost = shares * price
            self.cash -= cost

            self.positions[code] = {
                'buy_price': price,
                'shares': shares,
                'buy_date': row['date'],
                'max_profit': 0
            }

            print(f"🟢 BUY {code} | price={price:.2f} | shares={shares} | cost={cost:.2f}")

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

        remove_list = []

        for code, pos in self.positions.items():

            row = self.get_row(code, date)
            if row is None:
                continue

            price = row['close']
            ret = (price - pos['buy_price']) / pos['buy_price']

            pos['max_profit'] = max(pos['max_profit'], ret)

            sell = False
            reason = ""

            if 'max_hold_days' in self.sell_config:
                days = (date - pos['buy_date']).days
                if days >= self.sell_config['max_hold_days']:
                    sell = True
                    reason = f"持仓天数 {days}"

            if 'take_profit' in self.sell_config and ret >= self.sell_config['take_profit']:
                sell = True
                reason = f"止盈 {ret:.2%}"

            if 'stop_loss' in self.sell_config and ret <= -self.sell_config['stop_loss']:
                sell = True
                reason = f"止损 {ret:.2%}"

            if 'drawdown' in self.sell_config:
                if pos['max_profit'] - ret >= self.sell_config['drawdown']:
                    sell = True
                    reason = f"回撤 {pos['max_profit'] - ret:.2%}"

            if sell:
                self.cash += pos['shares'] * price

                print(f"🔴 SELL {code} | price={price:.2f} | return={ret:.2%} | 原因={reason}")

                self.trade_log.append({
                    'date': date,
                    'code': code,
                    'action': 'sell',
                    'price': price,
                    'shares': pos['shares'],
                    'return': ret
                })

                remove_list.append(code)

        for code in remove_list:
            del self.positions[code]
    def export_to_excel(self, file_path="backtest_result.xlsx"):

        # ===== 1. 净值曲线 =====
        equity_df = self.equity_df.copy()

        # ===== 2. 交易记录 =====
        trades_df = pd.DataFrame(self.trade_log)

        # ===== 3. 当前持仓 =====
        pos_list = []

        final_date = self.equity_df['date'].iloc[-1]

        for code, pos in self.positions.items():

            row = self.get_row(code, final_date)

            if row is not None:
                price = row['close']
            else:
                tmp = self.df[self.df['code'] == code]
                tmp = tmp[tmp['date'] <= final_date]
                if tmp.empty:
                    continue
                price = tmp.iloc[-1]['close']

            ret = (price - pos['buy_price']) / pos['buy_price']

            pos_list.append({
                'code': code,
                'buy_price': pos['buy_price'],
                'shares': pos['shares'],
                'buy_date': pos['buy_date'],
                'current_price': price,
                'return': ret
            })

        positions_df = pd.DataFrame(pos_list)

        # ===== 写入Excel =====
        with pd.ExcelWriter(file_path, engine='xlsxwriter') as writer:
            equity_df.to_excel(writer, sheet_name='equity', index=False)
            trades_df.to_excel(writer, sheet_name='trades', index=False)
            positions_df.to_excel(writer, sheet_name='positions', index=False)

        print(f"\n📊 回测结果已导出：{file_path}")


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
        # ==========================
        # 最终清算（强制卖出所有持仓）
        # ==========================
        final_date = dates[-1]

        final_value = self.cash

        for code, pos in self.positions.items():
            row = self.get_row(code, final_date)

            if row is not None:
                price = row['close']
            else:
                # ⚠️ fallback：用最近价格（避免丢失）
                tmp = self.df[self.df['code'] == code]
                tmp = tmp[tmp['date'] <= final_date]
                if tmp.empty:
                    continue
                price = tmp.iloc[-1]['close']

            final_value += pos['shares'] * price

        print(f"\n🏁 最终资产: {final_value:.2f} | 现金: {self.cash:.2f} | 持仓数: {len(self.positions)}")
        return self.equity_df                         # 返回结果
if __name__ == '__main__':
    engine = create_engine(
            "mysql+pymysql://root:chen@127.0.0.1:3306/gp"
        )
    # signal_df = pd.DataFrame({
    #     'date': [...],
    #     'code': [...]
    # })
    signal_df = pd.read_sql("select stock_code as code,trade_date as date from stock_abnormal_monitor where trade_date>='2026-01-01'",con=engine)


    engine = BacktestEngine(
        signal_df=signal_df,
        buy_mode='open',
        buy_priority='turnover',
        sell_config={
            'max_hold_days': 5,
            'take_profit': 0.1,
            'stop_loss': 0.05,
            'drawdown': 0.05
        },
        init_cash=1000000,
        max_positions=5
    )
    result = engine.run()
    engine.export_to_excel("回测结果.xlsx")
    print(result)
    pass

