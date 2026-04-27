import pandas as pd
from sqlalchemy import create_engine


class BacktestEngine:

    def __init__(self, signal_df,
                 init_cash=1000000,     # 初始资金
                 max_positions=5,       # 最大持仓数量（控制分散度）
                 max_daily_buys=5,   # 控制当天开仓节奏
                 fee=0.00025,           # 手续费（双边）
                 buy_priority='turnover',#买入优先级：turnover/amount/market_cap
                 buy_priority_sort=False,
                 sell_config={},           #卖出规则
                 buy_time='open',          # 买入时机：close/open
                 slippage=0.001):       # 滑点（买贵卖便宜）
        
        self.buy_priority_sort=buy_priority_sort
        # self.buy_priority_sort=buy_priority_sort
        self.sell_config=sell_config
        self.buy_time=buy_time
        self.max_daily_buys=max_daily_buys
        self.trade_id_counter = 0
        self.signal_df = signal_df      # 触发信号（必须包含 date, code）
        self.init_cash = init_cash      # 初始资金
        self.cash = init_cash           # 当前剩余现金
        self.max_positions = max_positions  # 最大持仓数
        self.buy_priority=buy_priority
        self.fee = fee                  # 手续费比例
        self.slippage = slippage        # 滑点比例

        self.positions = []             # 当前持仓（list，每一笔交易一个仓位）
        self.trade_log = []             # 所有交易记录（买卖）
        
        # 创建数据库连接
        self.engine = create_engine(
            "mysql+pymysql://root:chen@127.0.0.1:3306/gp"
        )

        self.load_data()                # 加载行情数据

        # ==========================
        # 数据统一（非常关键）
        # ==========================
        self.df['date'] = pd.to_datetime(self.df['date']).dt.normalize()   # 统一为 00:00:00
        self.signal_df['date'] = pd.to_datetime(self.signal_df['date']).dt.normalize()

        self.df['code'] = self.df['code'].astype(str)      # 股票代码统一字符串
        self.signal_df['code'] = self.signal_df['code'].astype(str)
        self.df = self.df.merge(
            self.signal_df[['code', 'date', 'min_sell_price']],
            on=['code', 'date'],
            how='left',
            validate='m:1'
        )
        # self.df['min_sell_price'] = self.df.groupby('code')['min_sell_price'].ffill()
    # ==========================
    # 加载数据库数据
    # ==========================
    def load_data(self):
        sql = """
        SELECT date, code, open, high, low, close,
               volume, amount, turnover
        FROM stock
        ORDER BY code, date
        """
        self.df = pd.read_sql(sql, self.engine)   # 读取数据

    # ==========================
    # 获取下一交易日（用于次日买入）
    # ==========================
    def get_next_day(self, code, date):

        # 找到该股票未来日期的数据
        df = self.df[(self.df['code'] == code) & (self.df['date'] > date)]

        if df.empty:          # 如果没有未来数据（比如最后一天）
            return None

        return df.iloc[0]     # 返回下一交易日
    

    # ==========================
    # 计算买入优先级（核心函数）
    # ==========================
    def rank_signals(self, today_signal, date):

        if today_signal.empty:
            return today_signal

        # 取当日行情数据（用于排序）
        market_df = self.df[self.df['date'] == date]

        # 合并信号 + 行情
        merged = today_signal.merge(
            market_df,
            on=['code', 'date'],
            how='left'
        )

        # ==========================
        # 不同排序逻辑
        # ==========================
        if self.buy_priority == 'volume':
            merged = merged.sort_values('volume', ascending=self.buy_priority_sort)

        elif self.buy_priority == 'turnover':
            merged = merged.sort_values('turnover', ascending=self.buy_priority_sort)

        elif self.buy_priority == 'market_cap':
            # 需要流通股本字段
            if 'outstanding_share' not in merged.columns:
                raise ValueError("缺少 outstanding_share 字段，无法计算市值")
            merged['market_cap'] = merged['close'] * merged['outstanding_share']
            merged = merged.sort_values('market_cap', ascending=self.buy_priority_sort)

        else:
            # 默认不排序
            pass

        return merged
    def dynamic_take_profit(self, pos, row):

        close = row['close']
        high = row['high']

        # 更新最高价
        pos['peak_price'] = max(pos.get('peak_price', pos['buy_price']), high)

        buy_price = pos['buy_price']
        peak_price = pos['peak_price']

        profit = (peak_price - buy_price) / buy_price
        drawdown = (peak_price - close) / peak_price
        # print(close,high,peak_price,profit,drawdown)
        # =========================
        # 动态止盈曲线（核心）
        # =========================

        # ① 小波段：不动
        if profit < 0.1:
            return False, 0, ''

        # ② 10%~30%：回撤3%减仓
        if 0.05 <= profit < 0.1:
            if drawdown > 0.03:
                return True, 1, '10~30%区间回撤减仓'

        # ③ 30%~50%：回撤5%减仓
        if 0.3 <= profit < 0.5:
            if drawdown > 0.05:
                return True, 1, '30~50%区间回撤减仓'

        # ④ >50%：回撤8%清仓
        if profit >= 0.5:
            if drawdown > 0.08:
                return True, 1.0, '高位回撤止盈'

        return False, 0, ''


    # ==========================
    # 计算卖出规则
    # ==========================
    def check_sell_signal(self, pos, row, prev_row, date):

        config = self.sell_config or {}

        buy_price = pos['buy_price']
        buy_date = pos['buy_date']

        close = row['close']
        open_price = row['open']
        high = row['high']

        prev_close = prev_row['close'] if prev_row is not None else close

        # ===== 收益计算 =====
        total_ret = (close - buy_price) / buy_price
        daily_ret = (close - prev_close) / prev_close
        intraday_ret = (close - open_price) / open_price

        hold_days = (date - buy_date).days
        avg_ret = total_ret / max(hold_days, 1)

        # ===== 回撤相关 =====
        pos['max_price'] = max(pos.get('max_price', buy_price), high)
        drawdown = (pos['max_price'] - close) / pos['max_price']
        high_to_close = (high - close) / high if high > 0 else 0
        # min_sell_price = row.get('min_sell_price')
        min_sell_price = pos.get('min_sell_price')
        # ==========================
        # 规则判断（全部可选）
        # ==========================
        # print('min_sell_price',min_sell_price)
        if min_sell_price is not None:
            # 收盘跌破（稳健）
            # print(close <= min_sell_price)
            if close <= min_sell_price:
                return True, 1, '硬止损'

            # 如果你想更激进（盘中触发）
            # if row['low'] < min_sell_price:
            #     return True, 1, '盘中跌破止损价'
        tp_flag, tp_pct, tp_reason = self.dynamic_take_profit(pos, row)
        # print('dynamic_take_profit',tp_flag, tp_pct, tp_reason)
        if tp_flag:
            return True, tp_pct, tp_reason

        # 1️⃣ 总盈利
        if config.get('total_profit') is not None:
            if total_ret >= config['total_profit']:
                return True, 1, '总盈利止盈'

        # 2️⃣ 当天盈利
        if config.get('daily_profit') is not None:
            if intraday_ret >= config['daily_profit']:
                return True, 1, '当天盈利止盈'

        # 3️⃣ 涨停卖出
        if config.get('sell_on_limit_up') is True:
            if close >= prev_close * 1.095:
                return True, 1, '涨停卖出'

        # 4️⃣ 平均收益
        if config.get('avg_daily_profit') is not None:
            if avg_ret >= config['avg_daily_profit']:
                return True, 1, '平均收益止盈'

        # 5️⃣ 回撤
        if config.get('drawdown') is not None:
            if drawdown >= config['drawdown']:
                return True, 1, '回撤止损'

        # 6️⃣ 冲高回落
        if config.get('high_to_close_drop') is not None:
            if high_to_close >= config['high_to_close_drop']:
                return True, 1, '冲高回落'

        # 7️⃣ 持仓天数
        if config.get('max_hold_days') is not None:
            if hold_days >= config['max_hold_days']:
                return True, 1, '持仓到期'

        # 8️⃣ 当日跌幅
        if config.get('daily_drop') is not None:
            if daily_ret <= config['daily_drop']:
                return True, 1, '当日大跌止损'

        # 9️⃣ 总亏损
        if config.get('total_loss') is not None:
            if total_ret <= config['total_loss']:
                return True, 1, '总亏损止损'

        # 🔟 阶梯止盈（部分卖出）
        ladder = config.get('ladder')
        if ladder is not None:
            if daily_ret >= ladder.get('trigger', 999):  # 默认不会触发
                return True, ladder.get('sell_pct', 0), '阶梯止盈'

        return False, 0, ''



    # ==========================
    # 买入逻辑（含滑点 + 手续费 + 涨停过滤）
    # ==========================
    def try_buy(self, date):
        
        # 找当天的信号
        today_signal = self.signal_df[self.signal_df['date'] == date]
        

        if today_signal.empty:   # 没有信号直接返回
            return
        
        #买入优先级
        today_signal = self.rank_signals(today_signal, date)

        # 还能买几个仓位
        available_slots = self.max_positions - len(self.positions)

        if available_slots <= 0:  # 仓位满了
            # print(f"🚫 {date} 仓位已满，无法买入")
            return

        buy_count = 0

        for _, row in today_signal.iterrows():

            # ===== 仓位限制 =====
            if len(self.positions) >= self.max_positions:
                # print(f"🚫 {date} 仓位已满，无法买入")
                break

            if self.max_daily_buys and buy_count >= self.max_daily_buys:
                # print(f"🚫 {date} 今日买入次数已达到上限 {self.max_daily_buys}")
                break

            code = row['code']

            next_day = self.get_next_day(code, date)
            if next_day is None:
                continue

            # ===== 动态资金分配 =====
            remain_slots = self.max_positions - len(self.positions)
            if remain_slots <= 0:
                break
            if self.buy_time=='open':
                price = next_day['open'] * (1 - self.slippage)

            elif self.buy_time=='close':
                price = next_day['close'] * (1 - self.slippage)

            elif self.buy_time=='high':
                price = next_day['high'] * (1 - self.slippage)

            
            # price = next_day['open'] * (1 + self.slippage)

            cash_per_trade = self.cash / remain_slots
            shares = int(cash_per_trade // price)

            if shares <= 0:
                # print(f"🚫 {date} {code} 买入金额不足")
                continue

            cost = shares * price * (1 + self.fee)
            self.cash -= cost
            print(f"🟢 { date} BUY {code} | price={price:.2f} | shares={shares} | cost={cost:.2f}")
            # ⭐ 生成唯一交易ID
            self.trade_id_counter += 1
            trade_id = self.trade_id_counter
            # print(row)
            min_sell_price = row['min_sell_price_y']

            self.positions.append({
                'trade_id': trade_id,
                'code': code,
                'buy_price': price,
                'shares': shares,
                'buy_date': next_day['date'],

                # ⭐ 强制落地（关键）
                'min_sell_price': None if pd.isna(min_sell_price) else float(min_sell_price),

                'peak_price': price
            })

            self.trade_log.append({
                'date': next_day['date'],
                'code': code,
                'action': 'buy',

                'price': price,
                'shares': shares,

                'position_after': shares,     # ⭐ 当前仓位
                'cash_after': self.cash,      # ⭐ 当前现金

                'trade_id': trade_id,         # ⭐ 交易ID

                'pnl': 0,
                'pnl_pct': 0,

                'reason': 'open'
            })

            buy_count += 1

    # ==========================
    # 卖出逻辑（含 T+1 + 跌停限制）
    # ==========================
    def try_sell(self, date):

        new_positions = []

        for pos in self.positions:

            code = pos['code']

            # T+1限制
            if date <= pos['buy_date']:
                new_positions.append(pos)
                # print(f"🚫 {date} {code} T+1限制，无法卖出")
                continue

            # 当前行情
            df = self.df[(self.df['code'] == code) & (self.df['date'] == date)]
            if df.empty:
                new_positions.append(pos)
                # print(f"🚫 {date} {code} 没有行情数据，无法卖出")
                continue

            row = df.iloc[0]

            # 前一天数据（用于计算涨跌幅）
            prev_df = self.df[(self.df['code'] == code) & (self.df['date'] < date)]
            prev_row = prev_df.iloc[-1] if not prev_df.empty else None

            # ===== 调用止盈止损函数 =====
            sell_flag, sell_pct, reason = self.check_sell_signal(pos, row, prev_row, date)

            if not sell_flag:
                new_positions.append(pos)
                # print(f"🚫 {date} {code} 没有卖出信号，保留仓位")
                continue

            # ===== 执行卖出 =====
            sell_shares = int(pos['shares'] * sell_pct)

            if sell_shares <= 0:
                new_positions.append(pos)
                # print(f"🚫 {date} {code} 卖出数量不足，保留仓位")
                continue

            price = row['close'] * (1 - self.slippage)
            proceeds = sell_shares * price * (1 - self.fee)

            self.cash += proceeds
            # ===== 盈亏计算 =====
            cost = pos['buy_price'] * sell_shares
            revenue = price * sell_shares

            pnl = revenue - cost
            pnl_pct = (price - pos['buy_price']) / pos['buy_price']


            # 更新剩余仓位
            remain_shares = pos['shares'] - sell_shares

            if remain_shares > 0:
                pos['shares'] = remain_shares
                new_positions.append(pos)

            ret = (price - pos['buy_price']) / pos['buy_price']

            print(f"🔴卖出日期{date}|| {code}| | 比例:{sell_pct:.0%} | 收益:{ret:.2%} | 原因:{reason}")

            self.trade_log.append({
                'date': date,
                'code': code,
                'action': 'sell',
                'buy_date': pos['buy_date'],
                'price': price,
                'shares': sell_shares,

                'position_after': remain_shares,  # ⭐ 剩余仓位
                'cash_after': self.cash,          # ⭐ 当前现金

                'trade_id': pos['trade_id'],      # ⭐ 关联买入

                'pnl': pnl,
                'pnl_pct': pnl_pct,

                'reason': reason
            })

        self.positions = new_positions

    # ==========================
    # 主回测流程
    # ==========================
    def run(self):

        # 所有交易日
        dates = sorted(self.df['date'].unique())

        equity_curve = []   # 净值曲线

        for date in dates:

            # 先卖再买（符合现实）
            self.try_sell(date)
            self.try_buy(date)

            # ==========================
            # 计算总资产
            # ==========================
            total = self.cash   # 现金

            # 加上持仓市值
            for pos in self.positions:

                df = self.df[(self.df['code'] == pos['code']) & (self.df['date'] == date)]

                if df.empty:
                    continue

                total += pos['shares'] * df.iloc[0]['close']

            # 记录每日净值
            equity_curve.append({
                'date': date,
                'equity': total
            })

        self.equity_df = pd.DataFrame(equity_curve)

        print(f"\n🏁 最终资产: {self.equity_df.iloc[-1]['equity']:.2f}")

        return self.equity_df
if __name__ == '__main__':
    engine = create_engine(
                "mysql+pymysql://root:chen@127.0.0.1:3306/gp"
            )
        # signal_df = pd.DataFrame({
        #     'date': [...],
        #     'code': [...]
        # })
    # signal_df = pd.read_sql("select stock_code as code,trade_date as date,min_sell_price from stock_abnormal_monitor where trade_date > '2026-01-01'",con=engine)
    signal_df=pd.DataFrame([
        {'date':'2025-02-24',
        'code':'sh603327',
        'min_sell_price':9.5256
        },
        {'date':'2025-02-24',
        'code':'sz001238',
        'min_sell_price':29.4098
        },
        {'date':'2025-02-24',
        'code':'sh600749',
        'min_sell_price':10.6428
        },
        {'date':'2025-02-25',
        'code':'sz002189',
        'min_sell_price':18.7278
        },
        {'date':'2025-02-25',
        'code':'sz002139',
        'min_sell_price':16.1700
        },
        {'date':'2025-02-25',
        'code':'sh605086',
        'min_sell_price':30.8798
        },
        {'date':'2025-02-25',
        'code':'sh603040',
        'min_sell_price':23.7160
        },
        {'date':'2025-02-26',
        'code':'sz002441',
        'min_sell_price':7.6146
        },
        {'date':'2025-02-26',
        'code':'sz002295',
        'min_sell_price':6.9384
        },
        {'date':'2025-02-26',
        'code':'sz001358',
        'min_sell_price':21.2072
        },
        {'date':'2025-02-26',
        'code':'sz002585',
        'min_sell_price':4.9588}
    ])
    
    
    sell_config = {
    'total_profit': 0.5,          # 总盈利20%
    # 'daily_profit': 0.1,          # 当天盈利10%
    # 'sell_on_limit_up': True,     # 涨停是否卖出
    # 'avg_daily_profit': 0.07,     # 平均每天收益5%
    'drawdown': 0.1,              # 从最高回撤10%
    'high_to_close_drop': 0.05,   # 最高到收盘回落5%
    'max_hold_days': 5,           # 持股天数
    'daily_drop': -0.08,          # 当天跌幅
    'total_loss': -0.1,           # 总亏损
    # 'ladder': {
    #     'trigger': 0.05,          # 当天涨幅>5%
    #     'sell_pct': 0.3           # 卖出30%
    # }
}



    engine = BacktestEngine(signal_df,sell_config=sell_config)
    engine.run()

    pd.DataFrame(engine.trade_log).to_excel("trades.xlsx", index=False)
    engine.equity_df.to_excel("equity.xlsx", index=False)