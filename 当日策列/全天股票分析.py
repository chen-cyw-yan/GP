import pandas as pd
import numpy as np
from datetime import time, timedelta, datetime
import logging

import numpy as np
import pandas as pd
import akshare as ak

import warnings
warnings.filterwarnings("ignore", category=UserWarning)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)
class StockIntradayAnalyzer:
    def __init__(self, df):
        """
        初始化分析器
        :param df: DataFrame，必须包含列：['成交时间', '成交价格', '成交量', '成交金额', '性质']
        """
        # 数据预处理
        self.df = df.copy()
        self.df['成交时间'] = pd.to_datetime(self.df['成交时间'])
        self.df.set_index('成交时间', inplace=True)
        
        # 确保性质列标准化 (买盘/卖盘 -> 1/-1 或 直接字符串判断)
        # 假设 '性质' 列内容为 '买盘' 或 '卖盘'
        
        # 定义大单阈值 (可根据动态调整，这里先设固定值作为示例)
        self.large_order_amount_threshold = 1000000  # 100万
        self.large_order_vol_threshold_factor = 5    # 均量的5倍

    def _get_time_range_data(self, start_time_str, end_time_str):
        """获取指定时间段的数据"""
        # 构造今天的日期对象用于时间筛选
        today = self.df.index[0].date()
        start_dt = pd.Timestamp(datetime.combine(today, pd.to_datetime(start_time_str).time()))
        end_dt = pd.Timestamp(datetime.combine(today, pd.to_datetime(end_time_str).time()))
        
        # 筛选数据 (注意：集合竞价9:25的数据可能在9:30之前，需根据需求决定是否包含)
        mask = (self.df.index >= start_dt) & (self.df.index <= end_dt)
        return self.df.loc[mask]

    def analyze_opening_15min(self):
        """一、开盘后 15 分钟分析 (09:30 - 09:45)"""
        print("\n" + "="*30)
        print("📊 模块一：开盘后 15 分钟深度分析 (09:30-09:45)")
        print("="*30)
        
        data = self._get_time_range_data('09:30:00', '09:45:00')
        
        if data.empty:
            print("⚠️ 该时间段无数据")
            return

        # 1. 主动买卖强度
        buy_mask = data['性质'] == '买盘'
        sell_mask = data['性质'] == '卖盘'
        
        active_buy_amt = data.loc[buy_mask, '成交金额'].sum()
        active_sell_amt = data.loc[sell_mask, '成交金额'].sum()
        net_flow = active_buy_amt - active_sell_amt
        
        # 防止除以零
        buy_sell_ratio = active_buy_amt / active_sell_amt if active_sell_amt > 0 else np.inf
        
        print(f"💰 主动买入额: {active_buy_amt:,.0f}")
        print(f"💸 主动卖出额: {active_sell_amt:,.0f}")
        print(f"🌊 净主动资金: {net_flow:,.0f}")
        print(f"⚖️ 买卖比 (Buy/Sell): {buy_sell_ratio:.2f}")
        
        # 判断逻辑
        if buy_sell_ratio > 1.5:
            signal = "🔥 抢筹信号强烈"
        elif buy_sell_ratio < 0.7:
            signal = "📉 抛压沉重"
        else:
            signal = "⚖️ 多空平衡"
        print(f"   -> 结论: {signal}")

        # 2. 大单行为分析
        # 动态计算均量
        avg_vol = data['成交量'].mean()
        dynamic_vol_threshold = avg_vol * self.large_order_vol_threshold_factor
        
        # 定义大单：金额>100万 或 成交量>动态阈值
        large_order_mask = (data['成交金额'] > self.large_order_amount_threshold) | \
                           (data['成交量'] > dynamic_vol_threshold)
        
        large_orders = data[large_order_mask]
        large_buy = large_orders[large_orders['性质'] == '买盘']['成交金额'].sum()
        large_sell = large_orders[large_orders['性质'] == '卖盘']['成交金额'].sum()
        large_net = large_buy - large_sell
        large_ratio = len(large_orders) / len(data) * 100
        
        print(f"\n🐋 大单统计 (>{self.large_order_amount_threshold/10000:.0f}万 或 >{dynamic_vol_threshold:.0f}手):")
        print(f"   大单占比: {large_ratio:.2f}%")
        print(f"   大单净流入: {large_net:,.0f}")
        
        # 检测连续大买单 (简化版：前5分钟是否有连续大买)
        first_5_min = data.loc[data.index[0]:data.index[0]+timedelta(minutes=5)]
        first_5_large_buy = first_5_min[(first_5_min['成交金额'] > self.large_order_amount_threshold) & (first_5_min['性质']=='买盘')]
        
        if len(first_5_large_buy) >= 3: # 假设5分钟内出现3笔以上大买单视为连续
            print("   -> 🚀 预警: 开盘5分钟内发现连续大单买入，主力强势启动迹象！")

        # 3. 开盘波动结构
        open_price = data.iloc[0]['成交价格']
        close_price_15m = data.iloc[-1]['成交价格']
        high_price = data['成交价格'].max()
        low_price = data['成交价格'].min()
        
        pct_change = (close_price_15m - open_price) / open_price * 100
        
        print(f"\n📈 价格结构:")
        print(f"   开盘: {open_price:.2f}, 15分收盘: {close_price_15m:.2f}, 最高: {high_price:.2f}, 最低: {low_price:.2f}")
        print(f"   区间涨跌幅: {pct_change:.2f}%")
        
        structure_signal = ""
        if open_price > self.df.iloc[0]['成交价格']: # 假设对比昨日收盘或集合竞价
             if close_price_15m > open_price: structure_signal = "高开高走 (机构抢筹)"
             else: structure_signal = "高开回落 (套牢盘出货)"
        else:
            if close_price_15m > open_price: structure_signal = "低开拉升 (主力做盘)"
            else: structure_signal = "低开低走 (弱势)"
            
        # 简单修正：如果没有昨日数据，仅看区间内走势
        if close_price_15m > open_price and high_price == close_price_15m:
            structure_signal = "单边上行 (极强)"
        elif close_price_15m < open_price and low_price == close_price_15m:
            structure_signal = "单边下行 (极弱)"
        elif high_price > open_price and close_price_15m < open_price:
            structure_signal = "冲高回落 (诱多?)"
        elif low_price < open_price and close_price_15m > open_price:
            structure_signal = "探底回升 (承接强)"
            
        print(f"   -> 形态判定: {structure_signal}")

    def analyze_closing_15min(self):
        """二、收盘前 15 分钟分析 (14:45 - 15:00)"""
        print("\n" + "="*30)
        print("🌇 模块二：收盘前 15 分钟分析 (14:45-15:00)")
        print("="*30)
        
        data = self._get_time_range_data('14:45:00', '15:00:00')
        if data.empty:
            # 尝试兼容不同市场（如港股/美股）或数据不全情况，这里默认A股
            print("⚠️ 未找到14:45-15:00数据，请确认交易时间或数据完整性")
            return

        buy_amt = data[data['性质'] == '买盘']['成交金额'].sum()
        sell_amt = data[data['性质'] == '卖盘']['成交金额'].sum()
        net_flow = buy_amt - sell_amt
        ratio = buy_amt / sell_amt if sell_amt > 0 else np.inf
        
        start_price = data.iloc[0]['成交价格']
        end_price = data.iloc[-1]['成交价格']
        tail_lift = (end_price - start_price) / start_price * 100
        
        print(f"💰 尾盘净流入: {net_flow:,.0f}")
        print(f"⚖️ 尾盘买卖比: {ratio:.2f}")
        print(f"📉 尾盘价格变动: {tail_lift:.2f}%")
        
        if tail_lift > 1.0 and net_flow > 0:
            print("   -> 🌟 结论: 尾盘抢筹，明日看涨概率大")
        elif tail_lift < -1.0 and net_flow < 0:
            print("   -> 💣 结论: 尾盘跳水，避险情绪浓厚")
        else:
            print("   -> 😐 结论: 尾盘平稳，随波逐流")

    def analyze_full_day(self):
        """三、全天交易情况概览"""
        print("\n" + "="*30)
        print("📅 模块三：全天交易全景")
        print("="*30)
        
        data = self.df
        total_vol = data['成交量'].sum()
        total_amt = data['成交金额'].sum()
        vwap = total_amt / total_vol if total_vol > 0 else 0 # 成交量加权平均价
        
        open_p = data.iloc[0]['成交价格']
        close_p = data.iloc[-1]['成交价格']
        high_p = data['成交价格'].max()
        low_p = data['成交价格'].min()
        
        daily_ret = (close_p - open_p) / open_p * 100
        
        buy_total = data[data['性质'] == '买盘']['成交金额'].sum()
        sell_total = data[data['性质'] == '卖盘']['成交金额'].sum()
        full_day_ratio = buy_total / sell_total if sell_total > 0 else np.inf
        
        print(f"🏁 开盘: {open_p:.2f} | 🛑 收盘: {close_p:.2f}")
        print(f"🔝 最高: {high_p:.2f} | 🔻 最低: {low_p:.2f}")
        print(f"📊 涨跌幅: {daily_ret:.2f}%")
        print(f"💵 总成交额: {total_amt/10000:.2f} 万")
        print(f"⚖️ 全天买卖比: {full_day_ratio:.2f}")
        print(f"🎯 VWAP (均价): {vwap:.2f}")
        
        if close_p > vwap:
            print("   -> ✅ 收盘价在均价之上，多头强势")
        else:
            print("   -> ❌ 收盘价在均价之下，空头占优")

# ==========================================
# 使用示例 (模拟你的数据输入)
# ==========================================

if __name__ == "__main__":
    # 模拟数据构建 (实际使用时，请直接读取你的CSV或数据库)
    # 这里为了演示，复制了你提供的几行数据并扩充了一些模拟数据以覆盖全天
    df = ak.stock_zh_a_tick_tx_js(symbol='sz001339')
    # 实例化并运行分析
    analyzer = StockIntradayAnalyzer(df)
    
    # 注意：由于模拟数据不全（缺少09:30-14:45的数据），部分统计可能不准确
    # 在实际运行时，请传入完整的DataFrame
    try:
        analyzer.analyze_opening_15min()
        analyzer.analyze_closing_15min()
        analyzer.analyze_full_day()
    except Exception as e:
        print(f"分析过程中出现错误（可能是模拟数据时间跨度不够）: {e}")
        print("提示：请确保输入数据覆盖 09:30-15:00 的全天时段。")