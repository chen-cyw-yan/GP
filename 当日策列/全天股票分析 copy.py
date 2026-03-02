import pandas as pd
import numpy as np
from datetime import datetime, time, timedelta
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
import pandas as pd
import numpy as np
from datetime import datetime, time, timedelta

class FinalQuantAnalyzer:
    def __init__(self, df):
        self.df = df.copy()
        # 时间处理
        self.df['成交时间'] = pd.to_datetime(self.df['成交时间'])
        self.df.set_index('成交时间', inplace=True)
        self.df.sort_index(inplace=True)
        
        # 1. 性质标准化与中性盘归因
        self.df['raw_type'] = self.df['性质'].astype(str)
        self.df['type_code'] = 0
        self.df.loc[self.df['raw_type'].str.contains('买'), 'type_code'] = 1
        self.df.loc[self.df['raw_type'].str.contains('卖'), 'type_code'] = -1
        
        # 中性盘归因 (基于后一笔价格变动)
        self.df['price_shift'] = self.df['成交价格'].shift(-1) - self.df['成交价格']
        neutral_mask = (self.df['type_code'] == 0)
        self.df.loc[neutral_mask & (self.df['price_shift'] > 0), 'type_code'] = 1
        self.df.loc[neutral_mask & (self.df['price_shift'] < 0), 'type_code'] = -1
        
        # 2. 大小单定义 (1手=100股)
        self._define_order_size()

    def _define_order_size(self):
        avg_vol_hand = self.df['成交量'].mean()
        amt_thresh = 1000000  # 100万
        vol_thresh_hand = avg_vol_hand * 5
        
        self.df['is_large'] = (self.df['成交金额'] > amt_thresh) | (self.df['成交量'] > vol_thresh_hand)
        self.df['is_small'] = ~self.df['is_large']

    def _get_time_range(self, start_str, end_str):
        today = self.df.index[0].date()
        start = pd.Timestamp(datetime.combine(today, time.fromisoformat(start_str)))
        end = pd.Timestamp(datetime.combine(today, time.fromisoformat(end_str)))
        return self.df[(self.df.index >= start) & (self.df.index <= end)]

    # =========================================================
    # 【核心模块 1】承接力评分 (严格按您的分级标准)
    # =========================================================
    def calculate_support_recovery_refined(self, data_segment):
        if len(data_segment) < 10:
            return 0.0, "数据不足"
            
        scores = []
        step = max(3, len(data_segment) // 30) 
        lookforward_steps = step * 2 
        
        for i in range(0, len(data_segment) - step - lookforward_steps, step):
            peak_zone = data_segment.iloc[i:i+step]
            drop_zone = data_segment.iloc[i+step:i+step*2]
            recovery_zone = data_segment.iloc[i+step*2:i+step*2+lookforward_steps]
            
            if len(recovery_zone) == 0: continue
            
            p_high = peak_zone['成交价格'].max()
            p_low = drop_zone['成交价格'].min()
            p_recovery_max = recovery_zone['成交价格'].max()
            
            drop_depth = p_high - p_low
            # 过滤微小波动噪音
            if drop_depth <= p_high * 0.003:
                continue
                
            recovery_height = p_recovery_max - p_low
            recovery_ratio = recovery_height / drop_depth if drop_depth > 0 else 0
            
            # --- 严格执行您的评分标准 ---
            if p_recovery_max >= p_high:
                score = 1.0  # 完全收复并创新高
            elif recovery_ratio > 0.5:
                score = 0.7  # 收复超过50% (中)
            elif recovery_ratio > 0:
                score = 0.4  # 仅止跌，未收复过半 (弱)
            else:
                score = 0.0  # 继续下跌 (极弱)
            
            scores.append(score)
        
        if not scores:
            return 1.0, "无明显回撤或单边上涨"
            
        return np.mean(scores), f"检测到{len(scores)}次回撤测试"

    # =========================================================
    # 【核心模块 2】推价有效性 (严格按您提供的代码逻辑)
    # =========================================================
    def calculate_push_score_custom(self, data_segment):
        """
        完全复刻用户提供的算法逻辑：
        1. 成交量加权方向性 (满分20)
        2. 连续推价奖励 (满分5)
        总分满分25
        """
        price = data_segment['成交价格']
        volume = data_segment['成交量'] # 单位：手
        
        # 1️⃣ 成交量加权方向性
        price_diff = price.diff()
        
        # 上涨时的成交量总和
        up_vol = volume[price_diff > 0].sum()
        # 下跌时的成交量总和
        down_vol = volume[price_diff < 0].sum()
        
        # 防止除以零
        push_ratio = up_vol / (up_vol + down_vol + 1e-6)
        
        # 映射到 0-20 分
        direction_score = min(push_ratio * 20, 20)
        
        # 2️⃣ 连续推价奖励
        # 获取涨跌符号 (1:涨, -1:跌, 0:平)
        sign = np.sign(price_diff.dropna())
        up_flag = (sign == 1).astype(int)
        
        if up_flag.empty:
            streak_bonus = 0
            max_up_streak = 0
        else:
            # 计算连续上涨段长度
            # 逻辑：当当前值与前一个值不同时，分组ID加1
            groups = (up_flag != up_flag.shift()).cumsum()
            streak_len = up_flag.groupby(groups).sum()
            
            # 只取值为1的组（即上涨组）的最大长度
            # 注意：groupby.sum()会对0和1求和，如果组内全是0，和为0；如果有1，和为连续长度
            # 为了保险，我们过滤出那些起始标记为1的组，或者直接取最大值（因为下跌组的和也是0或负数逻辑不适用这里，up_flag只有0和1）
            # 更严谨的做法：只统计 up_flag==1 的连续段
            # 简化处理：直接取所有分组和的最大值，因为 up_flag 非0即1，下跌段 sum 为 0
            max_up_streak = streak_len.max() if not streak_len.empty else 0
            
            # 映射到 0-5 分 (每连续10笔得1分，最高5分)
            streak_bonus = min(max_up_streak / 10, 1) * 5

        # 3️⃣ 合成推价有效性
        push_score = round(direction_score + streak_bonus, 2)
        
        return push_score, {
            "push_ratio": round(push_ratio, 3),
            "max_up_streak": int(max_up_streak),
            "direction_score": round(direction_score, 2),
            "streak_bonus": round(streak_bonus, 2),
            "total_score": push_score
        }

    def analyze_period(self, name, start_time, end_time):
        print(f"\n{'='*30} {name} {'='*30}")
        data = self._get_time_range(start_time, end_time)
        if data.empty:
            print("⚠️ 无数据")
            return

        # 1. 资金流向
        buy_amt = data[data['type_code'] == 1]['成交金额'].sum()
        sell_amt = data[data['type_code'] == -1]['成交金额'].sum()
        net_flow = buy_amt - sell_amt
        ratio = buy_amt / sell_amt if sell_amt > 0 else np.inf
        
        print(f"💰 净主动流: {net_flow:,.0f} | 买卖比: {ratio:.2f}")

        # 2. 大小单拆解
        l_buy = data[(data['is_large']) & (data['type_code']==1)]['成交金额'].sum()
        l_sell = data[(data['is_large']) & (data['type_code']==-1)]['成交金额'].sum()
        s_buy = data[(data['is_small']) & (data['type_code']==1)]['成交金额'].sum()
        s_sell = data[(data['is_small']) & (data['type_code']==-1)]['成交金额'].sum()
        
        print(f"🐋 大单净流入: {l_buy - l_sell:,.0f}")
        print(f"🐜 小单净流入: {s_buy - s_sell:,.0f}")

        # 3. 承接力评分
        support_score, support_msg = self.calculate_support_recovery_refined(data)
        print(f"\n🛡️ 承接力评分: {support_score:.1f} ({support_msg})")
        if support_score == 1.0: desc = "极强 (完全收复)"
        elif support_score >= 0.7: desc = "中等 (收复>50%)"
        elif support_score >= 0.4: desc = "弱 (仅止跌)"
        else: desc = "极弱 (继续下跌)"
        print(f"   -> 评价: {desc}")

        # 4. 推价有效性 (新算法)
        push_score, details = self.calculate_push_score_custom(data)
        print(f"\n🎯 推价有效性评分: {push_score:.2f} / 25.00")
        print(f"   📊 细节:")
        print(f"      - 上涨成交量占比 (Push Ratio): {details['push_ratio']:.1%} -> 得分: {details['direction_score']}/20")
        print(f"      - 最长连续上涨笔数: {details['max_up_streak']} 笔 -> 奖励: {details['streak_bonus']}/5")
        
        # 评分解读
        if push_score >= 20:
            verdict = "✅ 极强推升 (量价齐升，趋势连贯)"
        elif push_score >= 15:
            verdict = "🟢 良性推升 (多头占优)"
        elif push_score >= 10:
            verdict = "⚖️ 震荡整理 (多空交织)"
        else:
            verdict = "🔴 弱势推升 (下跌放量或连续下跌)"
        print(f"   -> 结论: {verdict}")

        # 5. 形态
        o_p, c_p = data['成交价格'].iloc[0], data['成交价格'].iloc[-1]
        print(f"📈 区间表现: {'涨' if c_p > o_p else '跌'} ({o_p:.2f} -> {c_p:.2f})")

    def run_full_analysis(self):
        print("🚀 启动最终版量化分析 (承接力分级 + 自定义推价算法)")
        self.analyze_period("开盘后15分钟", "09:30:00", "09:45:00")
        self.analyze_period("收盘前15分钟", "14:45:00", "15:00:00")
        
        print(f"\n{'='*30} 全天交易全景 {'='*30}")
        data = self.df
        total_amt = data['成交金额'].sum()
        vwap = total_amt / (data['成交量'].sum() * 100)
        
        all_l_net = data[(data['is_large']) & (data['type_code']==1)]['成交金额'].sum() - \
                    data[(data['is_large']) & (data['type_code']==-1)]['成交金额'].sum()
        all_s_net = data[(data['is_small']) & (data['type_code']==1)]['成交金额'].sum() - \
                    data[(data['is_small']) & (data['type_code']==-1)]['成交金额'].sum()
        
        print(f"📊 总成交额: {total_amt/10000:.1f}万 | VWAP: {vwap:.2f}")
        print(f"🐋 全天大单净额: {all_l_net:,.0f}")
        print(f"🐜 全天小单净额: {all_s_net:,.0f}")
        
        sup_score, sup_msg = self.calculate_support_recovery_refined(data)
        push_score, details = self.calculate_push_score_custom(data)
        
        print(f"\n🛡️ 全天承接力: {sup_score:.1f}")
        print(f"🎯 全天推价有效性: {push_score:.2f} / 25.00 (上涨占比:{details['push_ratio']:.1%}, 连涨:{details['max_up_streak']}笔)")
        
        # 综合结论
        conclusion = ""
        if all_l_net > 0 and sup_score >= 0.7 and push_score >= 15:
            conclusion = "🌟【强烈看好】主力流入，承接有力，量价配合完美。"
        elif all_l_net > 0 and push_score < 10:
            conclusion = "⚠️【警惕诱多】主力虽买入但推升无力（下跌放量或缺乏连续性），需防回落。"
        elif sup_score == 0.0:
            conclusion = "📉【风险警示】承接力崩溃，跌破无反抽。"
        else:
            conclusion = "⚖️【震荡观察】多空博弈，等待方向选择。"
            
        print(f"\n💡 综合研判: {conclusion}")

# ==========================================
# 模拟数据测试 (验证新算法)
# ==========================================
if __name__ == "__main__":
    df = ak.stock_zh_a_tick_tx_js(symbol='sh603530')
    analyzer = FinalQuantAnalyzer(df)
    analyzer.run_full_analysis()