import pandas as pd
import sys
import os
from sqlalchemy import create_engine
sys.path.append(
    os.path.abspath(
        os.path.join(os.path.dirname(__file__), "../../")
    )
)
import numpy as np
from datetime import datetime, time, timedelta
import logging
import warnings
import requests
import akshare as ak
import urllib3
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from io import BytesIO
import platform
import locale
import prod_online.services.filter_stock as filter_stock
import prod_online.config.utils as utils
import logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)
# 忽略警告
warnings.filterwarnings("ignore", category=UserWarning)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
today_dt = datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)
today_str = today_dt.strftime("%Y-%m-%d")
# today_str ='2026-03-06'
logger.info(f"当前任务日期: {today_str}")
engine = create_engine("mysql+pymysql://root:chen@127.0.0.1:3306/gp")
if not utils.is_trading_day_ak(today_str):
    logger.warning(f"⚠️ {today_str} 不是 A 股交易日，程序安全退出。")
    sys.exit(0)
# ================= 配置 Matplotlib 中文显示 =================
def setup_matplotlib_font():
    """自动设置适合当前操作系统的中文字体"""
    system_name = platform.system()
    
    # 常见中文字体映射
    if system_name == 'Windows':
        font_name = 'SimHei'  # 黑体
    elif system_name == 'Darwin':  # macOS
        font_name = 'Arial Unicode MS'  # 或 'Heiti TC'
    else:  # Linux
        # 尝试常见 Linux 中文字体，如果没有则 fallback
        font_name = 'WenQuanYi Micro Hei' 
    
    # 设置字体
    plt.rcParams['font.sans-serif'] = [font_name, 'DejaVu Sans']
    # 解决负号显示为方块的问题
    plt.rcParams['axes.unicode_minus'] = False
    
    # 验证字体是否加载成功（可选）
    try:
        plt.title("测试")
        plt.close()
    except:
        logger.warning(f"无法加载字体 {font_name}，中文可能无法显示。尝试使用默认字体。")

setup_matplotlib_font()

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

class FinalQuantAnalyzer:
    def __init__(self, df, stock_info):
        self.df = df.copy()
        self.stock_info = stock_info
        self.logs = []
        
        code = stock_info.get('code', 'UNKNOWN')
        name = stock_info.get('name', '未知股票')
        header = f"*🚀 A 股量化深度分析报告*\n"
        header += f"# {name} ({code})\n"
        header += f"`🕒 生成时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}`\n"
        header += "\n"
        self.logs.append(header)

        if '成交时间' in self.df.columns:
            self.df['成交时间'] = pd.to_datetime(self.df['成交时间'])
            self.df.set_index('成交时间', inplace=True)
            self.df.sort_index(inplace=True)
        
        self.df['raw_type'] = self.df['性质'].astype(str)
        self.df['type_code'] = 0
        self.df.loc[self.df['raw_type'].str.contains('买', na=False), 'type_code'] = 1
        self.df.loc[self.df['raw_type'].str.contains('卖', na=False), 'type_code'] = -1
        
        if 'price_shift' not in self.df.columns:
            self.df['price_shift'] = self.df['成交价格'].shift(-1) - self.df['成交价格']
        neutral_mask = (self.df['type_code'] == 0)
        self.df.loc[neutral_mask & (self.df['price_shift'] > 0), 'type_code'] = 1
        self.df.loc[neutral_mask & (self.df['price_shift'] < 0), 'type_code'] = -1
        
        self._define_order_size()

    def _define_order_size(self):
        if '成交量' not in self.df.columns: return
        avg_vol_hand = self.df['成交量'].mean()
        amt_thresh = 1000000
        vol_thresh_hand = avg_vol_hand * 5
        self.df['is_large'] = (self.df['成交金额'] > amt_thresh) | (self.df['成交量'] > vol_thresh_hand)
        self.df['is_small'] = ~self.df['is_large']

    def _get_time_range(self, start_str, end_str):
        if self.df.empty: return pd.DataFrame()
        today = self.df.index[0].date()
        start = pd.Timestamp(datetime.combine(today, time.fromisoformat(start_str)))
        end = pd.Timestamp(datetime.combine(today, time.fromisoformat(end_str)))
        return self.df[(self.df.index >= start) & (self.df.index <= end)]

    def _log(self, text=""):
        self.logs.append(text)

    def calculate_support_recovery_refined(self, data_segment):
        if len(data_segment) < 10: return 0.0, "数据不足"
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
            if drop_depth <= p_high * 0.003: continue
            recovery_height = p_recovery_max - p_low
            recovery_ratio = recovery_height / drop_depth if drop_depth > 0 else 0
            if p_recovery_max >= p_high: score = 1.0
            elif recovery_ratio > 0.5: score = 0.7
            elif recovery_ratio > 0: score = 0.4
            else: score = 0.0
            scores.append(score)
        if not scores: return 1.0, "无明显回撤或单边上涨"
        return np.mean(scores), f"检测到{len(scores)}次回撤测试"

    def calculate_push_score_custom(self, data_segment):
        price = data_segment['成交价格']
        volume = data_segment['成交量']
        price_diff = price.diff()
        up_vol = volume[price_diff > 0].sum()
        down_vol = volume[price_diff < 0].sum()
        push_ratio = up_vol / (up_vol + down_vol + 1e-6)
        direction_score = min(push_ratio * 20, 20)
        sign = np.sign(price_diff.dropna())
        up_flag = (sign == 1).astype(int)
        if up_flag.empty:
            streak_bonus = 0
            max_up_streak = 0
        else:
            groups = (up_flag != up_flag.shift()).cumsum()
            streak_len = up_flag.groupby(groups).sum()
            max_up_streak = streak_len.max() if not streak_len.empty else 0
            streak_bonus = min(max_up_streak / 10, 1) * 5
        push_score = round(direction_score + streak_bonus, 2)
        return push_score, {
            "push_ratio": round(push_ratio, 3), "max_up_streak": int(max_up_streak),
            "direction_score": round(direction_score, 2), "streak_bonus": round(streak_bonus, 2), "total_score": push_score
        }

    def analyze_period(self, name, start_time, end_time):
        self._log(f"\n### 🕒 时段分析：{name}")
        self._log(f"`⏱️ 区间：{start_time} - {end_time}`")
        self._log("---")
        data = self._get_time_range(start_time, end_time)
        if data.empty:
            self._log("> ⚠️ 该时段无数据")
            return

        buy_amt = data[data['type_code'] == 1]['成交金额'].sum()
        sell_amt = data[data['type_code'] == -1]['成交金额'].sum()
        net_flow = buy_amt - sell_amt
        ratio = buy_amt / sell_amt if sell_amt > 0 else np.inf
        
        self._log(f"**💰 资金流向**")
        self._log(f"- 净主动流：`{net_flow:,.0f}`")
        self._log(f"- 买卖比率：`{ratio:.2f}`")

        l_buy = data[(data['is_large']) & (data['type_code']==1)]['成交金额'].sum()
        l_sell = data[(data['is_large']) & (data['type_code']==-1)]['成交金额'].sum()
        s_buy = data[(data['is_small']) & (data['type_code']==1)]['成交金额'].sum()
        s_sell = data[(data['is_small']) & (data['type_code']==-1)]['成交金额'].sum()
        
        self._log(f"\n**📊 订单结构拆解**")
        self._log(f"- 🐋 大单净流入：`{l_buy - l_sell:,.0f}`")
        self._log(f"- 🐜 小单净流入：`{s_buy - s_sell:,.0f}`")

        support_score, support_msg = self.calculate_support_recovery_refined(data)
        if support_score == 1.0: desc = "极强 (完全收复)"
        elif support_score >= 0.8: desc = "中等 (收复>50%)"
        elif support_score >= 0.5: desc = "弱 (仅止跌)"
        else: desc = "极弱 (继续下跌)"
        
        self._log(f"\n**🛡️ 承接力评分**")
        self._log(f"- 得分：`{support_score:.1f}` ({support_msg})")
        self._log(f"> 💡 评价：{desc}")

        push_score, details = self.calculate_push_score_custom(data)
        self._log(f"\n**🎯 推价有效性评分**")
        self._log(f"- 总分：`{push_score:.2f} / 25.00`")
        self._log(f"- 细节分析：")
        self._log(f"  • 上涨成交量占比：`{details['push_ratio']:.1%}` (得分 `{details['direction_score']}/20`)")
        self._log(f"  • 最长连续上涨：`{details['max_up_streak']}` 笔 (奖励 `{details['streak_bonus']}/5`)")
        
        if push_score >= 20: verdict = "✅ 极强推升 (量价齐升，趋势连贯)"
        elif push_score >= 15: verdict = "🟢 良性推升 (多头占优)"
        elif push_score >= 10: verdict = "⚖️ 震荡整理 (多空交织)"
        else: verdict = "🔴 弱势推升 (下跌放量或连续下跌)"
        self._log(f"> 💡 结论：{verdict}")

        o_p, c_p = data['成交价格'].iloc[0], data['成交价格'].iloc[-1]
        status_text = "📈 涨" if c_p > o_p else "📉 跌"
        self._log(f"\n**📈 区间表现**")
        self._log(f"- 状态：{status_text}")
        self._log(f"- 价格：`{o_p:.2f}` → `{c_p:.2f}`")
    def detect_main_force_behavior(self):
        df = self.df.copy()
        if df.empty:
            return "无法判断", {}

        # ===== 1 资金结构 =====
        large_buy = df[(df['is_large']) & (df['type_code'] == 1)]['成交金额'].sum()
        large_sell = df[(df['is_large']) & (df['type_code'] == -1)]['成交金额'].sum()

        small_buy = df[(df['is_small']) & (df['type_code'] == 1)]['成交金额'].sum()
        small_sell = df[(df['is_small']) & (df['type_code'] == -1)]['成交金额'].sum()

        large_net = large_buy - large_sell
        small_net = small_buy - small_sell

        total_amt = df['成交金额'].sum()

        # ===== 2 主力参与度 =====
        large_ratio = (large_buy + large_sell) / total_amt if total_amt > 0 else 0

        # ===== 3 价格变化 =====
        open_price = df['成交价格'].iloc[0]
        close_price = df['成交价格'].iloc[-1]

        price_change = (close_price - open_price) / open_price

        # ===== 行为判断 =====

        # 主力吸筹
        if large_net > 0 and price_change < 0.01:
            behavior = "🟢 主力吸筹"

        # 主力拉升
        elif large_net > 0 and price_change > 0.02:
            behavior = "🚀 主力拉升"

        # 主力派发
        elif large_net < 0 and price_change > 0:
            behavior = "⚠️ 主力派发"

        # 主力砸盘
        elif large_net < 0 and price_change < -0.02:
            behavior = "🔴 主力砸盘"

        # 对倒
        elif large_ratio > 0.5 and abs(large_net) < total_amt * 0.02:
            behavior = "🌀 主力对倒"

        else:
            behavior = "⚖️ 多空博弈"

        details = {
            "large_net": large_net,
            "small_net": small_net,
            "large_ratio": large_ratio,
            "price_change": price_change
        }

        return behavior, details
    def detect_main_force_action(self):

        df = self.df.copy()

        large_buy = df[(df['is_large']) & (df['type_code']==1)]['成交金额'].sum()
        large_sell = df[(df['is_large']) & (df['type_code']==-1)]['成交金额'].sum()

        small_buy = df[(df['is_small']) & (df['type_code']==1)]['成交金额'].sum()
        small_sell = df[(df['is_small']) & (df['type_code']==-1)]['成交金额'].sum()

        large_net = large_buy - large_sell
        small_net = small_buy - small_sell

        total_amt = df['成交金额'].sum()

        large_ratio = (large_buy + large_sell) / total_amt if total_amt>0 else 0

        open_price = df['成交价格'].iloc[0]
        close_price = df['成交价格'].iloc[-1]

        price_change = (close_price-open_price)/open_price

        push_score,_ = self.calculate_push_score_custom(df)

        # ===== 动作判断 =====

        # 吸筹
        if large_net>0 and price_change<0.01 and large_ratio>0.3:
            action="🟢 主力吸筹"

        # 洗盘
        elif large_net<0 and abs(price_change)<0.02 and large_ratio>0.35:
            action="🧹 主力洗盘"

        # 试盘
        elif large_net>0 and 0.01<price_change<0.03 and push_score<12:
            action="🧪 主力试盘"

        # 拉升
        elif large_net>0 and price_change>0.03 and push_score>=15:
            action="🚀 主力拉升"

        # 派发
        elif large_net<0 and price_change>0.01:
            action="📦 主力派发"

        # 对倒
        elif large_ratio>0.5 and abs(large_net)<total_amt*0.02:
            action="🔁 主力对倒"

        else:
            action="⚖️ 多空博弈"

        details={
            "large_net":large_net,
            "small_net":small_net,
            "large_ratio":large_ratio,
            "price_change":price_change,
            "push_score":push_score
        }

        return action,details
    def detect_main_force_strategy(self):

        df = self.df.copy()

        large_buy = df[(df['is_large']) & (df['type_code']==1)]['成交金额'].sum()
        large_sell = df[(df['is_large']) & (df['type_code']==-1)]['成交金额'].sum()

        large_net = large_buy - large_sell

        total_amt = df['成交金额'].sum()

        large_ratio = (large_buy+large_sell)/total_amt if total_amt>0 else 0

        open_p = df['成交价格'].iloc[0]
        close_p = df['成交价格'].iloc[-1]

        price_change = (close_p-open_p)/open_p

        volatility = df['成交价格'].pct_change().std()

        push_score,_ = self.calculate_push_score_custom(df)

        # ===== 吸筹 =====

        if large_net>0 and abs(price_change)<0.01:

            if volatility<0.002:
                strategy="🟢 横盘吸筹"

            elif price_change<0:
                strategy="🟢 打压吸筹"

            else:
                strategy="🟢 拉高吸筹"

        # ===== 洗盘 =====

        elif large_net<0 and abs(price_change)<0.02:

            if volatility>0.01:
                strategy="🧹 震荡洗盘"

            elif price_change<-0.02:
                strategy="🧹 急跌洗盘"

            else:
                strategy="🧹 阶梯洗盘"

        # ===== 试盘 =====

        elif large_net>0 and push_score<12:

            if price_change>0:
                strategy="🧪 拉高试盘"
            else:
                strategy="🧪 打压试盘"

        # ===== 拉升 =====

        elif large_net>0 and push_score>=15:

            if price_change>0.04:
                strategy="🚀 主升拉升"
            else:
                strategy="🚀 突破拉升"

        # ===== 派发 =====

        elif large_net<0 and price_change>0:

            if volatility<0.005:
                strategy="📦 横盘派发"
            else:
                strategy="📦 拉高派发"

        # ===== 对倒 =====

        elif large_ratio>0.5 and abs(large_net)<total_amt*0.02:

            strategy="🔁 对倒控盘"

        else:

            strategy="⚖️ 多空博弈"

        return strategy



    def run_full_analysis(self):
        self.analyze_period("开盘后15分钟", "09:20:00", "09:45:00")
        self.analyze_period("收盘前15分钟", "14:45:00", "15:00:00")
        
        self._log(f"\n---\n")
        self._log("## 🌍 全天交易全景总览")
        self._log("---")
        
        data = self.df
        if data.empty:
            self._log("> ⚠️ 全天无数据")
            return self.get_report(), None, None

        total_amt = data['成交金额'].sum()
        vwap = total_amt / (data['成交量'].sum() * 100) if data['成交量'].sum() > 0 else 0
        
        all_l_net = data[(data['is_large']) & (data['type_code']==1)]['成交金额'].sum() - \
                    data[(data['is_large']) & (data['type_code']==-1)]['成交金额'].sum()
        all_s_net = data[(data['is_small']) & (data['type_code']==1)]['成交金额'].sum() - \
                    data[(data['is_small']) & (data['type_code']==-1)]['成交金额'].sum()
        
        self._log(f"**📊 基础数据**")
        self._log(f"- 总成交额：`{total_amt/10000:.1f}万`")
        self._log(f"- 均价 (VWAP)：`{vwap:.2f}`")
        
        self._log(f"\n**🐋 主力动向**")
        self._log(f"- 全天大单净额：`{all_l_net:,.0f}`")
        self._log(f"- 全天小单净额：`{all_s_net:,.0f}`")
        
        sup_score, sup_msg = self.calculate_support_recovery_refined(data)
        push_score, details = self.calculate_push_score_custom(data)
        
        self._log(f"\n**📈 核心评分汇总**")
        self._log(f"- 🛡️ 全天承接力：`{sup_score:.1f}`")
        self._log(f"- 🎯 全天推价有效性：`{push_score:.2f} / 25.00`")
        self._log(f"  *(细节：上涨占比 `{details['push_ratio']:.1%}`, 连涨 `{details['max_up_streak']}` 笔)*")
        


        action,details = self.detect_main_force_action()

        self._log("\n**🎬 主力动作识别**")

        self._log(f"- 动作判断：`{action}`")

        self._log(f"- 主力净流：`{details['large_net']:,.0f}`")
        self._log(f"- 散户净流：`{details['small_net']:,.0f}`")
        self._log(f"- 主力参与度：`{details['large_ratio']:.1%}`")

        self._log(f"- 推价评分：`{details['push_score']}`")
        self._log(f"- 日内涨跌：`{details['price_change']:.2%}`")
        
        behavior, details = self.detect_main_force_behavior()

        self._log(f"\n**🧠 主力行为识别**")
        self._log(f"- 行为判断：`{behavior}`")
        self._log(f"- 大单净流：`{details['large_net']:,.0f}`")
        self._log(f"- 小单净流：`{details['small_net']:,.0f}`")
        self._log(f"- 主力参与度：`{details['large_ratio']:.1%}`")
        self._log(f"- 日内涨跌：`{details['price_change']:.2%}`")

        strategy = self.detect_main_force_strategy()

        self._log("\n**🎯 主力策略识别**")
        self._log(f"- 当前策略：`{strategy}`")


        conclusion = ""
        icon = ""
        if all_l_net > 0 and sup_score >= 0.7 and push_score >= 15:
            conclusion = "【强烈看好】主力流入，承接有力，量价配合完美。"
            icon = "🌟"
        elif all_l_net > 0 and push_score < 10:
            conclusion = "【警惕诱多】主力虽买入但推升无力（下跌放量或缺乏连续性），需防回落。"
            icon = "⚠️"
        elif sup_score == 0.0:
            conclusion = "【风险警示】承接力崩溃，跌破无反抽。"
            icon = "📉"
        else:
            conclusion = "【震荡观察】多空博弈，等待方向选择。"
            icon = "⚖️"
        


        self._log(f"\n---")
        self._log(f"## {icon} 综合研判结论")
        self._log("---")
        self._log(f">>> **{conclusion}**")
        
        report_text = self.get_report()
        
        density_img = self._generate_density_plot()
        fundflow_img = self._generate_fundflow_plot()

        return report_text, density_img, fundflow_img

    def get_report(self, format_type="markdown"):
        if not self.logs: return "暂无分析报告生成。"
        return "\n".join(self.logs)

    # =========================================================
    # 【修改版】生成成交密度曲线图 (按价格 1% 分箱)
    # =========================================================
    def _generate_density_plot(self):
        try:
            df = self.df.copy()
            if df.empty: return None
            
            min_price = df['成交价格'].min()
            max_price = df['成交价格'].max()
            price_range = max_price - min_price
            
            # 【修改点 2】按价格 1% 进行分箱
            # 步长 = 价格范围 * 1%
            step = price_range * 0.01
            # 确保步长不为0，且至少有一个区间
            if step == 0: step = 0.01
            
            bins = np.arange(min_price, max_price + step, step)
            
            # 分箱并统计成交量
            df['price_bin'] = pd.cut(df['成交价格'], bins=bins, include_lowest=True)
            vol_by_price = df.groupby('price_bin', observed=True)['成交量'].sum()
            
            # 获取每个 bin 的中心价格作为 Y 轴标签
            bin_centers = [(interval.left + interval.right) / 2 for interval in vol_by_price.index]
            
            plt.figure(figsize=(10, 8))
            # 绘制横向柱状图
            plt.barh(bin_centers, vol_by_price.values, height=step*0.8, color='#1f77b4', alpha=0.7, edgecolor='white')
            
            # 【修改点 4】明确标注坐标轴
            plt.xlabel('成交量 (手)', fontsize=12, fontweight='bold')
            plt.ylabel('价格 (元)', fontsize=12, fontweight='bold')
            plt.title(f'{self.stock_info.get("name", "")} 成交密度分布 (按价格 1% 分档)', fontsize=14, fontweight='bold')
            
            # 格式化 Y 轴价格为 2 位小数
            from matplotlib.ticker import FormatStrFormatter
            plt.gca().yaxis.set_major_formatter(FormatStrFormatter('%.2f'))
            
            plt.grid(axis='x', linestyle='--', alpha=0.5)
            plt.tight_layout()
            
            img_buffer = BytesIO()
            plt.savefig(img_buffer, format='png', dpi=150, bbox_inches='tight')
            plt.close()
            img_buffer.seek(0)
            return img_buffer
        except Exception as e:
            logger.error(f"生成成交密度图失败: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return None

    # =========================================================
    # 【修改版】生成分时资金曲线图 (单位转中文)
    # =========================================================
    def _generate_fundflow_plot(self):
        try:
            df = self.df.copy()
            if df.empty: return None
            
            # 计算每笔净流
            df['net_flow'] = df.apply(
                lambda row: row['成交金额'] if row['type_code'] == 1 else 
                           (-row['成交金额'] if row['type_code'] == -1 else 0),
                axis=1
            )
            df['cum_net_flow'] = df['net_flow'].cumsum()
            
            # 【修改点 3】单位转换 (元 -> 万/亿)
            max_val = abs(df['cum_net_flow']).max()
            if max_val >= 100000000:
                df['plot_flow'] = df['cum_net_flow'] / 100000000
                unit_label = '亿元'
                fmt_str = '%.2f'
            elif max_val >= 10000:
                df['plot_flow'] = df['cum_net_flow'] / 10000
                unit_label = '万元'
                fmt_str = '%.1f'
            else:
                df['plot_flow'] = df['cum_net_flow']
                unit_label = '元'
                fmt_str = '%.0f'
            
            plt.figure(figsize=(12, 6))
            plt.plot(df.index, df['plot_flow'], linewidth=2.5, color='#ff7f0e', label='累计净主动资金')
            plt.axhline(y=0, color='gray', linestyle='--', alpha=0.5)
            
            # 填充颜色
            plt.fill_between(df.index, df['plot_flow'], 0, 
                            where=(df['plot_flow'] >= 0), color='#ff7f0e', alpha=0.3)
            plt.fill_between(df.index, df['plot_flow'], 0, 
                            where=(df['plot_flow'] < 0), color='#1f77b4', alpha=0.3)
            
            # 【修改点 4】明确标注坐标轴
            plt.xlabel('交易时间', fontsize=12, fontweight='bold')
            plt.ylabel(f'累计净主动资金 ({unit_label})', fontsize=12, fontweight='bold')
            plt.title(f'{self.stock_info.get("name", "")} 分时资金流向图', fontsize=14, fontweight='bold')
            
            # 设置 X 轴时间格式
            plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
            plt.gca().xaxis.set_major_locator(mdates.HourLocator(interval=1))
            plt.xticks(rotation=45)
            
            # Y 轴格式化
            from matplotlib.ticker import FormatStrFormatter
            plt.gca().yaxis.set_major_formatter(FormatStrFormatter(fmt_str))
            
            plt.grid(True, linestyle=':', alpha=0.6)
            plt.legend(loc='upper left')
            plt.tight_layout()
            
            img_buffer = BytesIO()
            plt.savefig(img_buffer, format='png', dpi=150, bbox_inches='tight')
            plt.close()
            img_buffer.seek(0)
            return img_buffer
        except Exception as e:
            logger.error(f"生成分时资金图失败: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return None

def send_telegram_message_with_images(token, chat_id, text, image_buffers, proxy_url=None):
    base_url = f"https://api.telegram.org/bot{token}"
    proxies = {"http": proxy_url, "https": proxy_url} if proxy_url else None
    
    # 1. 发送文字
    if text:
        text_payload = {"chat_id": chat_id, "text": text, "parse_mode": "Markdown"}
        try:
            resp = requests.post(f"{base_url}/sendMessage", json=text_payload, proxies=proxies, timeout=15)
            if not resp.json().get("ok"):
                logger.warning(f"文字消息发送失败：{resp.text}")
        except Exception as e:
            logger.error(f"发送文字消息异常：{e}")
    
    # 2. 发送图片
    if image_buffers:
        for i, img_buffer in enumerate(image_buffers):
            if img_buffer is None: continue
            try:
                img_buffer.seek(0)
                files = {'photo': ('chart.png', img_buffer, 'image/png')}
                data = {'chat_id': chat_id}
                resp = requests.post(f"{base_url}/sendPhoto", data=data, files=files, proxies=proxies, timeout=30)
                if resp.json().get("ok"):
                    logger.info(f"✅ 图表 {i+1} 发送成功")
                else:
                    logger.error(f"❌ 图表 {i+1} 发送失败：{resp.text}")
            except Exception as e:
                logger.error(f"❌ 发送图表 {i+1} 异常：{e}")
            finally:
                img_buffer.close()

if __name__ == "__main__":
    TG_TOKEN = "8760053592:AAGt8DcQ9_5Gu1OhwWYWtYz1IkHYHFXxL20"
    TG_CHAT_ID = "-1003787641029"
    PROXY_URL = "http://127.0.0.1:7890" 
    # dfs=filter_stock.filer_stock()
    dfs=pd.read_sql("select * from gp.stock_abnormal_monitor_analysis where need_to_analysis=1", con=engine)

    # dfs = pd.read_excel("GP/prod_online/script/ones.xlsx")
    print(dfs.index.size,dfs.iloc[0,:])
    today = datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)
    # dfs['日期']=dfs['日期'].
    # dfs=dfs.loc[dfs['日期'].astype(str) == today.strftime('%Y-%m-%d')]

    print(today.strftime('%Y-%m-%d'))

    if dfs.empty:
        print("今日无数据")
        exit()
    for k,v in dfs.iterrows():

        stock_code = v['stock_code']
        stock_name = v['stock_name']

        print(f"正在获取 {stock_name} ({stock_code}) 数据...")
        
        try:
            df = ak.stock_zh_a_tick_tx_js(symbol=stock_code)
        except Exception as e:
            print(f"数据获取失败：{e}")
            exit()

        analyzer = FinalQuantAnalyzer(df, stock_info={'code': stock_code, 'name': stock_name})
        report_text, density_img, fundflow_img = analyzer.run_full_analysis()
        
        print("\n--- 报告生成完成，正在发送至 Telegram ---")
        
        images_to_send = []
        if density_img: images_to_send.append(density_img)
        if fundflow_img: images_to_send.append(fundflow_img)
        
        send_telegram_message_with_images(TG_TOKEN, TG_CHAT_ID, report_text, images_to_send, proxy_url=PROXY_URL)
        
        print("✅ 报告及图表已成功推送至 Telegram!")