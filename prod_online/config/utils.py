import akshare as ak
from datetime import datetime
def is_trading_day_ak(date_str):
    try:
        df = ak.tool_trade_date_hist_sina()
        # print(df)
        trading_days = df['trade_date'].astype(str).tolist()
        # target = date_str.replace('-', '')
        return date_str in trading_days
    except Exception as e:
        logger.error(f"检查交易日历失败: {e}")
        return False

def get_prev_n_trading_days(n=5):
    """
    获取当前日期之前的 n 个交易日
    """

    try:

        # 获取交易日历
        df = ak.tool_trade_date_hist_sina()

        trading_days = (
            df["trade_date"]
            .astype(str)
            .sort_values()
            .tolist()
        )

        today = datetime.now().strftime("%Y-%m-%d")

        # 找到 <= 今天的最后一个交易日
        valid_days = [d for d in trading_days if d <= today]

        if len(valid_days) == 0:
            return []

        # 最近交易日
        last_day = valid_days[-1]

        idx = trading_days.index(last_day)

        start = max(0, idx - n)

        return trading_days[start:idx]

    except Exception as e:

        logger.error(f"获取交易日失败: {e}")
        return []
    
if __name__ == '__main__':
    print(get_prev_n_trading_days())