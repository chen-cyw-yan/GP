import akshare as ak

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