import akshare as ak

df = ak.fund_etf_hist_em(
    symbol="588170",       # ETF 代码
    period="daily",        # daily/weekly/monthly
    start_date="20251231",
    end_date="202600114",
    adjust="qfq"           # "qfq"/"hfq"/"" 等复权选项
)
df.to_excel('ETF数据.xlsx',index=False)
