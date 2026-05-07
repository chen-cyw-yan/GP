from mootdx.reader import Reader

# 创建读取器实例
reader = Reader.factory(market='std', tdxdir='C:/new_tdx')

# 读取日线数据
daily_data = reader.daily(symbol='600036')
minute_data= reader.minute(symbol='600036', suffix='1')
block_data=reader.block(symbol='block_zs',group=True)
print(daily_data)

print(minute_data)

print(block_data)

# 个股日线
from mootdx.quotes import Quotes
client = Quotes.factory(market='std')
print(client.minutes(symbol='000009', date='20260507'))


# 板块日线
print('板块日线')
from mootdx.quotes import Quotes
from mootdx.consts import MARKET_SH
client = Quotes.factory(market='std')
client.index(frequency=9, market=MARKET_SH, symbol='000001', start=1, offset=2)