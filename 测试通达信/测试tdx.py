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