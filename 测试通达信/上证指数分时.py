from mootdx.quotes import Quotes

# 初始化客户端
client = Quotes.factory(market='std', bestip=True)

# 获取上证指数（999999）的当日分时数据
# 其他指数同理，如沪深300传入 '000300'
index_minute_data = client.minute(symbol='999999') 

print(index_minute_data)