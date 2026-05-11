from mootdx.quotes import Quotes

client = Quotes.factory(market='std')

df = client.bars(
    symbol='sz000815',
    frequency=1,
    offset=0,
    count=10
)

print(df)