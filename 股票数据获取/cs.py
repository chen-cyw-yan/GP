from eltdx import TdxClient

with TdxClient() as client:
    # 获取某只股票历史某一天的 09:25 竞价结果
    row = client.get_auction_0925("603203", "2026-05-14")
    print(row.code, row.price, row.volume)
    auction =client.get_call_auction("sh603203",include_raw=True)
    # print(auction)
print(auction.count)
print(auction.items[0].time, auction.items[0].price, auction.items[0].flag)

for i in auction.items:
    print(i.time, i.price, i.flag,i.match,i.unmatched)

print(auction.items[0].raw_hex)