from mootdx.reader import Reader

reader = Reader.factory(market='std', tdxdir='C:/new_tdx')
reader.block(symbol='block_zs', group=False)