from gm.enum import PositionSide_Long

from database.finance import fin
from env import read_conn


def sell(context, date):
    result = []
    read = read_conn()
    l_pos_symbols = list(map(lambda x: x['symbol'], context.account().positions(side=PositionSide_Long)))
    for symbol in l_pos_symbols:
        if not fin(read, symbol, date):
            result.append({'symbol': symbol, 'policy': 'sf'})
    read.close()
    return result
