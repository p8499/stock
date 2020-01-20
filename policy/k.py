from gm.enum import PositionSide_Long

from env import read_conn
from policy.common import rpa, vfr


def score(read, symbol, date):
    return vfr(read, symbol, date) / rpa(read, symbol, date, 'pe')


def keep(context, date):
    # 构建result数组
    result = []
    read = read_conn()
    l_pos_symbols = list(map(lambda x: x['symbol'], context.account().positions(side=PositionSide_Long)))
    for symbol in l_pos_symbols:
        result.append({'symbol': symbol, 'score': score(read, symbol, date)})
    read.close()
    # 排序
    result.sort(key=lambda x: x['score'], reverse=True)
    return result
