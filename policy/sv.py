from gm.enum import PositionSide_Long
from psycopg2 import extras

from env import read_conn, c
from policy.p import finance, rpa


def alive(read, date):
    cursor = read.cursor(cursor_factory=extras.DictCursor)
    cursor.execute('SELECT symbol, bob, bpol FROM power WHERE bob <= %s AND (eob > %s OR eob IS NULL)', (date, date))
    rows = cursor.fetchall()
    cursor.close()
    return rows


def sv5(read, symbol, date):
    cursor = read.cursor()
    cursor.execute('SELECT sv5(%s,%s)', (symbol, date))
    row = cursor.fetchone()
    cursor.close()
    return row[0]


def sv10(read, symbol, date):
    cursor = read.cursor()
    cursor.execute('SELECT sv10(%s,%s)', (symbol, date))
    row = cursor.fetchone()
    cursor.close()
    return row[0]


def sv20(read, symbol, date):
    cursor = read.cursor()
    cursor.execute('SELECT sv20(%s,%s)', (symbol, date))
    row = cursor.fetchone()
    cursor.close()
    return row[0]


def sv60(read, symbol, date):
    cursor = read.cursor()
    cursor.execute('SELECT sv60(%s,%s)', (symbol, date))
    row = cursor.fetchone()
    cursor.close()
    return row[0]


def sv120(read, symbol, date):
    cursor = read.cursor()
    cursor.execute('SELECT sv120(%s,%s)', (symbol, date))
    row = cursor.fetchone()
    cursor.close()
    return row[0]


def sv250(read, symbol, date):
    cursor = read.cursor()
    cursor.execute('SELECT sv250(%s,%s)', (symbol, date))
    row = cursor.fetchone()
    cursor.close()
    return row[0]


def sell(context, date):
    result = []
    read = read_conn()

    l_pos_symbols = list(map(lambda x: x['symbol'], context.account().positions(side=PositionSide_Long)))
    for symbol in l_pos_symbols:
        if sv5(read, symbol, date):
            result.append((symbol, 'sv5'))
        elif sv10(read, symbol, date):
            result.append((symbol, 'sv10'))
        elif sv20(read, symbol, date):
            result.append((symbol, 'sv20'))
        elif sv60(read, symbol, date):
            result.append((symbol, 'sv60'))
        elif sv120(read, symbol, date):
            result.append((symbol, 'sv120'))
        elif sv250(read, symbol, date):
            result.append((symbol, 'sv250'))

    # dicts = alive(read, date)
    # references = []
    # for dict in dicts:
    #     if finance(read, dict['symbol'], date):
    #         references.append((dict['symbol'], dict['bpol']))
    # references.sort(key=lambda x: rpa(read, x[0], date, 'pe'))
    # if len(references) > 0:
    #     rpa_ref = rpa(read, references[c - 1 if len(references) >= c else -1][0], date, 'pe')
    #     l_pos_symbols = list(map(lambda x: x['symbol'], context.account().positions(side=PositionSide_Long)))
    #     for symbol in l_pos_symbols:
    #         if rpa(read, symbol, date, 'pe') > rpa_ref:
    #             if sv5(read, symbol, date):
    #                 result.append((symbol, 'sv5'))
    #             elif sv10(read, symbol, date):
    #                 result.append((symbol, 'sv10'))
    #             elif sv20(read, symbol, date):
    #                 result.append((symbol, 'sv20'))
    #             elif sv60(read, symbol, date):
    #                 result.append((symbol, 'sv60'))
    #             elif sv120(read, symbol, date):
    #                 result.append((symbol, 'sv120'))
    #             elif sv250(read, symbol, date):
    #                 result.append((symbol, 'sv250'))
    read.close()
    return result
