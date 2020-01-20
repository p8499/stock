from gm.enum import PositionSide_Long
from psycopg2 import extras

from env import read_conn


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
            result.append({'symbol': symbol, 'policy': 'sv5'})
        elif sv10(read, symbol, date):
            result.append({'symbol': symbol, 'policy': 'sv10'})
        elif sv20(read, symbol, date):
            result.append({'symbol': symbol, 'policy': 'sv20'})
        elif sv60(read, symbol, date):
            result.append({'symbol': symbol, 'policy': 'sv60'})
        elif sv120(read, symbol, date):
            result.append({'symbol': symbol, 'policy': 'sv120'})
        elif sv250(read, symbol, date):
            result.append({'symbol': symbol, 'policy': 'sv250'})
    read.close()
    return result
