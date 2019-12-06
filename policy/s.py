from gm.api import *

from database import history_1d, power
from env import read_conn


def s5(read, symbol, date):
    cursor = read.cursor()
    cursor.execute('SELECT s5(%s,%s)', (symbol, date))
    row = cursor.fetchone()
    cursor.close()
    return row[0]


def s10(read, symbol, date):
    cursor = read.cursor()
    cursor.execute('SELECT s10(%s,%s)', (symbol, date))
    row = cursor.fetchone()
    cursor.close()
    return row[0]


def s20(read, symbol, date):
    cursor = read.cursor()
    cursor.execute('SELECT s20(%s,%s)', (symbol, date))
    row = cursor.fetchone()
    cursor.close()
    return row[0]


def s60(read, symbol, date):
    cursor = read.cursor()
    cursor.execute('SELECT s60(%s,%s)', (symbol, date))
    row = cursor.fetchone()
    cursor.close()
    return row[0]


def s120(read, symbol, date):
    cursor = read.cursor()
    cursor.execute('SELECT s120(%s,%s)', (symbol, date))
    row = cursor.fetchone()
    cursor.close()
    return row[0]


def s250(read, symbol, date):
    cursor = read.cursor()
    cursor.execute('SELECT s250(%s,%s)', (symbol, date))
    row = cursor.fetchone()
    cursor.close()
    return row[0]


def sell(context, date):
    result = []
    read = read_conn()
    yesterday = history_1d.yesterday(read, date)
    l_pos_symbols = list(map(lambda x: x['symbol'], context.account().positions(side=PositionSide_Long)))
    pow250 = power.pow(read, date, 'p250')
    pow250r1 = power.pow(read, yesterday, 'p250')
    pow120 = power.pow(read, date, 'p120')
    pow120r1 = power.pow(read, yesterday, 'p120')
    pow60 = power.pow(read, date, 'p60')
    pow60r1 = power.pow(read, yesterday, 'p60')
    pow20 = power.pow(read, date, 'p20')
    pow20r1 = power.pow(read, yesterday, 'p20')
    pow10 = power.pow(read, date, 'p10')
    pow10r1 = power.pow(read, yesterday, 'p10')
    pow5 = power.pow(read, date, 'p5')
    pow5r1 = power.pow(read, yesterday, 'p5')
    d5 = pow5 < pow5r1
    d10 = pow5 <= pow5r1 and pow10 < pow10r1
    d20 = pow5 <= pow5r1 and pow10 <= pow10r1 and pow20 < pow20r1
    d60 = pow5 <= pow5r1 and pow10 <= pow10r1 and pow20 <= pow20r1 and pow60 < pow60r1
    d120 = pow5 <= pow5r1 and pow10 <= pow10r1 and pow20 <= pow20r1 and pow60 <= pow60r1 and pow120 < pow120r1
    d250 = pow5 <= pow5r1 and pow10 <= pow10r1 and pow20 <= pow20r1 and pow60 <= pow60r1 and pow120 <= pow120r1 and pow250 < pow250r1
    if d5:
        for symbol in filter(lambda x: x not in result, l_pos_symbols):
            if symbol not in list(map(lambda x: x[0], result)) and s5(read, symbol, date):
                result.append((symbol, 's5'))
    if d10:
        for symbol in filter(lambda x: x not in result, l_pos_symbols):
            if symbol not in list(map(lambda x: x[0], result)) and s10(read, symbol, date):
                result.append((symbol, 's10'))
    if d20:
        for symbol in filter(lambda x: x not in result, l_pos_symbols):
            if symbol not in list(map(lambda x: x[0], result)) and s20(read, symbol, date):
                result.append((symbol, 's20'))
    if d60:
        for symbol in filter(lambda x: x not in result, l_pos_symbols):
            if symbol not in list(map(lambda x: x[0], result)) and s60(read, symbol, date):
                result.append((symbol, 's60'))
    if d120:
        for symbol in filter(lambda x: x not in result, l_pos_symbols):
            if symbol not in list(map(lambda x: x[0], result)) and s120(read, symbol, date):
                result.append((symbol, 's120'))
    if d250:
        for symbol in filter(lambda x: x not in result, l_pos_symbols):
            if symbol not in list(map(lambda x: x[0], result)) and s250(read, symbol, date):
                result.append((symbol, 's250'))
    read.close()
    return result
