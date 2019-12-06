from gm.api import *
from psycopg2 import extras

from database import history_1d, power
from env import read_conn


def alive_today(read, date):
    cursor = read.cursor(cursor_factory=extras.DictCursor)
    cursor.execute('SELECT symbol, bob, bpol FROM power WHERE bob = %s AND (eob > %s OR eob IS NULL)', (date, date))
    rows = cursor.fetchall()
    cursor.close()
    return rows


def alive_history(read, date):
    cursor = read.cursor(cursor_factory=extras.DictCursor)
    cursor.execute('SELECT symbol, bob, bpol FROM power WHERE bob < %s AND (eob > %s OR eob IS NULL)', (date, date))
    rows = cursor.fetchall()
    cursor.close()
    return rows


def rpa(read, symbol, date, kpi):
    cursor = read.cursor()
    cursor.execute('SELECT rpa(%s,%s,%s)', (symbol, date, kpi))
    row = cursor.fetchone()
    cursor.close()
    return row[0]


def finance(read, symbol, date):
    cursor = read.cursor()
    cursor.execute('SELECT finance(%s,%s)', (symbol, date))
    row = cursor.fetchone()
    cursor.close()
    return row[0]


def buy(context, date):
    read = read_conn()
    result_today = []
    result_history = []
    yesterday = history_1d.yesterday(read, date)
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
    i250 = pow250 > pow250r1
    i120 = pow250 >= pow250r1 and pow120 > pow120r1
    i60 = pow250 >= pow250r1 and pow120 >= pow120r1 and pow60 > pow60r1
    i20 = pow250 >= pow250r1 and pow120 >= pow120r1 and pow60 >= pow60r1 and pow20 > pow20r1
    i10 = pow250 >= pow250r1 and pow120 >= pow120r1 and pow60 >= pow60r1 and pow20 >= pow20r1 and pow10 > pow10r1
    i5 = pow250 >= pow250r1 and pow120 >= pow120r1 and pow60 >= pow60r1 and pow20 >= pow20r1 and pow10 >= pow10r1 and pow5 > pow5r1
    dicts_today = alive_today(read, date)
    dicts_history = alive_history(read, date)
    if i250:
        for dict in list(filter(lambda x: x['bpol'] == 'p250', dicts_today)):
            if dict['symbol'] not in list(
                    map(lambda x: x['symbol'], context.account().positions(side=PositionSide_Long))) \
                    and finance(read, dict['symbol'], date):
                result_today.append((dict['symbol'], dict['bpol']))
        for dict in list(filter(lambda x: x['bpol'] == 'p250', dicts_history)):
            if dict['symbol'] not in list(
                    map(lambda x: x['symbol'], context.account().positions(side=PositionSide_Long))) \
                    and finance(read, dict['symbol'], date):
                result_history.append((dict['symbol'], dict['bpol']))
    if i120:
        for dict in list(filter(lambda x: x['bpol'] == 'p120', dicts_today)):
            if dict['symbol'] not in list(
                    map(lambda x: x['symbol'], context.account().positions(side=PositionSide_Long))) \
                    and finance(read, dict['symbol'], date):
                result_today.append((dict['symbol'], dict['bpol']))
        for dict in list(filter(lambda x: x['bpol'] == 'p120', dicts_history)):
            if dict['symbol'] not in list(
                    map(lambda x: x['symbol'], context.account().positions(side=PositionSide_Long))) \
                    and finance(read, dict['symbol'], date):
                result_history.append((dict['symbol'], dict['bpol']))
    if i60:
        for dict in list(filter(lambda x: x['bpol'] == 'p60', dicts_today)):
            if dict['symbol'] not in list(
                    map(lambda x: x['symbol'], context.account().positions(side=PositionSide_Long))) \
                    and finance(read, dict['symbol'], date):
                result_today.append((dict['symbol'], dict['bpol']))
        for dict in list(filter(lambda x: x['bpol'] == 'p60', dicts_history)):
            if dict['symbol'] not in list(
                    map(lambda x: x['symbol'], context.account().positions(side=PositionSide_Long))) \
                    and finance(read, dict['symbol'], date):
                result_history.append((dict['symbol'], dict['bpol']))
    if i20:
        for dict in list(filter(lambda x: x['bpol'] == 'p20', dicts_today)):
            if dict['symbol'] not in list(
                    map(lambda x: x['symbol'], context.account().positions(side=PositionSide_Long))) \
                    and finance(read, dict['symbol'], date):
                result_today.append((dict['symbol'], dict['bpol']))
        for dict in list(filter(lambda x: x['bpol'] == 'p20', dicts_history)):
            if dict['symbol'] not in list(
                    map(lambda x: x['symbol'], context.account().positions(side=PositionSide_Long))) \
                    and finance(read, dict['symbol'], date):
                result_history.append((dict['symbol'], dict['bpol']))
    if i10:
        for dict in list(filter(lambda x: x['bpol'] == 'p10', dicts_today)):
            if dict['symbol'] not in list(
                    map(lambda x: x['symbol'], context.account().positions(side=PositionSide_Long))) \
                    and finance(read, dict['symbol'], date):
                result_today.append((dict['symbol'], dict['bpol']))
        for dict in list(filter(lambda x: x['bpol'] == 'p10', dicts_history)):
            if dict['symbol'] not in list(
                    map(lambda x: x['symbol'], context.account().positions(side=PositionSide_Long))) \
                    and finance(read, dict['symbol'], date):
                result_history.append((dict['symbol'], dict['bpol']))
    if i5:
        for dict in list(filter(lambda x: x['bpol'] == 'p5', dicts_today)):
            if dict['symbol'] not in list(
                    map(lambda x: x['symbol'], context.account().positions(side=PositionSide_Long))) \
                    and finance(read, dict['symbol'], date):
                result_today.append((dict['symbol'], dict['bpol']))
        for dict in list(filter(lambda x: x['bpol'] == 'p5', dicts_history)):
            if dict['symbol'] not in list(
                    map(lambda x: x['symbol'], context.account().positions(side=PositionSide_Long))) \
                    and finance(read, dict['symbol'], date):
                result_history.append((dict['symbol'], dict['bpol']))
    result_today.sort(key=lambda x: rpa(read, x[0], date, 'pe'))
    result_history.sort(key=lambda x: rpa(read, x[0], date, 'pe'))
    read.close()
    return result_today + result_history
