from psycopg2 import extras

from database import history_1d, power
from database.finance import fin
from env import read_conn
from policy.common import grow, rpa, vfr


def finance_today(read, date):
    cursor = read.cursor(cursor_factory=extras.DictCursor)
    cursor.execute(
        'SELECT symbol FROM finance t0 WHERE date=(SELECT DISTINCT t1.date FROM finance t1 WHERE t1.date<=%s ORDER BY date DESC LIMIT 1)',
        (date,))
    rows = cursor.fetchall()
    cursor.close()
    return rows


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


def alive(read, date):
    cursor = read.cursor(cursor_factory=extras.DictCursor)
    cursor.execute('SELECT symbol, bob, bpol FROM power WHERE bob <= %s AND (eob > %s OR eob IS NULL)', (date, date))
    rows = cursor.fetchall()
    cursor.close()
    return rows


def score(read, symbol, date):
    return vfr(read, symbol, date) / rpa(read, symbol, date, 'pe')


def price(read, symbol, date0, date1):
    return min([float(history_1d.close_r1(read, symbol, date0))] \
               + list(map(lambda x: float(x['close']), history_1d.all_close(read, symbol, date0, date1))))
    # return min([float(history_1d.close_r1(read, symbol, date1)), float(history_1d.close(read, symbol, date1))])
    # return float(history_1d.close(read, symbol, date1))


def buy(context, date):
    read = read_conn()
    # 计算i5至i250（市场大局风向标）
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
    # 从dict构建result数组
    dicts_today = alive_today(read, date)
    dicts_history = alive_history(read, date)
    result_today = []
    result_history = []
    if i250:
        for dict in list(filter(lambda x: x['bpol'] == 'p250', dicts_today)):
            result_today.append({'symbol': dict['symbol'], 'policy': dict['bpol'],
                                 'price': price(read, dict['symbol'], dict['bob'], date),
                                 'score': score(read, dict['symbol'], date)})
        for dict in list(filter(lambda x: x['bpol'] == 'p250', dicts_history)):
            result_history.append({'symbol': dict['symbol'], 'policy': dict['bpol'],
                                   'price': price(read, dict['symbol'], dict['bob'], date),
                                   'score': score(read, dict['symbol'], date)})
    if i120:
        for dict in list(filter(lambda x: x['bpol'] == 'p120', dicts_today)):
            result_today.append({'symbol': dict['symbol'], 'policy': dict['bpol'],
                                 'price': price(read, dict['symbol'], dict['bob'], date),
                                 'score': score(read, dict['symbol'], date)})
        for dict in list(filter(lambda x: x['bpol'] == 'p120', dicts_history)):
            result_history.append({'symbol': dict['symbol'], 'policy': dict['bpol'],
                                   'price': price(read, dict['symbol'], dict['bob'], date),
                                   'score': score(read, dict['symbol'], date)})
    if i60:
        for dict in list(filter(lambda x: x['bpol'] == 'p60', dicts_today)):
            result_today.append({'symbol': dict['symbol'], 'policy': dict['bpol'],
                                 'price': price(read, dict['symbol'], dict['bob'], date),
                                 'score': score(read, dict['symbol'], date)})
        for dict in list(filter(lambda x: x['bpol'] == 'p60', dicts_history)):
            result_history.append({'symbol': dict['symbol'], 'policy': dict['bpol'],
                                   'price': price(read, dict['symbol'], dict['bob'], date),
                                   'score': score(read, dict['symbol'], date)})
    if i20:
        for dict in list(filter(lambda x: x['bpol'] == 'p20', dicts_today)):
            result_today.append({'symbol': dict['symbol'], 'policy': dict['bpol'],
                                 'price': price(read, dict['symbol'], dict['bob'], date),
                                 'score': score(read, dict['symbol'], date)})
        for dict in list(filter(lambda x: x['bpol'] == 'p20', dicts_history)):
            result_history.append({'symbol': dict['symbol'], 'policy': dict['bpol'],
                                   'price': price(read, dict['symbol'], dict['bob'], date),
                                   'score': score(read, dict['symbol'], date)})
    if i10:
        for dict in list(filter(lambda x: x['bpol'] == 'p10', dicts_today)):
            result_today.append({'symbol': dict['symbol'], 'policy': dict['bpol'],
                                 'price': price(read, dict['symbol'], dict['bob'], date),
                                 'score': score(read, dict['symbol'], date)})
        for dict in list(filter(lambda x: x['bpol'] == 'p10', dicts_history)):
            result_history.append({'symbol': dict['symbol'], 'policy': dict['bpol'],
                                   'price': price(read, dict['symbol'], dict['bob'], date),
                                   'score': score(read, dict['symbol'], date)})
    if i5:
        for dict in list(filter(lambda x: x['bpol'] == 'p5', dicts_today)):
            result_today.append({'symbol': dict['symbol'], 'policy': dict['bpol'],
                                 'price': price(read, dict['symbol'], dict['bob'], date),
                                 'score': score(read, dict['symbol'], date)})
        for dict in list(filter(lambda x: x['bpol'] == 'p5', dicts_history)):
            result_history.append({'symbol': dict['symbol'], 'policy': dict['bpol'],
                                   'price': price(read, dict['symbol'], dict['bob'], date),
                                   'score': score(read, dict['symbol'], date)})
    # 排序
    result_today.sort(key=lambda x: x['score'], reverse=True)
    result_history.sort(key=lambda x: x['score'], reverse=True)
    result = result_today + result_history
    # 按finance过滤
    # finance = list(map(lambda x: x['symbol'], finance_today(read, date)))
    result = list(filter(lambda x: fin(read, x['symbol'], date), result))
    # result = list(filter(lambda x: rate(read, x['symbol'], date) > 1, result))

    result = list(filter(lambda x: grow(read, x['symbol'], date) > 1, result))
    result = list(filter(lambda x: rpa(read, x['symbol'], date, 'pe') < 0.5, result))
    result = list(filter(lambda x: not x['symbol'].startswith('SHSE.9') and
                                   not x['symbol'].startswith('SZSE.2'), result))
    read.close()
    return result
