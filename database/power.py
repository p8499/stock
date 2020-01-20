from psycopg2 import extras

from database import industry_constituents
from env import read_conn, write_conn


def pow(read, date, bpol):
    cursor = read.cursor()
    cursor.execute('SELECT pow(%s,%s)', (date, bpol))
    row = cursor.fetchone()
    cursor.close()
    return row[0]


def max_date(read):
    cursor = read.cursor()
    cursor.execute('SELECT max(bob) FROM power')
    row = cursor.fetchone()
    cursor.close()
    return row[0]


def todo_dates_buy(read):
    max = max_date(read)
    cursor = read.cursor()
    if max is None:
        cursor.execute('SELECT DISTINCT eob FROM history_1d WHERE eob>=%s ORDER BY eob ASC', ('2018-01-01',))
    else:
        cursor.execute('SELECT DISTINCT eob FROM history_1d WHERE eob>=%s ORDER BY eob ASC', (max,))
    rows = cursor.fetchall()
    cursor.close()
    return list(map(lambda x: x[0], rows))


def positions(read, bob):
    cursor = read.cursor()
    cursor.execute('SELECT symbol FROM power WHERE bob<=%s AND eob IS NULL', (bob,))
    rows = cursor.fetchall()
    cursor.close()
    return list(map(lambda x: x[0], rows))


def p5(read, symbol, date):
    cursor = read.cursor()
    cursor.execute('SELECT p5(%s,%s)', (symbol, date))
    row = cursor.fetchone()
    cursor.close()
    return row[0]


def p10(read, symbol, date):
    cursor = read.cursor()
    cursor.execute('SELECT p10(%s,%s)', (symbol, date))
    row = cursor.fetchone()
    cursor.close()
    return row[0]


def p20(read, symbol, date):
    cursor = read.cursor()
    cursor.execute('SELECT p20(%s,%s)', (symbol, date))
    row = cursor.fetchone()
    cursor.close()
    return row[0]


def p60(read, symbol, date):
    cursor = read.cursor()
    cursor.execute('SELECT p60(%s,%s)', (symbol, date))
    row = cursor.fetchone()
    cursor.close()
    return row[0]


def p120(read, symbol, date):
    cursor = read.cursor()
    cursor.execute('SELECT p120(%s,%s)', (symbol, date))
    row = cursor.fetchone()
    cursor.close()
    return row[0]


def p250(read, symbol, date):
    cursor = read.cursor()
    cursor.execute('SELECT p250(%s,%s)', (symbol, date))
    row = cursor.fetchone()
    cursor.close()
    return row[0]


def sellables(read, date):
    cursor = read.cursor(cursor_factory=extras.DictCursor)
    cursor.execute('SELECT bob,symbol FROM power WHERE bob<%s AND eob IS NULL', (date,))
    rows = cursor.fetchall()
    cursor.close()
    return rows


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


def increment_build():
    count = 0
    read = read_conn()
    write = write_conn()
    cursor = write.cursor()
    dates = todo_dates_buy(read)
    symbols = industry_constituents.symbols(read)
    for i, date in enumerate(dates):
        for j, symbol in enumerate(symbols):
            print('\rBuilding power.buy: %.2f%%' % ((i * len(symbols) + j) * 100 / (len(dates) * len(symbols))), end='')
            if symbol not in positions(read, date):
                if p250(read, symbol, date):
                    cursor.execute('INSERT INTO power(bob, eob, symbol, bpol, spol) VALUES (%s,%s,%s,%s,%s)',
                                   (date, None, symbol, 'p250', None))
                elif p120(read, symbol, date):
                    cursor.execute('INSERT INTO power(bob, eob, symbol, bpol, spol) VALUES (%s,%s,%s,%s,%s)',
                                   (date, None, symbol, 'p120', None))
                elif p60(read, symbol, date):
                    cursor.execute('INSERT INTO power(bob, eob, symbol, bpol, spol) VALUES (%s,%s,%s,%s,%s)',
                                   (date, None, symbol, 'p60', None))
                elif p20(read, symbol, date):
                    cursor.execute('INSERT INTO power(bob, eob, symbol, bpol, spol) VALUES (%s,%s,%s,%s,%s)',
                                   (date, None, symbol, 'p20', None))
                elif p10(read, symbol, date):
                    cursor.execute('INSERT INTO power(bob, eob, symbol, bpol, spol) VALUES (%s,%s,%s,%s,%s)',
                                   (date, None, symbol, 'p10', None))
                elif p5(read, symbol, date):
                    cursor.execute('INSERT INTO power(bob, eob, symbol, bpol, spol) VALUES (%s,%s,%s,%s,%s)',
                                   (date, None, symbol, 'p5', None))
        dicts = sellables(read, date)
        for j, dict in enumerate(dicts):
            if s250(read, dict['symbol'], date):
                cursor.execute('UPDATE power SET eob = %s, spol = %s WHERE bob=%s AND symbol=%s',
                               (date, 's250', dict['bob'], dict['symbol']))
            elif s120(read, dict['symbol'], date):
                cursor.execute('UPDATE power SET eob = %s, spol = %s WHERE bob=%s AND symbol=%s',
                               (date, 's120', dict['bob'], dict['symbol']))
            elif s60(read, dict['symbol'], date):
                cursor.execute('UPDATE power SET eob = %s, spol = %s WHERE bob=%s AND symbol=%s',
                               (date, 's60', dict['bob'], dict['symbol']))
            elif s20(read, dict['symbol'], date):
                cursor.execute('UPDATE power SET eob = %s, spol = %s WHERE bob=%s AND symbol=%s',
                               (date, 's20', dict['bob'], dict['symbol']))
            elif s10(read, dict['symbol'], date):
                cursor.execute('UPDATE power SET eob = %s, spol = %s WHERE bob=%s AND symbol=%s',
                               (date, 's10', dict['bob'], dict['symbol']))
            elif s5(read, dict['symbol'], date):
                cursor.execute('UPDATE power SET eob = %s, spol = %s WHERE bob=%s AND symbol=%s',
                               (date, 's5', dict['bob'], dict['symbol']))
    cursor.close()
    write.close()
    read.close()
    print('\rBuilding power: Finish')
    return count
