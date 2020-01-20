from psycopg2 import extras

from database import industry_constituents
from env import read_conn, write_conn


def fin(read, symbol, date):
    cursor = read.cursor()
    cursor.execute('SELECT fin(%s,%s)', (symbol, date))
    row = cursor.fetchone()
    cursor.close()
    return row[0]


def candicates(read, date):
    cursor = read.cursor(cursor_factory=extras.DictCursor)
    cursor.execute('SELECT symbol FROM finance WHERE date=%s', (date,))
    rows = cursor.fetchall()
    cursor.close()
    return rows


def max_date(read):
    cursor = read.cursor()
    cursor.execute('SELECT max(date) FROM finance')
    row = cursor.fetchone()
    cursor.close()
    return row[0]


def todo_dates(read):
    max = max_date(read)
    cursor = read.cursor()
    if max is None:
        cursor.execute('SELECT DISTINCT eob FROM history_1d WHERE eob>=%s ORDER BY eob ASC', ('2018-01-01',))
    else:
        cursor.execute('SELECT DISTINCT eob FROM history_1d WHERE eob>=%s ORDER BY eob ASC', (max,))
    rows = cursor.fetchall()
    cursor.close()
    return list(map(lambda x: x[0], rows))


def increment_build():
    count = 0
    read = read_conn()
    write = write_conn()
    cursor = write.cursor()
    dates = todo_dates(read)
    symbols = industry_constituents.symbols(read)
    for i, date in enumerate(dates):
        for j, symbol in enumerate(symbols):
            print('\rBuilding finance: %.2f%%' % ((i * len(symbols) + j) * 100 / (len(dates) * len(symbols))), end='')
            if symbol not in list(map(lambda x: x['symbol'], candicates(read, date))) and fin(read, symbol, date):
                cursor.execute('INSERT INTO finance(symbol, date) VALUES (%s,%s)', (symbol, date))
                count += 1
    cursor.close()
    write.close()
    read.close()
    print('\rBuilding finance: Finish')
    return count
