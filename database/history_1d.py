from datetime import timedelta

from dateutil.utils import today
from gm.api import history, ADJUST_NONE
from psycopg2 import extras

from database import instrumentinfos
from env import read_conn, write_conn


def close(read, symbol, date):
    cursor = read.cursor()
    cursor.execute('SELECT close FROM history_1d WHERE symbol=%s AND eob<=%s ORDER BY eob DESC LIMIT 1', (symbol, date))
    row = cursor.fetchone()
    cursor.close()
    return row[0]


def close_r1(read, symbol, date):
    cursor = read.cursor()
    cursor.execute('SELECT close FROM history_1d WHERE symbol=%s AND eob<%s ORDER BY eob DESC LIMIT 1', (symbol, date))
    row = cursor.fetchone()
    cursor.close()
    return row[0]


def close_r2(read, symbol, date):
    cursor = read.cursor()
    cursor.execute(
        'SELECT close FROM (SELECT eob, close FROM history_1d WHERE symbol=%s AND eob<%s ORDER BY eob DESC LIMIT 2) s1 ORDER BY eob ASC LIMIT 1',
        (symbol, date))
    row = cursor.fetchone()
    cursor.close()
    return row[0]


def all_close(read, symbol, bob, eob):
    cursor = read.cursor(cursor_factory=extras.DictCursor)
    cursor.execute('SELECT close FROM history_1d WHERE symbol=%s AND eob BETWEEN %s AND %s', (symbol, bob, eob))
    rows = cursor.fetchall()
    cursor.close()
    return rows


def max_eob(read, symbol):
    cursor = read.cursor()
    cursor.execute('SELECT max(eob) FROM history_1d WHERE symbol=%s', (symbol,))
    row = cursor.fetchone()
    cursor.close()
    return row[0]


def yesterday(read, date):
    cursor = read.cursor()
    cursor.execute("SELECT max(eob) FROM history_1d WHERE eob<%s", (date,))
    row = cursor.fetchone()
    cursor.close()
    return row[0]


def increment_build():
    count = 0
    read = read_conn()
    write = write_conn()
    cursor = write.cursor()
    infos = instrumentinfos.infos(read)
    for i, info in enumerate(infos):
        print('\rBuilding history_1d: %.2f%%' % (i * 100 / len(infos)), end='')
        last_eob = max_eob(read, info['symbol'])
        start = info['listed_date'] if last_eob is None else last_eob + timedelta(days=1)
        while True:
            end = start + timedelta(days=1000)
            bars = history(
                symbol=info['symbol'],
                frequency='1d',
                start_time=start,
                end_time=end,
                fields='bob,eob,open,close,high,low,amount,volume',
                skip_suspended=False,
                fill_missing='Last',
                adjust=ADJUST_NONE)
            bars.sort(key=lambda x: x['eob'])
            for bar in bars:
                cursor.execute(
                    'INSERT INTO history_1d (symbol, bob, eob, open, close, high, low, amount, volume) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)',
                    (info['symbol'],
                     bar['bob'],
                     bar['eob'],
                     bar['open'],
                     bar['close'],
                     bar['high'],
                     bar['low'],
                     bar['amount'],
                     bar['volume']))
                count += 1
            if end < min(info['delisted_date'], today().date()):
                start = end + timedelta(days=1)
            else:
                break
    cursor.close()
    write.close()
    read.close()
    print('\rBuilding history_1d: Finish')
    return count
