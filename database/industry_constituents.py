from gm.api import get_industry

from database import industries
from env import read_conn, write_conn


def exists(read, code, symbol):
    cursor = read.cursor()
    cursor.execute('SELECT count(*) FROM industry_constituents WHERE code=%s AND symbol=%s', (code, symbol))
    row = cursor.fetchone()
    cursor.close()
    return row[0] > 0


def code(read, symbol):
    cursor = read.cursor()
    cursor.execute('SELECT code FROM industry_constituents WHERE symbol=%s', (symbol,))
    row = cursor.fetchone()
    cursor.close()
    return row[0]


def symbols(read):
    cursor = read.cursor()
    cursor.execute('SELECT symbol FROM industry_constituents')
    rows = cursor.fetchall()
    cursor.close()
    return list(map(lambda x: x[0], rows))


def full_build():
    count = 0
    read = read_conn()
    write = write_conn()
    cursor = write.cursor()
    cursor.execute('DELETE FROM industry_constituents')
    codes = industries.codes(read)
    for i, code in enumerate(codes):
        print('\rBuilding industry_constituents: %.2f%%' % (i * 100 / len(codes)), end='')
        symbols = get_industry(code)
        for symbol in symbols:
            if not exists(read, code, symbol):
                cursor.execute('INSERT INTO industry_constituents (code, symbol) VALUES (%s,%s)', (code, symbol))
                count += 1
    cursor.close()
    write.close()
    read.close()
    print('\rBuilding industry_constituents: Finish')
    return count
