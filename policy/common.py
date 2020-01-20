def fin2(read, symbol, date):
    cursor = read.cursor()
    cursor.execute('SELECT fin2(%s,%s)', (symbol, date))
    row = cursor.fetchone()
    cursor.close()
    return row[0]


def rate(read, symbol, date):
    cursor = read.cursor()
    cursor.execute('SELECT rate(%s,%s)', (symbol, date))
    row = cursor.fetchone()
    cursor.close()
    return float(row[0]) if row[0] is not None else 0


def vfr(read, symbol, date):
    cursor = read.cursor()
    cursor.execute('SELECT vfr(%s,%s)', (symbol, date))
    row = cursor.fetchone()
    cursor.close()
    return float(row[0]) if row[0] is not None else 0


def grow(read, symbol, date):
    cursor = read.cursor()
    cursor.execute('SELECT grow(%s,%s)', (symbol, date))
    row = cursor.fetchone()
    cursor.close()
    return float(row[0]) if row[0] is not None else 0


def rpa(read, symbol, date, kpi):
    cursor = read.cursor()
    cursor.execute('SELECT rpa(%s,%s,%s)', (symbol, date, kpi))
    row = cursor.fetchone()
    cursor.close()
    return float(row[0]) if row[0] is not None else 1


def rpd(read, symbol, date, kpi):
    cursor = read.cursor()
    cursor.execute('SELECT rpd(%s,%s,%s)', (symbol, date, kpi))
    row = cursor.fetchone()
    cursor.close()
    return float(row[0]) if row[0] is not None else 1
