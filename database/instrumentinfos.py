from gm.api import get_instrumentinfos
from psycopg2 import extras

from env import write_conn, read_conn


def infos(read):
    cursor = read.cursor(cursor_factory=extras.DictCursor)
    cursor.execute('SELECT * FROM instrumentinfos')
    rows = cursor.fetchall()
    cursor.close()
    return rows


def full_build():
    count = 0
    read = read_conn()
    write = write_conn()
    cursor = write.cursor()
    cursor.execute('DELETE FROM instrumentinfos')
    infos = get_instrumentinfos(exchanges=['SHSE', 'SZSE'],
                                sec_types=[1],
                                fields='symbol,sec_name,sec_abbr,price_tick,listed_date,delisted_date')
    for i, info in enumerate(infos):
        print('\rBuilding instrumentinfos: %.2f%%' % (i * 100 / len(infos)), end='')
        cursor.execute(
            'INSERT INTO instrumentinfos (symbol, sec_name, sec_abbr, price_tick, listed_date, delisted_date) VALUES (%s,%s,%s,%s,%s,%s)',
            (info['symbol'],
             info['sec_name'],
             info['sec_abbr'],
             info['price_tick'],
             info['listed_date'],
             info['delisted_date']))
    cursor.close()
    write.close()
    read.close()
    print('\rBuilding instrumentinfos: Finish')
    return count
