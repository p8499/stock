from datetime import timedelta

from dateutil.utils import today
from gm.api import get_history_instruments

from database import instrumentinfos
from env import read_conn, write_conn


def max_date(read, symbol):
    cursor = read.cursor()
    cursor.execute('SELECT max(trade_date) FROM history_instruments WHERE symbol=%s', (symbol,))
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
        print('\rBuilding history_instruments: %.2f%%' % (i * 100 / len(infos)), end='')
        last_eob = max_date(read, info['symbol'])
        start = info['listed_date'] if last_eob is None else last_eob + timedelta(days=1)
        while True:
            end = start + timedelta(days=1000)
            instruments = get_history_instruments(
                symbols=info['symbol'],
                fields='symbol,trade_date,sec_level,is_suspended,multiplier,margin_ratio,settle_price,pre_settle,position,pre_close,upper_limit,lower_limit,adj_factor',
                start_date=start,
                end_date=end)
            instruments.sort(key=lambda x: x['trade_date'])
            for instrument in instruments:
                cursor.execute(
                    'INSERT INTO history_instruments (symbol, trade_date, sec_level, is_suspended, multiplier, margin_ratio, settle_price, pre_settle, position, pre_close, upper_limit, lower_limit, adj_factor) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)',
                    (instrument['symbol'],
                     instrument['trade_date'],
                     instrument['sec_level'],
                     instrument['is_suspended'],
                     instrument['multiplier'],
                     instrument['margin_ratio'],
                     instrument['settle_price'],
                     instrument['pre_settle'],
                     instrument['position'],
                     instrument['pre_close'],
                     instrument['upper_limit'],
                     instrument['lower_limit'],
                     instrument['adj_factor']))
                count += 1
            if end < min(info['delisted_date'], today().date()):
                start = end + timedelta(days=1)
            else:
                break
    cursor.close()
    write.close()
    read.close()
    print('\rBuilding history_instruments: Finish')
    return count
