from datetime import timedelta

from dateutil.utils import today
from gm.api import get_fundamentals

from database import instrumentinfos
from env import read_conn, write_conn


def max_end(read, symbol):
    cursor = read.cursor()
    cursor.execute('SELECT max("end") FROM trading_derivative_indicator WHERE symbol=%s', (symbol,))
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
        print('\rBuilding trading_derivative_indicator: %.2f%%' % (i * 100 / len(infos)), end='')
        last_eob = max_end(read, info['symbol'])
        start = info['listed_date'] if last_eob is None else last_eob + timedelta(days=1)
        while True:
            end = start + timedelta(days=1000)
            fundamentals = get_fundamentals(
                table='trading_derivative_indicator',
                symbols=info['symbol'],
                start_date=start,
                end_date=end,
                fields='DY,EV,EVEBITDA,EVPS,LYDY,NEGOTIABLEMV,PB,PCLFY,PCTTM,PELFY,PELFYNPAAEI,PEMRQ,PEMRQNPAAEI,PETTM,PETTMNPAAEI,PSLFY,PSMRQ,PSTTM,TCLOSE,TOTMKTCAP,TURNRATE,TOTAL_SHARE,FLOW_SHARE')
            fundamentals.sort(key=lambda x: x['end_date'])
            for fundamental in fundamentals:
                if fundamental['symbol'] == info['symbol']:
                    cursor.execute(
                        'INSERT INTO trading_derivative_indicator (symbol, pub, "end", dy, ev, evebitda, evps, lydy, negotiablemv, pb, pclfy, pcttm, pelfy, pelfynpaaei, pemrq, pemrqnpaaei, pettm, pettmnpaaei, pslfy, psmrq, psttm, tclose, totmktcap, turnrate, total_share, flow_share) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)',
                        (fundamental['symbol'],
                         fundamental['pub_date'],
                         fundamental['end_date'],
                         fundamental['DY'],
                         fundamental['EV'],
                         fundamental['EVEBITDA'],
                         fundamental['EVPS'],
                         fundamental['LYDY'],
                         fundamental['NEGOTIABLEMV'],
                         fundamental['PB'],
                         fundamental['PCLFY'],
                         fundamental['PCTTM'],
                         fundamental['PELFY'],
                         fundamental['PELFYNPAAEI'],
                         fundamental['PEMRQ'],
                         fundamental['PEMRQNPAAEI'],
                         fundamental['PETTM'],
                         fundamental['PETTMNPAAEI'],
                         fundamental['PSLFY'],
                         fundamental['PSMRQ'],
                         fundamental['PSTTM'],
                         fundamental['TCLOSE'],
                         fundamental['TOTMKTCAP'],
                         fundamental['TURNRATE'],
                         fundamental['TOTAL_SHARE'],
                         fundamental['FLOW_SHARE']))
                    count += 1
            if end < min(info['delisted_date'], today().date()):
                start = end + timedelta(days=1)
            else:
                break
    cursor.close()
    write.close()
    read.close()
    print('\rBuilding trading_derivative_indicator: Finish')
    return count
