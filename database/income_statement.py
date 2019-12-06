from datetime import timedelta

from dateutil.utils import today
from gm.api import get_fundamentals

from database import instrumentinfos
from env import read_conn, write_conn


def max_end(read, symbol):
    cursor = read.cursor()
    cursor.execute('SELECT max("end") FROM income_statement WHERE symbol=%s', (symbol,))
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
        print('\rBuilding income_statement: %.2f%%' % (i * 100 / len(infos)), end='')
        last_eob = max_end(read, info['symbol'])
        start = info['listed_date'] if last_eob is None else last_eob + timedelta(days=1)
        while True:
            end = start + timedelta(days=100000)
            fundamentals = get_fundamentals(
                table='income_statement',
                symbols=info['symbol'],
                start_date=start,
                end_date=end,
                fields='NETPROFIT,TOTPROFIT,PERPROFIT,BIZTOTINCO,BIZINCO,INTEINCO,EARNPREM,POUNINCO,REALSALE,OTHERBIZINCO,BIZTOTCOST,BIZCOST,INTEEXPE,POUNEXPE,REALSALECOST,DEVEEXPE,SURRGOLD,COMPNETEXPE,CONTRESS,POLIDIVIEXPE,REINEXPE,OTHERBIZCOST,BIZTAX,SALESEXPE,MANAEXPE,FINEXPE,ASSEIMPALOSS,VALUECHGLOSS,INVEINCO,ASSOINVEPROF,EXCHGGAIN,FUTULOSS,CUSTINCO,SUBSIDYINCOME,OTHERBIZPROF,NONOREVE,NONOEXPE,NONCASSETSDISL,INCOTAXEXPE,UNREINVELOSS,PARENETP,MERGEFORMNETPROF,MINYSHARRIGH,BASICEPS,DILUTEDEPS,AVAIDISTPROF,AVAIDISTSHAREPROF,CINAFORSFV,CINALIBOFRBP,COMDIVPAYBABLE,COMPINCOAMT,CPLTOHINCO,EARLYUNDIPROF,EPOCFHGL,EQUMCPOTHINCO,EUQMICOLOTHINCO,EXTRARBIRESE,EXTSTAFFFUND,HTMCCINAFORSFV,LEGALSURP,MAINBIZCOST,MAINBIZINCO,MINYSHARINCO,MINYSHARINCOAMT,NCPOTHINCO,NONCASSETSDISI,OTHERCOMPINCO,OTHERCPLTOHINCO,OTHERREASADJU,PARECOMPINCO,PARECOMPINCOAMT,PEXTCCAPIFD,PEXTCDEVEFD,PPROFRETUINVE,PREFSTOCKDIVI,PSUPPFLOWCAPI,RUNDISPROBYRREGCAP,STATEXTRUNDI,TDIFFFORCUR,TRUSTLOSS,TURNCAPSDIVI,UNDIPROF')
            fundamentals.sort(key=lambda x: x['end_date'])
            for fundamental in fundamentals:
                if fundamental['symbol'] == info['symbol']:
                    cursor.execute(
                        'INSERT INTO income_statement (symbol, pub, "end", netprofit, totprofit, perprofit, biztotinco, bizinco, inteinco, earnprem, pouninco, realsale, otherbizinco, biztotcost, bizcost, inteexpe, pounexpe, realsalecost, deveexpe, surrgold, compnetexpe, contress, polidiviexpe, reinexpe, otherbizcost, biztax, salesexpe, manaexpe, finexpe, asseimpaloss, valuechgloss, inveinco, assoinveprof, exchggain, futuloss, custinco, subsidyincome, otherbizprof, nonoreve, nonoexpe, noncassetsdisl, incotaxexpe, unreinveloss, parenetp, mergeformnetprof, minysharrigh, basiceps, dilutedeps, avaidistprof, avaidistshareprof, cinaforsfv, cinalibofrbp, comdivpaybable, compincoamt, cpltohinco, earlyundiprof, epocfhgl, equmcpothinco, euqmicolothinco, extrarbirese, extstafffund, htmccinaforsfv, legalsurp, mainbizcost, mainbizinco, minysharinco, minysharincoamt, ncpothinco, noncassetsdisi, othercompinco, othercpltohinco, otherreasadju, parecompinco, parecompincoamt, pextccapifd, pextcdevefd, pprofretuinve, prefstockdivi, psuppflowcapi, rundisprobyrregcap, statextrundi, tdiffforcur, trustloss, turncapsdivi, undiprof) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)',
                        (fundamental['symbol'],
                         fundamental['pub_date'],
                         fundamental['end_date'],
                         fundamental['NETPROFIT'],
                         fundamental['TOTPROFIT'],
                         fundamental['PERPROFIT'],
                         fundamental['BIZTOTINCO'],
                         fundamental['BIZINCO'],
                         fundamental['INTEINCO'],
                         fundamental['EARNPREM'],
                         fundamental['POUNINCO'],
                         fundamental['REALSALE'],
                         fundamental['OTHERBIZINCO'],
                         fundamental['BIZTOTCOST'],
                         fundamental['BIZCOST'],
                         fundamental['INTEEXPE'],
                         fundamental['POUNEXPE'],
                         fundamental['REALSALECOST'],
                         fundamental['DEVEEXPE'],
                         fundamental['SURRGOLD'],
                         fundamental['COMPNETEXPE'],
                         fundamental['CONTRESS'],
                         fundamental['POLIDIVIEXPE'],
                         fundamental['REINEXPE'],
                         fundamental['OTHERBIZCOST'],
                         fundamental['BIZTAX'],
                         fundamental['SALESEXPE'],
                         fundamental['MANAEXPE'],
                         fundamental['FINEXPE'],
                         fundamental['ASSEIMPALOSS'],
                         fundamental['VALUECHGLOSS'],
                         fundamental['INVEINCO'],
                         fundamental['ASSOINVEPROF'],
                         fundamental['EXCHGGAIN'],
                         fundamental['FUTULOSS'],
                         fundamental['CUSTINCO'],
                         fundamental['SUBSIDYINCOME'],
                         fundamental['OTHERBIZPROF'],
                         fundamental['NONOREVE'],
                         fundamental['NONOEXPE'],
                         fundamental['NONCASSETSDISL'],
                         fundamental['INCOTAXEXPE'],
                         fundamental['UNREINVELOSS'],
                         fundamental['PARENETP'],
                         fundamental['MERGEFORMNETPROF'],
                         fundamental['MINYSHARRIGH'],
                         fundamental['BASICEPS'],
                         fundamental['DILUTEDEPS'],
                         fundamental['AVAIDISTPROF'],
                         fundamental['AVAIDISTSHAREPROF'],
                         fundamental['CINAFORSFV'],
                         fundamental['CINALIBOFRBP'],
                         fundamental['COMDIVPAYBABLE'],
                         fundamental['COMPINCOAMT'],
                         fundamental['CPLTOHINCO'],
                         fundamental['EARLYUNDIPROF'],
                         fundamental['EPOCFHGL'],
                         fundamental['EQUMCPOTHINCO'],
                         fundamental['EUQMICOLOTHINCO'],
                         fundamental['EXTRARBIRESE'],
                         fundamental['EXTSTAFFFUND'],
                         fundamental['HTMCCINAFORSFV'],
                         fundamental['LEGALSURP'],
                         fundamental['MAINBIZCOST'],
                         fundamental['MAINBIZINCO'],
                         fundamental['MINYSHARINCO'],
                         fundamental['MINYSHARINCOAMT'],
                         fundamental['NCPOTHINCO'],
                         fundamental['NONCASSETSDISI'],
                         fundamental['OTHERCOMPINCO'],
                         fundamental['OTHERCPLTOHINCO'],
                         fundamental['OTHERREASADJU'],
                         fundamental['PARECOMPINCO'],
                         fundamental['PARECOMPINCOAMT'],
                         fundamental['PEXTCCAPIFD'],
                         fundamental['PEXTCDEVEFD'],
                         fundamental['PPROFRETUINVE'],
                         fundamental['PREFSTOCKDIVI'],
                         fundamental['PSUPPFLOWCAPI'],
                         fundamental['RUNDISPROBYRREGCAP'],
                         fundamental['STATEXTRUNDI'],
                         fundamental['TDIFFFORCUR'],
                         fundamental['TRUSTLOSS'],
                         fundamental['TURNCAPSDIVI'],
                         fundamental['UNDIPROF']))
                    count += 1
            if end < min(info['delisted_date'], today().date()):
                start = end + timedelta(days=1)
            else:
                break
    cursor.close()
    write.close()
    read.close()
    print('\rBuilding income_statement: Finish')
    return count
