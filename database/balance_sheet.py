from datetime import timedelta

from dateutil.utils import today
from gm.api import get_fundamentals

from database import instrumentinfos
from env import read_conn, write_conn


def max_end(read, symbol):
    cursor = read.cursor()
    cursor.execute('SELECT max("end") FROM balance_sheet WHERE symbol=%s', (symbol,))
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
        print('\rBuilding balance_sheet: %.2f%%' % (i * 100 / len(infos)), end='')
        last_eob = max_end(read, info['symbol'])
        start = info['listed_date'] if last_eob is None else last_eob + timedelta(days=1)
        while True:
            end = start + timedelta(days=100000)
            fundamentals = get_fundamentals(
                table='balance_sheet',
                symbols=info['symbol'],
                start_date=start,
                end_date=end,
                fields='TOTASSET,TOTCURRASSET,CURFDS,SETTRESEDEPO,PLAC,TRADFINASSET,DERIFINAASSET,NOTESRECE,ACCORECE,PREP,PREMRECE,REINRECE,REINCONTRESE,INTERECE,DIVIDRECE,OTHERRECE,EXPOTAXREBARECE,SUBSRECE,MARGRECE,INTELRECE,PURCRESAASSET,INVE,PREPEXPE,UNSEG,ACCHELDFORS,EXPINONCURRASSET,OTHERCURRASSE,TOTALNONCASSETS,LENDANDLOAN,AVAISELLASSE,HOLDINVEDUE,LONGRECE,EQUIINVE,OTHERLONGINVE,INVEPROP,FIXEDASSEIMMO,ACCUDEPR,FIXEDASSENETW,FIXEDASSEIMPA,FIXEDASSENET,CONSPROG,ENGIMATE,FIXEDASSECLEA,PRODASSE,COMASSE,HYDRASSET,INTAASSET,DEVEEXPE,GOODWILL,LOGPREPEXPE,TRADSHARTRAD,DEFETAXASSET,OTHERNONCASSE,TOTLIABSHAREQUI,TOTLIAB,TOTALCURRLIAB,SHORTTERMBORR,CENBANKBORR,DEPOSIT,FDSBORR,TRADFINLIAB,DERILIAB,NOTESPAYA,ACCOPAYA,ADVAPAYM,SELLREPASSE,COPEPOUN,COPEWORKERSAL,TAXESPAYA,INTEPAYA,DIVIPAYA,OTHERFEEPAYA,MARGREQU,INTELPAY,OTHERPAY,ACCREXPE,EXPECURRLIAB,COPEWITHREINRECE,INSUCONTRESE,WARLIABRESE,ACTITRADSECU,ACTIUNDESECU,INTETICKSETT,DOMETICKSETT,DEFEREVE,SHORTTERMBDSPAYA,LIABHELDFORS,DUENONCLIAB,OTHERCURRELIABI,TOTALNONCLIAB,LONGBORR,BDSPAYA,BDSPAYAPERBOND,BDSPAYAPREST,LONGPAYA,LCOPEWORKERSAL,SPECPAYA,EXPENONCLIAB,LONGDEFEINCO,DEFEINCOTAXLIAB,OTHERNONCLIABI,RIGHAGGR,PAIDINCAPI,OTHEQUIN,PREST,PERBOND,CAPISURP,TREASTK,OCL,SPECRESE,RESE,GENERISKRESE,UNREINVELOSS,UNDIPROF,TOPAYCASHDIVI,CURTRANDIFF,PARESHARRIGH,MINYSHARRIGH')
            fundamentals.sort(key=lambda x: x['end_date'])
            for fundamental in fundamentals:
                if fundamental['symbol'] == info['symbol']:
                    cursor.execute(
                        'INSERT INTO balance_sheet (symbol, pub, "end", totasset, totcurrasset, curfds, settresedepo, plac, tradfinasset, derifinaasset, notesrece, accorece, prep, premrece, reinrece, reincontrese, interece, dividrece, otherrece, expotaxrebarece, subsrece, margrece, intelrece, purcresaasset, inve, prepexpe, unseg, accheldfors, expinoncurrasset, othercurrasse, totalnoncassets, lendandloan, avaisellasse, holdinvedue, longrece, equiinve, otherlonginve, inveprop, fixedasseimmo, accudepr, fixedassenetw, fixedasseimpa, fixedassenet, consprog, engimate, fixedasseclea, prodasse, comasse, hydrasset, intaasset, deveexpe, goodwill, logprepexpe, tradshartrad, defetaxasset, othernoncasse, totliabsharequi, totliab, totalcurrliab, shorttermborr, cenbankborr, deposit, fdsborr, tradfinliab, deriliab, notespaya, accopaya, advapaym, sellrepasse, copepoun, copeworkersal, taxespaya, intepaya, divipaya, otherfeepaya, margrequ, intelpay, otherpay, accrexpe, expecurrliab, copewithreinrece, insucontrese, warliabrese, actitradsecu, actiundesecu, inteticksett, dometicksett, defereve, shorttermbdspaya, liabheldfors, duenoncliab, othercurreliabi, totalnoncliab, longborr, bdspaya, bdspayaperbond, bdspayaprest, longpaya, lcopeworkersal, specpaya, expenoncliab, longdefeinco, defeincotaxliab, othernoncliabi, righaggr, paidincapi, othequin, prest, perbond, capisurp, treastk, ocl, specrese, rese, generiskrese, unreinveloss, undiprof, topaycashdivi, curtrandiff, paresharrigh, minysharrigh) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)',
                        (fundamental['symbol'],
                         fundamental['pub_date'],
                         fundamental['end_date'],
                         fundamental['TOTASSET'],
                         fundamental['TOTCURRASSET'],
                         fundamental['CURFDS'],
                         fundamental['SETTRESEDEPO'],
                         fundamental['PLAC'],
                         fundamental['TRADFINASSET'],
                         fundamental['DERIFINAASSET'],
                         fundamental['NOTESRECE'],
                         fundamental['ACCORECE'],
                         fundamental['PREP'],
                         fundamental['PREMRECE'],
                         fundamental['REINRECE'],
                         fundamental['REINCONTRESE'],
                         fundamental['INTERECE'],
                         fundamental['DIVIDRECE'],
                         fundamental['OTHERRECE'],
                         fundamental['EXPOTAXREBARECE'],
                         fundamental['SUBSRECE'],
                         fundamental['MARGRECE'],
                         fundamental['INTELRECE'],
                         fundamental['PURCRESAASSET'],
                         fundamental['INVE'],
                         fundamental['PREPEXPE'],
                         fundamental['UNSEG'],
                         fundamental['ACCHELDFORS'],
                         fundamental['EXPINONCURRASSET'],
                         fundamental['OTHERCURRASSE'],
                         fundamental['TOTALNONCASSETS'],
                         fundamental['LENDANDLOAN'],
                         fundamental['AVAISELLASSE'],
                         fundamental['HOLDINVEDUE'],
                         fundamental['LONGRECE'],
                         fundamental['EQUIINVE'],
                         fundamental['OTHERLONGINVE'],
                         fundamental['INVEPROP'],
                         fundamental['FIXEDASSEIMMO'],
                         fundamental['ACCUDEPR'],
                         fundamental['FIXEDASSENETW'],
                         fundamental['FIXEDASSEIMPA'],
                         fundamental['FIXEDASSENET'],
                         fundamental['CONSPROG'],
                         fundamental['ENGIMATE'],
                         fundamental['FIXEDASSECLEA'],
                         fundamental['PRODASSE'],
                         fundamental['COMASSE'],
                         fundamental['HYDRASSET'],
                         fundamental['INTAASSET'],
                         fundamental['DEVEEXPE'],
                         fundamental['GOODWILL'],
                         fundamental['LOGPREPEXPE'],
                         fundamental['TRADSHARTRAD'],
                         fundamental['DEFETAXASSET'],
                         fundamental['OTHERNONCASSE'],
                         fundamental['TOTLIABSHAREQUI'],
                         fundamental['TOTLIAB'],
                         fundamental['TOTALCURRLIAB'],
                         fundamental['SHORTTERMBORR'],
                         fundamental['CENBANKBORR'],
                         fundamental['DEPOSIT'],
                         fundamental['FDSBORR'],
                         fundamental['TRADFINLIAB'],
                         fundamental['DERILIAB'],
                         fundamental['NOTESPAYA'],
                         fundamental['ACCOPAYA'],
                         fundamental['ADVAPAYM'],
                         fundamental['SELLREPASSE'],
                         fundamental['COPEPOUN'],
                         fundamental['COPEWORKERSAL'],
                         fundamental['TAXESPAYA'],
                         fundamental['INTEPAYA'],
                         fundamental['DIVIPAYA'],
                         fundamental['OTHERFEEPAYA'],
                         fundamental['MARGREQU'],
                         fundamental['INTELPAY'],
                         fundamental['OTHERPAY'],
                         fundamental['ACCREXPE'],
                         fundamental['EXPECURRLIAB'],
                         fundamental['COPEWITHREINRECE'],
                         fundamental['INSUCONTRESE'],
                         fundamental['WARLIABRESE'],
                         fundamental['ACTITRADSECU'],
                         fundamental['ACTIUNDESECU'],
                         fundamental['INTETICKSETT'],
                         fundamental['DOMETICKSETT'],
                         fundamental['DEFEREVE'],
                         fundamental['SHORTTERMBDSPAYA'],
                         fundamental['LIABHELDFORS'],
                         fundamental['DUENONCLIAB'],
                         fundamental['OTHERCURRELIABI'],
                         fundamental['TOTALNONCLIAB'],
                         fundamental['LONGBORR'],
                         fundamental['BDSPAYA'],
                         fundamental['BDSPAYAPERBOND'],
                         fundamental['BDSPAYAPREST'],
                         fundamental['LONGPAYA'],
                         fundamental['LCOPEWORKERSAL'],
                         fundamental['SPECPAYA'],
                         fundamental['EXPENONCLIAB'],
                         fundamental['LONGDEFEINCO'],
                         fundamental['DEFEINCOTAXLIAB'],
                         fundamental['OTHERNONCLIABI'],
                         fundamental['RIGHAGGR'],
                         fundamental['PAIDINCAPI'],
                         fundamental['OTHEQUIN'],
                         fundamental['PREST'],
                         fundamental['PERBOND'],
                         fundamental['CAPISURP'],
                         fundamental['TREASTK'],
                         fundamental['OCL'],
                         fundamental['SPECRESE'],
                         fundamental['RESE'],
                         fundamental['GENERISKRESE'],
                         fundamental['UNREINVELOSS'],
                         fundamental['UNDIPROF'],
                         fundamental['TOPAYCASHDIVI'],
                         fundamental['CURTRANDIFF'],
                         fundamental['PARESHARRIGH'],
                         fundamental['MINYSHARRIGH']))
                    count += 1
            if end < min(info['delisted_date'], today().date()):
                start = end + timedelta(days=1)
            else:
                break
    cursor.close()
    write.close()
    read.close()
    print('\rBuilding balance_sheet: Finish')
    return count
