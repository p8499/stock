from datetime import timedelta

from dateutil.utils import today
from gm.api import get_fundamentals

from database import instrumentinfos
from env import read_conn, write_conn


def max_end(read, symbol):
    cursor = read.cursor()
    cursor.execute('SELECT max("end") FROM cashflow_statement WHERE symbol=%s', (symbol,))
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
        print('\rBuilding cashflow_statement: %.2f%%' % (i * 100 / len(infos)), end='')
        last_eob = max_end(read, info['symbol'])
        start = info['listed_date'] if last_eob is None else last_eob + timedelta(days=1)
        while True:
            end = start + timedelta(days=100000)
            fundamentals = get_fundamentals(
                table='cashflow_statement',
                symbols=info['symbol'],
                start_date=start,
                end_date=end,
                fields='CASHNETR,MANANETR,BIZCASHINFL,LABORGETCASH,DEPONETR,BANKLOANNETINCR,FININSTNETR,INSPREMCASH,INSNETC,SAVINETR,DISPTRADNETINCR,CHARINTECASH,FDSBORRNETR,REPNETINCR,TAXREFD,RECEOTHERBIZCASH,BIZCASHOUTF,LABOPAYC,LOANSNETR,TRADEPAYMNETR,PAYCOMPGOLD,PAYINTECASH,PAYDIVICASH,PAYWORKCASH,PAYTAX,PAYACTICASH,INVNETCASHFLOW,INVCASHINFL,WITHINVGETCASH,INVERETUGETCASH,FIXEDASSETNETC,SUBSNETC,RECEINVCASH,REDUCASHPLED,INVCASHOUTF,ACQUASSETCASH,INVPAYC,LOANNETR,SUBSPAYNETCASH,PAYINVECASH,INCRCASHPLED,FINNETCFLOW,FINCASHINFL,INVRECECASH,SUBSRECECASH,RECEFROMLOAN,ISSBDRECECASH,RECEFINCASH,FINCASHOUTF,DEBTPAYCASH,DIVIPROFPAYCASH,SUBSPAYDIVID,FINRELACASH,CHGEXCHGCHGS,INICASHBALA,FINALCASHBALA,NETPROFIT,MINYSHARRIGH,UNREINVELOSS,ASSEIMPA,ASSEDEPR,INTAASSEAMOR,LONGDEFEEXPENAMOR,PREPEXPEDECR,ACCREXPEINCR,DISPFIXEDASSETLOSS,FIXEDASSESCRALOSS,VALUECHGLOSS,DEFEINCOINCR,ESTIDEBTS,FINEXPE,INVELOSS,DEFETAXASSETDECR,DEFETAXLIABINCR,INVEREDU,RECEREDU,PAYAINCR,UNSEPARACHG,UNFIPARACHG,OTHER,BIZNETCFLOW,DEBTINTOCAPI,EXPICONVBD,FINFIXEDASSET,CASHFINALBALA,CASHOPENBALA,EQUFINALBALA,EQUOPENBALA,CASHNETI,REALESTADEP')
            fundamentals.sort(key=lambda x: x['end_date'])
            for fundamental in fundamentals:
                if fundamental['symbol'] == info['symbol']:
                    cursor.execute(
                        'INSERT INTO cashflow_statement (symbol, pub, "end", cashnetr, mananetr, bizcashinfl, laborgetcash, deponetr, bankloannetincr, fininstnetr, inspremcash, insnetc, savinetr, disptradnetincr, charintecash, fdsborrnetr, repnetincr, taxrefd, receotherbizcash, bizcashoutf, labopayc, loansnetr, tradepaymnetr, paycompgold, payintecash, paydivicash, payworkcash, paytax, payacticash, invnetcashflow, invcashinfl, withinvgetcash, inveretugetcash, fixedassetnetc, subsnetc, receinvcash, reducashpled, invcashoutf, acquassetcash, invpayc, loannetr, subspaynetcash, payinvecash, incrcashpled, finnetcflow, fincashinfl, invrececash, subsrececash, recefromloan, issbdrececash, recefincash, fincashoutf, debtpaycash, diviprofpaycash, subspaydivid, finrelacash, chgexchgchgs, inicashbala, finalcashbala, netprofit, minysharrigh, unreinveloss, asseimpa, assedepr, intaasseamor, longdefeexpenamor, prepexpedecr, accrexpeincr, dispfixedassetloss, fixedassescraloss, valuechgloss, defeincoincr, estidebts, finexpe, inveloss, defetaxassetdecr, defetaxliabincr, inveredu, receredu, payaincr, unseparachg, unfiparachg, other, biznetcflow, debtintocapi, expiconvbd, finfixedasset, cashfinalbala, cashopenbala, equfinalbala, equopenbala, cashneti, realestadep) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)',
                        (fundamental['symbol'],
                         fundamental['pub_date'],
                         fundamental['end_date'],
                         fundamental['CASHNETR'],
                         fundamental['MANANETR'],
                         fundamental['BIZCASHINFL'],
                         fundamental['LABORGETCASH'],
                         fundamental['DEPONETR'],
                         fundamental['BANKLOANNETINCR'],
                         fundamental['FININSTNETR'],
                         fundamental['INSPREMCASH'],
                         fundamental['INSNETC'],
                         fundamental['SAVINETR'],
                         fundamental['DISPTRADNETINCR'],
                         fundamental['CHARINTECASH'],
                         fundamental['FDSBORRNETR'],
                         fundamental['REPNETINCR'],
                         fundamental['TAXREFD'],
                         fundamental['RECEOTHERBIZCASH'],
                         fundamental['BIZCASHOUTF'],
                         fundamental['LABOPAYC'],
                         fundamental['LOANSNETR'],
                         fundamental['TRADEPAYMNETR'],
                         fundamental['PAYCOMPGOLD'],
                         fundamental['PAYINTECASH'],
                         fundamental['PAYDIVICASH'],
                         fundamental['PAYWORKCASH'],
                         fundamental['PAYTAX'],
                         fundamental['PAYACTICASH'],
                         fundamental['INVNETCASHFLOW'],
                         fundamental['INVCASHINFL'],
                         fundamental['WITHINVGETCASH'],
                         fundamental['INVERETUGETCASH'],
                         fundamental['FIXEDASSETNETC'],
                         fundamental['SUBSNETC'],
                         fundamental['RECEINVCASH'],
                         fundamental['REDUCASHPLED'],
                         fundamental['INVCASHOUTF'],
                         fundamental['ACQUASSETCASH'],
                         fundamental['INVPAYC'],
                         fundamental['LOANNETR'],
                         fundamental['SUBSPAYNETCASH'],
                         fundamental['PAYINVECASH'],
                         fundamental['INCRCASHPLED'],
                         fundamental['FINNETCFLOW'],
                         fundamental['FINCASHINFL'],
                         fundamental['INVRECECASH'],
                         fundamental['SUBSRECECASH'],
                         fundamental['RECEFROMLOAN'],
                         fundamental['ISSBDRECECASH'],
                         fundamental['RECEFINCASH'],
                         fundamental['FINCASHOUTF'],
                         fundamental['DEBTPAYCASH'],
                         fundamental['DIVIPROFPAYCASH'],
                         fundamental['SUBSPAYDIVID'],
                         fundamental['FINRELACASH'],
                         fundamental['CHGEXCHGCHGS'],
                         fundamental['INICASHBALA'],
                         fundamental['FINALCASHBALA'],
                         fundamental['NETPROFIT'],
                         fundamental['MINYSHARRIGH'],
                         fundamental['UNREINVELOSS'],
                         fundamental['ASSEIMPA'],
                         fundamental['ASSEDEPR'],
                         fundamental['INTAASSEAMOR'],
                         fundamental['LONGDEFEEXPENAMOR'],
                         fundamental['PREPEXPEDECR'],
                         fundamental['ACCREXPEINCR'],
                         fundamental['DISPFIXEDASSETLOSS'],
                         fundamental['FIXEDASSESCRALOSS'],
                         fundamental['VALUECHGLOSS'],
                         fundamental['DEFEINCOINCR'],
                         fundamental['ESTIDEBTS'],
                         fundamental['FINEXPE'],
                         fundamental['INVELOSS'],
                         fundamental['DEFETAXASSETDECR'],
                         fundamental['DEFETAXLIABINCR'],
                         fundamental['INVEREDU'],
                         fundamental['RECEREDU'],
                         fundamental['PAYAINCR'],
                         fundamental['UNSEPARACHG'],
                         fundamental['UNFIPARACHG'],
                         fundamental['OTHER'],
                         fundamental['BIZNETCFLOW'],
                         fundamental['DEBTINTOCAPI'],
                         fundamental['EXPICONVBD'],
                         fundamental['FINFIXEDASSET'],
                         fundamental['CASHFINALBALA'],
                         fundamental['CASHOPENBALA'],
                         fundamental['EQUFINALBALA'],
                         fundamental['EQUOPENBALA'],
                         fundamental['CASHNETI'],
                         fundamental['REALESTADEP']))
                    count += 1
            if end < min(info['delisted_date'], today().date()):
                start = end + timedelta(days=1)
            else:
                break
    cursor.close()
    write.close()
    read.close()
    print('\rBuilding cashflow_statement: Finish')
    return count
