from env import c
from policy import s, sv, b, sf, k


def skb(context, date):
    sell = s.sell(context, date) + sv.sell(context, date)  # + sf.sell(context, date)
    s_symbols = list(map(lambda x: x['symbol'], sell))
    keep = k.keep(context, date)
    keep = list(filter(lambda x: x['symbol'] not in s_symbols, keep))
    k_symbols = list(map(lambda x: x['symbol'], keep))
    buy = b.buy(context, date)
    buy = list(filter(lambda x: x['symbol'] not in k_symbols and x['symbol'] not in s_symbols, buy))
    buy = buy[:(c - len(k_symbols))]
    return sell, keep, buy
