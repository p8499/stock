from math import floor

from gm.api import get_instrumentinfos

from env import c


def output(context, date, l_sell, l_keep, l_buy):
    message = '%s盘后计划' % date
    for item in l_sell:
        ins = get_instrumentinfos(symbols=item['symbol'])[0]
        name = ins['sec_name']
        message += '\n\033[1;32m%s %s 卖出%s\033[0m' \
                   % (item['symbol'], name, item['policy'])
    for item in l_keep:
        ins = get_instrumentinfos(symbols=item['symbol'])[0]
        name = ins['sec_name']
        message += '\n%s %s 分值：%s' \
                   % (item['symbol'], name, item['score'])
    for item in l_buy:
        ins = get_instrumentinfos(symbols=item['symbol'])[0]
        name = ins['sec_name']
        vacancies = c - len(l_keep)
        cash = context.account().cash['available']
        volume = floor(cash / vacancies / item['price'] / 100) * 100 if vacancies > 0 else 0
        message += '\n\033[1;31m%s %s 买入%s %.2f * %d\033[0m' \
                   % (item['symbol'], name, item['policy'], item['price'], volume)
    print(message)
