from math import floor

from gm.api import get_instrumentinfos

from database import history_1d
from env import read_conn, c


def output(context, date, l_sell, l_keep, l_buy):
    read = read_conn()
    message = '%s盘后计划' % date
    for item in l_sell:
        ins = get_instrumentinfos(symbols=item[0])[0]
        name = ins['sec_name']
        message += '\n\033[1;32m%s %s 卖出%s\033[0m' \
                   % (item[0], name, item[1])
    for item in l_keep:
        ins = get_instrumentinfos(symbols=item[0])[0]
        name = ins['sec_name']
        message += '\n%s %s %s' \
                   % (item[0], name, item[1])
    for item in l_buy:
        ins = get_instrumentinfos(symbols=item[0])[0]
        name = ins['sec_name']
        nav = context.account().cash['nav']
        price = float(min(history_1d.close(read, item[0], date), history_1d.close_r1(read, item[0], date)))
        volume = floor(nav / c / price / 100) * 100
        message += '\n\033[1;31m%s %s 买入%s %.2f * %d\033[0m' \
                   % (item[0], name, item[1], price, volume)
    print(message)
    read.close()
