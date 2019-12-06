from datetime import date, timedelta

from gm.api import *

from database import history_1d, build
from env import read_conn, c, cash
from policy import sell, buy
from sender import output


def init(context):
    # build()
    schedule(schedule_func=prepare, date_rule='1d', time_rule='18:00:00')
    schedule(schedule_func=action, date_rule='1d', time_rule='09:30:00')


def prepare(context):
    build()
    today = date(context.now.year, context.now.month, context.now.day)
    tomorrow = today + timedelta(days=1)
    # if len(get_trading_dates(exchange='SHSE', start_date=tomorrow, end_date=tomorrow)) + len(
    #         get_trading_dates(exchange='SZSE', start_date=tomorrow, end_date=tomorrow)) > 0:
    s, k, b = list_skb(context, today)
    output(context, today, s, k, b)


def action(context):
    today = date(context.now.year, context.now.month, context.now.day)
    yesterday = today - timedelta(days=1)
    if len(get_trading_dates(exchange='SHSE', start_date=today, end_date=today)) + len(
            get_trading_dates(exchange='SZSE', start_date=today, end_date=today)) > 0:
        s, k, b = list_skb(context, yesterday)
        read = read_conn()
        for item_sell in s:
            order_target_volume(symbol=item_sell[0], position_side=PositionSide_Long, volume=0,
                                order_type=OrderType_Market, order_duration=OrderDuration_GTC)
        for item_buy in b:
            close = min(history_1d.close(read, item_buy[0], yesterday),
                        history_1d.close_r1(read, item_buy[0], yesterday))
            order_target_percent(symbol=item_buy[0], position_side=PositionSide_Long, percent=1 / c,
                                 order_type=OrderType_Limit, price=close, order_duration=OrderDuration_FAK)
        read.close()


def list_skb(context, dt):
    l_sell = sell(context, dt)
    l_sell_symbols = list(map(lambda x: x[0], l_sell))
    l_pos_symbols = list(map(lambda x: x['symbol'], context.account().positions(side=PositionSide_Long)))
    l_keep_symbols = list(filter(lambda x: x not in l_sell_symbols, l_pos_symbols))
    l_keep = list(map(lambda x: (x, ''), l_keep_symbols))
    l_buy = buy(context, dt)[:(c - len(l_pos_symbols) + len(l_sell_symbols))]
    return l_sell, l_keep, l_buy


if __name__ == '__main__':
    run(strategy_id='',
        filename='main.py',
        mode=MODE_BACKTEST,
        token='',
        backtest_start_time='2018-01-01 16:00:00',
        backtest_end_time='2019-12-04 23:59:59',
        backtest_initial_cash=cash)
