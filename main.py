from datetime import date, datetime

from gm.api import *

from env import c, strategy_id, token, cash
from policy import skb
from sender import output


def init(context):
    # build()
    schedule(schedule_func=go, date_rule='1d', time_rule='00:00:00')


def go(context):
    today = date(context.now.year, context.now.month, context.now.day)
    # 如果今天是交易日
    if len(get_trading_dates(exchange='SHSE', start_date=today, end_date=today)) + len(
            get_trading_dates(exchange='SZSE', start_date=today, end_date=today)) > 0:
        yesterday = max(datetime.strptime(get_previous_trading_date('SHSE', today), "%Y-%m-%d").date(),
                        datetime.strptime(get_previous_trading_date('SZSE', today), "%Y-%m-%d").date())
        context.s, context.k, context.b = skb(context, yesterday)
        output(context, yesterday, context.s, context.k, context.b)
        unsubscribe(symbols='*', frequency='60s')
        # s中每只股票加入到买入监测tick
        subscribe(symbols=list(map(lambda x: x['symbol'], context.s)), frequency='60s', count=1, wait_group=False,
                  unsubscribe_previous=False)
        # b中每只股票加入到买入监测tick
        subscribe(symbols=list(map(lambda x: x['symbol'], context.b)), frequency='60s', count=1, wait_group=False,
                  unsubscribe_previous=False)


def on_bar(context, bars):
    items_sell = list(filter(lambda x: x['symbol'] == bars[0].symbol, context.s))
    items_buy = list(filter(lambda x: x['symbol'] == bars[0].symbol, context.b))
    if len(items_sell) > 0 \
            and bars[0].close > 0 \
            and context.account().position(symbol=items_sell[0]['symbol'], side=PositionSide_Long) is not None:
        order_target_volume(symbol=items_sell[0]['symbol'], position_side=PositionSide_Long, volume=0,
                            order_type=OrderType_Market,
                            order_duration=OrderDuration_GTC)
    elif len(items_buy) > 0 \
            and bars[0].close > 0 \
            and context.account().position(symbol=items_buy[0]['symbol'], side=PositionSide_Long) is None \
            and bars[0].close <= items_buy[0]['price']:
        vacancies = c - len(context.account().positions(side=PositionSide_Long))
        if vacancies > 0:
            cash = context.account().cash['available']
            order_target_value(symbol=items_buy[0]['symbol'], position_side=PositionSide_Long, value=cash / vacancies,
                               order_type=OrderType_Market,
                               order_duration=OrderDuration_FAK)
        else:
            for dict in reversed(context.k):
                if context.account().position(symbol=dict['symbol'], side=PositionSide_Long) is not None \
                        and dict['score'] < items_buy[0]['score']:
                    print('%s(%f) replaces %s(%f)'
                          % (items_buy[0]['symbol'], items_buy[0]['score'], dict['symbol'], dict['score']))
                    order_target_volume(symbol=dict['symbol'], position_side=PositionSide_Long, volume=0,
                                        order_type=OrderType_Market,
                                        order_duration=OrderDuration_GTC)
                    break


if __name__ == '__main__':
    run(strategy_id=strategy_id,
        filename='main.py',
        mode=MODE_BACKTEST,
        token=token,
        backtest_start_time='2018-01-01 00:00:00',
        backtest_end_time='2020-01-16 23:59:59',
        backtest_initial_cash=cash)
