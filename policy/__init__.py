from policy import p, s, sv


def buy(context, date):
    return p.buy(context, date)


def sell(context, date):
    return s.sell(context, date) + sv.sell(context, date)
