from sender import cmd, mail


def output(context, date, l_sell, l_keep, l_buy):
    cmd.output(context, date, l_sell, l_keep, l_buy)
    mail.output(context, date, l_sell, l_keep, l_buy)
