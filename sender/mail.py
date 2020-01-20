from email.header import Header
from email.mime.text import MIMEText
from math import floor
from smtplib import SMTP_SSL

from gm.api import get_instrumentinfos

from env import mail_receivers, smtp_server, smtp_port, password, from_addr, c


def output(context, date, l_sell, l_keep, l_buy):
    subject = '%s盘后计划' % date
    content = ''
    content += '<table border="1">'
    content += '<thead>'
    content += '<tr><th>代码</th><th>名称</th><th>策略</th></tr>'
    content += '</thead>'
    content += '<tbody>'
    for item in l_sell:
        ins = get_instrumentinfos(symbols=item['symbol'])[0]
        name = ins['sec_name']
        content += '<tr><td>%s</td><td>%s</td><td><font color="green">卖出%s</font></td></tr>' \
                   % (item['symbol'], name, item['policy'])
    for item in l_keep:
        ins = get_instrumentinfos(symbols=item['symbol'])[0]
        name = ins['sec_name']
        content += '<tr><td>%s</td><td>%s</td><td>%s</td></tr>' \
                   % (item['symbol'], name, '')
    for item in l_buy:
        ins = get_instrumentinfos(symbols=item['symbol'])[0]
        name = ins['sec_name']
        vacancies = c - len(l_keep)
        cash = context.account().cash['available']
        volume = floor(cash / vacancies / item['price'] / 100) * 100 if vacancies > 0 else 0
        content += '<tr><td>%s</td><td>%s</td><td><font color="red">买入%s %.2f * %d</font></td></tr>' \
                   % (item['symbol'], name, item['policy'], item['price'], volume)
    content += '</tbody>'
    content += '</table>'
    send_all(subject, content)


def send_all(subject, content):
    for receiver in mail_receivers:
        send(receiver, subject, content)


def send(to, subject, content):
    msg = MIMEText(content, 'html', 'utf-8')
    msg['Subject'] = Header(subject, 'utf-8').encode()
    server = SMTP_SSL(smtp_server, smtp_port)
    server.login(from_addr, password)
    server.sendmail(from_addr, [to], msg.as_string())
    server.quit()
