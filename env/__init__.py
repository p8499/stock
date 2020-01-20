from psycopg2 import connect


def read_conn():
    conn = connect(host='192.168.100.103',
                   port=5432,
                   database='postgres',
                   user='postgres',
                   password='helloJDE',
                   options='-c statement_timeout=1200000')
    conn.readonly = False
    conn.autocommit = True
    return conn


def write_conn():
    conn = connect(host='192.168.100.103',
                   port=5432,
                   database='postgres',
                   user='postgres',
                   password='helloJDE',
                   options='-c statement_timeout=1200000')
    conn.readonly = False
    conn.autocommit = True
    return conn

token = '10fd02a7933e4d2c9e52328f077eb321b276e2cb'
strategy_id = '8e0be803-27a3-11ea-9cc6-000c2959f4b2'

mail_receivers = ['583558556@qq.com']
from_addr = '583558556@qq.com'
password = 'gpwwqgfxdwtpbfhb'
smtp_server = 'smtp.qq.com'
smtp_port = 465

cash = 1000000
c = 8
