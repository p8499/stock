from psycopg2 import connect


def read_conn():
    conn = connect(host='localhost',
                   port=5432,
                   database='postgres',
                   user='postgres',
                   password='helloJDE',
                   options='-c statement_timeout=120000')
    conn.readonly = False
    conn.autocommit = True
    return conn


def write_conn():
    conn = connect(host='localhost',
                   port=5432,
                   database='postgres',
                   user='postgres',
                   password='helloJDE',
                   options='-c statement_timeout=120000')
    conn.readonly = False
    conn.autocommit = True
    return conn


mail_receivers = ['583558556@qq.com']
from_addr = '583558556@qq.com'
password = 'gpwwqgfxdwtpbfhb'
smtp_server = 'smtp.qq.com'
smtp_port = 465

cash = 1000000
c = 8
