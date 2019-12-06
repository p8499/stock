from env import read_conn


def codes(read):
    cursor = read.cursor()
    cursor.execute('SELECT code,name FROM industries')
    rows = cursor.fetchall()
    cursor.close()
    return list(map(lambda x: x[0], rows))


if __name__ == '__main__':
    codes(read_conn())
