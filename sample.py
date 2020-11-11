import sqlite3

def create_sample():
    # DB接続
    conn = sqlite3.connect('sqlitefile.db')
    # → ファイルがない場合、勝手に作られる

    # カーソルを作成
    cur = conn.cursor()

    # SQL発行
    # drop table
    cur.execute(
        'DROP TABLE IF EXISTS log_counts;'
    )
    # create table
    cur.execute(
        'CREATE TABLE IF NOT EXISTS log_counts ' +
        '( ' +
            'dataset_name STR NOT NULL, ' +
            'table_name STR NOT NULL, ' +
            'y INT NOT NULL, ' +
            'm INT NOT NULL, ' +
            'd INT NOT NULL, ' +
            'h INT NOT NULL, ' +
            'cnt INT NOT NULL' +
        ');'
    )

    # DB切断
    conn.close()

    return None


def insert_sample():
    # DB接続
    conn = sqlite3.connect('sqlitefile.db')

    # カーソルを作成
    cur = conn.cursor()

    # insertするデータ
    log_recs = [
        ('ninjatribes_log', 'active_', 2020, 10, 31, 15, 7739),
        ('ninjatribes_log', 'active_', 2020, 10, 31, 16, 5548),
        ('ninjatribes_log', 'active_', 2020, 10, 31, 17, 3852),
        ('ninjatribes_log', 'active_', 2020, 10, 31, 18, 2718)
    ]

    # insert
    cur.executemany(
        'INSERT INTO log_counts values (?,?,?,?,?,?,?);',
        log_recs
    )

    # commit
    conn.commit()

    # DB切断
    conn.close()

    return None


def select_sample():
    # DB接続
    conn = sqlite3.connect('sqlitefile.db')
    # → ファイルがない場合、勝手に作られる

    # カーソルを作成
    cur = conn.cursor()

    # select
    table = cur.execute('select * from log_counts')
    recs = table.fetchall()
    for rec in recs:
        print(rec)
        print(type(rec))    # tuple型になっている
        print(f'{rec[2]}-{rec[3]}-{rec[4]}({rec[5]}): {rec[6]}')

    # DB切断
    conn.close()

    return None


if __name__=='__main__':
    # コメントアウトを色々変えてお試しください
    #create_sample()
    #insert_sample()
    select_sample()
