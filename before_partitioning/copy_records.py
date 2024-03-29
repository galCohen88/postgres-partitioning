from os import listdir

import psycopg2

conn = psycopg2.connect('dbname=pgpartitions user=postgres password=password123 host=localhost port=5432')

cur = conn.cursor()


path = 'data'

files = []

for file in listdir(path):
    f = open(path + '/' + str(file), "r+")
    cur.copy_from(f, 'item_audit', columns=('item_id', 'logdate', 'action_id', 'metadata'), sep=",")
    conn.commit()
    f.close()
