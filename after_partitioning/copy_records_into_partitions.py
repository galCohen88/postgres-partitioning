from os import listdir

import psycopg2

conn = psycopg2.connect('dbname=pgpartitions user=postgres password=password123 host=localhost port=5432')

cur = conn.cursor()

partition_creation_table = """
    CREATE TABLE {partition} PARTITION OF item_audit_master;
"""

partition_prefix = 'item_audit_{}'
path = 'data'

files = []

for file in listdir(path):
    partition_date = file.split('_')[1].split('.')[0].replace('-', '_')
    partition_name = partition_prefix.format(partition_date)

    cur.execute(partition_creation_table.format(partition=partition_name))
    conn.commit()

    f = open(path + '/' + str(file), "r+")

    cur.copy_from(f, 'item_audit', columns=('item_id', 'logdate', 'action_id', 'metadata'), sep=",")
    conn.commit()
    f.close()
