from os import listdir
from os.path import isfile, join
import random
from datetime import datetime, timedelta
import uuid

import psycopg2

conn = psycopg2.connect('dbname=pgpartitions user=postgres password=password123 host=localhost port=5432')

cur = conn.cursor()

ONE_HUNDRED_MILLION_RECORDS = 100000000
BATCH_SIZE = 1000000
ACTION_TYPES = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
ITEMS = [i for i in range(0, 10000)]

current_values = []

CSV_FORMAT = "{item_id},'{log_date}',{action_id},'{metadata}'"
FILE_NAME = "data/records_{}.csv"


def yield_items():
    for i in range(0, ONE_HUNDRED_MILLION_RECORDS):
        dt = datetime.now() - timedelta(random.randrange(10))
        yield CSV_FORMAT.format(item_id=str(random.choice(ITEMS)),
                                                   log_date=dt,
                                                   action_id=random.choice(ACTION_TYPES),
                                                   metadata=str(uuid.uuid4()))

batch = 0
count = 0
f = open(FILE_NAME.format(count), "w+")
for item in yield_items():
    f.write(item + '\n')
    batch += 1
    count += 1
    if batch == BATCH_SIZE:
        f.close()
        f = open(FILE_NAME.format(count), "w+")
        print('batch %s' % str(count))
        batch = 0

f.close()

path = 'data'

files = []

# onlyfiles = [f for f in listdir(path) if isfile(join(path, f))]

for file in listdir(path):
    f = open(path + '/' + str(file), "r+")
    cur.copy_from(f, 'item_audit', columns=('item_id', 'logdate', 'action_id', 'metadata'), sep=",")
    conn.commit()
    f.close()
