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

CSV_FORMAT = "{item_id},{action_id},'{metadata}'"
FILE_NAME = "data/records_{}.csv"


def yield_items(date_delta):
    for i in range(0, ONE_HUNDRED_MILLION_RECORDS):
        dt = datetime.now() - timedelta(date_delta)
        yield CSV_FORMAT.format(item_id=str(random.choice(ITEMS)),
                                action_id=random.choice(ACTION_TYPES),
                                metadata=str(uuid.uuid4())), dt

batch = 0
count = 0
date_delta = 0
dt = datetime.now() - timedelta(date_delta)
f = open(FILE_NAME.format(dt.date()), "w+")
for item, dt in yield_items(date_delta):
    f.write(item + '\n')
    batch += 1
    count += 1
    if batch == BATCH_SIZE:
        f.close()
        date_delta += 1
        f = open(FILE_NAME.format(dt.date()), "w+")
        print('batch %s' % str(count))
        batch = 0
        dt = datetime.now() - timedelta(date_delta)

f.close()
