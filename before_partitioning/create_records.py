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

INSERT_STMT = """
INSERT INTO item_audit(item_id, logdate, action_id, metadata)
VALUES
{values}
"""

VALUES_FORMAT = "({item_id}, '{log_date}', {action_id}, '{metadata}')"

current_values = []

for i in range(0, ONE_HUNDRED_MILLION_RECORDS):
    dt = datetime.now() - timedelta(random.randrange(10))
    current_values.append(VALUES_FORMAT.format(item_id=str(random.choice(ITEMS)),
                                               log_date=dt,
                                               action_id=random.choice(ACTION_TYPES),
                                               metadata=str(uuid.uuid4())))
    if len(current_values) == BATCH_SIZE:
        values = ','.join(current_values)
        cur.execute(INSERT_STMT.format(values=values))
        conn.commit()
        current_values = []
        print('batch %s' % str(i))

