import psycopg2

conn = psycopg2.connect('dbname=pgpartitions user=postgres password=password123 host=localhost port=5432')

cur = conn.cursor()

create_master_tbl_stmt = """
CREATE TABLE item_audit (
    item_id         int not null,
    logdate         date not null,
    action_id       int,
    metadata       varchar(1000)
)
"""

create_index_stmt = """
CREATE INDEX idx_logdate on item_audit(logdate)
"""

cur.execute(create_master_tbl_stmt)
cur.execute(create_index_stmt)
conn.commit()
conn.close()
