import os
from psycopg2 import sql
from pipeline.database import connect, init_schema

init_schema()
with connect() as conn, conn.cursor() as cur:
    cur.execute("SELECT 1 FROM pg_roles WHERE rolname='recipe_reader'")
    if not cur.fetchone():
        cur.execute('CREATE ROLE recipe_reader LOGIN')
    cur.execute(sql.SQL('ALTER ROLE recipe_reader PASSWORD {}').format(sql.Literal(os.environ['DASHBOARD_PASSWORD'])))
    cur.execute('GRANT USAGE ON SCHEMA recipes TO recipe_reader')
    cur.execute('GRANT SELECT ON ALL TABLES IN SCHEMA recipes TO recipe_reader')
    cur.execute('ALTER DEFAULT PRIVILEGES IN SCHEMA recipes GRANT SELECT ON TABLES TO recipe_reader')
print('Recipe schema and read-only dashboard role ready')
