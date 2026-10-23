"""PostgreSQL persistence. Each run owns its staging and quality evidence."""
import os
import psycopg2
from psycopg2.extras import Json, RealDictCursor, execute_values

DDL = """
CREATE SCHEMA IF NOT EXISTS recipes;
CREATE TABLE IF NOT EXISTS recipes.runs (
 run_id text PRIMARY KEY, started_at timestamptz NOT NULL DEFAULT now(),
 finished_at timestamptz, status text NOT NULL DEFAULT 'running',
 settings jsonb NOT NULL DEFAULT '{}', quality jsonb NOT NULL DEFAULT '{}');
CREATE TABLE IF NOT EXISTS recipes.source_status (
 run_id text REFERENCES recipes.runs ON DELETE CASCADE, source text,
 status text NOT NULL, extracted_count int NOT NULL DEFAULT 0,
 valid_count int NOT NULL DEFAULT 0, rejected_count int NOT NULL DEFAULT 0,
 detail jsonb NOT NULL DEFAULT '{}', updated_at timestamptz NOT NULL DEFAULT now(),
 PRIMARY KEY(run_id,source));
CREATE TABLE IF NOT EXISTS recipes.staging (
 run_id text REFERENCES recipes.runs ON DELETE CASCADE, source text, source_id text,
 record jsonb NOT NULL, PRIMARY KEY(run_id,source,source_id));
CREATE TABLE IF NOT EXISTS recipes.quarantine (
 run_id text REFERENCES recipes.runs ON DELETE CASCADE, source text, source_id text,
 record jsonb NOT NULL);
CREATE TABLE IF NOT EXISTS recipes.canonical (
 canonical_id text PRIMARY KEY, title text NOT NULL, category text, cuisine text,
 minutes numeric, ingredient_count int NOT NULL, ingredients jsonb NOT NULL,
 sources jsonb NOT NULL, source_count int NOT NULL, fetched_at timestamptz NOT NULL,
 last_seen_run text REFERENCES recipes.runs, updated_at timestamptz NOT NULL DEFAULT now());
CREATE TABLE IF NOT EXISTS recipes.publication (
 singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton),
 run_id text NOT NULL REFERENCES recipes.runs, published_at timestamptz NOT NULL DEFAULT now());
CREATE OR REPLACE VIEW recipes.current_recipes AS
 SELECT c.* FROM recipes.canonical c JOIN recipes.publication p ON c.last_seen_run=p.run_id;
"""


def connect():
    return psycopg2.connect(os.environ['PIPELINE_DATABASE_URL'])


def init_schema():
    with connect() as conn, conn.cursor() as cur:
        cur.execute(DDL)


def start_run(run_id, settings):
    with connect() as conn, conn.cursor() as cur:
        cur.execute("INSERT INTO recipes.runs(run_id,settings) VALUES (%s,%s) ON CONFLICT(run_id) DO NOTHING", (run_id, Json(settings)))


def stage_source(run_id, source, records, rejected, status):
    with connect() as conn, conn.cursor() as cur:
        cur.execute('DELETE FROM recipes.staging WHERE run_id=%s AND source=%s', (run_id, source))
        cur.execute('DELETE FROM recipes.quarantine WHERE run_id=%s AND source=%s', (run_id, source))
        if records:
            # Defensive source-id deduplication makes retries idempotent.
            unique = {r['source_id']: r for r in records}
            execute_values(cur, 'INSERT INTO recipes.staging(run_id,source,source_id,record) VALUES %s',
                           [(run_id, source, k, Json(r)) for k, r in unique.items()])
        if rejected:
            execute_values(cur, 'INSERT INTO recipes.quarantine(run_id,source,source_id,record) VALUES %s',
                           [(run_id, source, r.get('source_id'), Json(r)) for r in rejected])
        cur.execute('''INSERT INTO recipes.source_status(run_id,source,status,extracted_count,valid_count,rejected_count,detail)
         VALUES(%s,%s,%s,%s,%s,%s,%s) ON CONFLICT(run_id,source) DO UPDATE SET
         status=EXCLUDED.status, extracted_count=EXCLUDED.extracted_count,valid_count=EXCLUDED.valid_count,
         rejected_count=EXCLUDED.rejected_count,detail=EXCLUDED.detail,updated_at=now()''',
                    (run_id, source, status['status'], status.get('extracted_count', 0), len(records), len(rejected), Json(status)))


def read_run(run_id):
    with connect() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute('SELECT record FROM recipes.staging WHERE run_id=%s ORDER BY source,source_id', (run_id,))
        records = [r['record'] for r in cur.fetchall()]
        cur.execute('SELECT record FROM recipes.quarantine WHERE run_id=%s', (run_id,))
        rejected = [r['record'] for r in cur.fetchall()]
        cur.execute('SELECT * FROM recipes.source_status WHERE run_id=%s ORDER BY source', (run_id,))
        statuses = [dict(r) for r in cur.fetchall()]
    return records, rejected, statuses


def save_quality(run_id, report):
    with connect() as conn, conn.cursor() as cur:
        cur.execute('UPDATE recipes.runs SET quality=%s WHERE run_id=%s', (Json(report), run_id))


def publish(run_id, merged):
    # One transaction: readers never observe a half-published set.
    with connect() as conn, conn.cursor() as cur:
        cur.execute('SELECT quality FROM recipes.runs WHERE run_id=%s FOR UPDATE', (run_id,))
        row = cur.fetchone()
        if not row or not row[0].get('passed'):
            raise ValueError('Publication requires a passing quality report')
        execute_values(cur, '''INSERT INTO recipes.canonical
          (canonical_id,title,category,cuisine,minutes,ingredient_count,ingredients,sources,source_count,fetched_at,last_seen_run)
          VALUES %s ON CONFLICT(canonical_id) DO UPDATE SET title=EXCLUDED.title,category=EXCLUDED.category,
          cuisine=EXCLUDED.cuisine,minutes=EXCLUDED.minutes,ingredient_count=EXCLUDED.ingredient_count,
          ingredients=EXCLUDED.ingredients,sources=EXCLUDED.sources,source_count=EXCLUDED.source_count,
          fetched_at=EXCLUDED.fetched_at,last_seen_run=EXCLUDED.last_seen_run,updated_at=now()''',
          [(r['canonical_id'], r['title'], r.get('category'), r.get('cuisine'), r.get('minutes'), r['ingredient_count'],
            Json(r['ingredients']), Json(r['sources']), r['source_count'], r['fetched_at'], run_id) for r in merged])
        cur.execute('''INSERT INTO recipes.publication(singleton,run_id) VALUES(true,%s)
         ON CONFLICT(singleton) DO UPDATE SET run_id=EXCLUDED.run_id,published_at=now()''', (run_id,))
        cur.execute("UPDATE recipes.runs SET status='published',finished_at=now() WHERE run_id=%s", (run_id,))


def block_publication(run_id):
    with connect() as conn, conn.cursor() as cur:
        cur.execute("UPDATE recipes.runs SET status='quality_failed',finished_at=now() WHERE run_id=%s", (run_id,))
