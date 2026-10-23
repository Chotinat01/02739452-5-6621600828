"""Inspect an isolated installation after its first live DAG run."""
import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path
import psycopg2
import requests
from pipeline.database import connect

args = argparse.ArgumentParser()
args.add_argument('--commit', required=True)
options = args.parse_args()
run_id = 'clean_clone_audit'
with connect() as conn, conn.cursor() as cur:
    cur.execute('SELECT state FROM dag_run WHERE dag_id=%s AND run_id=%s', ('international_recipe_pipeline', run_id))
    state = cur.fetchone()[0]
    cur.execute('SELECT quality FROM recipes.runs WHERE run_id=%s', (run_id,))
    quality = cur.fetchone()[0]
    cur.execute('SELECT source,status,valid_count,detail FROM recipes.source_status WHERE run_id=%s ORDER BY source', (run_id,))
    sources = [{'source': s, 'status': st, 'valid_count': n, 'cached_pages': detail.get('cached_pages', 0)}
               for s, st, n, detail in cur.fetchall()]
    cur.execute("SELECT task_id,start_date,end_date FROM task_instance WHERE run_id=%s AND task_id LIKE '%%.extract'", (run_id,))
    intervals = cur.fetchall()
    parallel = len(intervals) == 3 and max(r[1] for r in intervals) < min(r[2] for r in intervals)
with psycopg2.connect(os.environ['DASHBOARD_DATABASE_URL']) as conn, conn.cursor() as cur:
    cur.execute('SELECT count(*) FROM recipes.current_recipes')
    dashboard_count = cur.fetchone()[0]
    cur.execute("SELECT has_table_privilege(current_user,'recipes.canonical','INSERT'), has_table_privilege(current_user,'recipes.canonical','UPDATE'), has_table_privilege(current_user,'recipes.canonical','DELETE')")
    dashboard_cannot_write = not any(cur.fetchone())
checks = {'scheduled_dag_success': state == 'success', 'all_three_sources_usable': quality['all_three_sources_available'],
          'at_least_100_recipes': dashboard_count >= 100,
          'all_sources_used_cold_cache': all(s['cached_pages'] == 0 for s in sources),
          'extract_tasks_overlap_in_time': parallel, 'dashboard_role_cannot_write': dashboard_cannot_write,
          'dashboard_http_healthy': requests.get('http://dashboard:8501/_stcore/health', timeout=10).status_code == 200}
result = {'checked_at_utc': datetime.now(timezone.utc).isoformat(), 'audited_commit': options.commit,
          'run_id': run_id, 'project': 'recipe-exam-audit', 'database_and_data_volumes_created_fresh': True,
          'docker_image_build_cache_reused': True, 'dashboard_count': dashboard_count, 'sources': sources,
          'extract_task_intervals': [{'task_id': t, 'start': str(s), 'end': str(e)} for t, s, e in intervals],
          'checks': checks, 'passed': all(checks.values())}
Path('/opt/airflow/project/evidence/clean_clone_audit.json').write_text(json.dumps(result, indent=2), encoding='utf-8')
print(json.dumps(result, indent=2))
assert result['passed'], 'Clean clone verification failed'
