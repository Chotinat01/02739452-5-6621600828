"""Exercise the scheduled DAG through its REST API and inspect real PostgreSQL."""
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
import requests
from pipeline.database import connect

root = Path('/opt/airflow/project')
api = 'http://airflow-webserver:8080/api/v1/dags/international_recipe_pipeline'
session = requests.Session()
session.auth = (os.environ['AIRFLOW_ADMIN_USER'], os.environ['AIRFLOW_ADMIN_PASSWORD'])
stamp = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')
results = {'checked_at': stamp, 'runs': []}


def counts():
    with connect() as conn, conn.cursor() as cur:
        cur.execute('SELECT count(*) FROM recipes.canonical')
        canonical = cur.fetchone()[0]
        cur.execute('SELECT count(*) FROM recipes.current_recipes')
        current = cur.fetchone()[0]
        cur.execute('SELECT run_id FROM recipes.publication')
        row = cur.fetchone()
        return {'canonical': canonical, 'current': current, 'publication': row[0] if row else None}


def run(label, conf, expected, count=None):
    run_id = f'verify_{label}_{stamp}'
    before = counts()
    r = session.post(api + '/dagRuns', json={'dag_run_id': run_id, 'conf': conf}, timeout=30)
    r.raise_for_status()
    deadline = time.monotonic() + 900
    while time.monotonic() < deadline:
        r = session.get(api + '/dagRuns/' + run_id, timeout=30)
        r.raise_for_status()
        state = r.json()['state']
        if state in ('success', 'failed'):
            break
        time.sleep(3)
    else:
        raise TimeoutError('DAG exceeded verification time limit')
    after = counts()
    with connect() as conn, conn.cursor() as cur:
        cur.execute('SELECT status,quality FROM recipes.runs WHERE run_id=%s', (run_id,))
        status, quality = cur.fetchone()
        cur.execute('SELECT source,status,valid_count FROM recipes.source_status WHERE run_id=%s ORDER BY source', (run_id,))
        sources = [{'source': s, 'status': st, 'valid': n} for s, st, n in cur.fetchall()]
    item = {'label': label, 'run_id': run_id, 'airflow_state': state, 'status': status, 'quality': quality,
            'sources': sources, 'before': before, 'after': after}
    results['runs'].append(item)
    (root / 'evidence' / 'verification.json').write_text(json.dumps(results, indent=2), encoding='utf-8')
    assert state == expected, (label, state)
    if count is not None:
        assert after['current'] == count, (label, after)
    if label == 'repeat':
        assert before['canonical'] == after['canonical'], 'UPSERT grew canonical table on identical data'
        assert quality['all_three_sources_available'], 'Need all three live sources in the baseline'
    if label.endswith('_outage'):
        failed_source = conf['fail_sources'][0]
        assert next(s for s in sources if s['source'] == failed_source)['status'] == 'unavailable'
        assert quality['usable_sources'] == 2
        assert after['current'] >= 100
    if label == 'quality_failure':
        assert after == before, 'Failed quality gate changed publication'
        assert status == 'quality_failed'
    print(label, state, after, flush=True)


baseline_count = counts()['current']
run('repeat', {}, 'success', baseline_count)
run('mealdb_outage', {'fail_sources': ['mealdb']}, 'success')
run('quality_failure', {'force_quality_failure': True}, 'failed')
run('forkify_outage', {'fail_sources': ['forkify']}, 'success')
run('wikibooks_outage', {'fail_sources': ['wikibooks']}, 'success')
run('restore', {}, 'success', baseline_count)
results['passed'] = True
(root / 'evidence' / 'verification.json').write_text(json.dumps(results, indent=2), encoding='utf-8')
print('All scheduled DAG verification scenarios passed', flush=True)
