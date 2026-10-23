"""Airflow task functions. Large HTML payloads stay on a volume, never in XCom."""
import hashlib
import json
import os
from pathlib import Path
from pipeline import database as db
from pipeline.core import SOURCES, merge_records, now, quality_report, validate
from pipeline.sources import HttpClient, extract, safe_error, transform


def settings_for(context):
    conf = dict(context['dag_run'].conf or {})
    allowed = {'fail_sources', 'force_quality_failure', 'refresh', 'wiki_target', 'wiki_max_pages', 'mealdb_limit', 'spoonacular_limit'}
    return {k: v for k, v in conf.items() if k in allowed}


def path_for(run_id, source, kind):
    root = Path(os.getenv('RECIPE_DATA_DIR', 'data')) / 'runs' / hashlib.sha256(run_id.encode()).hexdigest()
    root.mkdir(parents=True, exist_ok=True)
    return root / f'{source}-{kind}.json'


def write(path, payload):
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding='utf-8')
    return str(path)


def begin(**context):
    db.init_schema()
    db.start_run(context['run_id'], settings_for(context))


def extract_task(source, **context):
    settings = settings_for(context)
    client = HttpClient(Path(os.getenv('RECIPE_DATA_DIR', 'data')) / 'http_cache', refresh=settings.get('refresh', False))
    try:
        payload = extract(source, client, settings)
        payload['status'] = 'partial' if payload.get('warnings') else 'ok'
    except Exception as exc:
        # Expected source failures become data/status; other sources continue.
        payload = {'items': [], 'status': 'unavailable', 'error': safe_error(exc), 'warnings': []}
    payload['fetched_at'] = now()
    return write(path_for(context['run_id'], source, 'raw'), payload)


def transform_task(source, **context):
    raw_path = context['ti'].xcom_pull(task_ids=f'{source}.extract')
    raw = json.loads(Path(raw_path).read_text(encoding='utf-8'))
    records = transform(source, raw)
    good, rejected = validate(records)
    status = {k: raw[k] for k in ('status', 'error', 'warnings', 'cached_pages', 'quota_left') if k in raw}
    status['extracted_count'] = len(raw['items'])
    if raw['status'] != 'unavailable' and not good:
        status['status'] = 'empty'
    elif rejected and raw['status'] != 'unavailable':
        status['status'] = 'partial'
    return write(path_for(context['run_id'], source, 'validated'), {'records': good, 'rejected': rejected, 'status': status})


def load_task(source, **context):
    p = context['ti'].xcom_pull(task_ids=f'{source}.transform')
    payload = json.loads(Path(p).read_text(encoding='utf-8'))
    db.stage_source(context['run_id'], source, payload['records'], payload['rejected'], payload['status'])
    return {'valid_count': len(payload['records']), 'status': payload['status']['status']}


def merge_task(**context):
    records, rejected, statuses = db.read_run(context['run_id'])
    merged = merge_records(records)
    report = quality_report(merged, rejected, len(records) + len(rejected), statuses,
                            bool(settings_for(context).get('force_quality_failure', False)))
    report['checks']['all_source_tasks_recorded'] = {s['source'] for s in statuses} == set(SOURCES)
    report['passed'] = all(report['checks'].values())
    db.save_quality(context['run_id'], report)
    write(path_for(context['run_id'], 'merged', 'validated'), merged)
    return report


def choose_path(**context):
    quality = context['ti'].xcom_pull(task_ids='merge_and_check')
    return 'publish' if quality['passed'] else 'quarantine_and_stop'


def publish_task(**context):
    merged = json.loads(path_for(context['run_id'], 'merged', 'validated').read_text(encoding='utf-8'))
    db.publish(context['run_id'], merged)
    return {'published_count': len(merged)}


def quarantine_task(**context):
    from airflow.exceptions import AirflowException
    db.block_publication(context['run_id'])
    raise AirflowException('Quality gate failed; previous published dataset preserved. See pipeline status.')
