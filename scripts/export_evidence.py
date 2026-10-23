"""Export source counts from the published Airflow run, not a separate mock run."""
import json
from pathlib import Path
from pipeline.database import connect

with connect() as conn, conn.cursor() as cur:
    cur.execute('SELECT run_id FROM recipes.publication')
    run_id = cur.fetchone()[0]
    cur.execute('SELECT source,status,valid_count,rejected_count,detail,updated_at FROM recipes.source_status WHERE run_id=%s ORDER BY source', (run_id,))
    rows = cur.fetchall()
    result = {}
    for source, status, valid, rejected, detail, checked_at in rows:
        cur.execute("SELECT count(DISTINCT record->>'canonical_id') FROM recipes.staging WHERE run_id=%s AND source=%s", (run_id, source))
        result[source] = {'run_id': run_id, 'status': status, 'valid': valid, 'unique': cur.fetchone()[0],
                          'rejected': rejected, 'warnings': len(detail.get('warnings', [])), 'checked_at': str(checked_at)}
out = Path('/opt/airflow/project/evidence/live_sources.json')
out.write_text(json.dumps(result, indent=2), encoding='utf-8')
print('Exported source evidence for', run_id)
