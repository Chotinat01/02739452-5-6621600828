"""Collect live source evidence without pretending an API key is present."""
import json
import logging
from pathlib import Path
from pipeline.core import merge_records, now, validate
from pipeline.sources import HttpClient, extract, transform, safe_error

logging.basicConfig(level=logging.INFO)
root = Path(__file__).resolve().parents[1]
(root / 'evidence').mkdir(exist_ok=True)
client = HttpClient(root / 'data' / 'http_cache')
results = {}
for source in ('mealdb', 'wikibooks', 'spoonacular'):
    try:
        payload = extract(source, client, {'wiki_target': 180})
        good, rejected = validate(transform(source, payload))
        results[source] = {'valid': len(good), 'unique': len(merge_records(good)), 'rejected': len(rejected),
                           'warnings': len(payload.get('warnings', [])), 'checked_at': now()}
        (root / 'data' / (source + '-validated.json')).write_text(json.dumps(good, ensure_ascii=False), encoding='utf-8')
    except Exception as exc:
        results[source] = {'error': safe_error(exc), 'checked_at': now()}
    print(source, results[source], flush=True)
    (root / 'evidence' / 'live_sources.json').write_text(json.dumps(results, indent=2), encoding='utf-8')
