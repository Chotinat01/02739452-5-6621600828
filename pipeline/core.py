import hashlib
import re
import unicodedata
from collections import defaultdict
from datetime import datetime, timezone

SOURCES = ('mealdb', 'wikibooks', 'spoonacular')


def now():
    return datetime.now(timezone.utc).isoformat()


def clean(value):
    return re.sub(r'\s+', ' ', str(value or '')).strip()


def key(value):
    value = unicodedata.normalize('NFKC', clean(value)).casefold()
    return re.sub(r'[^\w\s]', ' ', value, flags=re.UNICODE).strip()


def ingredient_key(value):
    value = key(value)
    # Conservative cleanup: keep ingredient identity, remove only leading amounts/units.
    value = re.sub(r'^[\d\s]+', '', value)
    value = re.sub(r'^(?:cups?|tbsps?|tsps?|tablespoons?|teaspoons?|grams?|kg|g|ml|oz|ounces?|lbs?|pounds?|pinch(?:es)?)\s+', '', value)
    return clean(value)


def recipe_id(record):
    # Quantities remain part of identity. Different recipes with the same name stay separate.
    ingredients = sorted({key(i['raw']) for i in record['ingredients']})
    text = key(record['title']) + '|' + '|'.join(ingredients)
    return hashlib.sha256(text.encode('utf-8')).hexdigest()


def validate(records):
    good, rejected = [], []
    for item in records:
        reasons = []
        if not clean(item.get('title')):
            reasons.append('missing title')
        if not clean(item.get('source_id')):
            reasons.append('missing source_id')
        if item.get('source') not in SOURCES:
            reasons.append('unknown source')
        if not item.get('url', '').startswith('https://'):
            reasons.append('invalid source URL')
        ingredients = item.get('ingredients') or []
        if not ingredients or any(not clean(i.get('name')) for i in ingredients):
            reasons.append('missing ingredients')
        minutes = item.get('minutes')
        if minutes is not None and (not isinstance(minutes, (int, float)) or minutes <= 0):
            reasons.append('invalid cooking time')
        if reasons:
            rejected.append({'source': item.get('source'), 'source_id': item.get('source_id'), 'title': item.get('title'), 'reasons': reasons})
        else:
            item = dict(item)
            item['title'] = clean(item['title'])
            item['canonical_id'] = recipe_id(item)
            good.append(item)
    return good, rejected


def merge_records(records):
    grouped = defaultdict(list)
    # Repeated source IDs in a response must not inflate counts either.
    unique = {(r['source'], r['source_id']): r for r in records}
    for record in unique.values():
        grouped[record['canonical_id']].append(record)
    merged = []
    for canonical_id, variants in sorted(grouped.items()):
        variants.sort(key=lambda r: (SOURCES.index(r['source']), r['source_id']))
        first = dict(variants[0])
        first['canonical_id'] = canonical_id
        first['sources'] = [{'source': r['source'], 'source_id': r['source_id'], 'url': r['url']} for r in variants]
        first['source_count'] = len({r['source'] for r in variants})
        first['ingredient_count'] = len({ingredient_key(i['name']) for i in first['ingredients']})
        for field in ('category', 'cuisine', 'minutes'):
            first[field] = next((r.get(field) for r in variants if r.get(field)), None)
        merged.append(first)
    return merged


def quality_report(merged, rejected, input_count, statuses, force_fail=False):
    rejected_fraction = len(rejected) / max(1, input_count)
    available = sum(s['status'] in ('ok', 'partial') and s.get('valid_count', 0) > 0 for s in statuses)
    checks = {
        'at_least_100_unique_recipes': len(merged) >= 100,
        'at_least_one_usable_source': available >= 1,
        'rejected_fraction_at_most_20_percent': rejected_fraction <= 0.20,
        'fault_injection_disabled': not force_fail,
    }
    return {'passed': all(checks.values()), 'checks': checks, 'unique_count': len(merged),
            'input_count': input_count, 'rejected_count': len(rejected),
            'rejected_fraction': rejected_fraction, 'usable_sources': available,
            'all_three_sources_available': available == 3}
