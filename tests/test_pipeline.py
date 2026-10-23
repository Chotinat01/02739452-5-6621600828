from copy import deepcopy
from pipeline.core import merge_records, quality_report, validate
from pipeline.sources import parse_wikibooks_page, transform


def sample():
    return {'source': 'mealdb', 'source_id': '1', 'title': 'Apple Pie', 'ingredients': [{'name': 'apple', 'raw': '2 apples'}],
            'url': 'https://example.org/recipe', 'fetched_at': '2026-10-23T00:00:00+00:00'}


def test_deduplicate_across_sources_but_keep_different_quantities():
    one = sample()
    two = deepcopy(one)
    two.update(source='wikibooks', source_id='2')
    three = deepcopy(two)
    three['source_id'] = '3'
    three['ingredients'] = [{'name': 'apple', 'raw': '3 apples'}]
    same_from_third_source = deepcopy(one)
    same_from_third_source.update(source='forkify', source_id='99')
    good, _ = validate([one, two, three, same_from_third_source, one])
    merged = merge_records(good)
    assert len(merged) == 2
    assert sorted(r['source_count'] for r in merged) == [1, 3]


def test_bad_fields_are_quarantined():
    bad = sample()
    bad.update(title='', minutes=-1, ingredients=[])
    good, rejected = validate([bad])
    assert not good and len(rejected[0]['reasons']) == 3


def test_quality_gate_requires_100_and_allows_source_outage():
    statuses = [{'status': 'ok', 'valid_count': 100}, {'status': 'unavailable'}, {'status': 'unavailable'}]
    assert quality_report([{}] * 100, [], 100, statuses)['passed']
    assert not quality_report([{}] * 99, [], 99, statuses)['passed']
    assert not quality_report([{}] * 100, [], 100, statuses, force_fail=True)['passed']


def test_wikibooks_heading_wrappers_sections_and_cached_timestamp():
    record = parse_wikibooks_page({'url': 'https://en.wikibooks.org/wiki/Cookbook:Apple_Pie_I',
        'fetched_at': '2026-10-20T00:00:00+00:00',
        'html': '''<h1 id="firstHeading">Cookbook: Apple Pie I</h1><div class="mw-parser-output">
        <div class="mw-heading2"><h2>Ingredients</h2></div><div class="mw-heading3"><h3>Filling</h3></div>
        <ul><li>2 apples</li><li>1 cup sugar</li></ul><div class="mw-heading2"><h2>Procedure</h2></div>
        <ol><li>Mix and bake</li></ol></div><div id="mw-normal-catlinks"><ul>
        <li><a>Easy recipes</a></li><li><a>Recipes for pie</a></li></ul></div>'''})
    assert record['title'] == 'Apple Pie I'
    assert len(record['ingredients']) == 2
    assert record['fetched_at'].startswith('2026-10-20')
    assert record['category'] == 'pie'


def test_mealdb_missing_time_and_blank_ingredients():
    result = transform('mealdb', {'items': [{'idMeal': 1, 'strMeal': 'Soup', 'strIngredient1': 'Salt', 'strMeasure1': '1 tsp'}]})
    assert result[0]['minutes'] is None
    assert result[0]['ingredients'] == [{'name': 'salt', 'raw': '1 tsp Salt'}]


def test_forkify_null_quantities_and_provider_reported_minutes():
    records = transform('forkify', {'fetched_at': '2026-10-23T00:00:00Z', 'items': [{
        'id': 'sample', 'title': 'Pizza', 'cooking_time': 45,
        'ingredients': [{'quantity': None, 'unit': '', 'description': 'Salt'},
                        {'quantity': 0.5, 'unit': 'cup', 'description': 'onion'}]}]})
    good, rejected = validate(records)
    assert not rejected and good[0]['minutes'] == 45
    assert good[0]['ingredients'][0]['raw'] == 'Salt'
    assert good[0]['ingredients'][1]['raw'] == '0.5 cup onion'
    assert good[0]['category'] is None


def test_forkify_cache_preserves_retrieval_time_and_avoids_http(tmp_path):
    import json
    from pipeline.sources import extract_forkify
    payload = {'items': [{'id': 'sample'}], 'warnings': [], 'fetched_at': '2026-10-20T00:00:00Z'}
    (tmp_path / 'forkify-pizza-10.json').write_text(json.dumps(payload), encoding='utf-8')

    class CachedClient:
        cache_dir = tmp_path
        refresh = False
        def get(self, url):
            raise AssertionError('Warm cache must not call HTTP')

    result = extract_forkify(CachedClient())
    assert result['fetched_at'] == payload['fetched_at']
    assert result['cached_pages'] == 2
