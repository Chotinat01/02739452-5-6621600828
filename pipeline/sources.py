"""Live HTTP sources. No Selenium, mock data, or embedded credentials."""
import json
import logging
import os
import re
import time
from pathlib import Path
from urllib.parse import urljoin, urlsplit, unquote
from urllib.robotparser import RobotFileParser

import requests
from bs4 import BeautifulSoup

from pipeline.core import clean, ingredient_key, now

LOG = logging.getLogger(__name__)
USER_AGENT = 'RecipeExamStudent/1.0 (educational recipe aggregation; low request rate)'
WIKI = 'https://en.wikibooks.org'
INDEX = WIKI + '/wiki/Category:Recipes'


class HttpClient:
    def __init__(self, cache_dir=None, refresh=False, delay=1.1):
        self.session = requests.Session()
        self.session.headers['User-Agent'] = USER_AGENT
        self.robots = {}
        self.last_request = {}
        self.cache_dir = Path(cache_dir) if cache_dir else None
        self.refresh = refresh
        self.delay = max(1.0, float(delay))
        if self.cache_dir:
            self.cache_dir.mkdir(parents=True, exist_ok=True)

    def get(self, url, *, headers=None, scraping=False):
        host = urlsplit(url).netloc
        if scraping:
            if host not in self.robots:
                robots_url = f'https://{host}/robots.txt'
                response = self.session.get(robots_url, timeout=(10, 45))
                # Fail closed: inaccessible robots.txt is not permission to scrape.
                response.raise_for_status()
                parser = RobotFileParser(robots_url)
                parser.parse(response.text.lstrip('\ufeff').splitlines())
                self.robots[host] = parser
            parser = self.robots[host]
            if not parser.can_fetch(USER_AGENT, url):
                raise PermissionError(f'robots.txt disallows {urlsplit(url).path}')
            delay = max(self.delay, parser.crawl_delay(USER_AGENT) or parser.crawl_delay('*') or 0)
        else:
            delay = self.delay
        wait = delay - (time.monotonic() - self.last_request.get(host, 0))
        if wait > 0:
            time.sleep(wait)
        for attempt in range(3):
            self.last_request[host] = time.monotonic()
            try:
                response = self.session.get(url, headers=headers, timeout=(10, 45))
            except requests.RequestException:
                if attempt < 2:
                    time.sleep(2 * (attempt + 1))
                    continue
                raise
            if response.status_code == 429 or response.status_code >= 500:
                if attempt < 2:
                    retry = response.headers.get('Retry-After', '5')
                    time.sleep(min(60, max(2, int(retry) if retry.isdigit() else 5) * (attempt + 1)))
                    continue
            # Do not log a requests exception containing a credential-bearing URL.
            if not response.ok:
                raise RuntimeError(f'HTTP {response.status_code} from {host}{urlsplit(url).path}')
            return response
        raise RuntimeError('HTTP retry limit reached')

    def wiki_html(self, url):
        import hashlib
        # Check current robots rules even when reusing a fresh cached response.
        host = urlsplit(url).netloc
        if host not in self.robots:
            robots_url = f'https://{host}/robots.txt'
            response = self.session.get(robots_url, timeout=(10, 45))
            response.raise_for_status()
            parser = RobotFileParser(robots_url)
            parser.parse(response.text.lstrip('\ufeff').splitlines())
            self.robots[host] = parser
        if not self.robots[host].can_fetch(USER_AGENT, url):
            raise PermissionError('robots.txt disallows requested article')
        cache = self.cache_dir / (hashlib.sha256(url.encode()).hexdigest() + '.html') if self.cache_dir else None
        if cache and cache.exists() and not self.refresh and time.time() - cache.stat().st_mtime < 86400:
            return cache.read_text(encoding='utf-8'), True
        html = self.get(url, scraping=True).text
        if cache:
            cache.write_text(html, encoding='utf-8')
        return html, False


def extract_mealdb(client, limit=100):
    # Developer/educational key 1 is explicitly published by TheMealDB.
    data = client.get('https://www.themealdb.com/api/json/v1/1/search.php?s=').json()
    meals = {m['idMeal']: m for m in (data.get('meals') or [])}
    # Search endpoint may return only 25 records. Use the documented first-letter endpoint.
    for letter in 'abcdefghijklmnopqrstuvwxyz':
        if len(meals) >= limit:
            break
        page = client.get(f'https://www.themealdb.com/api/json/v1/1/search.php?f={letter}').json()
        for meal in page.get('meals') or []:
            meals[meal['idMeal']] = meal
    return {'items': list(meals.values())[:limit], 'warnings': [], 'cached_pages': 0}


def extract_wikibooks(client, target=180, max_pages=320):
    pending, seen_index, seen_recipe, pages, errors = [], set(), set(), [], []
    index_url, cached = INDEX, 0
    # Only normal /wiki/ pages are followed, never /w/ dynamic category URLs.
    while index_url and len(seen_index) < 4 and len(pending) < max_pages:
        seen_index.add(index_url)
        html, hit = client.wiki_html(index_url)
        cached += int(hit)
        soup = BeautifulSoup(html, 'html.parser')
        for link in soup.select('#mw-pages .mw-category-group a'):
            url = urljoin(WIKI, link.get('href', ''))
            if url.startswith(WIKI + '/wiki/Cookbook:') and url not in seen_recipe:
                pending.append(url)
                seen_recipe.add(url)
        index_url = None
        for link in soup.select('#mw-pages a'):
            if 'next page' in link.get_text(' ', strip=True).casefold():
                candidate = urljoin(WIKI, link.get('href', ''))
                if candidate.startswith(WIKI + '/wiki/') and candidate not in seen_index:
                    index_url = candidate
                    break
    usable = 0
    for url in pending[:max_pages]:
        if usable >= target:
            break
        try:
            html, hit = client.wiki_html(url)
            cached += int(hit)
            import hashlib
            cache_file = client.cache_dir / (hashlib.sha256(url.encode()).hexdigest() + '.html') if client.cache_dir else None
            from datetime import datetime, timezone
            fetched_at = datetime.fromtimestamp(cache_file.stat().st_mtime, timezone.utc).isoformat() if cache_file and cache_file.exists() else now()
            page = {'url': url, 'html': html, 'fetched_at': fetched_at}
            # Count usable recipes while still saving raw HTML for the transform task.
            if parse_wikibooks_page(page)['ingredients']:
                pages.append(page)
                usable += 1
            else:
                errors.append({'page': url, 'reason': 'no ingredient section; index/disambiguation skipped'})
        except (requests.RequestException, RuntimeError, PermissionError, ValueError) as exc:
            errors.append({'page': url, 'reason': safe_error(exc)})
        if len(pages) and len(pages) % 25 == 0:
            LOG.info('Wikibooks usable recipes: %d', len(pages))
    if not pages:
        raise RuntimeError('No usable recipe pages; inspect selector/robots/network')
    return {'items': pages, 'warnings': errors, 'cached_pages': cached}


def extract_spoonacular(client, limit=10):
    api_key = os.getenv('SPOONACULAR_API_KEY', '').strip()
    if not api_key:
        raise RuntimeError('missing_api_key: set SPOONACULAR_API_KEY in .env')
    headers = {'x-api-key': api_key}
    limit = max(1, min(int(limit), 20))
    search = client.get(f'https://api.spoonacular.com/recipes/complexSearch?number={limit}&offset=0', headers=headers)
    ids = [str(r['id']) for r in search.json().get('results', [])]
    if not ids:
        raise RuntimeError('Spoonacular returned no recipes')
    left = search.headers.get('X-API-Quota-Left')
    # Reserve headroom rather than using the final points on a bulk request.
    if left is not None and float(left) < len(ids) + 3:
        raise RuntimeError('insufficient_quota: reserve remaining API points')
    info = client.get('https://api.spoonacular.com/recipes/informationBulk?ids=' + ','.join(ids) + '&includeNutrition=false', headers=headers)
    return {'items': info.json(), 'warnings': [], 'cached_pages': 0,
            'quota_left': info.headers.get('X-API-Quota-Left')}


def parse_wikibooks_page(page):
    soup = BeautifulSoup(page['html'], 'html.parser')
    title = soup.select_one('#firstHeading')
    title = clean(title.get_text(' ', strip=True) if title else unquote(page['url'].split('/')[-1]))
    title = re.sub(r'^Cookbook\s*:\s*', '', title)
    body = soup.select_one('.mw-parser-output')
    ingredients, active = [], False
    if body:
        for node in body.children:
            if not getattr(node, 'name', None):
                continue
            heading = node if node.name in ('h2', 'h3') else node.find(['h2', 'h3']) if node.name == 'div' else None
            if heading:
                text = heading.get_text(' ', strip=True).casefold()
                if heading.name == 'h2':
                    active = 'ingredient' in text
                elif 'ingredient' in text:
                    active = True
                continue
            if active and node.name in ('ul', 'ol', 'table'):
                elements = node.select('li') if node.name != 'table' else node.select('tr')
                for element in elements:
                    if element.name == 'li' and element.find_parent('li'):
                        continue
                    raw = clean(element.get_text(' ', strip=True))
                    if not raw or len(raw) > 500:
                        continue
                    name = ingredient_key(raw)
                    if name:
                        ingredients.append({'name': name, 'raw': raw})
    # Stable category labels only; do not infer cuisine/health claims from names.
    categories = [clean(a.get_text(' ', strip=True)) for a in soup.select('#mw-normal-catlinks li a')]
    # Prefer food-type categories. Difficulty/diet labels are not dish types.
    category = next((c[len('Recipes for '):] for c in categories if c.startswith('Recipes for ')), None)
    if category is None:
        known_types = {'Main course recipes', 'Side dish recipes', 'Appetizer recipes', 'Dessert recipes',
                       'Soup recipes', 'Salad recipes', 'Beverage recipes', 'Breakfast recipes'}
        category = next((c.removesuffix(' recipes') for c in categories if c in known_types), None)
    return {'source': 'wikibooks', 'source_id': unquote(urlsplit(page['url']).path),
            'title': title, 'category': category, 'cuisine': None, 'minutes': None,
            'ingredients': ingredients, 'url': page['url'], 'fetched_at': page.get('fetched_at', now())}


def transform(source, payload):
    records = []
    for item in payload.get('items', []):
        if source == 'wikibooks':
            records.append(parse_wikibooks_page(item))
        elif source == 'mealdb':
            ingredients = []
            for n in range(1, 21):
                name = clean(item.get(f'strIngredient{n}'))
                if name:
                    raw = clean(clean(item.get(f'strMeasure{n}')) + ' ' + name)
                    ingredients.append({'name': ingredient_key(name), 'raw': raw})
            records.append({'source': source, 'source_id': str(item['idMeal']), 'title': item.get('strMeal'),
                            'category': item.get('strCategory'), 'cuisine': item.get('strArea'), 'minutes': None,
                            'ingredients': ingredients, 'url': f"https://www.themealdb.com/meal/{item['idMeal']}", 'fetched_at': payload.get('fetched_at', now())})
        elif source == 'spoonacular':
            ingredients = [{'name': ingredient_key(i.get('name') or i.get('original')),
                            'raw': clean(i.get('original') or i.get('name'))} for i in item.get('extendedIngredients', [])]
            records.append({'source': source, 'source_id': str(item['id']), 'title': item.get('title'),
                            'category': ', '.join(item.get('dishTypes') or []) or None,
                            'cuisine': ', '.join(item.get('cuisines') or []) or None,
                            'minutes': item.get('readyInMinutes') or None, 'ingredients': ingredients,
                            'url': item.get('sourceUrl') if str(item.get('sourceUrl', '')).startswith('https://') else f"https://spoonacular.com/recipes/{item['id']}",
                            'fetched_at': payload.get('fetched_at', now())})
    return records


def safe_error(exc):
    # Network exceptions may contain keys in URLs; preserve only type or known safe messages.
    if isinstance(exc, requests.RequestException):
        return f'{type(exc).__name__}: request failed (URL/credentials omitted)'
    return re.sub(r'(?i)(apikey|x-api-key)=([^\s&]+)', r'\1=[redacted]', str(exc))[:300]


def extract(source, client, settings):
    if source in settings.get('fail_sources', []):
        raise RuntimeError('simulated_source_failure')
    if source == 'mealdb':
        return extract_mealdb(client, int(settings.get('mealdb_limit', 100)))
    if source == 'wikibooks':
        return extract_wikibooks(client, int(settings.get('wiki_target', 180)), int(settings.get('wiki_max_pages', 320)))
    if source == 'spoonacular':
        return extract_spoonacular(client, int(settings.get('spoonacular_limit', 10)))
    raise ValueError('Unknown source')
