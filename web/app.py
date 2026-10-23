import os
from collections import Counter
import pandas as pd
import plotly.express as px
import psycopg2
from psycopg2.extras import RealDictCursor
import streamlit as st

st.set_page_config(page_title='Recipe Atlas', page_icon='🍲', layout='wide')
st.markdown('''<style>
.block-container {padding-top:2.4rem; max-width:1450px;}
[data-testid="stMetric"] {background:var(--secondary-background-color); border:1px solid #81988e; border-radius:12px; padding:18px;}
[data-testid="stMetricValue"] {font-weight:700;}
[data-testid="stCaptionContainer"] {opacity:1;}
</style>''', unsafe_allow_html=True)


@st.cache_data(ttl=10)
def query(statement):
    # Each refresh reads PostgreSQL using a SELECT-only role. No CSV/JSON fallback.
    with psycopg2.connect(os.environ['DASHBOARD_DATABASE_URL']) as conn:
        conn.set_session(readonly=True)
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(statement)
            return [dict(r) for r in cur.fetchall()]


st.sidebar.markdown('## 🍲 Recipe Atlas')
st.sidebar.caption('BIG DATA PLATFORM & ANALYTICS')
page = st.sidebar.radio('Explore', ['Recipe explorer', 'Pipeline status'])
if st.sidebar.button('Refresh from PostgreSQL', use_container_width=True):
    st.cache_data.clear()
    st.rerun()
st.sidebar.divider()
st.sidebar.caption('TheMealDB · Wikibooks Cookbook · Forkify')
st.sidebar.caption('6621600828 | โชติณัช โสภาแปง')

try:
    runs = query('SELECT * FROM recipes.runs ORDER BY started_at DESC LIMIT 30')
    publication = query('SELECT * FROM recipes.publication')
except psycopg2.Error:
    st.error('PostgreSQL is not ready. Run docker compose up -d and inspect airflow-init logs.')
    st.stop()

if page == 'Pipeline status':
    st.caption('OPERATIONS / PIPELINE HEALTH')
    st.title('Every run, accounted for')
    st.write('สถานะการดึงข้อมูล คุณภาพ และผลการเผยแพร่จาก PostgreSQL')
    if not runs:
        st.info('No runs yet. Trigger international_recipe_pipeline in Airflow.')
        st.stop()
    run_id = st.selectbox('Run', [r['run_id'] for r in runs])
    selected = next(r for r in runs if r['run_id'] == run_id)
    a, b, c = st.columns(3)
    a.metric('Run status', selected['status'])
    b.metric('Unique recipes', selected['quality'].get('unique_count', 0))
    c.metric('Usable sources', f"{selected['quality'].get('usable_sources', 0)} / 3")
    statuses = query('SELECT run_id,source,status,extracted_count,valid_count,rejected_count,detail,updated_at FROM recipes.source_status ORDER BY updated_at DESC')
    statuses = [s for s in statuses if s['run_id'] == run_id]
    st.subheader('Source availability')
    st.dataframe(pd.DataFrame([{k: v for k, v in s.items() if k not in ('detail', 'run_id')} for s in statuses]), hide_index=True, use_container_width=True)
    for source in statuses:
        with st.expander(source['source'] + ' · source details'):
            st.json(source['detail'])
    st.subheader('Quality gate')
    checks = selected['quality'].get('checks', {})
    for name, passed in checks.items():
        st.write(('✅ ' if passed else '❌ ') + name.replace('_', ' '))
    st.caption('A failed quality gate preserves the previously published dataset. Source outages are visible even when the DAG succeeds.')
    with st.expander('Run history'):
        st.dataframe(pd.DataFrame([{k: v for k, v in r.items() if k not in ('settings', 'quality')} for r in runs]), hide_index=True, use_container_width=True)
    st.stop()

st.caption('INTERNATIONAL RECIPES / INGREDIENT INTELLIGENCE')
st.title('A world of recipes. One place to explore.')
st.write('สำรวจสูตรอาหารนานาชาติ ค้นหาวัตถุดิบ และติดตามที่มาของข้อมูล')
if not publication:
    st.info('Waiting for the first dataset to pass quality checks. Trigger the DAG in Airflow.')
    st.stop()
rows = query('SELECT * FROM recipes.current_recipes ORDER BY title')
if not rows:
    st.warning('No published recipes.')
    st.stop()
all_sources = sorted({s['source'] for r in rows for s in r['sources']})
st.caption(f"Published run: {publication[0]['run_id']} · Refreshed from PostgreSQL every 10 seconds")
unavailable = []
latest_status = query('SELECT * FROM recipes.source_status WHERE run_id=(SELECT run_id FROM recipes.publication)')
unavailable = [s['source'] for s in latest_status if s['status'] not in ('ok', 'partial')]
if unavailable:
    st.warning('Source unavailable in this publication: ' + ', '.join(unavailable) + '. View Pipeline status for details.')
c1, c2, c3, c4 = st.columns(4)
c1.metric('Published recipes', f'{len(rows):,}')
c2.metric('Available sources', f'{len(all_sources)} / 3')
c3.metric('Ingredient names', len({i['name'] for r in rows for i in r['ingredients']}))
c4.metric('Categories', len({r['category'] for r in rows if r['category']}))
st.markdown('### Find your next dish')
f1, f2, f3 = st.columns([2, 1, 1])
search = f1.text_input('Recipe or ingredient', placeholder='Try chicken, garlic, apple…')
categories = f2.multiselect('Category', sorted({r['category'] for r in rows if r['category']}))
sources = f3.multiselect('Source', all_sources)
filtered = [r for r in rows if (not search or search.casefold() in (r['title'] + ' ' + ' '.join(i['name'] for i in r['ingredients'])).casefold())
            and (not categories or r['category'] in categories)
            and (not sources or any(s['source'] in sources for s in r['sources']))]
left, right = st.columns([1.4, 1])
with left:
    st.subheader('Most common ingredients')
    counts = Counter(i['name'] for r in filtered for i in {i['name']: i for i in r['ingredients']}.values())
    if counts:
        chart = pd.DataFrame(counts.most_common(12), columns=['Ingredient', 'Recipes']).sort_values('Recipes')
        fig = px.bar(chart, x='Recipes', y='Ingredient', orientation='h', color_discrete_sequence=['#276753'])
        fig.update_layout(template='plotly_white', height=370, margin=dict(l=0, r=15, t=5, b=0),
                          paper_bgcolor='#ffffff', plot_bgcolor='#ffffff', font=dict(color='#172d2b', size=14),
                          xaxis=dict(gridcolor='#d9e3dd', tickfont=dict(color='#172d2b')),
                          yaxis=dict(tickfont=dict(color='#172d2b')))
        st.plotly_chart(fig, use_container_width=True, theme=None)
with right:
    st.subheader('Recipes by source')
    counts = Counter(s['source'] for r in filtered for s in {s['source']: s for s in r['sources']}.values())
    if counts:
        fig = px.pie(names=list(counts), values=list(counts.values()), hole=.68, color_discrete_sequence=['#17634f', '#a86010', '#435f84'])
        fig.update_traces(textfont=dict(color='#ffffff', size=15), marker=dict(line=dict(color='#ffffff', width=2)))
        fig.update_layout(template='plotly_white', height=370, margin=dict(l=5, r=5, t=5, b=5),
                          paper_bgcolor='#ffffff', font=dict(color='#172d2b', size=14), legend=dict(orientation='h'))
        st.plotly_chart(fig, use_container_width=True, theme=None)
    st.caption('A recipe present in several sources is counted once per source.')
st.subheader(f'Recipe collection · {len(filtered):,} results')
table = pd.DataFrame([{'Recipe': r['title'], 'Category': r['category'], 'Cuisine': r['cuisine'], 'Minutes': float(r['minutes']) if r['minutes'] else None,
                       'Ingredients': r['ingredient_count'], 'Sources': ', '.join(s['source'] for s in r['sources']), 'Original': r['sources'][0]['url']} for r in filtered])
st.dataframe(table, hide_index=True, use_container_width=True, column_config={'Original': st.column_config.LinkColumn('Original recipe')})
if filtered:
    chosen = st.selectbox('Inspect ingredients and provenance', range(len(filtered)), format_func=lambda n: filtered[n]['title'])
    item = filtered[chosen]
    with st.expander('Ingredients and attribution', expanded=False):
        for ingredient in item['ingredients']:
            st.write('• ' + ingredient['raw'])
        for source in item['sources']:
            st.link_button(source['source'] + ' · original recipe', source['url'])
        st.caption('Retrieved: ' + str(item['fetched_at']))
st.caption('Wikibooks content is attributed through original links and is subject to CC BY-SA terms. Missing category, cuisine and time remain unknown. Ingredient names are conservatively normalized and may include variants.')
