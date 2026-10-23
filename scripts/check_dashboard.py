from streamlit.testing.v1 import AppTest

app = AppTest.from_file('/app/web/app.py').run(timeout=30)
assert not app.exception, app.exception
assert int(app.metric[0].value.replace(',', '')) >= 100, app.metric[0].value
assert app.metric[1].value == '3 / 3', app.metric[1].value
app.text_input[0].set_value('apple').run(timeout=30)
assert not app.exception
assert any('results' in h.value for h in app.subheader)
app.text_input[0].set_value('no_such_recipe_123456789').run(timeout=30)
assert any('0 results' in h.value for h in app.subheader)
app.radio[0].set_value('Pipeline status').run(timeout=30)
assert not app.exception
assert any('Every run' in h.value for h in app.title)
assert len(app.metric) == 3
print('Dashboard: PostgreSQL metrics, ingredient search, empty filter and pipeline page passed')
