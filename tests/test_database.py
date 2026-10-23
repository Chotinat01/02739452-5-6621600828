"""Integration tests use real PostgreSQL and a transaction rollback for isolation."""
import os
import uuid
import pytest
from pipeline import database as db
from pipeline.core import merge_records, validate

pytestmark = pytest.mark.skipif(not os.getenv('PIPELINE_DATABASE_URL'), reason='PostgreSQL URL not configured')


def test_upsert_is_idempotent_and_failed_quality_preserves_publication(monkeypatch):
    conn = db.connect()
    conn.autocommit = False

    class BorrowedConnection:
        def __enter__(self):
            return conn
        def __exit__(self, *args):
            pass  # Every operation stays in one test transaction.

    monkeypatch.setattr(db, 'connect', lambda: BorrowedConnection())
    try:
        token = 'integration-' + uuid.uuid4().hex
        db.start_run(token, {})
        good, _ = validate([{'source': 'mealdb', 'source_id': token, 'title': token,
                            'ingredients': [{'name': 'salt', 'raw': 'salt'}],
                            'url': 'https://example.org/recipe', 'fetched_at': '2026-10-23T00:00:00Z'}])
        merged = merge_records(good)
        db.save_quality(token, {'passed': True})
        db.publish(token, merged)
        with conn.cursor() as cur:
            cur.execute('SELECT count(*) FROM recipes.canonical')
            before = cur.fetchone()[0]
        db.publish(token, merged)
        with conn.cursor() as cur:
            cur.execute('SELECT count(*) FROM recipes.canonical')
            assert cur.fetchone()[0] == before
        failed = token + '-fail'
        db.start_run(failed, {})
        db.save_quality(failed, {'passed': False})
        with pytest.raises(ValueError):
            db.publish(failed, merged)
        db.block_publication(failed)
        with conn.cursor() as cur:
            cur.execute('SELECT run_id FROM recipes.publication')
            assert cur.fetchone()[0] == token
        db.stage_source(token, 'mealdb', good, [], {'status': 'ok', 'extracted_count': 1})
        db.stage_source(token, 'mealdb', good, [], {'status': 'ok', 'extracted_count': 1})
        assert len(db.read_run(token)[0]) == 1
        db.start_run(token + '-empty', {})
        assert db.read_run(token + '-empty')[0] == []
    finally:
        conn.rollback()
        conn.close()
