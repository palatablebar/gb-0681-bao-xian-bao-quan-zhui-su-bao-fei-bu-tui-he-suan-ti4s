import random
import sqlite3

import pytest

from app import store
from app.canonical import canonicalize
from app.errors import DomainError
from app.workflow import load, run


def _reorder_rows(raw):
    text = raw.decode('utf-8-sig')
    header, *body = [line for line in text.split('\r\n') if line]
    random.Random(7).shuffle(body)
    return (header + '\r\n' + '\r\n'.join(body) + '\r\n').encode('utf-8-sig')


def test_encoding_row_and_filename_do_not_fork_batches():
    clean = load('clean.csv')
    legacy = load('legacy.csv')
    assert canonicalize(clean)['canonical'] == canonicalize(legacy)['canonical']
    assert canonicalize(clean)['canonical'] == canonicalize(_reorder_rows(clean))['canonical']
    assert run(clean, {})['run_id'] == run(legacy, {})['run_id']
    assert run(clean, {})['run_id'] == run(_reorder_rows(clean), {})['run_id']


def test_real_content_change_still_forks():
    clean = load('clean.csv')
    changed = clean.replace(b'3650', b'3651', 1)
    assert canonicalize(clean)['canonical'] != canonicalize(changed)['canonical']
    assert run(clean, {})['run_id'] != run(changed, {})['run_id']


def test_blank_line_is_not_a_formatting_difference():
    clean = load('clean.csv')
    extra = clean.replace(b'\r\n', b'\r\n\r\n', 1)
    assert canonicalize(clean)['canonical'] != canonicalize(extra)['canonical']


def test_two_channels_merge_into_one_batch(client):
    branch = client.post('/api/runs', json={'sample': 'clean.csv', 'source': '柜面',
                                            'filename': 'branch.csv'}).get_json()
    online = client.post('/api/runs', json={'sample': 'legacy.csv', 'source': '线上',
                                            'filename': 'web.csv'}).get_json()
    assert branch['batch_id'] == online['batch_id']
    assert online['reception_count'] == 2
    assert [r['source'] for r in online['receptions']] == ['柜面', '线上']
    assert [r['filename'] for r in online['receptions']] == ['branch.csv', 'web.csv']
    # startup seed batch + one merged batch, not one batch per delivery
    ids = [h['id'] for h in client.get('/api/runs').get_json()]
    assert ids.count(branch['batch_id']) == 1 and len(ids) == 2


def test_every_reception_keeps_its_own_original(client):
    client.post('/api/runs', json={'sample': 'clean.csv', 'source': '柜面'})
    batch = client.post('/api/runs', json={'sample': 'legacy.csv', 'source': '线上'}).get_json()
    first, second = batch['receptions']
    assert first['sha256'] != second['sha256']
    assert first['encoding'] == 'utf-8-sig' and second['encoding'] == 'gb18030'
    a = client.get('/api/receptions/%d/source' % first['id'])
    b = client.get('/api/receptions/%d/source' % second['id'])
    assert a.data == load('clean.csv') and b.data == load('legacy.csv')
    assert first['received_at'] <= second['received_at']


def test_pending_reception_is_held_then_identified(client):
    client.post('/api/runs', json={'sample': 'clean.csv', 'source': '柜面'})
    batch = client.post('/api/runs', json={'sample': 'clean.csv', 'source': '线上'}).get_json()
    assert [r['disposition'] for r in batch['receptions']] == ['采用', '待辨认']
    assert batch['pending_reception_count'] == 1
    result = client.post('/api/receptions/identify', json={'batch_id': batch['batch_id']}).get_json()
    assert (result['identified'], result['pending']) == (1, 0)
    after = client.get('/api/runs/' + batch['batch_id']).get_json()
    assert [r['disposition'] for r in after['receptions']] == ['采用', '归并']
    assert '不新增核算结果或签审版本' in after['receptions'][1]['basis']
    # identifying again is a no-op
    assert client.post('/api/receptions/identify',
                       json={'batch_id': batch['batch_id']}).get_json()['identified'] == 0


def test_signed_batch_reuses_conclusion_without_new_version(client):
    first = client.post('/api/runs', json={'sample': 'clean.csv', 'source': '柜面'}).get_json()
    signed = client.post('/api/runs/%s/review' % first['batch_id'],
                         json={'version': 1, 'decision': '通过'}).get_json()
    assert signed['review_version'] == 2
    later = client.post('/api/runs', json={'sample': 'legacy.csv', 'source': '线上',
                                           'filename': 'web.csv'}).get_json()
    assert later['batch_id'] == first['batch_id']
    assert later['review_version'] == 2
    assert len(later['calculations']) == len(first['calculations'])
    assert [r['disposition'] for r in later['receptions']] == ['采用', '复用签审']
    assert 'REV 2' in later['receptions'][1]['basis']
    assert later['pending_reception_count'] == 0


def test_retry_after_signoff_adds_no_calculation_or_version(client):
    first = client.post('/api/runs', json={'sample': 'dirty.csv', 'source': '柜面'}).get_json()
    signed = client.post('/api/runs/%s/review' % first['batch_id'],
                         json={'version': 1, 'decision': '通过'}).get_json()
    again = client.post('/api/runs', json={'sample': 'dirty.csv', 'source': '柜面'}).get_json()
    assert again['review_version'] == signed['review_version']
    assert again['calculations'] == signed['calculations']
    # startup seed + first delivery + this retry
    assert again['reception_count'] == 3
    assert len(store.history()) == 1


def test_rejected_delivery_is_still_on_record(client):
    client.post('/api/runs', json={'sample': 'clean.csv', 'source': '柜面'})
    bad = client.post('/api/runs', json={'csv': 'wrong\n1\n', 'source': '线上',
                                         'filename': 'bad.csv'})
    assert bad.status_code == 422 and bad.get_json()['code'] == 'HEADER'
    queue = client.get('/api/receptions').get_json()
    rejected = [r for r in queue if r['disposition'] == '拒收']
    assert len(rejected) == 1
    assert rejected[0]['source'] == '线上' and rejected[0]['filename'] == 'bad.csv'
    assert rejected[0]['basis'].startswith('HEADER')
    assert client.get('/api/receptions/%d/source' % rejected[0]['id']).data == b'wrong\n1\n'
    # a rejected delivery never becomes the visible batch
    assert client.get('/api/runs').get_json()[0]['id'] != ''


def test_publish_failure_keeps_old_conclusion(monkeypatch, tmp_path):
    monkeypatch.setenv('DB_PATH', str(tmp_path / 'publish.db'))
    first = store.receive(load('clean.csv'), {}, '柜面', 'old.csv')
    before = store.history()[0]

    def broken(_run_id):
        raise sqlite3.OperationalError('database is locked')

    monkeypatch.setattr(store, '_switch_latest', broken)
    state = store.receive(load('dirty.csv'), {}, '线上', 'new.csv')
    assert state['publish']['ok'] is False
    assert '原批次' in state['publish']['message']
    # the new arrival is durable and traceable...
    assert state['reception_count'] == 1
    # ...but the visible pointer still shows the old conclusion
    assert store.history()[0]['id'] == before['id']
    assert store.get(first['run_id'])['run_id'] == first['run_id']


def test_archive_readiness_requires_no_pending_reception(client):
    client.post('/api/runs', json={'sample': 'clean.csv', 'source': '柜面'})
    batch = client.post('/api/runs', json={'sample': 'clean.csv', 'source': '线上'}).get_json()
    assert batch['archive']['ready'] is False
    client.post('/api/receptions/identify', json={'batch_id': batch['batch_id']})
    assert client.get('/api/runs/' + batch['batch_id']).get_json()['archive']['ready'] is True


def test_page_and_download_share_batch_identity(client):
    import hashlib
    batch = client.post('/api/runs', json={'sample': 'clean.csv', 'source': '柜面'}).get_json()
    client.post('/api/runs', json={'sample': 'legacy.csv', 'source': '线上'})
    page = client.get('/m7_archive').data.decode()
    assert batch['batch_id'] in page and '接收 2 次' in page
    download = client.get('/api/archive/%s/export.csv' % batch['batch_id'])
    current = client.get('/api/runs/' + batch['batch_id']).get_json()
    assert hashlib.sha256(download.data).hexdigest() == current['archive']['sha256']
    assert current['archive']['sha256'] in page
    text = download.data.decode('utf-8-sig')
    assert text.count('接收') == 2 and '柜面' in text and '线上' in text


def test_closing_export_lists_receptions_and_basis(state):
    from app.modules.m7_archive.export_out import bundle
    state = dict(state, receptions=[{'id': 1, 'source': '柜面', 'filename': 'a.csv',
                                     'received_at': '2026-11-21T00:00:00+00:00',
                                     'sha256': 'x', 'encoding': 'utf-8-sig',
                                     'canonical': 'c', 'byte_size': 10,
                                     'disposition': '采用', 'basis': '首次到达'}],
                 reception_count=1, pending_reception_count=0)
    text = bundle(state)
    assert '批次身份' in text and '接收' in text and '首次到达' in text


def test_reception_source_is_required_to_be_traceable(client):
    bad = client.post('/api/runs', json={'sample': 'clean.csv', 'source': 123})
    assert bad.status_code == 422 and bad.get_json()['code'] == 'SOURCE'
    assert client.get('/api/receptions').get_json()[0]['source'] == '样例'


def test_legacy_byte_batches_are_merged_on_upgrade(monkeypatch, tmp_path):
    """A volume written before canonical identity keeps its arrivals."""
    import json
    monkeypatch.setenv('DB_PATH', str(tmp_path / 'legacy.db'))
    first = run(load('clean.csv'), {})
    second = run(load('legacy.csv'), {})
    for state, old_id in ((first, 'old-a'), (second, 'old-b')):
        state['run_id'] = old_id
        state['batch_id'] = old_id
        state['manifest']['canonical'] = ''
    with store.connection() as db:
        for state in (first, second):
            db.execute('INSERT INTO runs(id,body,version,decision) VALUES (?,?,?,?)',
                       (state['run_id'], json.dumps(state, ensure_ascii=False), 1, '待复核'))
        db.execute("INSERT INTO meta(key,value) VALUES('latest','old-b')")

    store.migrate_legacy()
    history = store.history()
    assert len(history) == 1
    keep = history[0]['id']
    assert keep == run(load('clean.csv'), {})['run_id']
    assert store.get('old-a')['run_id'] == keep
    assert store.get('old-b')['run_id'] == keep
    assert [(r['filename'], r['disposition']) for r in store.receptions(keep)] \
        == [('old-a', '采用'), ('old-b', '待辨认')]
    assert len(store.get(keep)['calculations']) == len(first['calculations'])

    before = len(store.receptions(keep))
    store.migrate_legacy()
    assert len(store.receptions(keep)) == before and len(store.history()) == 1
    # fresh deliveries of the same content land in the migrated batch
    again = store.receive(load('clean.csv'), {}, '柜面', 'again.csv')
    assert again['run_id'] == keep and again['reception_count'] == before + 1
