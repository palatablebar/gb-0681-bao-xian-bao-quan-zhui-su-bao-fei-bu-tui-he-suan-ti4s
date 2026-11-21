"""SQLite persistence for calculation batches and reception deliveries.

Two different things are stored on purpose:

``runs``
    One row per *logical* batch.  The id is the canonical fingerprint of the
    delivered content (encoding / row order / column order / filename are not
    part of it).  There is at most one body, one calculation set and one
    sign-off version per batch, no matter how many times it is delivered.

``receptions``
    One row per *arrival*, append-only.  Source channel, original file name,
    server reception time, raw bytes, decoded text, encoding and the
    disposition are kept forever.  A reception never overwrites another one,
    so two channels can not make the earlier arrival unexplainable.

The ``meta.latest`` pointer is published in its own transaction *after* the
batch and the reception are durable.  A failed publish therefore leaves the
previous conclusion visible while the arrival itself is still on record.

Volumes written before the canonical identity existed key runs by raw bytes.
:func:`migrate_legacy` folds those duplicates into receptions so the ledger
covers deliveries that arrived before this version.
"""
import json
import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from app.errors import DomainError

SCHEMA = (
    'CREATE TABLE IF NOT EXISTS runs (id TEXT PRIMARY KEY, body TEXT NOT NULL, '
    'version INTEGER NOT NULL DEFAULT 1, decision TEXT NOT NULL DEFAULT "待复核")',
    'CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY,value TEXT NOT NULL)',
    'CREATE TABLE IF NOT EXISTS receptions ('
    'id INTEGER PRIMARY KEY AUTOINCREMENT, batch_id TEXT NOT NULL, source TEXT NOT NULL, '
    'filename TEXT NOT NULL, received_at TEXT NOT NULL, sha256 TEXT NOT NULL, '
    'encoding TEXT NOT NULL, canonical TEXT NOT NULL, parameters TEXT NOT NULL, '
    'byte_size INTEGER NOT NULL, body BLOB NOT NULL, text TEXT NOT NULL, '
    'disposition TEXT NOT NULL, basis TEXT NOT NULL)',
    'CREATE INDEX IF NOT EXISTS receptions_batch ON receptions(batch_id,id)',
    'CREATE INDEX IF NOT EXISTS receptions_state ON receptions(disposition,id)',
    'CREATE TABLE IF NOT EXISTS batch_aliases (old_id TEXT PRIMARY KEY, batch_id TEXT NOT NULL)',
)

PENDING = '待辨认'


def _now():
    return datetime.now(timezone.utc).isoformat()


def connection():
    path = Path(os.environ.get('DB_PATH', 'data/records.db'))
    path.parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(path, timeout=10)
    db.row_factory = sqlite3.Row
    db.execute('PRAGMA journal_mode=WAL')
    for statement in SCHEMA:
        db.execute(statement)
    _ensure_columns(db)
    return db


def _ensure_columns(db):
    """Old volumes only had runs(id,body,version,decision); add timestamps."""
    columns = {row['name'] for row in db.execute('PRAGMA table_info(runs)')}
    for name in ('created_at', 'updated_at'):
        if name not in columns:
            db.execute('ALTER TABLE runs ADD COLUMN ' + name + ' TEXT')


def _clean_source(source):
    if source is None or (isinstance(source, str) and not source.strip()):
        return '接口'
    if not isinstance(source, str):
        raise DomainError('SOURCE', '来源渠道必须是文本')
    return source.strip()[:32]


def _clean_filename(filename):
    if filename is None or (isinstance(filename, str) and not filename.strip()):
        return '请求体'
    if not isinstance(filename, str):
        raise DomainError('FILENAME', '文件名必须是文本')
    return filename.strip()[:160]


def _insert_reception(db, batch_id, source, filename, sha256, encoding, canonical,
                      parameters, body, text, disposition, basis):
    db.execute(
        'INSERT INTO receptions(batch_id,source,filename,received_at,sha256,encoding,'
        'canonical,parameters,byte_size,body,text,disposition,basis) '
        'VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)',
        (batch_id, source, filename, _now(), sha256, encoding, canonical,
         json.dumps(parameters, sort_keys=True, ensure_ascii=False),
         len(body), sqlite3.Binary(body), text, disposition, basis),
    )


def _insert_batch(db, run_id, body):
    timestamp = _now()
    db.execute(
        'INSERT OR IGNORE INTO runs(id,body,version,decision,created_at,updated_at) '
        'VALUES (?,?,1,?,?,?)',
        (run_id, json.dumps(body, ensure_ascii=False), '待复核', timestamp, timestamp),
    )


def _switch_latest(run_id):
    """Publish the visible batch.  Runs alone: a failure keeps the old pointer."""
    with connection() as db:
        db.execute(
            "INSERT INTO meta(key,value) VALUES ('latest',?) "
            'ON CONFLICT(key) DO UPDATE SET value=excluded.value',
            (run_id,),
        )


def save(run_id, body, publish=True):
    """Persist one logical batch.  Re-saving the same id never bumps version."""
    with connection() as db:
        _insert_batch(db, run_id, body)
    if publish:
        _switch_latest(run_id)
    return get(run_id)


def migrate_legacy():
    """Fold batches that only differ by delivery form into one logical batch.

    Older volumes keyed ``runs`` by raw bytes, so the same flow delivered in two
    encodings or row orders exists as several batches.  Each such duplicate is
    turned into a reception of the surviving batch (channel ``历史批次``, the
    old batch id as file name, its generation time and original bytes), and the
    duplicate batch row is removed.  The surviving batch keeps the highest
    signed revision; duplicate arrivals of a signed batch are reusable, of an
    unsigned batch they stay ``待辨认``.  Running this repeatedly is a no-op.
    """
    import base64
    from app.canonical import canonicalize
    from app.manifest import fingerprint

    with connection() as db:
        rows = db.execute('SELECT id,body,version,decision,rowid AS seq FROM runs '
                          'ORDER BY rowid').fetchall()
        groups = {}
        for row in rows:
            body = json.loads(row['body'])
            raw = base64.b64decode(body['source_b64']) if body.get('source_b64') else b''
            canonical = body.get('manifest', {}).get('canonical', '')
            meta = None
            if raw:
                try:
                    meta = canonicalize(raw)
                    canonical = canonical or meta['canonical']
                except DomainError:
                    meta = None
            key = canonical or 'sha:' + body.get('manifest', {}).get('sha256', row['id'])
            groups.setdefault(key, []).append((row, body, raw, meta, canonical))

        for key, items in groups.items():
            signed = [item for item in items if item[0]['decision'] == '通过']
            keeper = max(signed, key=lambda item: item[0]['version']) \
                if signed else items[0]
            keep_id = keeper[0]['id']
            # Re-key to the canonical fingerprint so later deliveries of the
            # same content hit this batch instead of forking a new one.
            if keeper[4]:
                keep_id = fingerprint(keeper[4],
                                      keeper[1].get('manifest', {}).get('parameters', {}))
            if len(items) < 2 and keep_id == keeper[0]['id']:
                continue

            def remember(source, filename, when, body, raw, meta, disposition, basis):
                db.execute(
                    'INSERT INTO receptions(batch_id,source,filename,received_at,sha256,'
                    'encoding,canonical,parameters,byte_size,body,text,disposition,basis) '
                    'VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)',
                    (keep_id, source, filename, when,
                     body.get('manifest', {}).get('sha256', ''),
                     meta['encoding'] if meta else '',
                     meta['canonical'] if meta else '',
                     json.dumps(body.get('manifest', {}).get('parameters', {}),
                                sort_keys=True, ensure_ascii=False),
                     len(raw), sqlite3.Binary(raw), meta['text'] if meta else '',
                     disposition, basis),
                )

            # The surviving batch keeps its own first arrival, then the
            # duplicates are recorded in their original order.
            has_reception = db.execute('SELECT COUNT(*) AS n FROM receptions WHERE batch_id=?',
                                       (keep_id,)).fetchone()['n']
            if not has_reception and keeper[2] and keeper[0]['id'] == keep_id:
                remember('历史批次', keep_id,
                         keeper[1].get('manifest', {}).get('generated_at', ''),
                         keeper[1], keeper[2], keeper[3],
                         '采用', '升级前已登记批次，作为本批首次接收')
            for row, body, raw, meta, canonical in items:
                if row['id'] == keep_id:
                    continue
                if row['id'] == keeper[0]['id']:
                    displaced = keeper
                    disposition = '采用'
                    basis = '升级前已登记批次，作为本批首次接收'
                    remember('历史批次', row['id'],
                             displaced[1].get('manifest', {}).get('generated_at', ''),
                             displaced[1], displaced[2], displaced[3],
                             disposition, basis)
                else:
                    disposition = '复用签审' if signed else PENDING
                    basis = ('升级前按送达字节单独登记的批次 {}，逻辑内容与批次 {} 一致；'
                             '归并为同一核算结论').format(row['id'], keep_id)
                    if signed:
                        basis += '并复用签审结论'
                    remember('历史批次', row['id'],
                             body.get('manifest', {}).get('generated_at', ''),
                             body, raw, meta, disposition, basis)
                db.execute('DELETE FROM runs WHERE id=?', (row['id'],))
                db.execute('INSERT OR REPLACE INTO batch_aliases(old_id,batch_id) '
                           'VALUES (?,?)', (row['id'], keep_id))
                db.execute("UPDATE meta SET value=? WHERE key='latest' AND value=?",
                           (keep_id, row['id']))

            if keep_id != keeper[0]['id']:
                body = dict(keeper[1])
                body['run_id'] = keep_id
                body['batch_id'] = keep_id
                body.pop('receptions', None)
                body.pop('reception_count', None)
                body.pop('pending_reception_count', None)
                body.pop('adoption_basis', None)
                body.pop('archive', None)
                db.execute('INSERT INTO runs(id,body,version,decision,created_at,updated_at) '
                           'VALUES (?,?,?,?,?,?)',
                           (keep_id, json.dumps(body, ensure_ascii=False),
                            keeper[0]['version'], keeper[0]['decision'],
                            _now(), _now()))
                db.execute('DELETE FROM runs WHERE id=?', (keeper[0]['id'],))
                db.execute('INSERT OR REPLACE INTO batch_aliases(old_id,batch_id) '
                           'VALUES (?,?)', (keeper[0]['id'], keep_id))
                db.execute("UPDATE meta SET value=? WHERE key='latest' AND value=?",
                           (keep_id, keeper[0]['id']))


def receive(content, parameters=None, source=None, filename=None, stage=7):
    """Record one delivery and return the batch it belongs to.

    * new canonical content -> new batch, reception disposition ``采用``;
    * same content, unsigned batch -> reception stays ``待辨认``;
    * same content, signed batch -> reception is ``复用签审`` immediately;
    * malformed delivery -> reception is ``拒收`` with the error as basis.

    Everything the reviewer needs for a later source dispute is committed
    before the visible-batch pointer is touched.  A publish failure returns the
    state with ``publish.ok == False`` and leaves the previous pointer intact.
    """
    from app.canonical import canonicalize
    from app.manifest import fingerprint
    from app.workflow import run

    params = parameters or {}
    if not isinstance(params, dict):
        raise DomainError('PARAMETERS', '参数必须是对象')
    source = _clean_source(source)
    filename = _clean_filename(filename)

    def reject(error, encoding='', canonical='', text=''):
        # Own transaction, committed before the error propagates: a rejected
        # delivery must still be traceable to its source and original bytes.
        with connection() as db:
            _insert_reception(
                db, '', source, filename, state_sha(content), encoding,
                canonical, params, content, text, '拒收',
                error.code + '：' + error.message,
            )

    try:
        meta = canonicalize(content)
    except DomainError as error:
        reject(error)
        raise

    batch_id = fingerprint(meta['canonical'], params)
    publish_ok = True
    publish_error = ''

    with connection() as db:
        existing = db.execute('SELECT 1 FROM runs WHERE id=?', (batch_id,)).fetchone()
    state = None
    if existing is None:
        try:
            state = run(content, params, stage)
        except DomainError as error:
            reject(error, meta['encoding'], meta['canonical'], meta['text'])
            raise
        state['batch_id'] = batch_id

    # Batch and reception become durable in one transaction, and the
    # disposition is decided from what is actually stored, so two concurrent
    # deliveries can not both claim to be the first arrival.  The visible
    # pointer is published afterwards, so a half-written batch never shows.
    with connection() as db:
        row = db.execute('SELECT version,decision FROM runs WHERE id=?',
                         (batch_id,)).fetchone()
        if row is None:
            if state is None:  # batch vanished between the probe and the write
                state = run(content, params, stage)
                state['batch_id'] = batch_id
            _insert_batch(db, batch_id, state)
            row = db.execute('SELECT version,decision FROM runs WHERE id=?',
                             (batch_id,)).fetchone()
        earlier = db.execute('SELECT COUNT(*) AS n FROM receptions WHERE batch_id=?',
                             (batch_id,)).fetchone()['n']
        signed = row['decision'] == '通过'
        if earlier == 0:
            disposition = '采用'
            basis = '首次到达，建立批次 {}；口径为本批核算与签审'.format(batch_id)
        elif signed:
            disposition = '复用签审'
            basis = ('与已签审批次 {} 内容一致（编码/行序/文件名差异不计），'
                     '直接复用 REV {} 结论').format(batch_id, row['version'])
        else:
            disposition = PENDING
            basis = '与批次 {} 内容一致，待复核员辨认后归并'.format(batch_id)
        _insert_reception(
            db, batch_id, source, filename, state_sha(content), meta['encoding'],
            meta['canonical'], params, content, meta['text'], disposition, basis,
        )

    try:
        _switch_latest(batch_id)
    except Exception as error:  # publish is deliberately non-fatal
        publish_ok = False
        publish_error = str(error)

    state = get(batch_id)
    state['publish'] = {'ok': publish_ok,
                        'message': '批次已登记' if publish_ok else
                        '接收已登记，发布切换失败，页面继续展示原批次'}
    if publish_error:
        state['publish']['error'] = publish_error
    return state


def state_sha(content):
    import hashlib
    return hashlib.sha256(content).hexdigest()


def _reception_rows(batch_id):
    with connection() as db:
        query = ('SELECT id,batch_id,source,filename,received_at,sha256,encoding,'
                 'canonical,parameters,byte_size,disposition,basis FROM receptions')
        if batch_id is None:
            rows = db.execute(query + ' ORDER BY id').fetchall()
        else:
            rows = db.execute(query + ' WHERE batch_id=? ORDER BY id', (batch_id,)).fetchall()
    return [dict(row) for row in rows]


def receptions(batch_id=None):
    rows = _reception_rows(resolve(batch_id) if batch_id else None)
    for row in rows:
        row['parameters'] = json.loads(row['parameters'])
        row['received_at'] = row['received_at']
        row['byte_size'] = row['byte_size']
    return rows


def reception_source(reception_id):
    with connection() as db:
        row = db.execute('SELECT filename,body FROM receptions WHERE id=?',
                         (reception_id,)).fetchone()
    if not row:
        raise DomainError('NOT_FOUND', '接收记录不存在', 404)
    return row['filename'], bytes(row['body'])


def resolve(run_id):
    """Follow a legacy batch alias to the batch that absorbed its content."""
    with connection() as db:
        for _ in range(8):
            alias = db.execute('SELECT batch_id FROM batch_aliases WHERE old_id=?',
                               (run_id,)).fetchone()
            if not alias or alias['batch_id'] == run_id:
                break
            run_id = alias['batch_id']
    return run_id


def get(run_id):
    run_id = resolve(run_id)
    with connection() as db:
        row = db.execute('SELECT * FROM runs WHERE id=?', (run_id,)).fetchone()
    if not row:
        raise DomainError('NOT_FOUND', '批次不存在', 404)
    body = json.loads(row['body'])
    body.update(run_id=row['id'], batch_id=row['id'], review_version=row['version'],
                decision=row['decision'])
    receipts = receptions(run_id)
    body['receptions'] = receipts
    body['reception_count'] = len(receipts)
    body['pending_reception_count'] = sum(1 for r in receipts if r['disposition'] == PENDING)
    body['adoption_basis'] = [{'reception': r['id'], 'source': r['source'],
                               'disposition': r['disposition'], 'basis': r['basis']}
                              for r in receipts]
    if body.get('archive'):
        # Closing is only possible once every arrival has a destination and the
        # counts balance; the checksum covers the reception ledger too, so the
        # page and the download agree on one batch identity.
        body['archive']['evidence_count'] = (len(body.get('calculations', []))
                                             + len(body['quarantine']) + len(receipts))
        body['archive']['ready'] = bool(body.get('reconcile', {}).get('balanced')) \
            and body['pending_reception_count'] == 0
        from app.modules.m7_archive.export_out import checksum
        body['archive']['sha256'] = checksum(body)
    return body


def history():
    with connection() as db:
        rows = db.execute(
            'SELECT r.id,r.version,r.decision,'
            '(SELECT COUNT(*) FROM receptions p WHERE p.batch_id=r.id) AS reception_count '
            'FROM runs r ORDER BY CASE WHEN r.id=(SELECT value FROM meta WHERE key=\'latest\') '
            'THEN 0 ELSE 1 END, r.rowid DESC LIMIT 100'
        ).fetchall()
    return [dict(row) for row in rows]


def review(run_id, version, decision):
    if decision not in ('通过', '退回'):
        raise DomainError('DECISION', '复核状态无效')
    run_id = resolve(run_id)
    with connection() as db:
        cur = db.execute('UPDATE runs SET version=version+1,decision=?,updated_at=? '
                         'WHERE id=? AND version=?', (decision, _now(), run_id, version))
        if cur.rowcount != 1:
            if not db.execute('SELECT 1 FROM runs WHERE id=?', (run_id,)).fetchone():
                raise DomainError('NOT_FOUND', '批次不存在', 404)
            raise DomainError('STALE', '复核版本已更新，请刷新', 409)
    return get(run_id)


def identify(batch_id=None, reception_id=None):
    """Resolve deliveries that were held for identification.

    Only ``待辨认`` receptions are touched.  Identification never recomputes the
    batch and never changes its sign-off version: it writes the destination and
    the adoption basis of each held arrival.  Repeating it is a no-op.
    """
    if batch_id:
        batch_id = resolve(batch_id)
    with connection() as db:
        query = 'SELECT id,batch_id FROM receptions WHERE disposition=?'
        args = [PENDING]
        if batch_id:
            query += ' AND batch_id=?'
            args.append(batch_id)
        if reception_id is not None:
            query += ' AND id=?'
            args.append(reception_id)
        rows = db.execute(query + ' ORDER BY id', args).fetchall()
        resolved = []
        for row in rows:
            batch = db.execute('SELECT version,decision FROM runs WHERE id=?',
                               (row['batch_id'],)).fetchone()
            first = db.execute('SELECT MIN(id) AS id FROM receptions WHERE batch_id=?',
                               (row['batch_id'],)).fetchone()
            if batch and batch['decision'] == '通过':
                disposition = '复用签审'
                basis = ('经辨认与批次 {} 首次接收同内容（编码/行序/文件名差异不计），'
                         '归并并复用 REV {} 已签审结论').format(row['batch_id'], batch['version'])
            elif first and row['id'] == first['id']:
                disposition = '采用'
                basis = '经辨认确认为批次 {} 首次到达内容'.format(row['batch_id'])
            else:
                disposition = '归并'
                basis = ('经辨认与批次 {} 首次接收同内容（编码/行序/文件名差异不计），'
                         '归并为同一核算结论；不新增核算结果或签审版本').format(row['batch_id'])
            db.execute('UPDATE receptions SET disposition=?,basis=? WHERE id=?',
                       (disposition, basis, row['id']))
            resolved.append(row['id'])
    remaining = len(_reception_rows_by_state(PENDING, batch_id))
    return {'identified': len(resolved), 'reception_ids': resolved,
            'pending': remaining, 'batch_id': batch_id or ''}


def settle(batch_id):
    """After a review decision every arrival of the batch has a destination.

    The first reception stays ``采用``.  Later identical arrivals become
    ``复用签审`` when the batch was signed, otherwise ``归并``.  No calculation
    row is added and the review version was already advanced by :func:`review`.
    """
    batch_id = resolve(batch_id)
    with connection() as db:
        batch = db.execute('SELECT version,decision FROM runs WHERE id=?',
                           (batch_id,)).fetchone()
        if not batch:
            return {'identified': 0, 'reception_ids': [], 'pending': 0,
                    'batch_id': batch_id}
        signed = batch['decision'] == '通过'
        first = db.execute('SELECT MIN(id) AS id FROM receptions WHERE batch_id=?',
                           (batch_id,)).fetchone()
        rows = db.execute(
            'SELECT id FROM receptions WHERE batch_id=? AND disposition IN (?,?) '
            'AND id<>? ORDER BY id',
            (batch_id, PENDING, '归并', first['id'] if first else 0),
        ).fetchall()
        for row in rows:
            if signed:
                disposition = '复用签审'
                basis = ('同内容重复送达（编码/行序/文件名差异不计），归并并复用批次 {} 的 '
                         'REV {} 签审结论').format(batch_id, batch['version'])
            else:
                disposition = '归并'
                basis = ('同内容重复送达（编码/行序/文件名差异不计），归并为批次 {} 的同一'
                         '核算结论；本次签审退回，不产生已签复用').format(batch_id)
            db.execute('UPDATE receptions SET disposition=?,basis=? WHERE id=?',
                       (disposition, basis, row['id']))
    return {'identified': len(rows), 'reception_ids': [r['id'] for r in rows],
            'pending': len(_reception_rows_by_state(PENDING, batch_id)),
            'batch_id': batch_id}


def _reception_rows_by_state(disposition, batch_id=None):
    with connection() as db:
        if batch_id:
            return db.execute('SELECT id FROM receptions WHERE disposition=? AND batch_id=?',
                              (disposition, batch_id)).fetchall()
        return db.execute('SELECT id FROM receptions WHERE disposition=?',
                          (disposition,)).fetchall()
