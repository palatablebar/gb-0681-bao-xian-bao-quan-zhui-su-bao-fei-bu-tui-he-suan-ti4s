"""Logical content identity for endorsement deliveries.

Deliveries of the same conservation/endorsement flow may arrive from different
channels with different encodings, row order, filenames, column order or number
formatting.  The physical bytes are always kept by the reception ledger; this
module derives the *logical* fingerprint used to decide whether a delivery can
reuse an already signed calculation conclusion.

Canonicalisation rules:
  * decode with the same UTF-8 BOM / UTF-8 / GB18030 fallback as M1;
  * parse with ``csv`` so quoted newlines stay inside one logical record;
  * NFKC-normalise values, normalise newlines and trim outer whitespace;
  * normalise the numeric columns and ISO dates with the same business rules
    used by M2 (so ``3,650`` / ``３,６５０`` / ``3650`` are the same amount);
  * compare rows as a sorted multiset and columns by their header name, so row
    order and column order do not fork a batch.

Structural lines are *not* formatting: a blank line or a repeated header line
changes the row-count reconciliation, so it stays in the canonical content.
Anything else that is *not* an encoding/order/format difference also stays a
difference: duplicate records, missing sentinels, conflicting versions, widths
etc. remain part of the canonical content and therefore never silently share a
conclusion.
"""
import csv
import hashlib
import io
import json
import unicodedata
from datetime import date
from decimal import Decimal, InvalidOperation

from app.config import DATES, NUMERIC
from app.errors import DomainError
from app.modules.m1_ingest.decode import decode

SENTINELS = {'', '-', 'N/A', 'NULL', '未知'}


def _text(value):
    # NFC only: compatibility folding (e.g. fullwidth letters) would merge
    # genuinely different identifiers; numeric columns are normalised separately.
    value = unicodedata.normalize('NFC', str(value))
    return value.replace('\r\n', '\n').replace('\r', '\n').strip()


def _number(value):
    text = unicodedata.normalize('NFKC', str(value)).strip().replace(',', '')
    if text.upper() in SENTINELS:
        return text.upper()
    try:
        number = Decimal(text)
        if not number.is_finite():
            raise InvalidOperation
        return str(number.normalize())
    except InvalidOperation:
        return text


def _date(value):
    text = unicodedata.normalize('NFKC', str(value)).strip()
    try:
        return date.fromisoformat(text).isoformat()
    except ValueError:
        return text


def _value(column, value):
    if column in NUMERIC:
        return _number(value)
    if column in DATES:
        return _date(value)
    return _text(value)


def canonicalize(content):
    """Return encoding + logical fingerprint for one delivery's raw bytes."""
    text, encoding = decode(content)
    try:
        records = list(csv.reader(io.StringIO(text, newline='')))
    except csv.Error as exc:
        raise DomainError('CSV', str(exc))
    if not records:
        raise DomainError('EMPTY_FILE', '文件为空')

    header = [_text(name).lstrip('﻿') for name in records[0]]
    rows = []
    for record in records[1:]:
        normalised = [_text(value) for value in record]
        if not any(normalised):
            # Keep structural lines in the identity: blank lines and repeated
            # headers are part of the row-count reconciliation, so a delivery
            # with an extra blank line is not "the same content".
            rows.append(['BLANK'])
            continue
        if normalised == header:
            rows.append(['HEADER'])
            continue
        if len(record) != len(header):
            # Width errors are content, not formatting: keep them visible.
            rows.append(['WIDTH', len(record), normalised])
            continue
        pairs = sorted(
            ([name, _value(name, value)] for name, value in zip(header, record)),
            key=lambda pair: pair[0],
        )
        rows.append(['ROW', pairs])

    rows.sort(key=lambda row: json.dumps(row, ensure_ascii=False, sort_keys=True))
    payload = json.dumps(
        {'header': sorted(header), 'rows': rows},
        ensure_ascii=False, sort_keys=True, separators=(',', ':'),
    )
    return {
        'canonical': hashlib.sha256(payload.encode('utf-8')).hexdigest()[:32],
        'encoding': encoding,
        'text': text,
        'header': header,
        'record_count': len(rows),
    }
