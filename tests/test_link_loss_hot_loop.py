"""hs-uploader#4 — link loss must not hot-loop duplicates or storm the gateway.

Observed 2026-08-22 on DASI002: the wspr-wsprdaemon pipeline runs in drain
mode (max_records_per_pump=20000); with DNS dead the first attempt returned
retry_later, the cursor stayed put, _drain_source looped, the source re-yielded
the same 17-record cycle, and 20000/17 = 1,176 duplicate deliverables were
enqueued in 47 s.  On restore all 1,176 retried back-to-back (~4 conn/s
against gw2) until they dead-lettered an hour later.
"""
from __future__ import annotations

from hs_uploader.core import Outcome, Pipeline, Uploader
from hs_uploader.watermark.sqlite import SqliteWatermarkStore
from tests.conftest import MemorySource
from tests.test_core_orchestration import _ident, _records


def _pipe(tmp_path, transport, src, **kw):
    wm = SqliteWatermarkStore(tmp_path / "wm.db")
    return Pipeline(name="test", source=src, transport=transport, watermark=wm,
                    identity=_ident(), **kw), wm


def test_drain_mode_stops_after_a_failed_first_attempt(tmp_path, memory_transport):
    """Drain mode + a failing destination: ONE deliverable, not budget/batch."""
    src = MemorySource(records=_records(17))          # one "cycle" of 17 records
    memory_transport.next_outcomes = [Outcome.retry_later("Could not resolve host")] * 5000
    pipe, wm = _pipe(tmp_path, memory_transport, src, max_records_per_pump=20000)
    Uploader([pipe]).pump()
    assert wm.deliverable_count("test") == 1
    assert len(memory_transport.shipped) == 1      # one attempt, not 1,176


def test_drain_mode_stops_after_a_permanent_failure_too(tmp_path, memory_transport):
    src = MemorySource(records=_records(3))
    memory_transport.next_outcomes = [Outcome.permanent_failure("rejected")] * 500
    pipe, wm = _pipe(tmp_path, memory_transport, src, max_records_per_pump=20000)
    Uploader([pipe]).pump()
    assert len(memory_transport.shipped) == 1
    assert wm.dead_letter_count() == 1


def test_drain_mode_still_drains_on_success(tmp_path, memory_transport):
    """The fix must not break drain mode's purpose: many small batches per pump."""
    src = MemorySource(records=_records(6))
    pipe, wm = _pipe(tmp_path, memory_transport, src, max_records_per_pump=20000,
                     batch_limit=2)
    Uploader([pipe]).pump()
    assert len(memory_transport.shipped) == 3      # 6 records / 2 per batch, all acked
    assert wm.get_cursor("memory:test", "memory", "test.spots") == b"6"


def test_replay_drain_probes_once_per_pump_while_destination_fails(tmp_path, memory_transport):
    """N due deliverables + a failing destination → ONE replay this pump
    (the pump interval is the rate cap), not N back-to-back connections."""
    src = MemorySource(records=_records(0))
    pipe, wm = _pipe(tmp_path, memory_transport, src)
    for i in range(5):
        wm.enqueue_deliverable(pipeline="test", payload_blob=b"p%d" % i,
                               enqueued_at="2026-08-22T18:12:00+00:00",
                               next_attempt_at="1970-01-01T00:00:00+00:00",
                               cursor_after=b"c%d" % i)
    memory_transport.next_outcomes = [Outcome.retry_later("Connection closed")] * 50
    Uploader([pipe]).pump()
    assert len(memory_transport.replayed) == 1
    assert wm.deliverable_count("test") == 5       # nothing lost, 4 untouched


def test_replay_drain_continues_while_destination_acks(tmp_path, memory_transport):
    src = MemorySource(records=_records(0))
    pipe, wm = _pipe(tmp_path, memory_transport, src)
    for i in range(4):
        wm.enqueue_deliverable(pipeline="test", payload_blob=b"p%d" % i,
                               enqueued_at="2026-08-22T18:12:00+00:00",
                               next_attempt_at="1970-01-01T00:00:00+00:00",
                               cursor_after=b"c%d" % i)
    Uploader([pipe]).pump()                         # default outcome = acked
    assert len(memory_transport.replayed) == 4
    assert wm.deliverable_count("test") == 0


def test_enqueue_dedupes_on_pipeline_and_cursor(tmp_path):
    wm = SqliteWatermarkStore(tmp_path / "wm.db")
    a = wm.enqueue_deliverable(pipeline="p", payload_blob=b"x", enqueued_at="t",
                               next_attempt_at="t", cursor_after=b"2026-08-21T18:10:00Z")
    b = wm.enqueue_deliverable(pipeline="p", payload_blob=b"y", enqueued_at="t2",
                               next_attempt_at="t2", cursor_after=b"2026-08-21T18:10:00Z")
    assert wm.deliverable_count("p") == 1
    assert a == b                                     # the existing row's id is returned
    # a different pipeline with the same cursor is NOT a duplicate
    wm.enqueue_deliverable(pipeline="q", payload_blob=b"z", enqueued_at="t",
                           next_attempt_at="t", cursor_after=b"2026-08-21T18:10:00Z")
    assert wm.deliverable_count("q") == 1


def test_enqueue_with_empty_cursor_never_dedupes(tmp_path):
    """File-tree sources enqueue with an empty cursor (each file is its own
    payload) — those must never be collapsed."""
    wm = SqliteWatermarkStore(tmp_path / "wm.db")
    wm.enqueue_deliverable(pipeline="hb", payload_blob=b"f1", enqueued_at="t", next_attempt_at="t")
    wm.enqueue_deliverable(pipeline="hb", payload_blob=b"f2", enqueued_at="t", next_attempt_at="t")
    assert wm.deliverable_count("hb") == 2
