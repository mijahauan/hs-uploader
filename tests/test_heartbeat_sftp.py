"""HeartbeatSftp transport tests — no network; subprocess.run is mocked.

Mirrors ``test_transport_wsprdaemon.py``'s approach to faking the SFTP
subprocess boundary (recording ``cmd``/``input`` via a fake
``subprocess.run``), since this transport deliberately copies
wsprdaemon.py's hardened sftp invocation rather than sharing it.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

from hs_uploader import Pipeline, StationIdentity, Uploader
from hs_uploader.core import Record, RecordBatch
from hs_uploader.sources import FileSpec, FileTreeSource
from hs_uploader.transports.heartbeat_sftp import HeartbeatSftp, TABLE
from hs_uploader.watermark import SqliteWatermarkStore


def _ident(call="AC0G/S", key="/etc/hs-uploader/keys/id_ed25519"):
    return StationIdentity(call=call, grid="EM38ww", ssh_key_file=key)


def _rec(path: Path) -> Record:
    return Record(
        table=TABLE,
        time=datetime.now(tz=timezone.utc),
        columns={},
        payload_path=path,
    )


def _run_ok(*args, **kwargs):
    out = MagicMock()
    out.returncode = 0
    out.stdout = b""
    out.stderr = b""
    return out


def _run_fail(*args, **kwargs):
    out = MagicMock()
    out.returncode = 1
    out.stdout = b""
    out.stderr = b"connection refused"
    return out


# ---- pure accessors ----


def test_accepts_and_primary_table():
    t = HeartbeatSftp(host="drop.example")
    assert t.ACCEPTS == {"station.heartbeat": [1]}
    assert t.primary_table() == "station.heartbeat"


def test_batch_policy_max_records_8():
    t = HeartbeatSftp(host="drop.example")
    assert t.batch_policy().max_records == 8


def test_name_defaults_to_host_scoped():
    t = HeartbeatSftp(host="drop.hamsci.org")
    assert t.name == "heartbeat-sftp:drop.hamsci.org"


def test_name_override_wins():
    t = HeartbeatSftp(host="drop.hamsci.org", name="custom-name")
    assert t.name == "custom-name"


def test_constructor_defaults():
    t = HeartbeatSftp(host="drop.example")
    assert t.port == 22
    assert t.sftp_user == "hamsci-hb"
    assert t.remote_path == "incoming"


# ---- ship() argv + batch shape ----


def test_ship_argv_has_hardened_options_and_port_and_login(tmp_path):
    p1 = tmp_path / "AC0G_1.json"
    p1.write_bytes(b"one")
    p2 = tmp_path / "AC0G_2.json"
    p2.write_bytes(b"two")
    batch = RecordBatch(records=(_rec(p1), _rec(p2)), cursor_after=b"")
    t = HeartbeatSftp(
        host="drop.example", port=2222,
        sftp_user="hamsci-hb", remote_path="incoming",
    )

    captured = {}

    def fake_run(cmd, input=None, capture_output=False, timeout=None):
        captured["cmd"] = cmd
        captured["input"] = input
        return _run_ok()

    with patch("subprocess.run", side_effect=fake_run):
        outcome = t.ship(batch, _ident())

    assert outcome.kind == "acked"
    cmd = captured["cmd"]
    assert cmd[0] == "sftp"
    assert "-b" in cmd and "-" in cmd
    joined = " ".join(cmd)
    assert "BatchMode=yes" in joined
    assert f"ConnectTimeout={t.connect_timeout_sec}" in joined
    assert "UserKnownHostsFile=" in joined
    assert "GlobalKnownHostsFile=/dev/null" in joined
    assert "StrictHostKeyChecking=accept-new" in joined
    assert "-P" in cmd
    assert cmd[cmd.index("-P") + 1] == "2222"
    assert cmd[-1] == "hamsci-hb@drop.example"
    assert any("/etc/hs-uploader/keys/id_ed25519" in a for a in cmd)


def test_ship_batch_is_puts_then_renames_in_record_order(tmp_path):
    p1 = tmp_path / "AC0G_1.json"
    p1.write_bytes(b"one")
    p2 = tmp_path / "AC0G_2.json"
    p2.write_bytes(b"two")
    batch = RecordBatch(records=(_rec(p1), _rec(p2)), cursor_after=b"")
    t = HeartbeatSftp(host="drop.example", remote_path="incoming")

    captured = {}

    def fake_run(cmd, input=None, capture_output=False, timeout=None):
        captured["input"] = input
        return _run_ok()

    with patch("subprocess.run", side_effect=fake_run):
        outcome = t.ship(batch, _ident())

    assert outcome.kind == "acked"
    lines = captured["input"].decode().strip().splitlines()
    put_lines = [l for l in lines if l.startswith("put ")]
    rename_lines = [l for l in lines if l.startswith("rename ")]
    assert len(put_lines) == 2
    assert len(rename_lines) == 2
    # ONE invocation carrying both files, puts before renames, in
    # record order.
    assert (
        lines.index(put_lines[0]) < lines.index(put_lines[1])
        < lines.index(rename_lines[0]) < lines.index(rename_lines[1])
    )
    assert put_lines[0].endswith("incoming/AC0G_1.json.part")
    assert put_lines[1].endswith("incoming/AC0G_2.json.part")
    # Remote basename == local basename, unchanged.
    assert rename_lines[0] == "rename incoming/AC0G_1.json.part incoming/AC0G_1.json"
    assert rename_lines[1] == "rename incoming/AC0G_2.json.part incoming/AC0G_2.json"


def test_ship_success_acked_with_n_bytes(tmp_path):
    p1 = tmp_path / "a.json"
    p1.write_bytes(b"12345")
    p2 = tmp_path / "b.json"
    p2.write_bytes(b"1234567")
    batch = RecordBatch(records=(_rec(p1), _rec(p2)), cursor_after=b"")
    t = HeartbeatSftp(host="drop.example")

    with patch("subprocess.run", side_effect=_run_ok):
        outcome = t.ship(batch, _ident())

    assert outcome.kind == "acked"
    assert outcome.n_bytes == 12


def test_ship_survives_concurrent_prune_during_upload(tmp_path):
    """n_bytes must be captured BEFORE the sftp invocation, not after.

    A concurrent prune (root's 24h spool prune) can unlink the source
    file the instant the transfer completes — between the successful
    sftp call and a post-upload stat.  ship() must still return acked
    with the pre-captured size, not raise.
    """
    p1 = tmp_path / "a.json"
    p1.write_bytes(b"12345")
    p2 = tmp_path / "b.json"
    p2.write_bytes(b"1234567")
    batch = RecordBatch(records=(_rec(p1), _rec(p2)), cursor_after=b"")
    t = HeartbeatSftp(host="drop.example")

    def fake_run(cmd, input=None, capture_output=False, timeout=None):
        # Simulate a prune deleting the files mid-"upload", after
        # ship() has already stat'd them but before any post-upload
        # stat could run.
        p1.unlink()
        p2.unlink()
        return _run_ok()

    with patch("subprocess.run", side_effect=fake_run):
        outcome = t.ship(batch, _ident())

    assert outcome.kind == "acked"
    assert outcome.n_bytes == 12


def test_ship_empty_batch_acked():
    t = HeartbeatSftp(host="drop.example")
    outcome = t.ship(RecordBatch(records=(), cursor_after=b""), _ident())
    assert outcome.kind == "acked"


# ---- failure handling — always retry_later, never permanent ----


def test_ship_nonzero_rc_retry_later(tmp_path):
    p = tmp_path / "a.json"
    p.write_bytes(b"x")
    batch = RecordBatch(records=(_rec(p),), cursor_after=b"")
    t = HeartbeatSftp(host="drop.example")

    with patch("subprocess.run", side_effect=_run_fail):
        outcome = t.ship(batch, _ident())

    assert outcome.kind == "retry_later"


def test_ship_missing_file_is_retry_later_not_permanent(tmp_path):
    """The brief is explicit: ANY failure -> retry_later, never
    permanent — the file IS the payload and retry is free."""
    missing = tmp_path / "gone.json"
    batch = RecordBatch(records=(_rec(missing),), cursor_after=b"")
    t = HeartbeatSftp(host="drop.example")

    outcome = t.ship(batch, _ident())
    assert outcome.kind == "retry_later"


def test_host_key_change_triggers_one_retry(tmp_path):
    p = tmp_path / "a.json"
    p.write_bytes(b"x")
    batch = RecordBatch(records=(_rec(p),), cursor_after=b"")
    t = HeartbeatSftp(host="drop.example")

    attempts = [0]

    def fake_run(cmd, input=None, capture_output=False, timeout=None):
        if cmd[0] == "ssh-keygen":
            out = MagicMock()
            out.returncode = 0
            out.stdout = b""
            out.stderr = b""
            return out
        attempts[0] += 1
        out = MagicMock()
        if attempts[0] == 1:
            out.returncode = 1
            out.stderr = b"REMOTE HOST IDENTIFICATION HAS CHANGED!"
            out.stdout = b""
        else:
            out.returncode = 0
            out.stderr = b""
            out.stdout = b""
        return out

    with patch("subprocess.run", side_effect=fake_run):
        outcome = t.ship(batch, _ident())

    assert outcome.kind == "acked"
    assert attempts[0] == 2  # initial + one retry


# ---- station key auto-generation ----


def test_ship_calls_ensure_identity_key(tmp_path):
    p = tmp_path / "a.json"
    p.write_bytes(b"x")
    batch = RecordBatch(records=(_rec(p),), cursor_after=b"")
    t = HeartbeatSftp(host="drop.example")

    calls = []

    class FakeIdent(StationIdentity):
        def ensure_ssh_key(self):
            calls.append(True)

    ident = FakeIdent(call="AC0G/S", ssh_key_file=str(tmp_path / "key"))

    with patch("subprocess.run", side_effect=_run_ok):
        t.ship(batch, ident)

    assert calls == [True]


# ---- replay byte-stability ----


def test_replay_ships_identical_bytes_after_source_file_deleted(tmp_path):
    src = tmp_path / "AC0G_S_20260820T120000Z.json"
    payload = b'{"call": "AC0G/S", "seq": 1}'
    src.write_bytes(payload)
    batch = RecordBatch(records=(_rec(src),), cursor_after=b"")
    t = HeartbeatSftp(host="drop.example", remote_path="incoming")

    blob = t.serialize_for_retry(batch, _ident())

    src.unlink()
    assert not src.exists()

    captured = {}

    def fake_run(cmd, input=None, capture_output=False, timeout=None):
        text = input.decode()
        put_line = next(l for l in text.splitlines() if l.startswith("put "))
        local_path = put_line.split()[1]
        # Read the temp file's bytes NOW — before the transport's
        # finally-block cleanup removes it.
        captured["bytes"] = Path(local_path).read_bytes()
        captured["text"] = text
        return _run_ok()

    with patch("subprocess.run", side_effect=fake_run):
        outcome = t.replay(blob, _ident())

    assert outcome.kind == "acked"
    assert captured["bytes"] == payload
    assert "incoming/AC0G_S_20260820T120000Z.json.part" in captured["text"]
    assert "rename incoming/AC0G_S_20260820T120000Z.json.part incoming/AC0G_S_20260820T120000Z.json" in captured["text"]


def test_replay_failure_is_retry_later(tmp_path):
    src = tmp_path / "a.json"
    src.write_bytes(b"x")
    batch = RecordBatch(records=(_rec(src),), cursor_after=b"")
    t = HeartbeatSftp(host="drop.example")
    blob = t.serialize_for_retry(batch, _ident())

    with patch("subprocess.run", side_effect=_run_fail):
        outcome = t.replay(blob, _ident())

    assert outcome.kind == "retry_later"


def test_serialize_for_retry_empty_batch_replays_as_acked():
    t = HeartbeatSftp(host="drop.example")
    blob = t.serialize_for_retry(RecordBatch(records=(), cursor_after=b""), _ident())
    outcome = t.replay(blob, _ident())
    assert outcome.kind == "acked"


# ---- integration: real FileTreeSource + real watermark store ----


def test_integration_pump_ships_and_deletes_on_ack(tmp_path):
    hb_dir = tmp_path / "heartbeat"
    hb_dir.mkdir()
    f = hb_dir / "AC0G_S_20260820T120000Z.json"
    f.write_text('{"call": "AC0G/S"}')

    source = FileTreeSource(
        root=hb_dir,
        specs=[FileSpec(pattern="*.json", parser=None, table=TABLE)],
    )
    wm = SqliteWatermarkStore(tmp_path / "wm.db")
    transport = HeartbeatSftp(host="drop.example")
    pipe = Pipeline(
        name="heartbeat", source=source, transport=transport,
        watermark=wm, identity=_ident(),
    )
    up = Uploader([pipe])

    with patch("subprocess.run", side_effect=_run_ok):
        did_work = up.pump()

    assert did_work is True
    assert not f.exists()  # deleted on ack (delete_on_ack retention)

    # Second pump: nothing left to do.
    with patch("subprocess.run", side_effect=_run_ok):
        did_work_2 = up.pump()
    assert did_work_2 is False
