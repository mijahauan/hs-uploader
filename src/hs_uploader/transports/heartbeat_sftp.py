"""HeartbeatSftp transport — ships station heartbeat JSON files to a
central drop directory over SFTP.

Station heartbeat JSONs accumulate in ``/var/lib/sigmond/heartbeat``
(written by sigmond, consumed by the EXISTING ``FileTreeSource`` — this
transport adds no source). They ship to a central drop dir over SFTP.

This module deliberately COPIES the proven sftp mechanics from
``transports/wsprdaemon.py`` (``_sftp_batch_cmd``, the hardened
invocation incl. the ``ProtectHome`` known_hosts fix, and the
host-key-changed retry via ``ssh-keygen -R``) rather than sharing them
— wsprdaemon.py and this transport are on separate release cadences,
and a shared helper would couple them.  The one exception is
``_ensure_identity_key``, which IS imported (not copied) because it is
identity-generic, not transport-specific.

Wire protocol: one sftp invocation per batch — every file's ``put`` (in
record order), then every file's ``rename`` (in record order).  Each
file goes to ``<remote_path>/<basename>.part`` and is then renamed to
``<remote_path>/<basename>`` — the remote basename is the local
basename UNCHANGED (station identity travels in-band, in the JSON
payload itself, AND via the SFTP login; the server cross-checks the
two).

Retry semantics: the file IS the payload and a retry is free, so ANY
failure (sftp rc != 0, or the source file having vanished) returns
``Outcome.retry_later`` — never a permanent failure.
"""

from __future__ import annotations

import base64
import json
import logging
import os
import re
import subprocess
import tempfile
from pathlib import Path
from typing import Optional

from ..core import BatchPolicy, Outcome, RecordBatch
from .wsprdaemon import _ensure_identity_key

logger = logging.getLogger(__name__)

TABLE = "station.heartbeat"

_HOST_KEY_ERR = re.compile(
    r"REMOTE HOST IDENTIFICATION HAS CHANGED|"
    r"Host key verification failed|"
    r"host key.*has changed",
    re.I,
)


def _default_known_hosts() -> str:
    # Same ProtectHome=read-only fix as wsprdaemon.py's
    # _default_known_hosts: the recorder unit can't write
    # ~/.ssh/known_hosts, so use the uploader's writable state dir.
    base = os.environ.get("HS_UPLOADER_STATE_DIR", "/var/lib/hs-uploader")
    return str(Path(base) / "known_hosts")


class HeartbeatSftp:
    """Ships a batch of station heartbeat JSON files to one host via SFTP."""

    ACCEPTS = {TABLE: [1]}

    def __init__(
        self,
        *,
        host: str,
        port: int = 22,
        sftp_user: str = "hamsci-hb",
        remote_path: str = "incoming",
        connect_timeout_sec: int = 10,
        xfer_timeout_sec: int = 30,
        known_hosts_file: Optional[str] = None,
        name: Optional[str] = None,
    ):
        self.host = host
        self.port = port
        self.sftp_user = sftp_user
        self.remote_path = remote_path
        self.connect_timeout_sec = connect_timeout_sec
        self.xfer_timeout_sec = xfer_timeout_sec
        self.known_hosts_file = known_hosts_file or _default_known_hosts()
        # Distinct watermark key per destination.
        self.name = name or f"heartbeat-sftp:{host}"

    # -- Transport protocol --

    def primary_table(self) -> str:
        return TABLE

    def batch_policy(self) -> BatchPolicy:
        # Post-outage backlog ships a few per pump; files are ~2 KB.
        return BatchPolicy(max_records=8)

    def ship(self, batch: RecordBatch, identity) -> Outcome:
        paths = [Path(r.payload_path) for r in batch.records if r.payload_path]
        if not paths:
            return Outcome.acked()
        for p in paths:
            if not p.exists():
                # File IS the payload; retry is free — never permanent.
                return Outcome.retry_later(f"file vanished before upload: {p}")
        _ensure_identity_key(identity)
        files = [(p, p.name) for p in paths]
        rc, out = self._run_sftp_with_retry(self._batch_cmd(files), identity)
        if rc != 0:
            logger.warning("HeartbeatSftp: sftp rc=%d: %s", rc, out[-300:].strip())
            return Outcome.retry_later(f"sftp rc={rc}: {out[-200:].strip()}")
        n_bytes = sum(p.stat().st_size for p in paths)
        return Outcome(kind="acked", n_bytes=n_bytes)

    def serialize_for_retry(self, batch: RecordBatch, identity) -> bytes:
        items = []
        for r in batch.records:
            if not r.payload_path:
                continue
            p = Path(r.payload_path)
            try:
                data = p.read_bytes()
            except OSError:
                continue
            items.append({
                "basename": p.name,
                "b64": base64.b64encode(data).decode("ascii"),
            })
        return json.dumps(items).encode("utf-8")

    def replay(self, payload_blob: bytes, identity) -> Outcome:
        try:
            items = json.loads(payload_blob.decode("utf-8")) if payload_blob else []
        except (ValueError, UnicodeDecodeError) as exc:
            return Outcome.retry_later(f"corrupt replay blob: {exc}")
        if not items:
            return Outcome.acked()
        _ensure_identity_key(identity)
        tmp_paths: list[Path] = []
        try:
            files = []
            n_bytes = 0
            for item in items:
                data = base64.b64decode(item["b64"])
                fd, tmp_name = tempfile.mkstemp(prefix="hb-replay-")
                with os.fdopen(fd, "wb") as fh:
                    fh.write(data)
                tmp_path = Path(tmp_name)
                tmp_paths.append(tmp_path)
                files.append((tmp_path, item["basename"]))
                n_bytes += len(data)
            rc, out = self._run_sftp_with_retry(self._batch_cmd(files), identity)
            if rc != 0:
                logger.warning(
                    "HeartbeatSftp: replay sftp rc=%d: %s", rc, out[-300:].strip(),
                )
                return Outcome.retry_later(f"sftp rc={rc}: {out[-200:].strip()}")
            return Outcome(kind="acked", n_bytes=n_bytes)
        finally:
            for tp in tmp_paths:
                try:
                    tp.unlink()
                except OSError:
                    pass

    # -- internals --

    def _batch_cmd(self, files: list[tuple[Path, str]]) -> bytes:
        """``files`` is ``[(local_path, remote_basename), ...]``.

        One sftp invocation for the whole batch: every file's ``put``
        (record order) first, then every file's ``rename`` (record
        order) — the multi-file extension of wsprdaemon.py's
        ``.part``-then-rename convention.
        """
        lines = []
        for local, basename in files:
            lines.append(f"put {local} {self.remote_path}/{basename}.part")
        for local, basename in files:
            part = f"{self.remote_path}/{basename}.part"
            dest = f"{self.remote_path}/{basename}"
            lines.append(f"rename {part} {dest}")
        return ("\n".join(lines) + "\n").encode()

    def _run_sftp_with_retry(
        self, batch_cmd: bytes, identity,
    ) -> tuple[int, str]:
        rc, out = self._run_sftp(batch_cmd, identity)
        if rc != 0 and _HOST_KEY_ERR.search(out):
            logger.warning(
                "HeartbeatSftp: host key change for %s — clearing known_hosts entry",
                self.host,
            )
            self._remove_host_key()
            rc, out = self._run_sftp(
                batch_cmd, identity,
                extra_opts=["StrictHostKeyChecking=accept-new"],
            )
        return rc, out

    def _run_sftp(
        self,
        batch_cmd: bytes,
        identity,
        extra_opts: Optional[list[str]] = None,
    ) -> tuple[int, str]:
        cmd = [
            "sftp", "-b", "-",
            "-o", "BatchMode=yes",
            "-o", f"ConnectTimeout={self.connect_timeout_sec}",
            "-o", f"UserKnownHostsFile={self.known_hosts_file}",
            "-o", "GlobalKnownHostsFile=/dev/null",
            "-o", "StrictHostKeyChecking=accept-new",
        ]
        if getattr(identity, "ssh_key_file", None):
            cmd += ["-i", identity.ssh_key_file]
        for opt in extra_opts or []:
            cmd += ["-o", opt]
        cmd += ["-P", str(self.port)]
        cmd.append(f"{self.sftp_user}@{self.host}")
        try:
            result = subprocess.run(
                cmd,
                input=batch_cmd,
                capture_output=True,
                timeout=self.xfer_timeout_sec,
            )
            return result.returncode, (
                result.stdout + result.stderr
            ).decode(errors="replace")
        except subprocess.TimeoutExpired:
            return 1, "sftp timed out"

    def _remove_host_key(self) -> None:
        subprocess.run(
            ["ssh-keygen", "-f", self.known_hosts_file, "-R", self.host],
            capture_output=True,
        )
