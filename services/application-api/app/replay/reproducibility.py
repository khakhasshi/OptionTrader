"""Deterministic hashing of a replay snapshot stream (P1-8).

A replay is reproducible iff the same standardized dataset always yields the
same snapshot sequence. We anchor that guarantee on a stable SHA-256 over the
canonical JSON of the stream: same bars in, same digest out. Any drift in
replay logic, feature math, or column ordering changes the digest and fails
the reproducibility test.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable, Mapping


def hash_snapshots(snapshots: Iterable[Mapping[str, object]]) -> str:
    """Return a stable SHA-256 hex digest over a snapshot stream.

    Canonicalization: each snapshot is serialized with sorted keys and compact
    separators, one per line, so the digest is independent of dict insertion
    order but sensitive to any value change.
    """
    h = hashlib.sha256()
    for snap in snapshots:
        line = json.dumps(snap, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        h.update(line.encode("utf-8"))
        h.update(b"\n")
    return h.hexdigest()
