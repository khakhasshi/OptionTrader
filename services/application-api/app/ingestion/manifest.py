"""Dataset manifest: the traceability record persisted alongside replay Parquet.

PostgreSQL stores the dataset inventory (path / checksum / covered period /
import status); this module is the in-process representation of one manifest,
written next to the partitions as ``_manifest.json`` and hashed so replay is
reproducible and auditable.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, field
from pathlib import Path

MANIFEST_SCHEMA_VERSION = "1.0"
_CHUNK = 1 << 20


def sha256_file(path: Path) -> str:
    """Streaming SHA-256 of a file, hex-encoded."""
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        while chunk := fh.read(_CHUNK):
            h.update(chunk)
    return h.hexdigest()


@dataclass(frozen=True)
class PartitionEntry:
    """One trading-date partition file within a dataset."""

    trading_date: str  # YYYY-MM-DD (Eastern)
    relative_path: str  # path relative to the dataset root
    rows: int
    sha256: str
    bytes: int

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass
class DatasetManifest:
    """Inventory record for one standardized dataset.

    ``import_status`` mirrors the DB lifecycle: PENDING while partitions are
    being written, COMPLETE once the manifest is finalized and hashed.
    """

    provider: str
    data_type: str  # equity_1m | index_1m | option_1m ...
    symbol: str  # canonical, e.g. QQQ.US
    interval: str  # 1m
    source_file: str
    partitions: list[PartitionEntry] = field(default_factory=list)
    import_status: str = "PENDING"
    manifest_schema_version: str = MANIFEST_SCHEMA_VERSION

    @property
    def rows(self) -> int:
        return sum(p.rows for p in self.partitions)

    @property
    def coverage_start(self) -> str | None:
        return min((p.trading_date for p in self.partitions), default=None)

    @property
    def coverage_end(self) -> str | None:
        return max((p.trading_date for p in self.partitions), default=None)

    def content_checksum(self) -> str:
        """Deterministic hash over partition checksums (order-independent).

        Two datasets with identical per-partition content produce the same
        value regardless of write order — the anchor for replay reproducibility.
        """
        h = hashlib.sha256()
        for p in sorted(self.partitions, key=lambda e: e.trading_date):
            h.update(p.trading_date.encode())
            h.update(b"\x00")
            h.update(p.sha256.encode())
            h.update(b"\x00")
        return h.hexdigest()

    def to_dict(self) -> dict[str, object]:
        return {
            "manifest_schema_version": self.manifest_schema_version,
            "provider": self.provider,
            "data_type": self.data_type,
            "symbol": self.symbol,
            "interval": self.interval,
            "source_file": self.source_file,
            "import_status": self.import_status,
            "coverage_start": self.coverage_start,
            "coverage_end": self.coverage_end,
            "rows": self.rows,
            "partition_count": len(self.partitions),
            "content_checksum": self.content_checksum(),
            "partitions": [p.to_dict() for p in self.partitions],
        }

    def write(self, path: Path) -> Path:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(self.to_dict(), indent=2, sort_keys=True))
        return path
