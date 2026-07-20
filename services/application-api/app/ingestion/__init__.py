"""Historical data ingestion: THETADATA exports -> standardized replay Parquet.

Reuses the download/clean pipeline in ~/Documents/THETADATA (kept as an
external raw-data source) and standardizes its output into the OptionTrader
replay layout with a checksummed, DB-traceable dataset manifest.

The raw source stores 1-minute bars with a *fixed* -04:00 offset all year
(``Etc/GMT+4``), which is wrong across DST boundaries. Standardization
re-derives the correct UTC instant from the naive Eastern wall-clock via the
``America/New_York`` zone, so downstream ``occurred_at_utc`` is authoritative.
"""

from app.ingestion.manifest import DatasetManifest, PartitionEntry
from app.ingestion.standardize import (
    OPTION_QUOTE_COLUMNS,
    STANDARD_COLUMNS,
    StandardizeResult,
    standardize_bars,
    standardize_option_parquet,
    standardize_option_quotes,
    standardize_parquet,
)
from app.ingestion.thetadata import ThetaDataHistoricalAdapter

__all__ = [
    "DatasetManifest",
    "OPTION_QUOTE_COLUMNS",
    "PartitionEntry",
    "STANDARD_COLUMNS",
    "StandardizeResult",
    "standardize_bars",
    "standardize_option_parquet",
    "standardize_option_quotes",
    "standardize_parquet",
    "ThetaDataHistoricalAdapter",
]
