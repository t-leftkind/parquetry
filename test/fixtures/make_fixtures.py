"""Writes pyarrow.parquet, the files parquetry must read but cannot write itself.

    uv run --with pyarrow python test/fixtures/make_fixtures.py
"""

import datetime as dt
import pathlib
from decimal import Decimal

import pyarrow as pa
import pyarrow.parquet as pq

HERE = pathlib.Path(__file__).parent
TS = dt.datetime(2026, 9, 15, 12, 0, 0, 123456)

# pyarrow stores decimal128 as FIXED_LEN_BYTE_ARRAY unless told otherwise.
table = pa.table(
    {
        "small": pa.array([Decimal("1.500"), Decimal("-9999.999"), None], pa.decimal128(7, 3)),
        "large": pa.array([Decimal("2.50"), Decimal("0.00"), None], pa.decimal128(18, 2)),
        "naive_us": pa.array([TS, TS, TS], pa.timestamp("us")),
        "naive_ms": pa.array([TS, TS, TS], pa.timestamp("ms")),
        "naive_ns": pa.array([TS, TS, TS], pa.timestamp("ns")),
        "utc_ms": pa.array([TS, TS, TS], pa.timestamp("ms", tz="UTC")),
    }
)
pq.write_table(table, HERE / "pyarrow.parquet", version="2.6")
