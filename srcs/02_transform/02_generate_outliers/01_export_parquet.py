#!/usr/bin/env python3
"""
Export Supabase staging buffer tables to local parquet files.

The export is read-only on Supabase and chunked on large fact tables so the
download does not need to hold the whole table in RAM. Output files are prefixed
with temp_ because these are local temporary copies of staging buffer tables.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
import os
from pathlib import Path
from time import perf_counter

from dotenv import load_dotenv
import pandas as pd
import psycopg2
import pyarrow as pa
import pyarrow.parquet as pq
import yaml


REPO_ROOT = Path(__file__).resolve().parents[3]
ENV_FILE = REPO_ROOT / ".env.local"
DEFAULT_CONFIG = REPO_ROOT / "config" / "02_transform" / "01_generate_outliers.yaml"

with DEFAULT_CONFIG.open(encoding="utf-8") as _file:
    _config = yaml.safe_load(_file)

SCHEMA = _config["database"]["schema"]
DEFAULT_CHUNK_SIZE = int(_config["runtime"]["parquet_chunk_size"])


@dataclass(frozen=True)
class ExportSpec:
    table: str
    columns: tuple[str, ...]
    chunked: bool = False

    @property
    def output_name(self) -> str:
        return f"temp_{self.table}.parquet"

    @property
    def select_sql(self) -> str:
        column_list = ", ".join(self.columns)
        return f"SELECT {column_list} FROM {SCHEMA}.{self.table}"

    @property
    def count_sql(self) -> str:
        return f"SELECT COUNT(*) FROM {SCHEMA}.{self.table}"


EXPORT_SPECS: tuple[ExportSpec, ...] = (
    ExportSpec(
        table="dim_date",
        columns=("full_date", "day", "month", "year", "is_holiday", "is_semester", "is_exam"),
    ),
    ExportSpec(
        table="dim_geography",
        columns=("sitekey", "latitude", "longitude", "location_name"),
    ),
    ExportSpec(
        table="dim_solar_site",
        columns=(
            "campuskey",
            "sitekey",
            "campus_name",
            "capacity_kw",
            "number_of_panels",
            "panel",
            "inverter",
            "optimizers",
            "metric",
        ),
    ),
    ExportSpec(
        table="dim_time",
        columns=("time_string", "hour", "minute"),
    ),
    ExportSpec(
        table="dim_weather_type",
        columns=("weather_code", "is_day", "weather_condition", "description"),
    ),
    ExportSpec(
        table="fact_solar_energy_gen",
        columns=("sitekey", "timestamp", "energy_generated_kwh"),
        chunked=True,
    ),
    ExportSpec(
        table="fact_weather",
        columns=(
            "sitekey",
            "timestamp",
            "weather_code",
            "is_day",
            "shortwave_radiation",
            "temperature_c",
            "cloud_cover_total",
            "cloud_cover_low",
            "cloud_cover_mid",
            "cloud_cover_high",
            "diffuse_solar_radiation",
            "direct_normal_irradiance",
            "wind_speed",
            "precipitation_mm",
            "sunshine_duration",
        ),
        chunked=True,
    ),
)


def connect() -> psycopg2.extensions.connection:
    load_dotenv(ENV_FILE)
    required = ["DB_HOST", "DB_NAME", "DB_USER", "DB_PASSWORD"]
    missing = [name for name in required if not os.getenv(name)]
    if missing:
        raise RuntimeError(f"Missing env vars in {ENV_FILE}: {', '.join(missing)}")

    conn = psycopg2.connect(
        host=os.environ["DB_HOST"],
        port=int(os.environ.get("DB_PORT", "5432")),
        database=os.environ["DB_NAME"],
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
    )
    cur = conn.cursor()
    cur.execute("SET default_transaction_read_only = on")
    cur.execute("SET statement_timeout = '15min'")
    cur.close()
    return conn


def table_count(conn: psycopg2.extensions.connection, spec: ExportSpec) -> int:
    cur = conn.cursor()
    cur.execute(spec.count_sql)
    count = int(cur.fetchone()[0])
    cur.close()
    return count


def normalize_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    """Keep local parquet stable across chunks."""
    if "sitekey" in df.columns:
        df["sitekey"] = df["sitekey"].astype("string")
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    if "full_date" in df.columns:
        df["full_date"] = pd.to_datetime(df["full_date"], errors="coerce")
    return df


def write_single_parquet(conn: psycopg2.extensions.connection, spec: ExportSpec, path: Path) -> int:
    df = pd.read_sql_query(spec.select_sql, conn)
    df = normalize_dtypes(df)
    df.to_parquet(path, index=False, engine="pyarrow")
    return len(df)


def write_chunked_parquet(
    conn: psycopg2.extensions.connection,
    spec: ExportSpec,
    path: Path,
    chunk_size: int,
) -> int:
    writer: pq.ParquetWriter | None = None
    rows_written = 0
    try:
        for chunk_idx, chunk in enumerate(
            pd.read_sql_query(spec.select_sql, conn, chunksize=chunk_size),
            start=1,
        ):
            chunk = normalize_dtypes(chunk)
            table = pa.Table.from_pandas(chunk, preserve_index=False)
            if writer is None:
                writer = pq.ParquetWriter(path, table.schema, compression="snappy")
            writer.write_table(table)
            rows_written += len(chunk)
            print(
                f"    chunk={chunk_idx:>3} rows={len(chunk):>9,} "
                f"total={rows_written:>12,}"
            )
    finally:
        if writer is not None:
            writer.close()
    return rows_written


def parse_table_filter(table_args: list[str]) -> set[str] | None:
    if not table_args:
        return None
    names = {name.strip() for arg in table_args for name in arg.split(",") if name.strip()}
    valid = {spec.table for spec in EXPORT_SPECS}
    unknown = names - valid
    if unknown:
        raise ValueError(f"Unknown table(s): {', '.join(sorted(unknown))}")
    return names


def run(args: argparse.Namespace) -> int:
    # Đọc output_dir từ config nếu không truyền trực tiếp
    if args.output_dir:
        output_dir = Path(args.output_dir).resolve()
    else:
        config_path = Path(args.config).resolve() if args.config else DEFAULT_CONFIG
        with config_path.open(encoding="utf-8") as f:
            cfg = yaml.safe_load(f)
        output_dir = (REPO_ROOT / cfg["paths"]["parquet_dir"]).resolve()

    output_dir.mkdir(parents=True, exist_ok=True)
    selected = parse_table_filter(args.tables)

    specs = [spec for spec in EXPORT_SPECS if selected is None or spec.table in selected]
    print(f"Output dir: {output_dir}")
    print(f"Tables: {', '.join(spec.table for spec in specs)}")
    print("Mode: Supabase read-only, local parquet write")

    conn = connect()
    manifest_rows: list[dict[str, object]] = []
    started_all = perf_counter()
    try:
        for spec in specs:
            started = perf_counter()
            path = output_dir / spec.output_name
            expected_rows = table_count(conn, spec)
            print(f"\n[{spec.table}] expected_rows={expected_rows:,}")

            if path.exists() and not args.overwrite:
                raise FileExistsError(f"{path} exists. Re-run with --overwrite to replace it.")
            if path.exists():
                path.unlink()

            if spec.chunked:
                rows_written = write_chunked_parquet(conn, spec, path, args.chunk_size)
            else:
                rows_written = write_single_parquet(conn, spec, path)

            size_mb = path.stat().st_size / (1024 * 1024)
            status = "PASS" if rows_written == expected_rows else "FAIL"
            elapsed = perf_counter() - started
            print(
                f"  wrote={path.name} rows={rows_written:,} "
                f"size={size_mb:.2f}MB status={status} elapsed={elapsed:.1f}s"
            )
            manifest_rows.append(
                {
                    "table": spec.table,
                    "file": path.name,
                    "expected_rows": expected_rows,
                    "rows_written": rows_written,
                    "size_mb": round(size_mb, 3),
                    "status": status,
                    "elapsed_seconds": round(elapsed, 3),
                }
            )
            if status != "PASS":
                raise RuntimeError(f"{spec.table}: wrote {rows_written:,}, expected {expected_rows:,}")
    finally:
        conn.rollback()
        conn.close()

    manifest = pd.DataFrame(manifest_rows)
    manifest_path = output_dir / "temp_export_manifest.csv"
    manifest.to_csv(manifest_path, index=False)
    print(f"\nManifest: {manifest_path}")
    print(f"Total elapsed: {perf_counter() - started_all:.1f}s")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--config",
        default=None,
        help=f"Path to generate_outliers config YAML. Default: {DEFAULT_CONFIG}",
    )
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Output directory (overrides config). Default: read from config.",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=DEFAULT_CHUNK_SIZE,
        help="Chunk size for large fact tables.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Replace existing parquet files.",
    )
    parser.add_argument(
        "--tables",
        nargs="*",
        default=[],
        help="Optional table filter, comma-separated or space-separated.",
    )
    return parser


if __name__ == "__main__":
    raise SystemExit(run(build_parser().parse_args()))
