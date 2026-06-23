#!/usr/bin/env python3
"""
Inspect Supabase staging tables and apply rolling outlier flags when explicitly requested.

Default behavior is safe:
- `inspect` only reads information_schema and row counts.
- `upload` is dry-run unless `--execute` is passed.
- `apply-to-fact` writes the final boolean flag into staging.fact_solar_energy_gen
  by exact (sitekey, timestamp) matching after strict prechecks.

The intermediate sparse review table is:
    staging.fact_solar_energy_gen_rolling_outlier_flags

It stores only rows where rolling_outlier_flag = true and is used as the verified
key source for `apply-to-fact`. It can be dropped manually after the team accepts
the fact-table flag.
"""

from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass
from decimal import Decimal
import os
from pathlib import Path
from typing import Iterable

import psycopg2
from psycopg2.extras import execute_values
import yaml

ROOT = Path(__file__).resolve().parents[2]
CONFIG_FILE = ROOT / "config" / "02_transform" / "01_generate_outliers.yaml"

with CONFIG_FILE.open(encoding="utf-8") as _file:
    _config = yaml.safe_load(_file)

DEFAULT_FULL_CSV = (
    ROOT / _config["paths"]["full_outlier_csv"]
)
DEFAULT_UPLOAD_CSV = (
    ROOT / _config["paths"]["sparse_outlier_csv"]
)

SCHEMA = _config["database"]["schema"]
TARGET_TABLE = _config["database"]["outlier_table"]
SOURCE_FACT_TABLE = _config["database"]["source_fact_table"]
UPLOAD_BATCH_SIZE = int(_config["runtime"]["upload_batch_size"])

REQUIRED_COLUMNS = [
    "sitekey",
    "timestamp",
    "energy_generated_kwh",
    "hour",
    "rolling_outlier_flag",
]


@dataclass(frozen=True)
class CsvAudit:
    path: Path
    rows: int
    flagged_rows: int
    flag_pct: float


def quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def qname(schema: str, table: str) -> str:
    return f"{quote_ident(schema)}.{quote_ident(table)}"


def fetch_one(cur: any, sql: str, params: tuple = ()) -> object:
    cur.execute(sql, params)
    row = cur.fetchone()
    return row[0] if row else None


ENERGY_ROUND_QUANT = Decimal("0.000000000001")


def normalize_energy_text(value: str) -> str:
    if not value.strip():
        return ""
    return f"{Decimal(value).quantize(ENERGY_ROUND_QUANT):.12f}"


def fingerprint_supabase(conn: any) -> dict[str, object]:
    cur = conn.cursor()
    try:
        cur.execute(
            f"""
            SELECT
                COUNT(*)::bigint AS rows,
                COUNT(DISTINCT sitekey)::bigint AS distinct_sitekeys,
                MIN(to_char(timestamp, 'YYYY-MM-DD HH24:MI:SS')) AS min_timestamp,
                MAX(to_char(timestamp, 'YYYY-MM-DD HH24:MI:SS')) AS max_timestamp,
                SUM(('x' || substr(md5(sitekey::text), 1, 15))::bit(60)::bigint)::numeric AS site_sum,
                SUM(('x' || substr(md5(to_char(timestamp, 'YYYY-MM-DD HH24:MI:SS')), 1, 15))::bit(60)::bigint)::numeric AS timestamp_sum,
                SUM(energy_generated_kwh::numeric) AS energy_sum,
                SUM(
                    ('x' || substr(
                        md5(sitekey::text || '|' || to_char(timestamp, 'YYYY-MM-DD HH24:MI:SS')),
                        1,
                        15
                    ))::bit(60)::bigint
                )::numeric AS key_checksum,
                SUM(
                    ('x' || substr(
                        md5(
                            sitekey::text || '|' ||
                            to_char(timestamp, 'YYYY-MM-DD HH24:MI:SS') || '|' ||
                            COALESCE(
                                to_char(
                                    round(energy_generated_kwh::numeric, 12),
                                    'FM999999999999999990.000000000000'
                                ),
                                ''
                            )
                        ),
                        1,
                        15
                    ))::bit(60)::bigint
                )::numeric AS key_value_checksum
            FROM {qname(SCHEMA, SOURCE_FACT_TABLE)}
            """
        )
        row = cur.fetchone()
        return {
            "rows": int(row[0]),
            "distinct_sitekeys": int(row[1]),
            "min_timestamp": row[2],
            "max_timestamp": row[3],
            "site_sum": Decimal(str(row[4] or 0)),
            "timestamp_sum": Decimal(str(row[5] or 0)),
            "energy_sum": Decimal(str(row[6] or 0)),
            "key_checksum": Decimal(str(row[7] or 0)),
            "key_value_checksum": Decimal(str(row[8] or 0)),
        }
    finally:
        cur.close()


def fingerprint_csv_md5_compatible(path: Path) -> dict[str, object]:
    import hashlib

    rows = 0
    flagged_rows = 0
    sitekeys: set[str] = set()
    min_timestamp: str | None = None
    max_timestamp: str | None = None
    site_sum = Decimal("0")
    timestamp_sum = Decimal("0")
    energy_sum = Decimal("0")
    key_checksum = Decimal("0")
    key_value_checksum = Decimal("0")

    def md5_60(text: str) -> int:
        return int(hashlib.md5(text.encode("utf-8")).hexdigest()[:15], 16)

    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        columns = reader.fieldnames or []
        missing = [col for col in REQUIRED_COLUMNS if col not in columns]
        if missing:
            raise RuntimeError(f"CSV missing required columns: {missing}")

        for row in reader:
            rows += 1
            sitekey = row["sitekey"]
            timestamp = row["timestamp"]
            energy_text = row["energy_generated_kwh"].strip()
            energy_rounded = normalize_energy_text(energy_text)
            flag = parse_bool(row["rolling_outlier_flag"])
            key = f"{sitekey}|{timestamp}"

            sitekeys.add(sitekey)
            min_timestamp = (
                timestamp
                if min_timestamp is None or timestamp < min_timestamp
                else min_timestamp
            )
            max_timestamp = (
                timestamp
                if max_timestamp is None or timestamp > max_timestamp
                else max_timestamp
            )
            flagged_rows += int(flag)
            site_sum += Decimal(md5_60(sitekey))
            timestamp_sum += Decimal(md5_60(timestamp))
            if energy_text:
                energy_sum += Decimal(energy_text)
            key_checksum += Decimal(md5_60(key))
            key_value_checksum += Decimal(md5_60(f"{key}|{energy_rounded}"))

    return {
        "rows": rows,
        "flagged_rows": flagged_rows,
        "distinct_sitekeys": len(sitekeys),
        "min_timestamp": min_timestamp,
        "max_timestamp": max_timestamp,
        "site_sum": site_sum,
        "timestamp_sum": timestamp_sum,
        "energy_sum": energy_sum,
        "key_checksum": key_checksum,
        "key_value_checksum": key_value_checksum,
    }


def run_verify_source(csv_path: Path, conn: any) -> int:
    local_fp = fingerprint_csv_md5_compatible(csv_path)

    try:
        cur = conn.cursor()
        cur.execute("SET default_transaction_read_only = on")
        cur.execute("SET statement_timeout = '120s'")
        db_fp = fingerprint_supabase(conn)
        cur.execute("SET default_transaction_read_only = off")
        cur.execute("SET statement_timeout = '0'")
        cur.close()
    except Exception as e:
        print(f"Verify source failed: {e}")
        raise

    print("Source verification: Supabase fact table vs local flag CSV")
    print(f"  Supabase table = {SCHEMA}.{SOURCE_FACT_TABLE}")
    print(f"  Local CSV      = {csv_path}")
    print()

    checks = [
        ("rows", db_fp["rows"], local_fp["rows"], db_fp["rows"] == local_fp["rows"]),
        (
            "distinct_sitekeys",
            db_fp["distinct_sitekeys"],
            local_fp["distinct_sitekeys"],
            db_fp["distinct_sitekeys"] == local_fp["distinct_sitekeys"],
        ),
        (
            "min_timestamp",
            db_fp["min_timestamp"],
            local_fp["min_timestamp"],
            db_fp["min_timestamp"] == local_fp["min_timestamp"],
        ),
        (
            "max_timestamp",
            db_fp["max_timestamp"],
            local_fp["max_timestamp"],
            db_fp["max_timestamp"] == local_fp["max_timestamp"],
        ),
        (
            "site_sum",
            db_fp["site_sum"],
            local_fp["site_sum"],
            db_fp["site_sum"] == local_fp["site_sum"],
        ),
        (
            "timestamp_sum",
            db_fp["timestamp_sum"],
            local_fp["timestamp_sum"],
            db_fp["timestamp_sum"] == local_fp["timestamp_sum"],
        ),
        (
            "key_checksum",
            db_fp["key_checksum"],
            local_fp["key_checksum"],
            db_fp["key_checksum"] == local_fp["key_checksum"],
        ),
    ]

    energy_diff = abs(
        Decimal(str(db_fp["energy_sum"])) - Decimal(str(local_fp["energy_sum"]))
    )
    checks.append(
        (
            "energy_sum_abs_diff",
            "0 tolerance <= 0.01",
            energy_diff,
            energy_diff <= Decimal("0.01"),
        )
    )

    ok = True
    for name, db_value, local_value, passed in checks:
        ok = ok and passed
        status = "PASS" if passed else "FAIL"
        print(f"  {status:<4s} {name:<22s} supabase={db_value} local={local_value}")

    value_checksum_passed = (
        db_fp["key_value_checksum"] == local_fp["key_value_checksum"]
    )
    value_checksum_status = "PASS" if value_checksum_passed else "WARN"
    print(
        f"  {value_checksum_status:<4s} {'key_value_checksum':<22s} "
        f"supabase={db_fp['key_value_checksum']} local={local_fp['key_value_checksum']}"
    )

    print()
    print(f"  local flagged_rows = {local_fp['flagged_rows']:,}")
    print(
        f"  local flag_pct     = {local_fp['flagged_rows'] / local_fp['rows'] * 100:.12f}%"
    )
    if not value_checksum_passed:
        print(
            "  note: key_value_checksum is warning-only because double precision values can "
            "serialize differently between PostgreSQL and CSV. Upload joins by "
            "(sitekey, timestamp); key_checksum and energy_sum_abs_diff are the hard checks."
        )

    if not ok:
        raise RuntimeError(
            "Source verification failed: local CSV does not match Supabase source"
        )
    print("Source verification passed.")
    return 0


def inspect_staging(conn: any) -> None:
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT table_name, table_type
            FROM information_schema.tables
            WHERE table_schema = %s
            ORDER BY table_name
            """,
            (SCHEMA,),
        )
        tables = cur.fetchall()
        print(f"Schema: {SCHEMA}")
        print(f"Tables/views found: {len(tables)}")
        for table_name, table_type in tables:
            try:
                row_count = fetch_one(
                    cur, f"SELECT COUNT(*) FROM {qname(SCHEMA, table_name)}"
                )
            except (
                Exception
            ) as exc:  # keep inspect usable even if one object is not countable
                row_count = f"ERROR: {exc}"
            print(f"  - {table_name:<48s} {table_type:<12s} rows={row_count}")

        print()
        for table_name in [SOURCE_FACT_TABLE, TARGET_TABLE]:
            cur.execute(
                """
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_schema = %s
                  AND table_name = %s
                ORDER BY ordinal_position
                """,
                (SCHEMA, table_name),
            )
            columns = cur.fetchall()
            if not columns:
                print(f"{SCHEMA}.{table_name}: not found")
                continue
            print(f"{SCHEMA}.{table_name}:")
            for column_name, data_type, is_nullable in columns:
                print(f"    {column_name:<28s} {data_type:<24s} nullable={is_nullable}")
    finally:
        cur.close()


def create_target_table(conn: any) -> None:
    cur = conn.cursor()
    try:
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {qname(SCHEMA, TARGET_TABLE)} (
                sitekey varchar(255) NOT NULL,
                timestamp timestamp NOT NULL,
                energy_generated_kwh double precision,
                hour integer,
                rolling_outlier_flag boolean NOT NULL,
                loaded_at timestamptz NOT NULL DEFAULT now(),
                PRIMARY KEY (sitekey, timestamp)
            )
            """
        )
        cur.execute(
            f"""
            CREATE INDEX IF NOT EXISTS idx_{TARGET_TABLE}_flag
            ON {qname(SCHEMA, TARGET_TABLE)} (rolling_outlier_flag)
            """
        )
        cur.execute(
            f"""
            CREATE INDEX IF NOT EXISTS idx_{TARGET_TABLE}_timestamp
            ON {qname(SCHEMA, TARGET_TABLE)} (timestamp)
            """
        )
    finally:
        cur.close()


def parse_bool(value: str) -> bool:
    normalized = value.strip().lower()
    if normalized in {"true", "t", "1", "yes", "y"}:
        return True
    if normalized in {"false", "f", "0", "no", "n"}:
        return False
    raise ValueError(f"Invalid boolean value: {value!r}")


def audit_csv(path: Path) -> CsvAudit:
    if not path.exists():
        raise FileNotFoundError(path)

    rows = 0
    flagged_rows = 0
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        columns = reader.fieldnames or []
        missing = [col for col in REQUIRED_COLUMNS if col not in columns]
        if missing:
            raise RuntimeError(f"CSV missing required columns: {missing}")

        for row in reader:
            rows += 1
            if parse_bool(row["rolling_outlier_flag"]):
                flagged_rows += 1

    flag_pct = flagged_rows / rows * 100 if rows else 0.0
    return CsvAudit(path=path, rows=rows, flagged_rows=flagged_rows, flag_pct=flag_pct)


def read_csv_batches(path: Path, batch_size: int) -> Iterable[list[tuple]]:
    batch: list[tuple] = []
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            energy = row["energy_generated_kwh"].strip()
            hour = row["hour"].strip()
            batch.append(
                (
                    row["sitekey"],
                    row["timestamp"],
                    float(energy) if energy else None,
                    int(hour) if hour else None,
                    parse_bool(row["rolling_outlier_flag"]),
                )
            )
            if len(batch) >= batch_size:
                yield batch
                batch = []
    if batch:
        yield batch


def truncate_target(conn: any) -> None:
    cur = conn.cursor()
    try:
        cur.execute(f"TRUNCATE TABLE {qname(SCHEMA, TARGET_TABLE)}")
    finally:
        cur.close()


def insert_batch(conn: any, batch: list[tuple]) -> None:
    cur = conn.cursor()
    try:
        execute_values(
            cur,
            f"""
            INSERT INTO {qname(SCHEMA, TARGET_TABLE)} (
                sitekey,
                timestamp,
                energy_generated_kwh,
                hour,
                rolling_outlier_flag
            )
            VALUES %s
            ON CONFLICT (sitekey, timestamp) DO UPDATE SET
                energy_generated_kwh = EXCLUDED.energy_generated_kwh,
                hour = EXCLUDED.hour,
                rolling_outlier_flag = EXCLUDED.rolling_outlier_flag,
                loaded_at = now()
            """,
            batch,
            page_size=len(batch),
        )
    finally:
        cur.close()


def verify_target(conn: any, expected: CsvAudit) -> None:
    cur = conn.cursor()
    try:
        cur.execute(
            f"""
            SELECT
                COUNT(*)::bigint AS rows,
                COUNT(*) FILTER (WHERE rolling_outlier_flag)::bigint AS flagged_rows
            FROM {qname(SCHEMA, TARGET_TABLE)}
            """
        )
        rows, flagged_rows = cur.fetchone()
        print()
        print("Upload verification:")
        print(f"  expected rows         = {expected.rows:,}")
        print(f"  uploaded rows         = {int(rows):,}")
        print(f"  expected flagged rows = {expected.flagged_rows:,}")
        print(f"  uploaded flagged rows = {int(flagged_rows):,}")
        if int(rows) != expected.rows or int(flagged_rows) != expected.flagged_rows:
            raise RuntimeError("Upload verification mismatch")
    finally:
        cur.close()


def run_upload(csv_path: Path, batch_size: int, replace: bool, execute: bool, conn: any) -> int:
    audit = audit_csv(csv_path)

    print("CSV audit:")
    print(f"  path         = {csv_path}")
    print(f"  rows         = {audit.rows:,}")
    print(f"  flagged_rows = {audit.flagged_rows:,}")
    print(f"  flag_pct     = {audit.flag_pct:.12f}%")
    print(f"  target       = {SCHEMA}.{TARGET_TABLE}")
    print("  load_style   = sparse: only rolling_outlier_flag=True rows are stored")
    print(f"  mode         = {'EXECUTE' if execute else 'DRY-RUN'}")

    if not execute:
        print()
        print("Dry-run only. Execution is disabled.")
        return 0

    if audit.flagged_rows != audit.rows:
        raise RuntimeError(
            "Sparse upload expects a candidate-only CSV where every row has "
            "rolling_outlier_flag=True. Use the default "
            "02_rolling_iqr_candidates.csv or regenerate the rolling flag output."
        )

    try:
        create_target_table(conn)
        if replace:
            truncate_target(conn)

        inserted = 0
        for batch in read_csv_batches(csv_path, batch_size):
            insert_batch(conn, batch)
            inserted += len(batch)
            print(f"  uploaded {inserted:,}/{audit.rows:,}", flush=True)

        verify_target(conn, audit)
        print(f"      Upload complete. (Batch logic finished, committing deferred to parent)")
        return 0
    except Exception:
        conn.rollback()
        raise


def run_apply_to_fact(conn: any, expected_flags: int) -> int:
    cur = conn.cursor()
    try:
        cur.execute("SET statement_timeout = '10min'")
        print(
            "BEGIN apply rolling_outlier_flag to staging.fact_solar_energy_gen",
            flush=True,
        )

        cur.execute(
            f"""
            SELECT
              COUNT(*)::bigint AS rows,
              COUNT(DISTINCT (sitekey, timestamp))::bigint AS distinct_keys,
              COUNT(*)::bigint - COUNT(DISTINCT (sitekey, timestamp))::bigint AS duplicate_rows,
              COUNT(*) FILTER (WHERE rolling_outlier_flag)::bigint AS flagged_rows
            FROM {qname(SCHEMA, TARGET_TABLE)}
            """
        )
        flag_rows, flag_distinct, flag_dupes, flag_true = map(int, cur.fetchone())
        print(
            f"flag table rows={flag_rows:,} distinct_keys={flag_distinct:,} "
            f"duplicate_rows={flag_dupes:,} true_flags={flag_true:,}",
            flush=True,
        )
        if (
            flag_rows != expected_flags
            or flag_distinct != expected_flags
            or flag_dupes != 0
            or flag_true != expected_flags
        ):
            raise RuntimeError(
                "ABORT: flag table precheck failed. "
                f"Expected rows/distinct/true={expected_flags:,}, "
                f"got rows={flag_rows:,}, distinct={flag_distinct:,}, "
                f"duplicates={flag_dupes:,}, true={flag_true:,}"
            )

        cur.execute(
            f"""
            SELECT COUNT(*)::bigint
            FROM {qname(SCHEMA, TARGET_TABLE)} o
            LEFT JOIN {qname(SCHEMA, SOURCE_FACT_TABLE)} f
              ON f.sitekey = o.sitekey
             AND f.timestamp = o.timestamp
            WHERE f.sitekey IS NULL
            """
        )
        unmatched = int(cur.fetchone()[0])
        print(f"unmatched flag rows to fact={unmatched:,}", flush=True)
        if unmatched != 0:
            raise RuntimeError(
                "ABORT: some flag rows do not match fact by sitekey+timestamp"
            )

        cur.execute(
            f"""
            SELECT
              COUNT(*)::bigint AS rows,
              COUNT(DISTINCT (sitekey, timestamp))::bigint AS distinct_keys,
              COUNT(*)::bigint - COUNT(DISTINCT (sitekey, timestamp))::bigint AS duplicate_keys
            FROM {qname(SCHEMA, SOURCE_FACT_TABLE)}
            """
        )
        fact_rows, fact_distinct, fact_dupes = map(int, cur.fetchone())
        print(
            f"fact rows={fact_rows:,} distinct_keys={fact_distinct:,} "
            f"duplicate_keys={fact_dupes:,}",
            flush=True,
        )
        if fact_rows == 0 or fact_distinct != fact_rows or fact_dupes != 0:
            raise RuntimeError(
                "ABORT: fact key precheck failed. "
                f"rows={fact_rows:,}, distinct={fact_distinct:,}, "
                f"duplicates={fact_dupes:,}"
            )
        if flag_rows > fact_rows:
            raise RuntimeError(
                "ABORT: flag rows exceed fact rows. "
                f"flags={flag_rows:,}, facts={fact_rows:,}"
            )

        print("ALTER TABLE add rolling_outlier_flag if needed", flush=True)
        cur.execute(
            f"""
            ALTER TABLE {qname(SCHEMA, SOURCE_FACT_TABLE)}
            ADD COLUMN IF NOT EXISTS rolling_outlier_flag boolean
            """
        )

        print("Reset all fact flags to false", flush=True)
        cur.execute(
            f"UPDATE {qname(SCHEMA, SOURCE_FACT_TABLE)} SET rolling_outlier_flag = false"
        )
        print(f"rows reset false={cur.rowcount:,}", flush=True)
        if cur.rowcount != fact_rows:
            raise RuntimeError("ABORT: reset rowcount mismatch")

        print("Update true flags by exact sitekey + timestamp match", flush=True)
        cur.execute(
            f"""
            UPDATE {qname(SCHEMA, SOURCE_FACT_TABLE)} f
            SET rolling_outlier_flag = true
            FROM {qname(SCHEMA, TARGET_TABLE)} o
            WHERE f.sitekey = o.sitekey
              AND f.timestamp = o.timestamp
            """
        )
        print(f"rows updated true={cur.rowcount:,}", flush=True)
        if cur.rowcount != flag_rows:
            raise RuntimeError("ABORT: true update rowcount mismatch")

        cur.execute(
            f"""
            SELECT
              COUNT(*)::bigint AS rows,
              COUNT(*) FILTER (WHERE rolling_outlier_flag = true)::bigint AS true_flags,
              COUNT(*) FILTER (WHERE rolling_outlier_flag = false)::bigint AS false_flags,
              COUNT(*) FILTER (WHERE rolling_outlier_flag IS NULL)::bigint AS null_flags
            FROM {qname(SCHEMA, SOURCE_FACT_TABLE)}
            """
        )
        rows, true_flags, false_flags, null_flags = map(int, cur.fetchone())
        print(
            f"verify fact rows={rows:,} true={true_flags:,} "
            f"false={false_flags:,} null={null_flags:,}",
            flush=True,
        )
        expected_false = fact_rows - expected_flags
        if (
            rows != fact_rows
            or true_flags != expected_flags
            or false_flags != expected_false
            or null_flags != 0
        ):
            raise RuntimeError(
                "ABORT: final count verification failed. "
                f"Expected rows={fact_rows:,}, true={expected_flags:,}, "
                f"false={expected_false:,}, null=0; got rows={rows:,}, "
                f"true={true_flags:,}, false={false_flags:,}, "
                f"null={null_flags:,}"
            )

        cur.execute(
            f"""
            SELECT COUNT(*)::bigint
            FROM {qname(SCHEMA, SOURCE_FACT_TABLE)} f
            LEFT JOIN {qname(SCHEMA, TARGET_TABLE)} o
              ON o.sitekey = f.sitekey
             AND o.timestamp = f.timestamp
            WHERE f.rolling_outlier_flag = true
              AND o.sitekey IS NULL
            """
        )
        true_without_flag = int(cur.fetchone()[0])

        cur.execute(
            f"""
            SELECT COUNT(*)::bigint
            FROM {qname(SCHEMA, TARGET_TABLE)} o
            JOIN {qname(SCHEMA, SOURCE_FACT_TABLE)} f
              ON f.sitekey = o.sitekey
             AND f.timestamp = o.timestamp
            WHERE f.rolling_outlier_flag IS DISTINCT FROM true
            """
        )
        flag_not_true = int(cur.fetchone()[0])
        print(
            f"verify true_without_flag={true_without_flag:,} "
            f"flag_not_true={flag_not_true:,}",
            flush=True,
        )
        if true_without_flag != 0 or flag_not_true != 0:
            raise RuntimeError("ABORT: mapping verification failed")

        print(
            "UPDATE OK: rolling_outlier_flag mapped (commit deferred to parent)",
            flush=True,
        )
        return 0
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()


def run_inspect(conn: any) -> int:
    try:
        cur = conn.cursor()
        cur.execute("SET default_transaction_read_only = on")
        cur.execute("SET statement_timeout = '30s'")
        cur.close()
        inspect_staging(conn)
        return 0
    except Exception as e:
        print(f"Inspect staging failed: {e}")
        raise


def apply_outlier_flags(conn: any, full_csv_path: Path, sparse_csv_path: Path, execute: bool = True) -> None:
    if not full_csv_path.exists():
        raise FileNotFoundError(f"Full CSV not found: {full_csv_path}. Cannot verify checksum!")
    if not sparse_csv_path.exists():
        raise FileNotFoundError(f"Sparse Outlier CSV not found: {sparse_csv_path}. Cannot proceed without outlier flags!")

    print("\n" + "="*88)
    print("Applying Rolling Outlier Flags")
    print("="*88)

    # Force rollback of any previous uncommitted stuff before starting our own blocks
    conn.rollback()

    print("\n[1/3] Verifying Source Checksums...")
    run_verify_source(full_csv_path, conn)
    sparse_audit = audit_csv(sparse_csv_path)

    print("\n[2/3] Uploading outlier flags to sparse table...")
    run_upload(
        sparse_csv_path,
        batch_size=UPLOAD_BATCH_SIZE,
        replace=True,
        execute=execute,
        conn=conn,
    )

    if execute:
        print("\n[3/3] Applying flags to fact_solar_energy_gen...")
        run_apply_to_fact(conn, expected_flags=sparse_audit.flagged_rows)
    else:
        print("\n[3/3] Dry-run: Skipping apply to fact table.")

    print("\nOutlier application finished successfully.")
