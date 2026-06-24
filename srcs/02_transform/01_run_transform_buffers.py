#!/usr/bin/env python3
"""
Load Supabase staging raw tables into staging buffer tables.

Default mode is dry-run: it prints target row counts and does not write data.
Use --execute to TRUNCATE and INSERT into buffer tables.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
import os
import importlib.util
from pathlib import Path
import yaml

# Load outlier logic from sibling file
def load_outlier_module():
    import sys
    current_dir = Path(__file__).resolve().parent
    path = current_dir / "02_run_apply_outlier_flags.py"
    spec = importlib.util.spec_from_file_location("pipeline_outlier", path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules["pipeline_outlier"] = mod
    spec.loader.exec_module(mod)
    return mod

pipeline_outlier = load_outlier_module()

PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONFIG_FILE = (
    PROJECT_ROOT
    / "config"
    / "02_transform"
    / "02_transform_buffers.yaml"
)

with CONFIG_FILE.open(encoding="utf-8") as _file:
    _config = yaml.safe_load(_file)

OUTLIER_CSV = (
    PROJECT_ROOT
    / "reports"
    / "outlier_flag_pipeline_rolling_only"
    / "02_rolling_iqr_candidates.csv"
)

SCHEMA = _config["database"]["schema"]
TIMESTAMP_ISO_REGEX = _config["timestamp_parsing"]["iso_regex"]
TIMESTAMP_FALLBACK_FORMAT = _config["timestamp_parsing"]["fallback_format"]
CALENDAR_DATE_FORMAT = _config["calendar"]["date_format"]

SOLAR_TIMESTAMP_EXPR = f"""
CASE
    WHEN timestamp ~ '{TIMESTAMP_ISO_REGEX}'
        THEN timestamp::timestamp
    ELSE to_timestamp(timestamp, '{TIMESTAMP_FALLBACK_FORMAT}')::timestamp
END
"""

WEATHER_TYPE_MAP_SQL = """
WITH weather_type_map(weather_code, is_day, weather_condition, description) AS (
    SELECT
        w.weather_code,
        w.is_day,
        CASE
            WHEN w.weather_code = 0 AND w.is_day = 1 THEN 'Sunny / Clear Sky'
            WHEN w.weather_code IN (0, 1, 2, 3) AND w.is_day = 0 THEN 'Clear / Cloudy Night'
            WHEN w.weather_code IN (1, 2) AND w.is_day = 1 THEN 'Partly Cloudy'
            WHEN w.weather_code = 3 AND w.is_day = 1 THEN 'Overcast'
            WHEN w.weather_code IN (45, 48) THEN 'Fog / Mist'
            WHEN w.weather_code IN (51, 53, 61, 80) THEN 'Light Rain / Drizzle'
            WHEN w.weather_code IN (55, 63, 65, 81, 82) THEN 'Moderate to Heavy Rain'
            WHEN w.weather_code IN (56, 57, 66, 67) THEN 'Freezing Rain'
            WHEN w.weather_code IN (71, 73, 75, 77, 85, 86) THEN 'Snowfall'
            WHEN w.weather_code IN (95, 96, 99) THEN 'Thunderstorm / Hail'
        END AS weather_condition,
        CASE
            WHEN w.weather_code = 0 AND w.is_day = 1
                THEN 'Troi quang dang, khong co may che phu. Hieu suat toi da; nhiet do cao co the lam giam nhe voltage.'
            WHEN w.weather_code IN (0, 1, 2, 3) AND w.is_day = 0
                THEN 'Dem quang may hoac co may rai rac. Buc xa bang 0, khong phat dien.'
            WHEN w.weather_code IN (1, 2) AND w.is_day = 1
                THEN 'Troi co may rai rac. Hieu suat cao, co dao dong nhe khi may di chuyen qua tam pin.'
            WHEN w.weather_code = 3 AND w.is_day = 1
                THEN 'Troi am u, may che phu hoan toan. San luong sut giam manh, chu yeu thu duoc buc xa tan xa.'
            WHEN w.weather_code IN (45, 48)
                THEN 'Suong mu hoac suong mu rime dong bang. Suy giam buc xa truc tiep, co the gay dong nuoc tren mat pin.'
            WHEN w.weather_code IN (51, 53, 61, 80)
                THEN 'Mua phun hoac mua rao nhe. Giam san luong, co the rua troi bui ban tren tam pin.'
            WHEN w.weather_code IN (55, 63, 65, 81, 82)
                THEN 'Mua vua den mua to hoac mua rao xoi xa. San luong tiem can 0, may thuong che phu cao.'
            WHEN w.weather_code IN (56, 57, 66, 67)
                THEN 'Mua dong bang. Nguy co tao lop bang tren tam pin, ngan can hap thu quang hoc.'
            WHEN w.weather_code IN (71, 73, 75, 77, 85, 86)
                THEN 'Tuyet roi. Nguy hiem cho van hanh, mang pin co the bi che phu boi tuyet.'
            WHEN w.weather_code IN (95, 96, 99)
                THEN 'Giong bao, sam set hoac mua da. Thoi tiet cuc doan, rui ro ve dien ap va hong hoc vat ly.'
        END AS description
    FROM (
        SELECT DISTINCT weather_code::int AS weather_code, is_day::int AS is_day
        FROM staging.stg_open_meteo_weather_raw
    ) w
)
"""


@dataclass(frozen=True)
class LoadSpec:
    table: str
    columns: tuple[str, ...]
    select_sql: str

    @property
    def insert_sql(self) -> str:
        column_list = ", ".join(self.columns)
        return f"INSERT INTO {SCHEMA}.{self.table} ({column_list})\n{self.select_sql}"


LOAD_SPECS: tuple[LoadSpec, ...] = (
    LoadSpec(
        table="dim_date",
        columns=(
            "full_date",
            "day",
            "month",
            "year",
            "is_holiday",
            "is_semester",
            "is_exam",
        ),
        select_sql="""
            SELECT
                to_date(date, 'DD/MM/YYYY') AS full_date,
                EXTRACT(day FROM to_date(date, 'DD/MM/YYYY'))::int AS day,
                EXTRACT(month FROM to_date(date, 'DD/MM/YYYY'))::int AS month,
                EXTRACT(year FROM to_date(date, 'DD/MM/YYYY'))::int AS year,
                is_holiday,
                is_semester,
                is_exam
            FROM staging.stg_calender
        """,
    ),
    LoadSpec(
        table="dim_geography",
        columns=("sitekey", "latitude", "longitude", "location_name"),
        select_sql="""
            SELECT DISTINCT
                w.sitekey,
                w.latitude::numeric AS latitude,
                w.longitude::numeric AS longitude,
                c.name AS location_name
            FROM staging.stg_open_meteo_weather_raw w
            LEFT JOIN staging.stg_solar_site_details s
                ON w.sitekey = s.sitekey
            LEFT JOIN staging.stg_campus_meta c
                ON s.campuskey = c.id
        """,
    ),
    LoadSpec(
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
        select_sql="""
            SELECT
                s.campuskey,
                s.sitekey,
                c.name AS campus_name,
                NULLIF(s.kwp, '')::numeric AS capacity_kw,
                CASE
                    WHEN NULLIF(s.number_of_panels, '') IS NULL THEN NULL
                    ELSE ROUND(NULLIF(s.number_of_panels, '')::numeric)::int
                END AS number_of_panels,
                s.panel,
                s.inverter,
                s.optimizers,
                s.metric
            FROM staging.stg_solar_site_details s
            LEFT JOIN staging.stg_campus_meta c
                ON s.campuskey = c.id
        """,
    ),
    LoadSpec(
        table="dim_time",
        columns=("time_string", "hour", "minute"),
        select_sql=f"""
            WITH all_times AS (
                SELECT {SOLAR_TIMESTAMP_EXPR} AS ts
                FROM staging.stg_solar_energy_generation
                UNION
                SELECT timestamp::timestamp AS ts
                FROM staging.stg_open_meteo_weather_raw
            )
            SELECT DISTINCT
                to_char(ts, 'HH24:MI') AS time_string,
                EXTRACT(hour FROM ts)::int AS hour,
                EXTRACT(minute FROM ts)::int AS minute
            FROM all_times
        """,
    ),
    LoadSpec(
        table="dim_weather_type",
        columns=("weather_code", "is_day", "weather_condition", "description"),
        select_sql=f"""
            {WEATHER_TYPE_MAP_SQL}
            SELECT DISTINCT
                w.weather_code::int AS weather_code,
                w.is_day::int AS is_day,
                m.weather_condition::varchar AS weather_condition,
                m.description::varchar AS description
            FROM staging.stg_open_meteo_weather_raw w
            LEFT JOIN weather_type_map m
                ON w.weather_code::int = m.weather_code
               AND w.is_day::int = m.is_day
        """,
    ),
    LoadSpec(
        table="fact_solar_energy_gen",
        columns=("sitekey", "timestamp", "energy_generated_kwh"),
        select_sql=f"""
            SELECT
                sitekey,
                {SOLAR_TIMESTAMP_EXPR} AS timestamp,
                CASE
                    WHEN NULLIF(solargeneration, '') IS NULL THEN NULL
                    WHEN solargeneration ~ '^-?[0-9]+(\\.[0-9]+)?$'
                        THEN solargeneration::numeric
                    ELSE NULL
                END AS energy_generated_kwh
            FROM staging.stg_solar_energy_generation
        """,
    ),
    LoadSpec(
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
        select_sql="""
            SELECT
                sitekey,
                timestamp::timestamp AS timestamp,
                weather_code::int AS weather_code,
                is_day::int AS is_day,
                shortwave_radiation::numeric AS shortwave_radiation,
                temperature_2m::numeric AS temperature_c,
                cloud_cover::numeric AS cloud_cover_total,
                cloud_cover_low::numeric AS cloud_cover_low,
                cloud_cover_mid::numeric AS cloud_cover_mid,
                cloud_cover_high::numeric AS cloud_cover_high,
                diffuse_radiation::numeric AS diffuse_solar_radiation,
                direct_radiation::numeric AS direct_normal_irradiance,
                wind_speed_10m::numeric AS wind_speed,
                precipitation::numeric AS precipitation_mm,
                sunshine_duration::numeric AS sunshine_duration
            FROM staging.stg_open_meteo_weather_raw
        """,
    ),
)

# Chỉ thay schema theo config; toàn bộ SQL nghiệp vụ bên trong giữ nguyên.
LOAD_SPECS = tuple(
    LoadSpec(
        table=spec.table,
        columns=spec.columns,
        select_sql=(
            spec.select_sql
            .replace("staging.", f"{SCHEMA}.")
            .replace("DD/MM/YYYY", CALENDAR_DATE_FORMAT)
        ),
    )
    for spec in LOAD_SPECS
)

TRUNCATE_ORDER = (
    "fact_weather",
    "fact_solar_energy_gen",
    "dim_weather_type",
    "dim_time",
    "dim_solar_site",
    "dim_geography",
    "dim_date",
)

FACT_TABLES = {
    "fact_solar_energy_gen",
    "fact_weather",
}


def count_select(conn: any, sql: str) -> int:
    cur = conn.cursor()
    try:
        cur.execute(f"SELECT COUNT(*) FROM ({sql}) AS q")
        return int(cur.fetchone()[0])
    finally:
        cur.close()


def count_table(conn: any, table: str) -> int:
    cur = conn.cursor()
    try:
        cur.execute(f"SELECT COUNT(*) FROM {SCHEMA}.{table}")
        return int(cur.fetchone()[0])
    finally:
        cur.close()


def truncate_buffers(conn: any) -> None:
    table_list = ", ".join(f"{SCHEMA}.{table}" for table in TRUNCATE_ORDER)
    cur = conn.cursor()
    try:
        cur.execute(f"TRUNCATE TABLE {table_list} RESTART IDENTITY")
    finally:
        cur.close()


def execute_load(conn: any, spec: LoadSpec) -> int:
    cur = conn.cursor()
    try:
        cur.execute(spec.insert_sql)
    finally:
        cur.close()
    return count_table(conn, spec.table)


def get_month_ranges(conn: any, spec: LoadSpec) -> list[tuple]:
    """Read distinct month boundaries from the unchanged fact SELECT."""
    cur = conn.cursor()
    try:
        cur.execute(
            f"""
            SELECT DISTINCT
                date_trunc('month', source.timestamp)::timestamp AS month_start,
                (
                    date_trunc('month', source.timestamp)
                    + interval '1 month'
                )::timestamp AS month_end
            FROM ({spec.select_sql}) AS source
            WHERE source.timestamp IS NOT NULL
            ORDER BY month_start
            """
        )
        return list(cur.fetchall())
    finally:
        cur.close()


def execute_fact_month(
    conn: any,
    spec: LoadSpec,
    month_start: any,
    month_end: any,
) -> int:
    """Insert one month while preserving the original fact SELECT."""
    column_list = ", ".join(spec.columns)
    cur = conn.cursor()
    try:
        cur.execute(
            f"""
            INSERT INTO {SCHEMA}.{spec.table} ({column_list})
            SELECT *
            FROM ({spec.select_sql}) AS source
            WHERE source.timestamp >= %s
              AND source.timestamp < %s
            """,
            (month_start, month_end),
        )
        return cur.rowcount
    finally:
        cur.close()


def load_fact_in_monthly_batches(
    conn: any,
    spec: LoadSpec,
    expected_total: int,
) -> None:
    """Load and commit one fact-table month at a time."""
    month_ranges = get_month_ranges(conn, spec)
    loaded_total = 0

    print(
        f"  - {spec.table:<24s} "
        f"monthly_batches={len(month_ranges):>3}"
    )

    for batch_number, (month_start, month_end) in enumerate(
        month_ranges,
        start=1,
    ):
        try:
            loaded_batch = execute_fact_month(
                conn,
                spec,
                month_start,
                month_end,
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise

        loaded_total += loaded_batch
        print(
            f"      [{batch_number:>2}/{len(month_ranges):>2}] "
            f"{month_start:%Y-%m} rows={loaded_batch:>10,} "
            f"total={loaded_total:>12,}"
        )

    actual_total = count_table(conn, spec.table)
    status = (
        "OK"
        if actual_total == expected_total == loaded_total
        else "MISMATCH"
    )
    print(
        f"      final loaded={actual_total:>12,} "
        f"expected={expected_total:>12,} [{status}]"
    )

    if status != "OK":
        raise RuntimeError(
            f"{spec.table} loaded {actual_total:,}, "
            f"batch total {loaded_total:,}, expected {expected_total:,}"
        )


def transform_staging_to_buffers(conn: any, *, execute: bool = True) -> None:
    """Run the original dry-run/count/load workflow using a managed connection."""
    print("=" * 88)
    print("Load staging.stg_* -> staging buffer tables")
    print("=" * 88)
    print(f"mode={'EXECUTE' if execute else 'DRY-RUN'}")

    print("\nPlanned row counts:")
    planned_counts: dict[str, int] = {}
    for spec in LOAD_SPECS:
        planned_counts[spec.table] = count_select(conn, spec.select_sql)
        print(f"  - {spec.table:<24s} {planned_counts[spec.table]:>12,}")

    if not execute:
        print("\nDry-run completed. No buffer table was modified.")
        return

    print("\nTruncating buffer tables...")
    truncate_buffers(conn)
    conn.commit()

    print("\nLoading dimensions:")
    for spec in LOAD_SPECS:
        if spec.table in FACT_TABLES:
            continue

        loaded = execute_load(conn, spec)
        expected = planned_counts[spec.table]
        status = "OK" if loaded == expected else "MISMATCH"
        print(
            f"  - {spec.table:<24s} "
            f"loaded={loaded:>12,} expected={expected:>12,} [{status}]"
        )
        if loaded != expected:
            raise RuntimeError(
                f"{spec.table} loaded {loaded:,}, expected {expected:,}"
            )
    conn.commit()

    print("\nLoading facts in monthly batches:")
    for spec in LOAD_SPECS:
        if spec.table not in FACT_TABLES:
            continue

        load_fact_in_monthly_batches(
            conn,
            spec,
            planned_counts[spec.table],
        )

    print("\nBuffer tables loaded successfully.")
