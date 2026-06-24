#!/usr/bin/env python3
"""
Run step 03 Load: Extract from Buffers and load to Data Warehouse.
Refactored to Clean Code: Each table load is separated into its own clearly documented function.
"""

import argparse
from pathlib import Path
import sys
import importlib.util
import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[3]
CONFIG_FILE = (
    PROJECT_ROOT
    / "config"
    / "03_load"
    / "03_load_buffers_to_datawarehouse.yaml"
)

with CONFIG_FILE.open(encoding="utf-8") as _file:
    _config = yaml.safe_load(_file)

SOURCE_SCHEMA = _config["database"]["source_schema"]
TARGET_SCHEMA = _config["database"]["target_schema"]




def truncate_all_datawarehouse_tables(cur):
    """BƯỚC 0: Tẩy trắng toàn bộ dữ liệu cũ trên schema Data Warehouse."""
    print("  -> Truncating old Data Warehouse tables...")
    cur.execute(f"TRUNCATE TABLE {TARGET_SCHEMA}.dim_date CASCADE;")
    cur.execute(f"TRUNCATE TABLE {TARGET_SCHEMA}.dim_geography CASCADE;")
    cur.execute(f"TRUNCATE TABLE {TARGET_SCHEMA}.dim_solar_site CASCADE;")
    cur.execute(f"TRUNCATE TABLE {TARGET_SCHEMA}.dim_time CASCADE;")
    cur.execute(f"TRUNCATE TABLE {TARGET_SCHEMA}.dim_weather_type CASCADE;")
    cur.execute(f"TRUNCATE TABLE {TARGET_SCHEMA}.fact_solar_energy_gen CASCADE;")
    cur.execute(f"TRUNCATE TABLE {TARGET_SCHEMA}.fact_weather CASCADE;")


def load_dim_solar_site(cur):
    """BƯỚC 1.1: Nạp bảng datawarehouse.dim_solar_site từ staging."""
    print("  -> Loading dim_solar_site...")
    sql = f"""
    INSERT INTO {TARGET_SCHEMA}.dim_solar_site(
          site_id, campus_name, capacity_kw, number_of_panels, 
          panel, inverter, optimizers, metric
    )
    SELECT 
          CAST(sitekey as INT), campus_name, capacity_kw, number_of_panels, 
          panel, inverter, optimizers, metric
    FROM {SOURCE_SCHEMA}.dim_solar_site
    ON CONFLICT (site_id) DO NOTHING;
    """
    cur.execute(sql)


def load_dim_weather_type(cur):
    """BƯỚC 1.2: Nạp bảng datawarehouse.dim_weather_type từ staging."""
    print("  -> Loading dim_weather_type...")
    sql = f"""
    INSERT INTO {TARGET_SCHEMA}.dim_weather_type(
          weather_type_id, weather_code, is_day, weather_condition, description
    )
    SELECT
          ROW_NUMBER() OVER(ORDER BY weather_code, is_day) + (SELECT COALESCE(MAX(weather_type_id), 0) FROM {TARGET_SCHEMA}.dim_weather_type),
          weather_code, is_day, weather_condition, description
    FROM {SOURCE_SCHEMA}.dim_weather_type
    ON CONFLICT (weather_type_id) DO NOTHING;
    """
    cur.execute(sql)


def load_dim_geography(cur):
    """BƯỚC 1.3: Nạp bảng datawarehouse.dim_geography từ staging."""
    print("  -> Loading dim_geography...")
    # Giữ đúng 4 cột nghiệp vụ của staging/Supabase.
    # Cột capacity cũ trong schema local không thuộc luồng load này.
    sql = f"""
    INSERT INTO {TARGET_SCHEMA}.dim_geography(
          geo_id, latitude, longitude, location_name
    )
    SELECT
          CAST(sitekey as int), latitude, longitude, location_name
    FROM {SOURCE_SCHEMA}.dim_geography
    ON CONFLICT (geo_id) DO NOTHING;
    """
    cur.execute(sql)


def load_dim_date(cur):
    """BƯỚC 1.4: Nạp bảng datawarehouse.dim_date từ staging."""
    print("  -> Loading dim_date...")
    sql = f"""
    INSERT INTO {TARGET_SCHEMA}.dim_date (
          date_id, full_date, day, month, year, is_holiday, is_semester, is_exam
    )
    SELECT 
          CAST(TO_CHAR(full_date,'YYYYMMDD') AS INT) AS date_id,
          full_date, day, month, year, 
          CAST(CAST(NULLIF(is_holiday, '') AS FLOAT) AS INT), 
          CAST(CAST(NULLIF(is_semester, '') AS FLOAT) AS INT), 
          CAST(CAST(NULLIF(is_exam, '') AS FLOAT) AS INT)
    FROM {SOURCE_SCHEMA}.dim_date
    WHERE full_date IS NOT NULL
    ON CONFLICT (date_id) DO NOTHING;
    """
    cur.execute(sql)


def load_dim_time(cur):
    """BƯỚC 1.5: Nạp bảng datawarehouse.dim_time từ staging."""
    print("  -> Loading dim_time...")
    sql = f"""
    INSERT INTO {TARGET_SCHEMA}.dim_time(
          time_id, time_string, hour, minute
    )
    SELECT DISTINCT
          (CAST(hour AS INT) * 100) + CAST(minute AS INT) AS time_id,
          time_string, CAST(hour AS INT) AS hour, CAST(minute AS INT) AS minute
    FROM {SOURCE_SCHEMA}.dim_time
    WHERE hour IS NOT NULL AND minute IS NOT NULL
    ON CONFLICT (time_id) DO NOTHING;
    """
    cur.execute(sql)


def load_fact_solar_energy_gen(cur):
    """BƯỚC 2.1: Nạp bảng datawarehouse.fact_solar_energy_gen từ staging."""
    print("  -> Loading fact_solar_energy_gen in monthly batches...")

    cur.execute(f"""
        SELECT DISTINCT 
            date_trunc('month', timestamp)::timestamp AS month_start,
            (date_trunc('month', timestamp) + interval '1 month')::timestamp AS month_end
        FROM {SOURCE_SCHEMA}.fact_solar_energy_gen
        ORDER BY month_start
    """)
    months = cur.fetchall()

    for idx, (m_start, m_end) in enumerate(months, 1):
        print(f"    - Batch {idx}/{len(months)}: {m_start.strftime('%Y-%m')}")
        sql_batch = f"""
        INSERT INTO {TARGET_SCHEMA}.fact_solar_energy_gen (
            gen_id, site_id, geo_id, date_id, time_id, energy_generated_kwh, rolling_outlier_flag
        )
        SELECT
            (SELECT COALESCE(SUM(c), 0) FROM (
                SELECT COUNT(*) as c FROM {SOURCE_SCHEMA}.fact_solar_energy_gen s2
                WHERE s2.timestamp < '{m_start}'::timestamp
            ) t) + row_number() OVER (ORDER BY s.timestamp, s.sitekey::int) AS gen_id,
            s.sitekey::int AS site_id,
            s.sitekey::int AS geo_id,
            dd.date_id,
            dt.time_id,
            s.energy_generated_kwh,
            COALESCE(s.rolling_outlier_flag, false) AS rolling_outlier_flag
        FROM {SOURCE_SCHEMA}.fact_solar_energy_gen s
        JOIN {TARGET_SCHEMA}.dim_date dd ON dd.full_date = s.timestamp::date
        JOIN {TARGET_SCHEMA}.dim_time dt ON dt.time_string = to_char(s.timestamp, 'HH24:MI')
        JOIN {TARGET_SCHEMA}.dim_solar_site ds ON ds.site_id = s.sitekey::int
        JOIN {TARGET_SCHEMA}.dim_geography dg ON dg.geo_id = s.sitekey::int
        WHERE s.timestamp >= '{m_start}'::timestamp AND s.timestamp < '{m_end}'::timestamp;
        """
        cur.execute(sql_batch)

    cur.execute(f"""
    DO $$
    DECLARE
        v_source_count BIGINT;
        v_target_count BIGINT;
    BEGIN
        SELECT COUNT(*) INTO v_source_count FROM {SOURCE_SCHEMA}.fact_solar_energy_gen;
        SELECT COUNT(*) INTO v_target_count FROM {TARGET_SCHEMA}.fact_solar_energy_gen;
        
        IF v_source_count != v_target_count THEN
            RAISE EXCEPTION 'Validation Failed: Source=%s, Target=%s', v_source_count, v_target_count;
        END IF;
    END $$;
    """)


def load_fact_weather(cur):
    """BƯỚC 2.2: Nạp bảng datawarehouse.fact_weather từ staging."""
    print("  -> Loading fact_weather...")
    sql = f"""
    INSERT INTO {TARGET_SCHEMA}.fact_weather (
          weather_id, geo_id, date_id, time_id, weather_type_id, is_day,
          shortwave_radiation, temperature_c, cloud_cover_total, cloud_cover_low,
          cloud_cover_mid, cloud_cover_high, diffuse_solar_radiation, 
          direct_normal_irradiance, wind_speed, precipitation_mm, sunshine_duration
    )
    SELECT
          ROW_NUMBER() OVER(ORDER BY f.timestamp) + m.max_id AS weather_id,
          CAST(f.SiteKey AS INT) AS geo_id,
          CAST(TO_CHAR(f.timestamp, 'YYYYMMDD') AS INT) AS date_id,
          (EXTRACT(HOUR FROM f.timestamp) * 100 + EXTRACT(MINUTE FROM f.timestamp))::INT AS time_id,
          w.weather_type_id, f.is_day, f.shortwave_radiation, f.temperature_c,
          f.cloud_cover_total, f.cloud_cover_low, f.cloud_cover_mid, f.cloud_cover_high,
          f.diffuse_solar_radiation, f.direct_normal_irradiance, f.wind_speed,
          f.precipitation_mm, f.sunshine_duration
    FROM {SOURCE_SCHEMA}.fact_weather f
    INNER JOIN {TARGET_SCHEMA}.dim_weather_type w
        ON f.weather_code = w.weather_code AND f.is_day = w.is_day
    CROSS JOIN (
        SELECT COALESCE(MAX(weather_id), 0) AS max_id FROM {TARGET_SCHEMA}.fact_weather
    ) m
    ON CONFLICT (weather_id) DO NOTHING;
    """
    cur.execute(sql)
