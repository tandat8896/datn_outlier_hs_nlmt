import logging
from pathlib import Path
from sqlalchemy import text
import yaml

log = logging.getLogger("pipeline.ml_mart")

PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONFIG_FILE = (
    PROJECT_ROOT
    / "config"
    / "04_machine_learning"
    / "02_ml_mart.yaml"
)

with CONFIG_FILE.open(encoding="utf-8") as _file:
    _config = yaml.safe_load(_file)

SOURCE_SCHEMA = _config["database"]["source_schema"]
TARGET_SCHEMA = _config["database"]["target_schema"]

def build_ml_mart(engine):
    log.info(">> Bắt đầu build ML Mart...")

    with engine.begin() as conn:
        log.info("Tạo schema %s nếu chưa tồn tại", TARGET_SCHEMA)
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {TARGET_SCHEMA}"))

        log.info("Tạo bảng tạm %s.base_build", TARGET_SCHEMA)
        conn.execute(text(f"DROP TABLE IF EXISTS {TARGET_SCHEMA}.base_build"))
        conn.execute(
            text(
                f"""
                CREATE TABLE {TARGET_SCHEMA}.base_build AS
                SELECT
                    -- Audit keys: giữ trong mart để truy vết, không đưa thẳng vào X.
                    f.gen_id,
                    f.site_id,
                    f.geo_id,
                    f.date_id,
                    f.time_id,
                    d.full_date + make_time(t.hour, t.minute, 0) AS timestamp,
                    f.is_dst_repeat,

                    -- Calendar context.
                    d.full_date,
                    d.year,
                    d.month,
                    d.day,
                    EXTRACT(DOW FROM d.full_date)::smallint AS day_of_week,
                    t.hour,
                    t.minute,

                    -- Endogenous target source.
                    f.energy_generated_kwh,
                    COALESCE(f.rolling_outlier_flag, false)
                        AS rolling_outlier_flag,

                    -- Static site and geography context.
                    s.campus_name,
                    s.capacity_kw,
                    s.number_of_panels,
                    s.panel,
                    s.inverter,
                    s.optimizers,
                    s.metric AS site_metric,
                    geo.location_name,
                    geo.latitude,
                    geo.longitude,

                    -- Weather audit keys and observation time.
                    w.weather_id,
                    w.weather_type_id,
                    d.full_date
                        + make_time(w.weather_hour, w.weather_minute, 0)
                        AS weather_timestamp,

                    -- Raw exogenous weather variables.
                    w.is_day AS weather_is_day,
                    w.shortwave_radiation,
                    w.direct_normal_irradiance,
                    w.diffuse_solar_radiation,
                    w.temperature_c,
                    w.cloud_cover_total,
                    w.cloud_cover_low,
                    w.cloud_cover_mid,
                    w.cloud_cover_high,
                    w.wind_speed,
                    w.precipitation_mm,
                    w.sunshine_duration,

                    -- Human-readable weather context.
                    wt.weather_code,
                    wt.is_day AS weather_type_is_day,
                    wt.weather_condition,
                    wt.description AS weather_description
                FROM (
                    SELECT 
                        fact_solar_energy_gen.*,
                        ROW_NUMBER() OVER(PARTITION BY site_id, date_id, time_id ORDER BY gen_id) - 1 AS is_dst_repeat
                    FROM {SOURCE_SCHEMA}.fact_solar_energy_gen
                ) f
                JOIN {SOURCE_SCHEMA}.dim_time t
                  ON t.time_id = f.time_id
                JOIN {SOURCE_SCHEMA}.dim_date d
                  ON d.date_id = f.date_id
                LEFT JOIN {SOURCE_SCHEMA}.dim_solar_site s
                  ON s.site_id = f.site_id
                LEFT JOIN {SOURCE_SCHEMA}.dim_geography geo
                  ON geo.geo_id = f.geo_id
                LEFT JOIN (
                    SELECT * FROM (
                        SELECT fw.*, weather_time.hour AS weather_hour, weather_time.minute AS weather_minute,
                        ROW_NUMBER() OVER(PARTITION BY fw.geo_id, fw.date_id, weather_time.hour ORDER BY fw.weather_id) as rn
                        FROM {SOURCE_SCHEMA}.fact_weather fw
                        JOIN {SOURCE_SCHEMA}.dim_time weather_time
                          ON weather_time.time_id = fw.time_id
                    ) sub WHERE rn = 1
                ) w
                  ON w.geo_id = f.geo_id
                 AND w.date_id = f.date_id
                 AND w.weather_hour = t.hour
                LEFT JOIN {SOURCE_SCHEMA}.dim_weather_type wt
                  ON wt.weather_type_id = w.weather_type_id
                """
            )
        )

        source_rows = conn.execute(
            text(f"SELECT COUNT(*) FROM {SOURCE_SCHEMA}.fact_solar_energy_gen")
        ).scalar_one()
        mart_rows = conn.execute(
            text(f"SELECT COUNT(*) FROM {TARGET_SCHEMA}.base_build")
        ).scalar_one()
        duplicate_gen_ids = conn.execute(
            text(
                f"""
                SELECT COUNT(*) - COUNT(DISTINCT gen_id)
                FROM {TARGET_SCHEMA}.base_build
                """
            )
        ).scalar_one()
        duplicate_site_timestamps = conn.execute(
            text(
                f"""
                SELECT
                    COUNT(*)
                    - COUNT(DISTINCT (site_id, timestamp, is_dst_repeat))
                FROM {TARGET_SCHEMA}.base_build
                """
            )
        ).scalar_one()

        log.info(
            "Kiểm tra dữ liệu | source_rows=%s | mart_rows=%s | "
            "duplicate_gen_id=%s | duplicate_site_timestamp=%s",
            f"{source_rows:,}",
            f"{mart_rows:,}",
            f"{duplicate_gen_ids:,}",
            f"{duplicate_site_timestamps:,}",
        )

        if mart_rows != source_rows:
            raise RuntimeError(
                "ML Mart sai row count: "
                f"source={source_rows:,}, mart={mart_rows:,}"
            )
        if duplicate_gen_ids:
            raise RuntimeError(
                f"ML Mart có {duplicate_gen_ids:,} gen_id bị trùng"
            )
        if duplicate_site_timestamps:
            raise RuntimeError(
                "ML Mart có "
                f"{duplicate_site_timestamps:,} khóa "
                "(site_id, timestamp, is_dst_repeat) bị trùng"
            )

        log.info("Các kiểm tra tính toàn vẹn ML Mart đã pass")
        log.info("Đổi base_build thành bảng chính %s.base", TARGET_SCHEMA)
        conn.execute(text(f"DROP TABLE IF EXISTS {TARGET_SCHEMA}.base CASCADE"))
        conn.execute(
            text(f"ALTER TABLE {TARGET_SCHEMA}.base_build RENAME TO base")
        )
        conn.execute(
            text(
                f"""
                ALTER TABLE {TARGET_SCHEMA}.base
                ADD CONSTRAINT pk_ml_mart_base PRIMARY KEY (gen_id)
                """
            )
        )
        conn.execute(
            text(
                f"""
                CREATE UNIQUE INDEX ux_ml_base_site_timestamp
                ON {TARGET_SCHEMA}.base (site_id, timestamp, is_dst_repeat)
                """
            )
        )
        conn.execute(
            text(
                f"""
                CREATE INDEX ix_ml_base_timestamp
                ON {TARGET_SCHEMA}.base (timestamp)
                """
            )
        )
        conn.execute(
            text(
                f"""
                CREATE INDEX ix_ml_base_geo_timestamp
                ON {TARGET_SCHEMA}.base (geo_id, timestamp)
                """
            )
        )

        log.info("Đã tạo primary key và các index cho %s.base", TARGET_SCHEMA)
        log.info("Tạo view %s.v_model_input_1h", TARGET_SCHEMA)
        conn.execute(
            text(
                f"""
                CREATE OR REPLACE VIEW {TARGET_SCHEMA}.v_model_input_1h AS
                WITH target_windows AS (
                    SELECT
                        b.*,
                        LEAD(timestamp, 4) OVER (
                            PARTITION BY site_id
                            ORDER BY timestamp, is_dst_repeat
                        ) AS target_end_timestamp,
                        COUNT(energy_generated_kwh) OVER (
                            PARTITION BY site_id
                            ORDER BY timestamp, is_dst_repeat
                            ROWS BETWEEN 1 FOLLOWING AND 4 FOLLOWING
                        ) AS target_window_rows,
                        SUM(energy_generated_kwh) OVER (
                            PARTITION BY site_id
                            ORDER BY timestamp, is_dst_repeat
                            ROWS BETWEEN 1 FOLLOWING AND 4 FOLLOWING
                        ) AS target_energy_next_1h_kwh
                    FROM {TARGET_SCHEMA}.base b
                )
                SELECT
                    *,
                    SIN(2 * pi() * hour / 24.0) AS hour_sin,
                    COS(2 * pi() * hour / 24.0) AS hour_cos,
                    SIN(2 * pi() * month / 12.0) AS month_sin,
                    COS(2 * pi() * month / 12.0) AS month_cos
                FROM target_windows
                WHERE target_window_rows = 4
                  AND target_end_timestamp = timestamp + interval '1 hour'
                """
            )
        )

        missing_weather_rows = conn.execute(
            text(
                f"""
                SELECT COUNT(*)
                FROM {TARGET_SCHEMA}.base
                WHERE weather_id IS NULL
                """
            )
        ).scalar_one()

        log.info(
            ">> ML Mart audit: rows=%s, duplicate_gen_id=0, "
            "duplicate_site_timestamp=0, missing_weather=%s",
            f"{mart_rows:,}",
            f"{missing_weather_rows:,}",
        )

    log.info(">> ML Mart hoàn tất!\n")
