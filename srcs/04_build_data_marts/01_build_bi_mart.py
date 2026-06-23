import logging
from pathlib import Path
from sqlalchemy import text
import yaml

log = logging.getLogger("pipeline.bi_mart")

_REPO_ROOT = Path(__file__).resolve().parents[2]
_BI_PARAMS_CONFIG = _REPO_ROOT / "config" / "04_machine_learning" / "01_bi_mart_params.yaml"

with _BI_PARAMS_CONFIG.open(encoding="utf-8") as _f:
    _bi_cfg = yaml.safe_load(_f)

# Business params từ config
_FIT_RATE = _bi_cfg["financial"]["fit_rate_vnd_per_kwh"]
_CO2_FACTOR = _bi_cfg["environmental"]["co2_emission_factor_kg_per_kwh"]
_TREES_FACTOR = _bi_cfg["environmental"]["co2_per_tree_kg"]
_NOMINAL_PR = _bi_cfg["performance"]["nominal_pr"]
_TEMP_COEFF = _bi_cfg["performance"]["temp_coefficient_per_deg"]
_NOCT_FACTOR = _bi_cfg["performance"]["noct_radiation_factor"]
_MIN_RADIATION = _bi_cfg["performance"]["min_radiation_threshold_wm2"]
_SOURCE_SCHEMA = _bi_cfg["database"]["source_schema"]
_TARGET_SCHEMA = _bi_cfg["database"]["target_schema"]

def build_bi_mart(engine):
    sql_script = f"""
    CREATE SCHEMA IF NOT EXISTS {_TARGET_SCHEMA};

    DROP VIEW IF EXISTS {_TARGET_SCHEMA}.dim_solar_site CASCADE;
    DROP VIEW IF EXISTS {_TARGET_SCHEMA}.dim_geography CASCADE;
    DROP VIEW IF EXISTS {_TARGET_SCHEMA}.dim_date CASCADE;
    DROP VIEW IF EXISTS {_TARGET_SCHEMA}.dim_weather_type CASCADE;

    DROP VIEW IF EXISTS {_TARGET_SCHEMA}.dim_solar_site CASCADE;
    DROP VIEW IF EXISTS {_TARGET_SCHEMA}.dim_geography CASCADE;
    DROP VIEW IF EXISTS {_TARGET_SCHEMA}.dim_date CASCADE;
    DROP VIEW IF EXISTS {_TARGET_SCHEMA}.dim_weather_type CASCADE;

    DROP TABLE IF EXISTS {_TARGET_SCHEMA}.fact_solar_performance_hourly CASCADE;

    CREATE OR REPLACE VIEW {_TARGET_SCHEMA}.dim_solar_site AS SELECT * FROM {_SOURCE_SCHEMA}.dim_solar_site;
    CREATE OR REPLACE VIEW {_TARGET_SCHEMA}.dim_geography AS SELECT * FROM {_SOURCE_SCHEMA}.dim_geography;
    CREATE OR REPLACE VIEW {_TARGET_SCHEMA}.dim_date AS SELECT * FROM {_SOURCE_SCHEMA}.dim_date;
    CREATE OR REPLACE VIEW {_TARGET_SCHEMA}.dim_weather_type AS SELECT * FROM {_SOURCE_SCHEMA}.dim_weather_type;

    CREATE TABLE {_TARGET_SCHEMA}.fact_solar_performance_hourly AS
    WITH Clean_Hourly_Gen AS (
        SELECT 
            f.site_id, f.geo_id, f.date_id, t.hour AS hourly_bucket,
            SUM(f.energy_generated_kwh) AS total_energy
        FROM {_SOURCE_SCHEMA}.fact_solar_energy_gen f
        JOIN {_SOURCE_SCHEMA}.dim_time t ON f.time_id = t.time_id
        -- WHERE COALESCE(f.rolling_outlier_flag, false) = false
        GROUP BY f.site_id, f.geo_id, f.date_id, t.hour
    )
    SELECT 
        gen.site_id, gen.geo_id, gen.date_id, gen.hourly_bucket, gen.total_energy,
        w.weather_type_id,
        w.shortwave_radiation, w.temperature_c, w.cloud_cover_total, w.precipitation_mm
    FROM Clean_Hourly_Gen gen
    LEFT JOIN (
        SELECT * FROM (
            SELECT fw.*, dw.hour as weather_hour,
            ROW_NUMBER() OVER(PARTITION BY fw.geo_id, fw.date_id, dw.hour ORDER BY fw.weather_id) as rn
            FROM {_SOURCE_SCHEMA}.fact_weather fw
            JOIN {_SOURCE_SCHEMA}.dim_time dw ON fw.time_id = dw.time_id
        ) sub WHERE rn = 1
    ) w 
    ON gen.geo_id = w.geo_id AND gen.date_id = w.date_id AND gen.hourly_bucket = w.weather_hour;

    ALTER TABLE {_TARGET_SCHEMA}.fact_solar_performance_hourly ADD CONSTRAINT fk_bi_fact_site FOREIGN KEY (site_id) REFERENCES {_SOURCE_SCHEMA}.dim_solar_site(site_id);
    ALTER TABLE {_TARGET_SCHEMA}.fact_solar_performance_hourly ADD CONSTRAINT fk_bi_fact_geo FOREIGN KEY (geo_id) REFERENCES {_SOURCE_SCHEMA}.dim_geography(geo_id);
    ALTER TABLE {_TARGET_SCHEMA}.fact_solar_performance_hourly ADD CONSTRAINT fk_bi_fact_date FOREIGN KEY (date_id) REFERENCES {_SOURCE_SCHEMA}.dim_date(date_id);
    ALTER TABLE {_TARGET_SCHEMA}.fact_solar_performance_hourly ADD CONSTRAINT fk_bi_fact_weather FOREIGN KEY (weather_type_id) REFERENCES {_SOURCE_SCHEMA}.dim_weather_type(weather_type_id);

    CREATE INDEX idx_fact_fk_site_date ON {_TARGET_SCHEMA}.fact_solar_performance_hourly (site_id, date_id);
    CREATE INDEX idx_fact_fk_geo_date ON {_TARGET_SCHEMA}.fact_solar_performance_hourly (geo_id, date_id);
    """
    log.info(">> Bắt đầu build BI Mart...")
    log.info(f"Đang đắp dữ liệu vào host: {engine.url.host}, database: {engine.url.database}")
    with engine.begin() as conn:
        statements = [s.strip() for s in sql_script.split(';') if s.strip()]
        log.info("BI Mart có %s SQL statements cần thực thi", len(statements))
        for index, statement in enumerate(statements, start=1):
            operation = " ".join(statement.split()[:5])
            log.info("[%s/%s] SQL | %s", index, len(statements), operation)
            conn.execute(text(statement))
        fact_rows = conn.execute(
            text(
                f"SELECT COUNT(*) "
                f"FROM {_TARGET_SCHEMA}.fact_solar_performance_hourly"
            )
        ).scalar_one()
        log.info(
            "BI Mart audit | fact_solar_performance_hourly rows=%s",
            f"{fact_rows:,}",
        )
    log.info(">> BI Mart hoàn tất!\n")


if __name__ == "__main__":
    log.info("==== KHỞI ĐỘNG PIPELINE (BI) ====\n")
    try:
        db_engine = get_engine()
        build_bi_mart(db_engine)
        log.info("==== PIPELINE THÀNH CÔNG ====")
    except Exception as error:
        log.error(f"PIPELINE THẤT BẠI: {error}")
