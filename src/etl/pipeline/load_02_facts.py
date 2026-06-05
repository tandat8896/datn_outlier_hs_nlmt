"""
Load fact tables từ MinIO vào PostgreSQL (chunk 50K rows)
fact_solar_energy_gen → fact_weather
"""
import io
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import boto3
from botocore.client import Config

S3 = dict(
    endpoint_url="http://localhost:9000",
    aws_access_key_id="minioadmin",
    aws_secret_access_key="minioadmin",
    config=Config(signature_version="s3v4"),
)
BUCKET = "raw-data"
DB     = "host=localhost port=5432 dbname=postgres user=postgres password=postgres"
CHUNK  = 50_000

def get_lookups(cur):
    cur.execute("SELECT full_date::text, date_id FROM dim_date")
    date_map = {r[0]: r[1] for r in cur.fetchall()}

    cur.execute("SELECT time_string, time_id FROM dim_time")
    time_map = {r[0]: r[1] for r in cur.fetchall()}

    cur.execute("SELECT site_id, geo_id FROM dim_solar_site s JOIN dim_geography g ON s.campus_name = g.location_name")
    site_geo_map = {r[0]: r[1] for r in cur.fetchall()}

    cur.execute("SELECT weather_code, is_day, weather_type_id FROM dim_weather_type")
    weather_type_map = {(r[0], r[1]): r[2] for r in cur.fetchall()}

    return date_map, time_map, site_geo_map, weather_type_map

if __name__ == "__main__":
    s3   = boto3.client("s3", **S3)
    conn = psycopg2.connect(DB)
    cur  = conn.cursor()

    try:
        cur.execute("TRUNCATE fact_solar_energy_gen, fact_weather RESTART IDENTITY")
        date_map, time_map, site_geo_map, weather_type_map = get_lookups(cur)

        # ── fact_solar_energy_gen ──────────────────────────────────
        print("\n  Loading fact_solar_energy_gen...")
        obj  = s3.get_object(Bucket=BUCKET, Key="Solar_Energy_Generation.csv")
        gen_id = 0
        total  = 0
        for chunk in pd.read_csv(io.BytesIO(obj["Body"].read()), chunksize=CHUNK):
            chunk["dt"] = pd.to_datetime(chunk["Timestamp"])
            rows = []
            for r in chunk.itertuples():
                date_key = str(r.dt.date())
                time_key = r.dt.strftime("%H:%M")
                date_id  = date_map.get(date_key)
                time_id  = time_map.get(time_key)
                geo_id   = site_geo_map.get(int(r.SiteKey))
                if not all([date_id, time_id, geo_id]):
                    continue
                gen_id += 1
                rows.append((gen_id, int(r.SiteKey), geo_id, date_id, time_id,
                             None if pd.isna(r.SolarGeneration) else float(r.SolarGeneration)))
            execute_values(cur,
                "INSERT INTO fact_solar_energy_gen (gen_id,site_id,geo_id,date_id,time_id,energy_generated_kwh) VALUES %s",
                rows)
            total += len(rows)
            print(f"    ... {total:,} rows", end="\r")
        print(f"  [OK] fact_solar_energy_gen: {total:,} rows")

        # ── fact_weather ───────────────────────────────────────────
        print("\n  Loading fact_weather...")
        obj      = s3.get_object(Bucket=BUCKET, Key="open_meteo_weather_raw_2020_2022.csv")
        wthr_id  = 0
        total    = 0

        for chunk in pd.read_csv(io.BytesIO(obj["Body"].read()), chunksize=CHUNK):
            chunk["dt"] = pd.to_datetime(chunk["timestamp"])
            rows = []
            for r in chunk.itertuples():
                date_key = str(r.dt.date())
                time_key = r.dt.strftime("%H:%M")
                date_id  = date_map.get(date_key)
                time_id  = time_map.get(time_key)
                geo_id   = site_geo_map.get(int(r.SiteKey))
                wt_id    = weather_type_map.get((int(r.weather_code), int(r.is_day)))
                if not all([date_id, time_id, wt_id]):
                    continue
                wthr_id += 1
                rows.append((wthr_id, geo_id, date_id, time_id, wt_id,
                             int(r.is_day), float(r.shortwave_radiation),
                             float(r.temperature_2m), float(r.cloud_cover),
                             float(r.cloud_cover_low), float(r.cloud_cover_mid),
                             float(r.cloud_cover_high), float(r.diffuse_radiation),
                             float(r.direct_radiation), float(r.wind_speed_10m),
                             float(r.precipitation), float(r.sunshine_duration)))
            execute_values(cur, """
                INSERT INTO fact_weather
                (weather_id,geo_id,date_id,time_id,weather_type_id,is_day,
                 shortwave_radiation,temperature_c,cloud_cover_total,cloud_cover_low,
                 cloud_cover_mid,cloud_cover_high,Diffuse_Solar_Radiation,
                 Direct_Normal_Irradiance,wind_speed,precipitation_mm,Sunshine_Duration)
                VALUES %s""", rows)
            total += len(rows)
            print(f"    ... {total:,} rows", end="\r")
        print(f"  [OK] fact_weather: {total:,} rows")

        conn.commit()
        print("\n  All fact tables loaded!\n")

    except Exception as e:
        conn.rollback()
        print(f"\n  Error: {e}")
        raise
    finally:
        cur.close()
        conn.close()
