"""
Validate toàn vẹn dữ liệu: so sánh local CSV vs PostgreSQL
- Row count khớp không
- Dữ liệu có bị lệch cột/dòng không
- Sample rows đầu/cuối khớp không
"""
import io
import pandas as pd
import psycopg2
import boto3
from botocore.client import Config

S3 = dict(
    endpoint_url="http://localhost:9000",
    aws_access_key_id="minioadmin",
    aws_secret_access_key="minioadmin",
    config=Config(signature_version="s3v4"),
)
BUCKET = "raw-data"
DB = "host=localhost port=5432 dbname=postgres user=postgres password=postgres"

PASS = "[PASS]"
FAIL = "[FAIL]"

def read_csv(s3, key):
    obj = s3.get_object(Bucket=BUCKET, Key=key)
    return pd.read_csv(io.BytesIO(obj["Body"].read()))

def section(t): print(f"\n{'='*60}\n  {t}\n{'='*60}")

issues = []

def check(ok, msg):
    status = PASS if ok else FAIL
    print(f"  {status} {msg}")
    if not ok:
        issues.append(msg)

if __name__ == "__main__":
    s3   = boto3.client("s3", **S3)
    conn = psycopg2.connect(DB)
    cur  = conn.cursor()

    # ── 1. ROW COUNT ──────────────────────────────────────────────
    section("1. ROW COUNT: CSV vs PostgreSQL")

    expected = {
        "dim_geography":         5,
        "dim_solar_site":       42,
        "dim_date":           2312,
        "dim_time":             96,
        "dim_weather_type":     22,
    }
    for table, exp in expected.items():
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        actual = cur.fetchone()[0]
        check(actual == exp, f"{table}: CSV={exp:,} | DB={actual:,}")

    # fact tables — compare vs CSV row count
    cur.execute("SELECT COUNT(*) FROM fact_solar_energy_gen")
    gen_db = cur.fetchone()[0]
    gen_csv = read_csv(s3, "Solar_Energy_Generation.csv")
    check(gen_db > 0, f"fact_solar_energy_gen loaded: {gen_db:,} rows (CSV has {len(gen_csv):,})")

    cur.execute("SELECT COUNT(*) FROM fact_weather")
    wthr_db = cur.fetchone()[0]
    wthr_csv = read_csv(s3, "open_meteo_weather_raw_2020_2022.csv")
    check(wthr_db > 0, f"fact_weather loaded: {wthr_db:,} rows (CSV has {len(wthr_csv):,})")

    # ── 2. COLUMN INTEGRITY — dim_geography ──────────────────────
    section("2. COLUMN INTEGRITY: dim_geography vs campus_meta.csv")
    campus = read_csv(s3, "campus_meta.csv")
    cur.execute("SELECT geo_id, location_name, capacity FROM dim_geography ORDER BY geo_id")
    db_geo = cur.fetchall()
    for db_row in db_geo:
        geo_id, loc_name, cap_db = db_row
        csv_row = campus[campus["id"] == geo_id]
        if csv_row.empty:
            check(False, f"geo_id={geo_id} không tồn tại trong CSV")
        else:
            check(csv_row.iloc[0]["name"] == loc_name,
                  f"geo_id={geo_id} name: CSV='{csv_row.iloc[0]['name']}' | DB='{loc_name}'")
            cap_csv = csv_row.iloc[0]["capacity"]
            if pd.isna(cap_csv):
                check(cap_db is None, f"geo_id={geo_id} capacity: CSV=NULL | DB={cap_db}")
            else:
                check(int(cap_csv) == int(cap_db), f"geo_id={geo_id} capacity: CSV={cap_csv} | DB={cap_db}")

    # ── 3. COLUMN INTEGRITY — dim_solar_site ─────────────────────
    section("3. COLUMN INTEGRITY: dim_solar_site vs Solar_Site_Details.csv")
    sites = read_csv(s3, "Solar_Site_Details.csv")
    cur.execute("SELECT site_id, capacity_kw FROM dim_solar_site ORDER BY site_id LIMIT 5")
    for site_id, cap_db in cur.fetchall():
        csv_row = sites[sites["SiteKey"] == site_id]
        if csv_row.empty:
            check(False, f"site_id={site_id} không tồn tại trong CSV")
        else:
            cap_csv = csv_row.iloc[0]["kWp"]
            if pd.isna(cap_csv):
                check(cap_db is None, f"site_id={site_id} capacity_kw: CSV=NULL | DB={cap_db}")
            else:
                check(abs(float(cap_csv) - float(cap_db)) < 0.01 if cap_db else False,
                      f"site_id={site_id} capacity_kw: CSV={cap_csv} | DB={cap_db}")

    # ── 3.5. COLUMN INTEGRITY — dim_date ─────────────────────────
    section("3.5. COLUMN INTEGRITY: dim_date vs calender.csv")
    cal = read_csv(s3, "calender.csv")
    cur.execute("SELECT date_id, full_date::text, is_holiday, is_semester, is_exam FROM dim_date ORDER BY date_id LIMIT 10")
    for date_id, date_str, holiday_db, semester_db, exam_db in cur.fetchall():
        csv_row = cal[cal["date"] == date_str]
        if csv_row.empty:
            check(False, f"date={date_str} không tồn tại trong CSV")
        else:
            r = csv_row.iloc[0]
            h_csv = int(r["is_holiday"]) if not pd.isna(r["is_holiday"]) else None
            s_csv = int(r["is_semester"]) if not pd.isna(r["is_semester"]) else None
            e_csv = int(r["is_exam"]) if not pd.isna(r["is_exam"]) else None
            
            check(h_csv == holiday_db, f"date={date_str} is_holiday: CSV={h_csv} | DB={holiday_db}")
            check(s_csv == semester_db, f"date={date_str} is_semester: CSV={s_csv} | DB={semester_db}")
            check(e_csv == exam_db, f"date={date_str} is_exam: CSV={e_csv} | DB={exam_db}")

    # ── 4. ROW INTEGRITY — fact_solar_energy_gen ─────────────────
    section("4. ROW INTEGRITY: fact_solar_energy_gen (sample 5 rows)")
    sample = gen_csv.dropna(subset=["SolarGeneration"]).head(5)
    for _, row in sample.iterrows():
        ts   = pd.to_datetime(row["Timestamp"])
        date = str(ts.date())
        time = ts.strftime("%H:%M")
        site = int(row["SiteKey"])
        val_csv = round(float(row["SolarGeneration"]), 4)

        cur.execute("""
            SELECT f.energy_generated_kwh
            FROM fact_solar_energy_gen f
            JOIN dim_date d ON f.date_id = d.date_id
            JOIN dim_time t ON f.time_id = t.time_id
            WHERE d.full_date = %s AND t.time_string = %s AND f.site_id = %s
        """, (date, time, site))
        result = cur.fetchone()
        if result is None:
            check(False, f"SiteKey={site} {date} {time} — NOT FOUND in DB")
        else:
            val_db = round(float(result[0]), 4)
            check(val_csv == val_db,
                  f"SiteKey={site} {date} {time}: CSV={val_csv} | DB={val_db}")

    # ── 5. ROW INTEGRITY — fact_weather (filter đúng SiteKey) ────
    section("5. ROW INTEGRITY: fact_weather (sample 5 rows)")

    # Build SiteKey → geo_id lookup
    cur.execute("SELECT s.site_id, g.geo_id FROM dim_solar_site s JOIN dim_geography g ON s.campus_name = g.location_name")
    site_geo = {r[0]: r[1] for r in cur.fetchall()}

    sample_w = wthr_csv.head(5)
    for _, row in sample_w.iterrows():
        ts       = pd.to_datetime(row["timestamp"])
        date     = str(ts.date())
        time     = ts.strftime("%H:%M")
        site_key = int(row["SiteKey"])
        geo_id   = site_geo.get(site_key)
        temp_csv = round(float(row["temperature_2m"]), 2)

        cur.execute("""
            SELECT f.temperature_c
            FROM fact_weather f
            JOIN dim_date d ON f.date_id = d.date_id
            JOIN dim_time t ON f.time_id = t.time_id
            WHERE d.full_date = %s AND t.time_string = %s AND f.geo_id = %s
            LIMIT 1
        """, (date, time, geo_id))
        result = cur.fetchone()
        if result is None:
            check(False, f"SiteKey={site_key} {date} {time} — NOT FOUND in DB")
        else:
            temp_db = round(float(result[0]), 2)
            check(temp_csv == temp_db,
                  f"SiteKey={site_key} {date} {time} temp: CSV={temp_csv} | DB={temp_db}")

    # ── SUMMARY ───────────────────────────────────────────────────
    section("SUMMARY")
    if not issues:
        print(f"  {PASS} Dữ liệu toàn vẹn — không bị lệch dòng/cột!\n")
    else:
        print(f"  {FAIL} Có {len(issues)} vấn đề:")
        for i in issues:
            print(f"    - {i}")
        print()

    cur.close()
    conn.close()
