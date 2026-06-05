"""
Load dimension tables từ MinIO vào PostgreSQL (raw layer)
Thứ tự: dim_geography → dim_solar_site → dim_date → dim_time → dim_weather_type
"""
import io
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import boto3
from botocore.client import Config

# MinIO config — đổi sang Supabase Storage khi lên production
S3 = dict(
    endpoint_url="http://localhost:9000",
    aws_access_key_id="minioadmin",
    aws_secret_access_key="minioadmin",
    config=Config(signature_version="s3v4"),
)
BUCKET = "raw-data"
DB = "host=localhost port=5432 dbname=postgres user=postgres password=postgres"

WMO = {
    0:("Clear sky","No clouds"), 1:("Mainly clear","Few clouds"),
    2:("Partly cloudy","Scattered clouds"), 3:("Overcast","Full cloud cover"),
    45:("Fog","Depositing rime fog"), 48:("Rime fog","Freezing fog"),
    51:("Light drizzle","Light intensity"), 53:("Moderate drizzle","Moderate intensity"),
    55:("Dense drizzle","Dense intensity"), 61:("Slight rain","Slight intensity"),
    63:("Moderate rain","Moderate intensity"), 65:("Heavy rain","Heavy intensity"),
    71:("Slight snow","Slight intensity"), 73:("Moderate snow","Moderate intensity"),
    75:("Heavy snow","Heavy intensity"), 77:("Snow grains","Snow grains"),
    80:("Slight showers","Slight"), 81:("Moderate showers","Moderate"),
    82:("Violent showers","Violent"), 85:("Slight snow showers","Slight"),
    86:("Heavy snow showers","Heavy"), 95:("Thunderstorm","Slight or moderate"),
    96:("Thunderstorm+hail","Slight hail"), 99:("Thunderstorm+hail","Heavy hail"),
}

def read_csv(s3, key):
    obj = s3.get_object(Bucket=BUCKET, Key=key)
    return pd.read_csv(io.BytesIO(obj["Body"].read()))

def log(msg): print(f"  [OK] {msg}")

if __name__ == "__main__":
    s3   = boto3.client("s3", **S3)
    conn = psycopg2.connect(DB)
    cur  = conn.cursor()

    try:
        cur.execute("""
            TRUNCATE dim_weather_type, dim_time, dim_date,
                     dim_solar_site, dim_geography RESTART IDENTITY CASCADE
        """)

        # 1. dim_geography
        campus = read_csv(s3, "campus_meta.csv")
        sites  = read_csv(s3, "Solar_Site_Details.csv")
        latlon = sites.groupby("CampusKey")[["lat","Lon"]].mean().reset_index()
        geo    = campus.merge(latlon, left_on="id", right_on="CampusKey", how="left")
        execute_values(cur,
            "INSERT INTO dim_geography (geo_id,latitude,longitude,location_name,capacity) VALUES %s",
            [(int(r["id"]), r["lat"], r["Lon"], r["name"], None if pd.isna(r["capacity"]) else int(r["capacity"])) for _, r in geo.iterrows()])
        log(f"dim_geography: {len(geo)} rows")

        # 2. dim_solar_site
        df = sites.merge(campus[["id","name"]], left_on="CampusKey", right_on="id", how="left")
        df = df.rename(columns={"Number of panels": "num_panels"})
        rows = []
        for _, r in df.iterrows():
            rows.append((
                int(r["SiteKey"]), r["name"],
                None if pd.isna(r["kWp"]) else float(r["kWp"]),
                None if pd.isna(r["num_panels"]) else int(r["num_panels"]),
                None if pd.isna(r["Panel"]) else r["Panel"],
                None if pd.isna(r["Inverter"]) else r["Inverter"],
                None if pd.isna(r["Optimizers"]) else r["Optimizers"],
                None if pd.isna(r["Metric"]) else r["Metric"],
            ))
        execute_values(cur,
            "INSERT INTO dim_solar_site (site_id,campus_name,capacity_kw,Number_of_panels,Panel,Inverter,Optimizers,Metric) VALUES %s",
            rows)
        log(f"dim_solar_site: {len(rows)} rows")

        # 3. dim_date
        cal = read_csv(s3, "calender.csv")
        cal["dt"] = pd.to_datetime(cal["date"])
        rows = []
        for i, r in enumerate(cal.itertuples()):
            is_holiday = int(r.is_holiday) if not pd.isna(r.is_holiday) else None
            is_semester = int(r.is_semester) if not pd.isna(r.is_semester) else None
            is_exam = int(r.is_exam) if not pd.isna(r.is_exam) else None
            rows.append((
                i+1, 
                str(r.dt.date()), 
                r.dt.day, 
                r.dt.month, 
                r.dt.year,
                is_holiday,
                is_semester,
                is_exam
            ))
        execute_values(cur,
            "INSERT INTO dim_date (date_id,full_date,day,month,year,is_holiday,is_semester,is_exam) VALUES %s", rows)
        log(f"dim_date: {len(rows)} rows")

        # 4. dim_time — unique HH:MM từ cả 2 file
        gen     = read_csv(s3, "Solar_Energy_Generation.csv")
        weather = read_csv(s3, "open_meteo_weather_raw_2020_2022.csv")
        times   = sorted(
            set(pd.to_datetime(gen["Timestamp"]).dt.strftime("%H:%M")) |
            set(pd.to_datetime(weather["timestamp"]).dt.strftime("%H:%M"))
        )
        rows = [(i+1, t, int(t[:2]), int(t[3:])) for i, t in enumerate(times)]
        execute_values(cur,
            "INSERT INTO dim_time (time_id,time_string,hour,minute) VALUES %s", rows)
        log(f"dim_time: {len(rows)} slots")

        # 5. dim_weather_type
        combos = weather[["weather_code","is_day"]].drop_duplicates().sort_values(["weather_code","is_day"])
        rows = []
        for i, r in enumerate(combos.itertuples()):
            cond, desc = WMO.get(int(r.weather_code), ("Unknown", f"Code {r.weather_code}"))
            rows.append((i+1, int(r.weather_code), int(r.is_day),
                         f"{cond} ({'Day' if r.is_day else 'Night'})", desc))
        execute_values(cur,
            "INSERT INTO dim_weather_type (weather_type_id,weather_code,is_day,weather_condition,description) VALUES %s",
            rows)
        log(f"dim_weather_type: {len(rows)} rows")

        conn.commit()
        print("\n  All dimension tables loaded!\n")

    except Exception as e:
        conn.rollback()
        print(f"\n  Error: {e}")
        raise
    finally:
        cur.close()
        conn.close()
