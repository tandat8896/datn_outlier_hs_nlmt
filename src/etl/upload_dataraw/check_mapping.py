import pandas as pd
import os

from pathlib import Path
DATA_DIR = str(Path(__file__).resolve().parents[3] / "data" / "raw")

files = sorted(os.listdir(DATA_DIR))
for fname in files:
    df_sample = pd.read_csv(os.path.join(DATA_DIR, fname), nrows=2)
    df_full = pd.read_csv(os.path.join(DATA_DIR, fname))
    print(f"\n{'='*60}")
    print(f"FILE: {fname}  ({len(df_full.columns)} cols | {len(df_full):,} rows)")
    print(f"{'='*60}")
    for col in df_sample.columns:
        print(f"  {col:<45} dtype: {str(df_sample[col].dtype):<10}  sample: {df_sample[col].iloc[0]}")

print("\n\n" + "="*60)
print("MAPPING ANALYSIS vs create_table.sql")
print("="*60)

mapping = {
    "Solar_Site_Details.csv": {
        "table": "dim_solar_site + dim_geography",
        "columns": {
            "CampusKey":        "dim_solar_site.campus_name (rename)",
            "SiteKey":          "dim_solar_site.site_id (rename)",
            "kWp":              "dim_solar_site.capacity_kw (rename)",
            "Number of panels": "dim_solar_site.Number_of_panels (rename)",
            "Panel":            "dim_solar_site.Panel",
            "Inverter":         "dim_solar_site.Inverter",
            "Optimizers":       "dim_solar_site.Optimizers",
            "Metric":           "dim_solar_site.Metric",
            "lat":              "dim_geography.latitude (rename)",
            "Lon":              "dim_geography.longitude (rename)",
        }
    },
    "campus_meta.csv": {
        "table": "dim_geography",
        "columns": {
            "id":       "dim_geography.geo_id (rename)",
            "name":     "dim_geography.location_name (rename)",
            "capacity": "dim_geography.capacity",
        }
    },
    "calender.csv": {
        "table": "dim_date",
        "columns": {
            "date":        "dim_date.full_date + derive day/month/year",
            "is_holiday":  "dim_date.is_holiday",
            "is_semester": "dim_date.is_semester",
            "is_exam":     "dim_date.is_exam",
        }
    },
    "open_meteo_weather_raw_2020_2022.csv": {
        "table": "fact_weather + dim_weather_type + dim_time + dim_geography",
        "columns": {
            "timestamp":          "derive date_id, time_id (split date & time)",
            "shortwave_radiation":"fact_weather.shortwave_radiation",
            "direct_radiation":   "fact_weather.Direct_Normal_Irradiance (rename)",
            "diffuse_radiation":  "fact_weather.Diffuse_Solar_Radiation (rename)",
            "temperature_2m":     "fact_weather.temperature_c (rename)",
            "weather_code":       "dim_weather_type.weather_code",
            "is_day":             "fact_weather.is_day + dim_weather_type.is_day",
            "cloud_cover":        "fact_weather.cloud_cover_total (rename)",
            "cloud_cover_low":    "fact_weather.cloud_cover_low",
            "cloud_cover_mid":    "fact_weather.cloud_cover_mid",
            "cloud_cover_high":   "fact_weather.cloud_cover_high",
            "wind_speed_10m":     "fact_weather.wind_speed (rename)",
            "precipitation":      "fact_weather.precipitation_mm (rename)",
            "sunshine_duration":  "fact_weather.Sunshine_Duration (rename)",
            "SiteKey":            "lookup -> geo_id qua join",
            "latitude":           "dim_geography.latitude",
            "longitude":          "dim_geography.longitude",
        }
    },
    "Solar_Energy_Generation.csv": {
        "table": "fact_solar_energy_gen",
        "columns": {
            "CampusKey":       "lookup -> site_id qua join",
            "SiteKey":         "fact_solar_energy_gen.site_id (rename)",
            "Timestamp":       "derive date_id, time_id (split date & time)",
            "SolarGeneration": "fact_solar_energy_gen.energy_generated_kwh (rename)",
        }
    },
}

for fname, info in mapping.items():
    path = os.path.join(DATA_DIR, fname)
    if not os.path.exists(path):
        print(f"\n[MISSING FILE] {fname}")
        continue
    df = pd.read_csv(path, nrows=0)
    print(f"\n{fname}  -->  {info['table']}")
    col_map = info["columns"]
    for col in df.columns:
        target = col_map.get(col, "??? (chua map)")
        status = "OK " if col in col_map else "???"
        print(f"  [{status}] {col:<45} -> {target}")
