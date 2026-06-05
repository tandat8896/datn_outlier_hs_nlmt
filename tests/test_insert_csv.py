import os
import csv
import pg8000
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parents[1] / ".env")

conn = pg8000.connect(
    host=os.getenv("DB_HOST"),
    port=int(os.getenv("DB_PORT")),
    database=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    ssl_context=True,
)

cur = conn.cursor()

cur.execute("""
    CREATE TABLE IF NOT EXISTS vn_solar_site_details (
        site_key    VARCHAR(50) PRIMARY KEY,
        lat         NUMERIC(15, 10),
        lon         NUMERIC(15, 10),
        address     TEXT,
        province    VARCHAR(100),
        found_plant TEXT
    )
""")
conn.commit()
print("Tao bang thanh cong!")

csv_path = Path(__file__).resolve().parents[1] / "data/raw/VN_Solar_Site_Details(1).csv"

with open(csv_path, encoding="utf-8") as f:
    reader = csv.DictReader(f)
    rows = []
    for i, row in enumerate(reader):
        if i >= 10:
            break
        rows.append((
            row["SiteKey"],
            float(row["lat"]) if row["lat"] else None,
            float(row["Lon"]) if row["Lon"] else None,
            row["Address"],
            row["OriginalName"],
            row["FoundPlant"],
        ))

cur.executemany("""
    INSERT INTO vn_solar_site_details (site_key, lat, lon, address, province, found_plant)
    VALUES (%s, %s, %s, %s, %s, %s)
    ON CONFLICT (site_key) DO NOTHING
""", rows)
conn.commit()
print(f"Insert thanh cong {len(rows)} dong!")

cur.execute("SELECT * FROM vn_solar_site_details LIMIT 10")
results = cur.fetchall()
for r in results:
    print(r)

cur.close()
conn.close()
