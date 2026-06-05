# Checklist Kiểm Tra Môi Trường Local

## 1. Kiểm tra Containers đang chạy

```bash
docker compose ps
```

**Kết quả đúng:**
- `postgres_local` → Status: `Up (healthy)`
- `minio_local` → Status: `Up (healthy)`

---

## 2. Kiểm tra PostgreSQL

**Kết nối được không:**
```bash
docker exec postgres_local pg_isready -U postgres
```
Kết quả đúng: `accepting connections`

**Vào psql kiểm tra tay:**
```bash
docker exec -it postgres_local psql -U postgres
```

Các lệnh trong psql:
```sql
-- Liệt kê tables
\dt

-- Kiểm tra từng bảng (sau khi load data)
SELECT COUNT(*) FROM dim_solar_site;
SELECT COUNT(*) FROM dim_geography;
SELECT COUNT(*) FROM dim_date;
SELECT COUNT(*) FROM dim_time;
SELECT COUNT(*) FROM dim_weather_type;
SELECT COUNT(*) FROM fact_solar_energy_gen;
SELECT COUNT(*) FROM fact_weather;

-- Thoát
\q
```

---

## 3. Kiểm tra MinIO (Object Storage)

**Cách 1 — Web UI (dễ nhất):**

Mở trình duyệt: `http://localhost:9001`
- Username: `minioadmin`
- Password: `minioadmin`

Vào bucket `raw-data` kiểm tra có đủ 5 file:
| File | Size |
|------|------|
| Solar_Energy_Generation.csv | ~80 MB |
| open_meteo_weather_raw_2020_2022.csv | ~78 MB |
| Solar_Site_Details.csv | ~3 KB |
| calender.csv | ~45 KB |
| campus_meta.csv | ~107 B |

**Cách 2 — Terminal:**
```bash
docker exec minio_local sh -c "
  mc alias set local http://localhost:9000 minioadmin minioadmin --quiet 2>/dev/null
  mc ls local/raw-data
"
```

---

## 4. Chạy create_table.sql lên Postgres

```bash
docker exec -i postgres_local psql -U postgres < create_table.sql
```

Kiểm tra lại:
```bash
docker exec postgres_local psql -U postgres -c "\dt"
```

Kết quả đúng — phải có đủ 7 bảng:
```
dim_date
dim_geography
dim_solar_site
dim_time
dim_weather_type
fact_solar_energy_gen
fact_weather
```

---

## 5. Thứ tự chạy scripts

```bash
# Bước 1: Upload CSV lên MinIO
python utils/script/upload_datacraw/upload_raw.py

# Bước 2: Verify toàn vẹn
python utils/script/upload_datacraw/verify_upload.py

# Bước 3: Tạo bảng Postgres
docker exec -i postgres_local psql -U postgres < create_table.sql

# Bước 4: Check lại tất cả
bash utils/script/upload_datacraw/check_docker.sh
```
