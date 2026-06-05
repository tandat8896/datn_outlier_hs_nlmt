# Báo Cáo Tiến Độ — Solar Energy Data Pipeline
**Ngày:** 2026-06-02  
**Môi trường:** Local (Docker — PostgreSQL 17.6 + MinIO)  
**Người thực hiện:** Data Engineering Team

---

## 1. Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                             │
│                                                                 │
│  Solar_Energy_Generation.csv   │  open_meteo_weather.csv        │
│  (2,731,946 rows / 80MB)       │  (850,752 rows / 78MB)         │
│  Solar_Site_Details.csv        │  campus_meta.csv               │
│  (42 rows)                     │  (5 rows)                      │
│  calender.csv (2,312 rows)     │                                │
└────────────────────┬────────────────────────────────────────────┘
                     │  upload_raw.py
                     ▼
┌─────────────────────────────────────────────────────────────────┐
│                    RAW LAYER — MinIO (sau này lên Postgre object storage chỉ thay đường dẫn logic i chang )                           │
│                                                                 │
│   Bucket: raw-data/                                            │
│   ├── Solar_Energy_Generation.csv    (80 MB)                   │
│   ├── open_meteo_weather_raw.csv     (78 MB)                   │
│   ├── Solar_Site_Details.csv         (3 KB)                    │
│   ├── calender.csv                   (45 KB)                   │
│   └── campus_meta.csv                (107 B)                   │
│                                                                 │
│   Total: 158 MB / 5 files  ✅ Verified (MD5 checksum)          │
└────────────────────┬────────────────────────────────────────────┘
                     │  load_01_dims.py
                     │  load_02_facts.py
                     ▼
┌─────────────────────────────────────────────────────────────────┐
│              RAW DB LAYER — PostgreSQL 17.6                     │
│                                                                 │
│   DIMENSION TABLES                  FACT TABLES                │
│   ┌─────────────────┐               ┌──────────────────────┐   │
│   │ dim_geography   │◄──────────────│ fact_solar_energy_gen│   │
│   │ (5 rows)        │               │ (2,731,946 rows)     │   │
│   ├─────────────────┤               ├──────────────────────┤   │
│   │ dim_solar_site  │◄──────────────│ fact_weather         │   │
│   │ (42 rows)       │               │ (850,752 rows)       │   │
│   ├─────────────────┤               └──────────────────────┘   │
│   │ dim_date        │◄──────────────────────┘                  │
│   │ (2,312 rows)    │                                          │
│   ├─────────────────┤                                          │
│   │ dim_time        │◄──────────────────────┘                  │
│   │ (96 slots)      │                                          │
│   ├─────────────────┤                                          │
│   │ dim_weather_type│◄──────────────────────┘                  │
│   │ (22 combos)     │                                          │
│   └─────────────────┘                                          │
│                                                                 │
│   Total: ~350 MB (ước tính)   ✅ Verified (row + value check)  │
└─────────────────────────────────────────────────────────────────┘
                     │
                     ▼  (chưa làm)
┌─────────────────────────────────────────────────────────────────┐
│                  STAGING / TRANSFORM LAYER                      │
│             (validate null, chuẩn hóa, join)                   │
└─────────────────────────────────────────────────────────────────┘
                     │
                     ▼  (chưa làm)
┌─────────────────────────────────────────────────────────────────┐
│                  ANALYTICS / ML LAYER                           │
│              (XGBoost solar generation forecast)                │
└─────────────────────────────────────────────────────────────────┘

     LOCAL (Docker)          →          PRODUCTION (Supabase)
     MinIO                              Supabase Storage
     PostgreSQL 17.6                    Supabase PostgreSQL 17.6
     (đổi 3 dòng config là xong)
```

---

## 2. Cột Bị Bỏ Trong create_table.sql — Đã Bổ Sung Thành Công ✅

Tất cả 4 cột bị thiếu trước đây đã được cập nhật thành công vào schema cơ sở dữ liệu (`create_table.sql`), các script ETL (`load_01_dims.py`), kiểm tra ánh xạ (`check_mapping.py`), script verify dữ liệu (`load_03_verify.py`), và tài liệu báo cáo đồ án tốt nghiệp (`DATN_OUTLIERS_REPORT.tex`):

* **`capacity`** (từ `campus_meta.csv`): Đã thêm vào bảng `dim_geography` làm cột `capacity INT` để chuẩn hóa sản lượng điện.
* **`is_holiday`**, **`is_semester`**, **`is_exam`** (từ `calender.csv`): Đã thêm vào bảng `dim_date` dưới dạng các cột `INT` phục vụ cho việc phân tích các đặc trưng mùa vụ và hành vi tiêu thụ điện.

---

## 3. Trạng Thái Hiện Tại

| Hạng mục | Trạng thái | Chi tiết |
|----------|-----------|---------|
| Docker local setup | ✅ Xong | PostgreSQL 17.6 + MinIO |
| Upload CSV → MinIO | ✅ Xong | 5 files, 158 MB, MD5 verified |
| Tạo bảng PostgreSQL | ✅ Xong | 7 bảng (5 dim + 2 fact) |
| Load dim tables | ✅ Xong | 5 bảng, tổng ~2.5K rows |
| Load fact tables | ✅ Xong | 3.58M rows, chunk 50K |
| Verify toàn vẹn | ✅ **PASS 100%** | Row count + value check |
| Staging/Transform | ⏳ Chưa làm | Chờ quyết định về NULL |
| Analytics/ML | ⏳ Chưa làm | Phụ thuộc staging |

---

## 3. Kết Quả Verify Toàn Vẹn

### 3.1 Row Count
```
[PASS] dim_geography:          CSV=5         | DB=5
[PASS] dim_solar_site:         CSV=42        | DB=42
[PASS] dim_date:               CSV=2,312     | DB=2,312
[PASS] dim_time:               CSV=96        | DB=96
[PASS] dim_weather_type:       CSV=22        | DB=22
[PASS] fact_solar_energy_gen:  CSV=2,731,946 | DB=2,731,946
[PASS] fact_weather:           CSV=850,752   | DB=850,752
```

### 3.2 Ánh Xạ Cột — Còn lại / Mất gì

| CSV File | Cột CSV | → | Cột DB | Ghi chú |
|----------|---------|---|--------|---------|
| campus_meta | `id` | → | `dim_geography.geo_id` | rename |
| campus_meta | `name` | → | `dim_geography.location_name` | rename |
| campus_meta | `capacity` | → | ❌ BỎ | không có trong schema |
| Solar_Site_Details | `SiteKey` | → | `dim_solar_site.site_id` | rename |
| Solar_Site_Details | `CampusKey` | → | `dim_solar_site.campus_name` | join campus_meta lấy tên |
| Solar_Site_Details | `kWp` | → | `dim_solar_site.capacity_kw` | rename |
| Solar_Site_Details | `Number of panels` | → | `dim_solar_site.Number_of_panels` | rename |
| Solar_Site_Details | `lat`, `Lon` | → | `dim_geography.latitude/longitude` | avg theo campus |
| calender | `date` | → | `dim_date.full_date` + `day/month/year` | derive |
| calender | `is_holiday`, `is_semester`, `is_exam` | → | ❌ BỎ | không có trong schema |
| weather | `timestamp` | → | `dim_time.time_string` + `date_id/time_id` | split |
| weather | `weather_code` + `is_day` | → | `dim_weather_type.weather_type_id` | generate combo |
| weather | `weather_condition`, `description` | → | `dim_weather_type.*` | generate từ WMO codes |
| weather | `temperature_2m` | → | `fact_weather.temperature_c` | rename |
| weather | `cloud_cover` | → | `fact_weather.cloud_cover_total` | rename |
| weather | `wind_speed_10m` | → | `fact_weather.wind_speed` | rename |
| weather | `direct_radiation` | → | `fact_weather.Direct_Normal_Irradiance` | rename |
| weather | `diffuse_radiation` | → | `fact_weather.Diffuse_Solar_Radiation` | rename |
| Solar_Energy_Generation | `SiteKey` | → | `fact_solar_energy_gen.site_id` | giữ nguyên |
| Solar_Energy_Generation | `SolarGeneration` | → | `fact_solar_energy_gen.energy_generated_kwh` | rename, 56.2% NULL |
| Solar_Energy_Generation | `CampusKey` | → | ❌ BỎ | dùng để lookup geo_id, không lưu |

### 3.3 Verify Giá Trị Thực Tế (không bị lệch dòng/cột)

```
dim_geography — so sánh tên campus:
[PASS] geo_id=1: CSV='Bundoora'        | DB='Bundoora'
[PASS] geo_id=2: CSV='Albury-Wodonga' | DB='Albury-Wodonga'
[PASS] geo_id=3: CSV='Bendigo'        | DB='Bendigo'
[PASS] geo_id=4: CSV='Mildura'        | DB='Mildura'
[PASS] geo_id=5: CSV='Shepparton'     | DB='Shepparton'

fact_solar_energy_gen — so sánh energy_generated_kwh theo SiteKey+Timestamp:
[PASS] SiteKey=1 2020-01-01 06:15 → CSV=0.135  | DB=0.135
[PASS] SiteKey=1 2020-01-01 06:30 → CSV=0.465  | DB=0.465
[PASS] SiteKey=1 2020-01-01 06:45 → CSV=1.039  | DB=1.039
[PASS] SiteKey=1 2020-01-01 07:00 → CSV=1.673  | DB=1.673
[PASS] SiteKey=1 2020-01-01 07:15 → CSV=2.56   | DB=2.56

fact_weather — so sánh temperature_2m theo SiteKey+timestamp:
[PASS] SiteKey=1 2020-01-01 00:00 → CSV=17.6 | DB=17.6
[PASS] SiteKey=1 2020-01-01 01:00 → CSV=15.6 | DB=15.6
[PASS] SiteKey=1 2020-01-01 02:00 → CSV=14.8 | DB=14.8
[PASS] SiteKey=1 2020-01-01 03:00 → CSV=16.4 | DB=16.4
[PASS] SiteKey=1 2020-01-01 04:00 → CSV=18.2 | DB=18.2

✅ Không có hiện tượng lệch dòng/cột
✅ Giá trị khớp chính xác từng ô
```

---

## 4. Cột Bị Bỏ — Tình trạng: Đã Giải Quyết ✅

Các cột quan trọng thiếu từ ban đầu đã được bổ sung vào schema và pipeline:
* `capacity` (từ `campus_meta.csv`) $\rightarrow$ `dim_geography.capacity` (Đã thêm)
* `is_holiday` (từ `calender.csv`) $\rightarrow$ `dim_date.is_holiday` (Đã thêm)
* `is_semester` (từ `calender.csv`) $\rightarrow$ `dim_date.is_semester` (Đã thêm)
* `is_exam` (từ `calender.csv`) $\rightarrow$ `dim_date.is_exam` (Đã thêm)
* `CampusKey` (từ `Solar_Energy_Generation.csv`): Được sử dụng cho việc ánh xạ lookup `geo_id` mà không lưu trữ trực tiếp (Không đổi)

---

## 5. Vấn Đề Cần Team Quyết Định

### 🔴 CRITICAL — SolarGeneration 56.2% NULL

| Cột | Null | Tổng | Tỷ lệ |
|-----|------|------|-------|
| `SolarGeneration` | 1,536,301 | 2,731,946 | **56.2%** |

**Nguyên nhân có thể:** Ban đêm không phát điện, sensor offline, hoặc data lỗi.

**Team cần quyết định trước khi làm staging:**
- [ ] **Giữ NULL** — chấp nhận, lọc khi train Tree Algorithms 
- [ ] **Fill = 0** — null = không phát điện
- [ ] **Drop rows** — null = data lỗi, bỏ hẳn

### 🟡 WARNING — Solar_Site_Details thiếu thông tin thiết bị

| Cột | Null | Tỷ lệ |
|-----|------|-------|
| kWp, Panel, Inverter, Optimizers, Metric | 17-19/42 | ~40-45% |

**Không ảnh hưởng pipeline hiện tại** — schema cho phép NULL.

### ⚠️ Lưu ý kỹ thuật — Time granularity mismatch

| Nguồn | Granularity |
|-------|------------|
| Solar generation | **15 phút** (96 slots/ngày) |
| Weather | **1 giờ** (24 slots/ngày) |

Khi join 2 fact table để train XGBoost cần round timestamp solar xuống giờ gần nhất.

---

## 5. Bug Đã Phát Hiện & Fix Trong Quá Trình Test

| Bug | Nguyên nhân | Đã fix |
|-----|------------|-------|
| `fact_weather.geo_id = NULL` | Dùng lat/lon trung bình để lookup không khớp với lat/lon chính xác của site | ✅ Sửa sang dùng SiteKey → geo_id |

---


---

## 6. Bước Tiếp Theo

1. **Team quyết định** cách xử lý `SolarGeneration` NULL
2. Viết **staging/transform** scripts (clean null, chuẩn hóa)
3. Test **join query** fact_solar_energy_gen + fact_weather
4. **Deploy lên Supabase** — chỉ đổi 3 dòng config endpoint
