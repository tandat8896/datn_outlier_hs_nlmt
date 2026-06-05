# Hướng Dẫn Cài Đặt & Chạy Project

---

## cái này không chạy trên windows chạy trên máy của Đạt nên mình viết hướng dẫn cài trên windows cho mọi người dễ làm theo, còn ai dùng NixOS thì cứ chạy mấy lệnh bên dưới là được nhé

```bash
# Set LD_LIBRARY_PATH (2 lần mỗi session)
export LD_LIBRARY_PATH="/nix/store/chqq9mpmpyfi9kgsngya71akv5xicn03-gcc-15.2.0-lib/lib:/nix/store/l7xwm1f6f3zj2x8jwdbi8gdyfbx07sh7-zlib-1.3.1/lib:$LD_LIBRARY_PATH"


# Chạy scripts
.venv/bin/python ultils/script/upload_dataraw/upload_raw.py
.venv/bin/python ultils/script/upload_dataraw/verify_upload.py
.venv/bin/python ultils/script/etl/load_01_dims.py
.venv/bin/python ultils/script/etl/load_02_facts.py
.venv/bin/python ultils/script/etl/load_03_verify.py
```

---

## Hướng Dẫn Cài Đặt Trên Windows

---

## Bước 1 — Cài Docker Desktop

> 🎥 Video hướng dẫn: https://www.youtube.com/watch?v=Etyn8ss-jwM

1. Vào: https://docs.docker.com/desktop/setup/install/windows-install/
2. Tải **Docker Desktop for Windows**
3. Chạy file `.exe`, chọn **WSL 2** khi được hỏi (tự cài, không cần làm gì thêm)
4. **Restart máy**
5. Mở Docker Desktop, chờ icon dưới taskbar chuyển sang **Running**

Kiểm tra:
```powershell
docker --version
docker compose version
```

---

## Bước 2 — Cài Python

Tải Python **3.10 trở lên**: https://python.org/downloads

> ⚠️ Tích chọn **"Add Python to PATH"** khi cài — quan trọng!

Kiểm tra:
```powershell
python --version
pip --version
```

---

## Bước 3 — Clone Project & Cài Thư Viện

```powershell
cd Du_An_Tot_Nghiep
pip install -r requirements.txt
```

---

## Bước 4 — Tạo Folder Cho Scripts

Tạo cấu trúc thư mục trong `ultils\`:

```powershell
mkdir ultils\script\etl
mkdir ultils\script\upload_dataraw
```

Cấu trúc sau khi tạo:
```
Du_An_Tot_Nghiep\
└── ultils\
    └── script\
        ├── etl\
        │   ├── load_01_dims.py
        │   ├── load_02_facts.py
        │   └── load_03_verify.py
        └── upload_dataraw\
            ├── upload_raw.py
            ├── verify_upload.py
            ├── check_null.py
            └── check_mapping.py
```

Sau đó copy các file `.py` từ team vào đúng folder tương ứng.

---

## Bước 5 — Start Docker

```powershell
cd Du_An_Tot_Nghiep
docker compose up -d
```

Kiểm tra containers đang chạy:
```powershell
docker compose ps
```

Kết quả đúng:
```
NAME             STATUS
postgres_local   Up (healthy)
minio_local      Up (healthy)
```

---

## Bước 6 — Tạo Bảng PostgreSQL

```powershell
Get-Content ultils\create_table.sql | docker exec -i postgres_local psql -U postgres
```

---

## Bước 7 — Chạy Scripts

Trên Windows **không cần** set `LD_LIBRARY_PATH` — chạy thẳng `python` là được.

**Upload CSV lên MinIO:**
```powershell
python ultils\script\upload_dataraw\upload_raw.py
```

**Verify upload toàn vẹn:**
```powershell
python ultils\script\upload_dataraw\verify_upload.py
```

**Check null trong data:**
```powershell
python ultils\script\upload_dataraw\check_null.py
```

**Load dim tables:**
```powershell
python ultils\script\etl\load_01_dims.py
```

**Load fact tables (2.7M + 850K rows, chờ ~5-10 phút):**
```powershell
python ultils\script\etl\load_02_facts.py
```

**Verify data toàn vẹn:**
```powershell
python ultils\script\etl\load_03_verify.py
```

Kết quả đúng — tất cả phải `[PASS]`:
```
[PASS] fact_solar_energy_gen: 2,731,946 rows
[PASS] fact_weather: 850,752 rows
[PASS] Dữ liệu toàn vẹn — không bị lệch dòng/cột!
```

---

## Bước 8 — Kiểm Tra MinIO

Mở trình duyệt: `http://localhost:9001`
- Username: `minioadmin`
- Password: `minioadmin`

Vào bucket `raw-data` — phải có đủ 5 file CSV.

---

## Bước 9 — Kiểm Tra PostgreSQL

```powershell
docker exec -it postgres_local psql -U postgres
```

Trong psql:
```sql
\dt
SELECT COUNT(*) FROM fact_solar_energy_gen;
SELECT COUNT(*) FROM fact_weather;
\q
```

---

## Quản Lý Docker Hàng Ngày

| Tình huống | Lệnh |
|-----------|------|
| Bật lên (sau khi tắt máy) | `docker compose start` |
| Tắt (giữ data) | `docker compose stop` |
| Restart | `docker compose restart` |
| Xem trạng thái | `docker compose ps` |
| Xem logs | `docker compose logs -f postgres` |
| **Xóa hoàn toàn + data** | `docker compose down -v` ⚠️ |

> ⚠️ `docker compose down -v` xóa toàn bộ data — phải load lại từ Bước 6.

---

