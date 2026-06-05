#!/bin/bash
# Kiểm tra containers và data sau khi upload

echo ""
echo "============================================================"
echo "  1. CONTAINER STATUS"
echo "============================================================"
docker compose ps

echo ""
echo "============================================================"
echo "  2. POSTGRES — kiểm tra kết nối"
echo "============================================================"
docker exec postgres_local pg_isready -U postgres

echo ""
echo "============================================================"
echo "  3. POSTGRES — liệt kê tables (sau khi chạy create_table.sql)"
echo "============================================================"
docker exec postgres_local psql -U postgres -c "\dt"

echo ""
echo "============================================================"
echo "  4. MINIO — liệt kê files trong bucket raw-data"
echo "============================================================"
docker exec minio_local sh -c "
  mc alias set local http://localhost:9000 minioadmin minioadmin --quiet 2>/dev/null
  mc ls local/raw-data
"

echo ""
echo "============================================================"
echo "  5. MINIO — dung luong bucket"
echo "============================================================"
docker exec minio_local sh -c "
  mc alias set local http://localhost:9000 minioadmin minioadmin --quiet 2>/dev/null
  mc du local/raw-data
"
