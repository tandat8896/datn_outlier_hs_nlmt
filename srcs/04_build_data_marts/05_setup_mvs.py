import os
import psycopg2
from dotenv import load_dotenv

# Load cấu hình Database
load_dotenv()

def get_db_connection():
    return psycopg2.connect(
        host=os.environ["DB_HOST"],
        port=int(os.environ.get("DB_PORT", "5432")),
        dbname=os.environ["DB_NAME"],
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
        sslmode="require"
    )

def setup_materialized_views():
    print("[Hệ thống] Đang khởi tạo 2 Materialized Views trên Supabase...")
    conn = get_db_connection()
    conn.autocommit = True
    cur = conn.cursor()
    
    sql_setup = """
    -- 1. MATERIALIZED VIEW CẤP GIỜ (HOURLY)
    DROP MATERIALIZED VIEW IF EXISTS bi_mart.mv_bi_mart_hourly_measures CASCADE;
    CREATE MATERIALIZED VIEW bi_mart.mv_bi_mart_hourly_measures AS
    SELECT * FROM bi_mart.vw_bi_mart_hourly_measures_replace;

    CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_hourly_pk 
    ON bi_mart.mv_bi_mart_hourly_measures (site_id, date_id, hourly_bucket);

    -- 2. MATERIALIZED VIEW CẤP NGÀY (DAILY)
    DROP MATERIALIZED VIEW IF EXISTS bi_mart.mv_bi_mart_daily_kpis CASCADE;
    CREATE MATERIALIZED VIEW bi_mart.mv_bi_mart_daily_kpis AS
    SELECT * FROM bi_mart.vw_bi_mart_daily_kpis_replace;

    CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_daily_pk 
    ON bi_mart.mv_bi_mart_daily_kpis (report_date, site_id);
    """

    try:
        cur.execute(sql_setup)
        print("[Thành công] Đã đúc xong 2 Materialized Views (Cấp Giờ & Cấp Ngày)!")
    except Exception as e:
        print(f"[Lỗi] Khởi tạo thất bại: {e}")
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    setup_materialized_views()