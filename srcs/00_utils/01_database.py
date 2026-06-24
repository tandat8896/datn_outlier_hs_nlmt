"""
Common database connection utilities for the pipeline.
"""

import os
from pathlib import Path

from dotenv import load_dotenv
import psycopg2
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

# Tự động load biến môi trường từ .env ở thư mục gốc
PROJECT_ROOT = Path(__file__).resolve().parents[2]
ENV_FILE = PROJECT_ROOT / ".env"
load_dotenv(ENV_FILE, override=True)


def get_db_params() -> dict:
    """Lấy tham số kết nối từ biến môi trường."""
    return {
        "host": os.getenv("DB_HOST", "localhost"),
        "port": os.getenv("DB_PORT", "5432"),
        "dbname": os.getenv("DB_NAME", "postgres"),
        "user": os.getenv("DB_USER", "postgres"),
        "password": os.getenv("DB_PASSWORD", "postgres"),
    }


def get_sqlalchemy_engine() -> Engine:
    """Tạo SQLAlchemy engine dùng chung (phù hợp cho pandas.to_sql)."""
    params = get_db_params()
    url = (
        f"postgresql+psycopg2://{params['user']}:{params['password']}"
        f"@{params['host']}:{params['port']}/{params['dbname']}"
    )
    # pool_pre_ping để tự động kiểm tra và khôi phục kết nối bị rớt
    return create_engine(url, pool_pre_ping=True)


def get_psycopg2_connection():
    """Tạo connection dùng cho các câu lệnh SQL thô (raw SQL) hoặc script."""
    params = get_db_params()
    return psycopg2.connect(
        host=params["host"],
        port=params["port"],
        dbname=params["dbname"],
        user=params["user"],
        password=params["password"],
    )
