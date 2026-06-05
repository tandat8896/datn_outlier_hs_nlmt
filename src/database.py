from supabase import create_client, Client
from src.config import SUPABASE_URL, SUPABASE_ANON_KEY, SUPABASE_SERVICE_ROLE_KEY


def get_client() -> Client:
    """Client dung cho truy van data thuong (anon key, ap dung RLS)."""
    if not SUPABASE_URL or not SUPABASE_ANON_KEY:
        raise ValueError("Thieu SUPABASE_URL hoac SUPABASE_ANON_KEY trong file .env")
    return create_client(SUPABASE_URL, SUPABASE_ANON_KEY)


def get_admin_client() -> Client:
    """Client danh cho ETL / admin (service role key, bypass RLS)."""
    if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY:
        raise ValueError("Thieu SUPABASE_URL hoac SUPABASE_SERVICE_ROLE_KEY trong file .env")
    return create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)
