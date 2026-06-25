import pandas as pd
import os
import time
from pathlib import Path

def convert_csv_to_parquet(input_file: str, output_file: str):
    """
    Hàm tiện ích (utils) để nén file CSV dung lượng lớn sang Parquet.
    Giúp tối ưu hóa tốc độ đọc/ghi và giảm dung lượng lưu trữ trên DVC/S3.
    """
    if not os.path.exists(input_file):
        print(f"[Loi] Khong tim thay file {input_file}")
        return

    print(f"[*] Bat dau doc file CSV: {input_file}...")
    start_time = time.time()
    
    # Đọc CSV với mã hóa UTF-8 để chống lỗi font chữ và hỏng format
    df = pd.read_csv(input_file, encoding='utf-8')
    csv_size = os.path.getsize(input_file) / (1024 * 1024)
    print(f"[+] Da doc xong CSV (UTF-8 chuan). Dung luong goc: {csv_size:.2f} MB")
    print(f"[*] So dong: {len(df):,}")

    print("[*] Dang nen sang dinh dang Parquet (compression='snappy')...")
    # Đảm bảo thư mục đầu ra tồn tại
    Path(output_file).parent.mkdir(parents=True, exist_ok=True)
    
    # Lưu sang Parquet bảo toàn định dạng cột, bỏ cột số thứ tự mặc định của pandas
    df.to_parquet(output_file, engine='pyarrow', compression='snappy', index=False)
    
    parquet_size = os.path.getsize(output_file) / (1024 * 1024)
    end_time = time.time()
    
    print("\n[+] HOAN TAT NEN DU LIEU BAO TOAN FORMAT!")
    print(f"[*] Dung luong sau khi nen: {parquet_size:.2f} MB")
    print(f"[*] Ty le nen giam duoc: {((csv_size - parquet_size) / csv_size) * 100:.2f}%")
    print(f"[*] Thoi gian chay: {end_time - start_time:.2f} giay")
    print(f"\n[!] Xin nhac: Chay lenh 'dvc add {output_file}' de luu vao DVC")

if __name__ == "__main__":
    # Cấu hình đường dẫn
    IN_FILE = "data/mlmart_base/ml_mart_base_data.csv"
    OUT_FILE = "data/mlmart_base/ml_mart_base_data.parquet"
    convert_csv_to_parquet(IN_FILE, OUT_FILE)
