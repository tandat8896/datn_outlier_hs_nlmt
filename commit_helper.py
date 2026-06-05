#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import subprocess
import re

def get_current_branch():
    try:
        branch = subprocess.check_output(["git", "rev-parse", "--abbrev-ref", "HEAD"]).decode("utf-8").strip()
        return branch
    except Exception:
        return ""

def main():
    print("==================================================")
    print("     QUY CHUẨN COMMIT MESSAGE - THE OUTLIERS      ")
    print("==================================================")

    # 1. Chọn loại thay đổi (type)
    types = {
        "1": ("feat", "Thêm tính năng mới, hàm xử lý mới, cấu trúc mới"),
        "2": ("fix", "Sửa lỗi logic code, lỗi cú pháp SQL, xử lý dữ liệu lỗi"),
        "3": ("perf", "Tối ưu hóa hiệu suất (tăng tốc truy vấn SQL, tối ưu bộ nhớ)"),
        "4": ("docs", "Thay đổi hoặc bổ sung tài liệu (Markdown, Word, TeX, v.v.)"),
        "5": ("chore", "Cấu hình hệ thống, quản lý Sprint, cài đặt thư viện"),
        "6": ("refactor", "Cấu trúc lại code cũ nhưng không thay đổi tính năng")
    }

    print("\n[1] Chọn loại thay đổi (Type):")
    for k, v in types.items():
        print(f"  {k}. {v[0]:8} : {v[1]}")
    
    type_choice = input("Nhập lựa chọn của bạn (1-6) [mặc định: 1]: ").strip()
    c_type = types.get(type_choice, ("feat", ""))[0]

    # 2. Chọn phạm vi ảnh hưởng (scope)
    scopes = {
        "1": ("api", "Mô-đun kết nối ngoại vi (NASA POWER API, v.v.)"),
        "2": ("source", "Mô-đun đọc dữ liệu thô tại chỗ (Pandas load CSV, v.v.)"),
        "3": ("db", "Hạ tầng Cơ sở dữ liệu (SQL DDL/DML, Schema, Supabase)"),
        "4": ("logic", "Mô-đun biến đổi & Logic nghiệp vụ (Làm sạch, tính KPI)"),
        "5": ("eda", "Mô-đun phân tích khám phá (Thống kê mô tả, tương quan)"),
        "6": ("ui", "Giao diện trực quan (Plotly/Dash, Layout, Dashboard)"),
        "7": ("ml", "Mô-đun thuật toán học máy (ARIMA, XGBoost, dự báo)"),
        "8": ("reports", "Tài liệu báo cáo đồ án tốt nghiệp"),
        "9": ("scope", "Đặc tả phạm vi dự án, Data Dictionary"),
        "10": ("sprint", "Quản trị Sprint, Jira Scrum Board")
    }

    print("\n[2] Chọn phạm vi ảnh hưởng (Scope):")
    for k, v in scopes.items():
        print(f"  {k:2}. {v[0]:8} : {v[1]}")
    
    scope_choice = input("Nhập lựa chọn (1-10) hoặc tự điền scope mới: ").strip()
    if scope_choice in scopes:
        c_scope = scopes[scope_choice][0]
    elif scope_choice:
        c_scope = scope_choice
    else:
        c_scope = "db" # default

    # 3. Mã công việc Jira (JIRA-KEY)
    print("\n[3] Nhập mã công việc Jira (JIRA-KEY):")
    branch_name = get_current_branch()
    # Thử tìm scrum key trong branch name
    default_key = "SCRUM-5"
    if "scrum-" in branch_name.lower():
        match = re.search(r'scrum-\d+', branch_name, re.IGNORECASE)
        if match:
            default_key = match.group(0).upper()
            
    jira_key = input(f"Nhập Jira Key (Ví dụ: SCRUM-5) [mặc định: {default_key}]: ").strip()
    if not jira_key:
        jira_key = default_key
    else:
        jira_key = jira_key.upper()
        if not jira_key.startswith("SCRUM-"):
            # Nếu nhập số thì tự thêm SCRUM-
            if jira_key.isdigit():
                jira_key = f"SCRUM-{jira_key}"

    # 4. Tiêu đề ngắn (subject)
    print("\n[4] Nhập mô tả ngắn (Subject):")
    print("  * Lưu ý: Viết bằng động từ thường, không viết hoa đầu dòng, không có dấu chấm ở cuối.")
    while True:
        subject = input("Mô tả ngắn: ").strip()
        if not subject:
            print("  -> Mô tả không được để trống. Vui lòng nhập lại.")
            continue
        
        # Kiểm tra quy chuẩn viết thường đầu dòng
        first_char = subject[0]
        if first_char.isupper() and first_char.isalpha():
            print("  -> Cảnh báo: Chữ cái đầu đang viết hoa. Hệ thống sẽ tự động chuyển thành viết thường.")
            subject = first_char.lower() + subject[1:]
            
        # Kiểm tra dấu chấm cuối câu
        if subject.endswith("."):
            print("  -> Cảnh báo: Có dấu chấm ở cuối câu. Hệ thống sẽ tự động xóa dấu chấm.")
            subject = subject.rstrip(".")
            
        break

    # Ghép thông điệp commit
    commit_msg = f"{c_type}({c_scope}): [{jira_key}] {subject}"
    print("\n==================================================")
    print("Thông điệp commit đề xuất:")
    print(f"👉 {commit_msg}")
    print("==================================================")

    confirm = input("Bạn có muốn thực hiện commit với thông điệp này không? (y/n) [y]: ").strip().lower()
    if confirm == 'n':
        print("Đã hủy bỏ commit.")
        sys.exit(0)

    # Thực hiện git commit
    try:
        # Kiểm tra xem có file nào được staged chưa
        status_out = subprocess.check_output(["git", "status", "--porcelain"]).decode("utf-8")
        if not status_out:
            print("Không có thay đổi nào trong working directory.")
            sys.exit(0)
            
        staged = [line for line in status_out.splitlines() if line.startswith(('A ', 'M ', 'D ', 'R '))]
        if not staged:
            add_all = input("Không có tệp tin nào được staged. Bạn có muốn add tất cả thay đổi hiện tại (git add .) trước khi commit không? (y/n) [y]: ").strip().lower()
            if add_all != 'n':
                subprocess.run(["git", "add", "."], check=True)
                print("Đã tự động chạy: git add .")
            else:
                print("Vui lòng chạy 'git add' các file cần thiết trước khi commit.")
                sys.exit(0)

        # Chạy git commit
        subprocess.run(["git", "commit", "-m", commit_msg], check=True)
        print("\n🎉 Commit thành công!")
        
        push = input("Bạn có muốn push code lên remote branch luôn không? (y/n) [n]: ").strip().lower()
        if push == 'y':
            branch = get_current_branch()
            if branch:
                print(f"Đang chạy: git push origin {branch}...")
                subprocess.run(["git", "push", "origin", branch], check=True)
                print("🎉 Đã push thành công!")
            else:
                print("Không tìm thấy tên branch hiện tại để push.")
    except subprocess.CalledProcessError as e:
        print(f"\n❌ Lỗi khi thực hiện lệnh Git: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Lỗi không xác định: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
