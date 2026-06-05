import os
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
    CREATE TABLE IF NOT EXISTS test_table (
        id SERIAL PRIMARY KEY,
        name VARCHAR(100) NOT NULL,
        created_at TIMESTAMPTZ DEFAULT NOW()
    )
""")
conn.commit()
print("Tao bang thanh cong!")

cur.execute("INSERT INTO test_table (name) VALUES (%s) RETURNING id", ("test_row",))
row = cur.fetchone()
conn.commit()
print(f"Insert thanh cong! id = {row[0]}")

cur.execute("SELECT * FROM test_table")
rows = cur.fetchall()
print(f"Du lieu trong bang: {rows}")

cur.close()
conn.close()
print("Done!")
