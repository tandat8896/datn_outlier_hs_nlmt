import pandas as pd

from pathlib import Path
FILE = str(Path(__file__).resolve().parents[3] / "data" / "raw" / "Solar_Energy_Generation.csv")

df = pd.read_csv(FILE)
total = len(df)
null_cols = df.isnull().sum()
null_cols = null_cols[null_cols > 0]

print(f"\nFILE: Solar_Energy_Generation.csv ({total:,} rows)\n")
for col, cnt in null_cols.items():
    pct = cnt / total * 100
    print(f"  Col '{col}': {cnt:,} / {total:,} null = {pct:.1f}%")
