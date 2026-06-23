#!/usr/bin/env python3
"""
Export local rolling-IQR outlier flag CSVs for review before any Supabase upload.

Input is generated audit output already produced by the rolling notebook:
- rolling 1h IQR candidates

This script does not write to Supabase.
"""

from __future__ import annotations

import hashlib
from pathlib import Path

import pandas as pd
import yaml


ROOT = Path(__file__).resolve().parents[3]
DEFAULT_CONFIG = ROOT / "config" / "02_transform" / "01_generate_outliers.yaml"

with DEFAULT_CONFIG.open(encoding="utf-8") as _f:
    _cfg = yaml.safe_load(_f)

SOLAR_PATH = ROOT / _cfg["paths"]["parquet_dir"] / "temp_fact_solar_energy_gen.parquet"
ROLLING_PATH = ROOT / _cfg["paths"]["rolling_iqr_csv"]
OUT_DIR = ROOT / _cfg["paths"]["outlier_out_dir"]

KEY_COLS = ["sitekey", "timestamp"]


def normalize_keys(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["sitekey"] = df["sitekey"].astype(str)
    df["timestamp"] = pd.to_datetime(df["timestamp"]).dt.strftime("%Y-%m-%d %H:%M:%S")
    return df


def read_candidate_keys(path: Path, flag_name: str) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=KEY_COLS + [flag_name])
    df = pd.read_csv(path, usecols=lambda c: c in set(KEY_COLS))
    df = normalize_keys(df).drop_duplicates(KEY_COLS)
    df[flag_name] = True
    return df


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def write_csv(df: pd.DataFrame, path: Path) -> None:
    df.to_csv(path, index=False)


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    solar = pd.read_parquet(SOLAR_PATH, columns=["sitekey", "timestamp", "energy_generated_kwh"])
    solar = normalize_keys(solar)
    solar["hour"] = pd.to_datetime(solar["timestamp"]).dt.hour

    candidate_frames = [
        read_candidate_keys(ROLLING_PATH, "rolling_outlier_flag"),
    ]

    flags = solar
    for frame in candidate_frames:
        flags = flags.merge(frame, on=KEY_COLS, how="left")

    flag_cols = ["rolling_outlier_flag"]
    for col in flag_cols:
        flags[col] = flags[col].fillna(False).astype(bool)

    full_path = OUT_DIR / "01_full_generation_rolling_flags.csv"
    candidates_path = OUT_DIR / "02_rolling_iqr_candidates.csv"
    summary_path = OUT_DIR / "03_rolling_flag_summary.csv"
    overlap_path = OUT_DIR / "04_rolling_flag_overlap_matrix.csv"
    manifest_path = OUT_DIR / "05_rolling_rule_manifest.csv"
    checksums_path = OUT_DIR / "99_sha256_checksums.csv"

    write_csv(flags, full_path)
    write_csv(flags[flags["rolling_outlier_flag"]].copy(), candidates_path)

    total_rows = len(flags)
    summary_rows = []
    for col in flag_cols:
        n = int(flags[col].sum())
        summary_rows.append(
            {
                "flag": col,
                "rows": n,
                "pct_of_total": n / total_rows * 100,
            }
        )
    keep_rows = int((~flags["rolling_outlier_flag"]).sum())
    summary_rows.append(
        {
            "flag": "keep_raw",
            "rows": keep_rows,
            "pct_of_total": keep_rows / total_rows * 100,
        }
    )
    summary = pd.DataFrame(summary_rows)
    write_csv(summary, summary_path)

    overlap_records = []
    for left in flag_cols:
        for right in flag_cols:
            overlap_records.append(
                {
                    "left_flag": left,
                    "right_flag": right,
                    "overlap_rows": int((flags[left] & flags[right]).sum()),
                }
            )
    write_csv(pd.DataFrame(overlap_records), overlap_path)

    manifest = pd.DataFrame(
        [
            {
                "flag": "rolling_outlier_flag",
                "source_file": str(ROLLING_PATH.relative_to(ROOT)),
                "rule": "rolling_1h_abs_deviation > Q3 + 1.5*IQR within sitekey + hour",
                "role": "main rolling-only 15-minute generation time-series audit",
            },
        ]
    )
    write_csv(manifest, manifest_path)

    checksum_rows = []
    for path in [full_path, candidates_path, summary_path, overlap_path, manifest_path]:
        checksum_rows.append(
            {
                "file": str(path.relative_to(ROOT)),
                "sha256": sha256_file(path),
                "bytes": path.stat().st_size,
            }
        )
    write_csv(pd.DataFrame(checksum_rows), checksums_path)

    print("Outlier flag export complete")
    print(f"rows={total_rows:,}")
    print(summary.to_string(index=False))
    print(f"output_dir={OUT_DIR}")


if __name__ == "__main__":
    main()
