#!/usr/bin/env python3
"""Manage the local staging -> buffer -> data warehouse pipeline."""

from __future__ import annotations

import argparse
import importlib.util
import logging
from pathlib import Path
import sys
import time

from dotenv import load_dotenv
import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[2]
ENV_FILE = PROJECT_ROOT / ".env"
LOG_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"

logging.basicConfig(
    level=logging.INFO,
    format=LOG_FORMAT,
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)
log = logging.getLogger("pipeline")

STAGING_CONFIG = (
    PROJECT_ROOT
    / "config"
    / "03_load"
    / "02_load_object_storage_to_staging.yaml"
)

OUTLIER_CONFIG = (
    PROJECT_ROOT
    / "config"
    / "02_transform"
    / "01_generate_outliers.yaml"
)

PIPELINE_CONFIG = (
    PROJECT_ROOT
    / "config"
    / "05_run_pipeline"
    / "pipeline.yaml"
)


def _load_outlier_paths():
    with OUTLIER_CONFIG.open(encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    return (
        PROJECT_ROOT / cfg["paths"]["full_outlier_csv"],
        PROJECT_ROOT / cfg["paths"]["sparse_outlier_csv"],
    )


def _load_sleep_seconds() -> int:
    with PIPELINE_CONFIG.open(encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    return int(cfg["pipeline"]["sleep_between_stages_seconds"])


def load_module(module_name: str, relative_path: str):
    module_path = PROJECT_ROOT / relative_path
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot load module: {module_path}")

    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


database = load_module(
    "pipeline_database",
    "srcs/00_utils/01_database.py",
)
storage = load_module(
    "pipeline_storage",
    "srcs/00_utils/02_storage.py",
)
staging_loader = load_module(
    "pipeline_staging_loader",
    "srcs/03_load/02_load_object_storage_to_staging/01_run_load_staging.py",
)
buffer_transform = load_module(
    "pipeline_buffer_transform",
    "srcs/02_transform/01_run_transform_buffers.py",
)
hybrid_imputation = load_module(
    "pipeline_hybrid_imputation",
    "srcs/02_transform/02_run_hybrid_imputation.py",
)
pipeline_outlier = load_module(
    "pipeline_outlier",
    "srcs/02_transform/02_run_apply_outlier_flags.py",
)
warehouse_loader = load_module(
    "pipeline_warehouse_loader",
    "srcs/03_load/03_load_buffers_to_datawarehouse/01_run_load_datawarehouse.py",
)
bi_view_builder = load_module(
    "pipeline_bi_view_builder",
    "srcs/04_build_data_marts/03_build_bi_view.py",
)
bi_mart_builder = load_module(
    "pipeline_bi_mart",
    "srcs/04_build_data_marts/01_build_bi_mart.py",
)
ml_mart_builder = load_module(
    "pipeline_ml_mart",
    "srcs/04_build_data_marts/02_build_ml_mart.py",
)


def read_yaml(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as file:
        return yaml.safe_load(file)


def run_staging() -> None:
    """Load object-storage CSV files into staging.stg_* tables."""
    log.info("Đọc cấu hình Object Storage và mở kết nối database")
    config = read_yaml(STAGING_CONFIG)
    storage_config = config["object_storage"]

    engine = database.get_sqlalchemy_engine()
    s3_client = storage.create_storage_client(storage_config)
    bucket = storage.read_bucket_name(storage_config)

    try:
        staging_loader.load_object_storage_to_staging(
            engine=engine,
            s3_client=s3_client,
            bucket=bucket,
            config=config,
        )
    finally:
        engine.dispose()
        log.info("Đã đóng SQLAlchemy engine của stage staging")


def run_transform(*, dry_run: bool) -> None:
    """Transform staging.stg_* into staging buffer tables."""
    log.info("Mở kết nối transform; mode=%s", "DRY-RUN" if dry_run else "EXECUTE")
    connection = database.get_psycopg2_connection()
    try:
        buffer_transform.transform_staging_to_buffers(
            connection,
            execute=not dry_run,
        )
        if dry_run:
            connection.rollback()
            log.info("Transform dry-run hoàn tất; transaction đã rollback")
        else:
            connection.commit()
            log.info("Transform hoàn tất; transaction đã commit")
    except Exception:
        connection.rollback()
        log.exception("Transform thất bại; transaction đã rollback")
        raise
    finally:
        connection.close()
        log.info("Đã đóng kết nối transform")


def run_generate_outliers(*, dry_run: bool) -> None:
    """Generate outlier flags CSV files using rolling IQR (replaces 3 notebooks)."""
    if dry_run:
        return
    import subprocess
    scripts = [
        ["srcs/02_transform/02_generate_outliers/01_export_parquet.py", "--overwrite"],
        ["srcs/02_transform/02_generate_outliers/02_iqr_rolling.py"],
        ["srcs/02_transform/02_generate_outliers/03_export_csv.py"],
    ]
    for script_args in scripts:
        script = script_args[0]
        args = script_args[1:]
        print(f"\n[Generate Outliers] Running {Path(script).name}...")
        cmd = [sys.executable, str(PROJECT_ROOT / script)] + args
        result = subprocess.run(cmd)
        if result.returncode != 0:
            raise RuntimeError(f"Script failed: {script}")
        log.info("Hoàn tất script %s", Path(script).name)


def run_imputation(*, dry_run: bool) -> None:
    """Fill missing solar generation before generating outlier flags."""
    engine = database.get_sqlalchemy_engine()
    try:
        hybrid_imputation.run_hybrid_imputation(
            engine,
            execute=not dry_run,
        )
    finally:
        engine.dispose()
        log.info("Đã đóng SQLAlchemy engine của Hybrid Imputation")


def run_outlier(*, dry_run: bool) -> None:
    """Apply outlier flags to staging buffer tables."""
    full_csv, sparse_csv = _load_outlier_paths()
    log.info(
        "Apply outlier flags; mode=%s; full_csv=%s; sparse_csv=%s",
        "DRY-RUN" if dry_run else "EXECUTE",
        full_csv,
        sparse_csv,
    )
    connection = database.get_psycopg2_connection()
    try:
        pipeline_outlier.apply_outlier_flags(connection, full_csv, sparse_csv, execute=not dry_run)
        if dry_run:
            connection.rollback()
            log.info("Outlier dry-run hoàn tất; transaction đã rollback")
        else:
            connection.commit()
            log.info("Outlier flags đã được commit")
    except Exception:
        connection.rollback()
        log.exception("Apply outlier flags thất bại; transaction đã rollback")
        raise
    finally:
        connection.close()
        log.info("Đã đóng kết nối outlier")


def run_warehouse() -> None:
    """Load staging buffer tables into datawarehouse tables."""
    log.info("Mở transaction load Data Warehouse")
    connection = database.get_psycopg2_connection()
    try:
        with connection.cursor() as cursor:
            warehouse_loader.truncate_all_datawarehouse_tables(cursor)
            warehouse_loader.load_dim_solar_site(cursor)
            warehouse_loader.load_dim_weather_type(cursor)
            warehouse_loader.load_dim_geography(cursor)
            warehouse_loader.load_dim_date(cursor)
            warehouse_loader.load_dim_time(cursor)
            warehouse_loader.load_fact_solar_energy_gen(cursor)
            warehouse_loader.load_fact_weather(cursor)
        connection.commit()
        log.info("Data Warehouse đã load xong và commit")
    except Exception:
        connection.rollback()
        log.exception("Load Data Warehouse thất bại; transaction đã rollback")
        raise
    finally:
        connection.close()
        log.info("Đã đóng kết nối Data Warehouse")


def run_marts() -> None:
    """Build Data Marts (BI and ML)."""
    log.info("Mở SQLAlchemy engine cho Data Marts")
    engine = database.get_sqlalchemy_engine()
    try:
        log.info("Bắt đầu BI Mart")
        bi_mart_builder.build_bi_mart(engine)
        log.info("BI Mart thành công")
        log.info("Bắt đầu build BI hourly measures view")
        bi_view_builder.build_bi_view(engine)
        log.info("BI hourly measures view thành công")
        log.info("Bắt đầu ML Mart")
        ml_mart_builder.build_ml_mart(engine)
        log.info("ML Mart thành công")
    finally:
        engine.dispose()
        log.info("Đã đóng SQLAlchemy engine của Data Marts")


def run_bi_view() -> None:
    """Build BI Mart Hourly View independently."""
    log.info("Mở SQLAlchemy engine cho BI View")
    engine = database.get_sqlalchemy_engine()
    try:
        bi_view_builder.build_bi_view(engine)
    finally:
        engine.dispose()
        log.info("Đã đóng SQLAlchemy engine của BI View")

def run_stage(stage_number: int, title: str, runner) -> None:
    """Run one pipeline stage with consistent operational logging."""
    started = time.perf_counter()
    log.info("[%s/7] START | %s", stage_number, title)
    try:
        runner()
    except Exception:
        elapsed = time.perf_counter() - started
        log.exception("[%s/7] FAILED | %s | %.2fs", stage_number, title, elapsed)
        raise
    elapsed = time.perf_counter() - started
    log.info("[%s/7] SUCCESS | %s | %.2fs", stage_number, title, elapsed)


def run_pipeline(stage: str, *, dry_run: bool) -> None:
    sleep_sec = _load_sleep_seconds()

    if stage in {"staging", "all"}:
        print("\n[1/7] Object Storage → Raw Staging")
        run_stage(1, "Object Storage → Raw Staging", run_staging)
        if stage == "all":
            print(f"... Đang nghỉ {sleep_sec}s để nhả RAM và Database Connection ...")
            time.sleep(sleep_sec)

    if stage in {"transform", "all"}:
        print("\n[2/7] Raw Staging → Buffer")
        run_stage(
            2,
            "Raw Staging → Buffer",
            lambda: run_transform(dry_run=dry_run),
        )
        if stage == "all":
            print(f"... Đang nghỉ {sleep_sec}s để nhả RAM và Database Connection ...")
            time.sleep(sleep_sec)

    if stage in {"imputation", "all"}:
        print("\n[3/7] Fill Missing Solar Energy (Hybrid Imputation)")
        run_stage(
            3,
            "Fill Missing Solar Energy (Hybrid Imputation)",
            lambda: run_imputation(dry_run=dry_run),
        )
        if stage == "all":
            print(f"... Đang nghỉ {sleep_sec}s để nhả RAM và Database Connection ...")
            time.sleep(sleep_sec)

    if stage in {"generate_outliers", "all"}:
        if dry_run:
            print("\n[SKIP] Generate Outliers is disabled in dry-run mode.")
        else:
            print("\n[4/7] Generate Outliers CSV (Rolling IQR)")
            run_stage(
                4,
                "Generate Outliers CSV (Rolling IQR)",
                lambda: run_generate_outliers(dry_run=dry_run),
            )
            if stage == "all":
                print(f"... Đang nghỉ {sleep_sec}s để nhả RAM và Database Connection ...")
                time.sleep(sleep_sec)

    if stage in {"outlier", "all"}:
        print("\n[5/7] Apply Outlier Flags to Buffer")
        run_stage(
            5,
            "Apply Outlier Flags to Buffer",
            lambda: run_outlier(dry_run=dry_run),
        )
        if stage == "all":
            print(f"... Đang nghỉ {sleep_sec}s để nhả RAM và Database Connection ...")
            time.sleep(sleep_sec)

    if stage in {"load", "all"}:
        if dry_run:
            print("\n[SKIP] Data warehouse load is disabled in dry-run mode.")
        else:
            print("\n[6/7] Buffer → Data Warehouse")
            run_stage(6, "Buffer → Data Warehouse", run_warehouse)
            if stage == "all":
                print(f"... Đang nghỉ {sleep_sec}s để nhả RAM và Database Connection ...")
                time.sleep(sleep_sec)

    if stage in {"marts", "all"}:
        if dry_run:
            print("\n[SKIP] Data marts build is disabled in dry-run mode.")
        else:
            print("\n[7/7] Data Warehouse → Data Marts")
            run_stage(7, "Data Warehouse → Data Marts", run_marts)

    if stage == "bi_view":
        print("\n[Standalone] Build BI Mart Hourly View")
        run_stage(8, "Build BI Mart Hourly View", run_bi_view)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--stage",
        choices=(
            "staging",
            "transform",
            "imputation",
            "generate_outliers",
            "outlier",
            "load",
            "marts",
            "bi_view",
            "all",
        ),
        default="all",
        help="Pipeline stage to execute.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Calculate row counts without modifying DB.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    load_dotenv(ENV_FILE, override=True)
    pipeline_started = time.perf_counter()
    log.info(
        "Pipeline start | stage=%s | dry_run=%s | project=%s",
        args.stage,
        args.dry_run,
        PROJECT_ROOT,
    )

    if args.dry_run and args.stage not in {
        "transform",
        "imputation",
        "outlier",
    }:
        print(
            "[ERROR] --dry-run is only valid with --stage "
            "transform, imputation or outlier.",
            file=sys.stderr,
        )
        return 2

    try:
        run_pipeline(args.stage, dry_run=args.dry_run)
    except KeyboardInterrupt:
        log.warning("Pipeline bị người dùng ngắt")
        print("\n[CANCELLED] Pipeline interrupted.", file=sys.stderr)
        return 130
    except Exception as exc:
        log.error(
            "Pipeline failed | stage=%s | elapsed=%.2fs",
            args.stage,
            time.perf_counter() - pipeline_started,
        )
        print(f"\n[ERROR] Pipeline failed: {exc}", file=sys.stderr)
        return 1

    log.info(
        "Pipeline success | stage=%s | elapsed=%.2fs",
        args.stage,
        time.perf_counter() - pipeline_started,
    )
    print("\n[DONE] Pipeline completed successfully.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
