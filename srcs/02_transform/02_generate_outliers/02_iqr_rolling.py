from pathlib import Path
import hashlib
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import yaml

pd.set_option('display.max_columns', 120)
pd.set_option('display.width', 180)


ROOT = Path(__file__).resolve().parents[3]
DEFAULT_CONFIG = ROOT / "config" / "02_transform" / "01_generate_outliers.yaml"

with DEFAULT_CONFIG.open(encoding="utf-8") as _f:
    _cfg = yaml.safe_load(_f)

SOLAR_PATH = ROOT / _cfg["paths"]["parquet_dir"] / "temp_fact_solar_energy_gen.parquet"
OUT_DIR = ROOT / _cfg["paths"]["iqr_out_dir"]
PIC_DIR = ROOT / _cfg["paths"]["iqr_pic_dir"]
OLD_CANDIDATE_PATHS = []  # optional legacy comparison, not required

# Algorithm params từ config
IQR_MULTIPLIER = _cfg["algorithm"]["iqr_multiplier"]
ROLLING_WINDOW = _cfg["algorithm"]["rolling_window"]
EWM_SPAN = _cfg["algorithm"]["ewm_span"]
ROLLING_MIN_PERIODS = _cfg["algorithm"]["rolling_min_periods"]

OUT_DIR.mkdir(parents=True, exist_ok=True)
PIC_DIR.mkdir(parents=True, exist_ok=True)

print('ROOT =', ROOT)
print('SOLAR_PATH exists =', SOLAR_PATH.exists(), SOLAR_PATH)
print('OUT_DIR =', OUT_DIR)
print('PIC_DIR =', PIC_DIR)


if not SOLAR_PATH.exists():
    raise FileNotFoundError(f'Missing solar parquet: {SOLAR_PATH}')

solar = pd.read_parquet(SOLAR_PATH, columns=['sitekey', 'timestamp', 'energy_generated_kwh'])
solar['timestamp'] = pd.to_datetime(solar['timestamp'])
solar['sitekey'] = solar['sitekey'].astype(str)
solar['hour'] = solar['timestamp'].dt.hour
solar = solar.sort_values(['sitekey', 'timestamp']).reset_index(drop=True)

load_audit = pd.DataFrame([{
    'solar_rows': len(solar),
    'site_count': solar['sitekey'].nunique(),
    'min_timestamp': solar['timestamp'].min(),
    'max_timestamp': solar['timestamp'].max(),
    'energy_null_rows': int(solar['energy_generated_kwh'].isna().sum()),
    'energy_negative_rows': int((solar['energy_generated_kwh'] < 0).sum()),
}])
load_audit.to_csv(OUT_DIR / '01_load_audit.csv', index=False)
print(load_audit.to_string(index=False))
load_audit


def build_time_rolling_features(df: pd.DataFrame) -> pd.DataFrame:
    parts = []
    for site, group in df.groupby('sitekey', sort=False):
        group = group.sort_values('timestamp').copy()
        rolling_mean = (
            group.set_index('timestamp')['energy_generated_kwh']
            .rolling(ROLLING_WINDOW, min_periods=ROLLING_MIN_PERIODS)
            .mean()
        )
        group['rolling_1h_mean'] = rolling_mean.to_numpy()
        parts.append(group)
    return pd.concat(parts, ignore_index=True)

solar_feat = build_time_rolling_features(solar)
solar_feat = solar_feat.sort_values(['sitekey', 'timestamp']).reset_index(drop=True)
solar_feat['rolling_1h_deviation'] = solar_feat['energy_generated_kwh'] - solar_feat['rolling_1h_mean']
solar_feat['rolling_1h_abs_deviation'] = solar_feat['rolling_1h_deviation'].abs()

solar_feat['ewm_span4_mean'] = (
    solar_feat.groupby('sitekey')['energy_generated_kwh']
    .transform(lambda s: s.ewm(span=EWM_SPAN, adjust=False, min_periods=ROLLING_MIN_PERIODS).mean())
)
solar_feat['ewm_span4_deviation'] = solar_feat['energy_generated_kwh'] - solar_feat['ewm_span4_mean']
solar_feat['ewm_span4_abs_deviation'] = solar_feat['ewm_span4_deviation'].abs()

feature_audit = solar_feat[['energy_generated_kwh', 'rolling_1h_mean', 'rolling_1h_abs_deviation', 'ewm_span4_mean', 'ewm_span4_abs_deviation']].describe().T
feature_audit.to_csv(OUT_DIR / '02_rolling_feature_audit.csv')
print('columns:', list(solar_feat.columns))
print(feature_audit.to_string())
solar_feat.head(10)


def add_iqr_candidate(df: pd.DataFrame, value_col: str, prefix: str):
    group_cols = ['sitekey', 'hour']
    stats = df.groupby(group_cols, observed=True).agg(
        context_rows=(value_col, 'size'),
        q1=(value_col, lambda s: s.quantile(.25)),
        median=(value_col, 'median'),
        q3=(value_col, lambda s: s.quantile(.75)),
        p95=(value_col, lambda s: s.quantile(.95)),
        p99=(value_col, lambda s: s.quantile(.99)),
        max_value=(value_col, 'max'),
    ).reset_index()
    stats['iqr'] = stats['q3'] - stats['q1']
    stats['upper_iqr'] = stats['q3'] + IQR_MULTIPLIER * stats['iqr']
    stats = stats.rename(columns={c: f'{prefix}_{c}' for c in ['context_rows','q1','median','q3','p95','p99','max_value','iqr','upper_iqr']})
    merged = df.merge(stats, on=group_cols, how='left')
    candidate_col = f'{prefix}_candidate'
    merged[candidate_col] = merged[value_col] > merged[f'{prefix}_upper_iqr']
    return merged, stats, candidate_col

rolling_df, rolling_stats, rolling_candidate_col = add_iqr_candidate(solar_feat, 'rolling_1h_abs_deviation', 'rolling_iqr')
ewm_df, ewm_stats, ewm_candidate_col = add_iqr_candidate(solar_feat, 'ewm_span4_abs_deviation', 'ewm_iqr')

rolling_stats.to_csv(OUT_DIR / '03_rolling_iqr_site_hour_stats.csv', index=False)
ewm_stats.to_csv(OUT_DIR / '04_ewm_iqr_site_hour_stats.csv', index=False)

rolling_candidates = rolling_df[rolling_df[rolling_candidate_col]].copy()
ewm_candidates = ewm_df[ewm_df[ewm_candidate_col]].copy()

rolling_cols = ['sitekey','timestamp','hour','energy_generated_kwh','rolling_1h_mean','rolling_1h_deviation','rolling_1h_abs_deviation','rolling_iqr_q1','rolling_iqr_median','rolling_iqr_q3','rolling_iqr_iqr','rolling_iqr_upper_iqr']
ewm_cols = ['sitekey','timestamp','hour','energy_generated_kwh','ewm_span4_mean','ewm_span4_deviation','ewm_span4_abs_deviation','ewm_iqr_q1','ewm_iqr_median','ewm_iqr_q3','ewm_iqr_iqr','ewm_iqr_upper_iqr']
rolling_candidates[rolling_cols].sort_values(['rolling_1h_abs_deviation','timestamp'], ascending=[False, True]).to_csv(OUT_DIR / '05_full_rolling_iqr_candidates.csv', index=False)
ewm_candidates[ewm_cols].sort_values(['ewm_span4_abs_deviation','timestamp'], ascending=[False, True]).to_csv(OUT_DIR / '06_full_ewm_iqr_candidates.csv', index=False)
rolling_candidates[rolling_cols].sort_values(['rolling_1h_abs_deviation','timestamp'], ascending=[False, True]).head(2000).to_csv(OUT_DIR / '07_top2000_rolling_iqr_candidates.csv', index=False)
ewm_candidates[ewm_cols].sort_values(['ewm_span4_abs_deviation','timestamp'], ascending=[False, True]).head(2000).to_csv(OUT_DIR / '08_top2000_ewm_iqr_candidates.csv', index=False)

summary_methods = pd.DataFrame([
    {'method': 'rolling_1h_abs_deviation_iqr_by_site_hour', 'total_rows': len(rolling_df), 'candidate_rows': len(rolling_candidates), 'candidate_pct': len(rolling_candidates) / len(rolling_df) * 100},
    {'method': 'ewm_span4_abs_deviation_iqr_by_site_hour', 'total_rows': len(ewm_df), 'candidate_rows': len(ewm_candidates), 'candidate_pct': len(ewm_candidates) / len(ewm_df) * 100},
])
summary_methods.to_csv(OUT_DIR / '09_method_candidate_summary.csv', index=False)
print(summary_methods.to_string(index=False))
summary_methods


old_path = next((p for p in OLD_CANDIDATE_PATHS if p.exists()), None)
if old_path is None:
    old_candidates = pd.DataFrame(columns=['sitekey','timestamp'])
    print('Old radiation candidate file not found. Skip overlap comparison.')
else:
    old_candidates = pd.read_csv(old_path, usecols=['sitekey','timestamp'])
    old_candidates['sitekey'] = old_candidates['sitekey'].astype(str)
    old_candidates['timestamp'] = pd.to_datetime(old_candidates['timestamp'])
    print('Old radiation candidate file:', old_path)
    print('Old radiation candidates:', len(old_candidates))

old_keys = set(zip(old_candidates['sitekey'], old_candidates['timestamp']))
rolling_keys = set(zip(rolling_candidates['sitekey'], rolling_candidates['timestamp']))
ewm_keys = set(zip(ewm_candidates['sitekey'], ewm_candidates['timestamp']))

def overlap_row(name, keys):
    overlap = len(keys & old_keys)
    return {
        'method': name,
        'candidate_rows': len(keys),
        'old_radiation_candidate_rows': len(old_keys),
        'overlap_with_old': overlap,
        'pct_of_method_overlapping_old': overlap / len(keys) * 100 if keys else 0,
        'pct_of_old_captured_by_method': overlap / len(old_keys) * 100 if old_keys else 0,
    }

overlap_summary = pd.DataFrame([
    overlap_row('rolling_1h_abs_deviation_iqr_by_site_hour', rolling_keys),
    overlap_row('ewm_span4_abs_deviation_iqr_by_site_hour', ewm_keys),
])
overlap_summary.to_csv(OUT_DIR / '10_overlap_with_old_radiation_iqr.csv', index=False)
print(overlap_summary.to_string(index=False))
overlap_summary


plt.style.use('seaborn-v0_8-whitegrid')

all_hours = list(range(24))
sitekeys = sorted(solar_feat['sitekey'].dropna().unique(), key=lambda x: int(x) if str(x).isdigit() else str(x))

def boxplot_by_hour(df, value_col, title, out_name, showfliers):
    data = [df.loc[df['hour'] == h, value_col].dropna().to_numpy() for h in all_hours]
    fig, ax = plt.subplots(figsize=(16, 6))
    ax.boxplot(data, tick_labels=[str(h) for h in all_hours], showfliers=showfliers, flierprops={'marker': '.', 'markersize': 0.8, 'alpha': 0.08})
    ax.set_title(title)
    ax.set_xlabel('Hour')
    ax.set_ylabel(value_col)
    ax.grid(axis='y', alpha=0.3)
    fig.tight_layout()
    fig.savefig(PIC_DIR / out_name, dpi=180)
    plt.close(fig)

boxplot_by_hour(rolling_df, 'rolling_1h_abs_deviation', 'Rolling 1h abs deviation by all 24 hours - with fliers', '01_boxplot_rolling_abs_deviation_by_hour_with_fliers.png', True)
boxplot_by_hour(rolling_df, 'rolling_1h_abs_deviation', 'Rolling 1h abs deviation by all 24 hours - no fliers', '02_boxplot_rolling_abs_deviation_by_hour_no_fliers.png', False)
boxplot_by_hour(ewm_df, 'ewm_span4_abs_deviation', 'EWM span4 abs deviation by all 24 hours - with fliers', '03_boxplot_ewm_abs_deviation_by_hour_with_fliers.png', True)
boxplot_by_hour(ewm_df, 'ewm_span4_abs_deviation', 'EWM span4 abs deviation by all 24 hours - no fliers', '04_boxplot_ewm_abs_deviation_by_hour_no_fliers.png', False)

# Site-level boxplots for both methods.
def boxplot_by_site(df, value_col, title, out_name, showfliers):
    data = [df.loc[df['sitekey'] == site, value_col].dropna().to_numpy() for site in sitekeys]
    fig, ax = plt.subplots(figsize=(18, 6))
    ax.boxplot(data, tick_labels=sitekeys, showfliers=showfliers, flierprops={'marker': '.', 'markersize': 0.7, 'alpha': 0.06})
    ax.set_title(title)
    ax.set_xlabel('Sitekey')
    ax.set_ylabel(value_col)
    ax.tick_params(axis='x', labelrotation=90)
    ax.grid(axis='y', alpha=0.3)
    fig.tight_layout()
    fig.savefig(PIC_DIR / out_name, dpi=180)
    plt.close(fig)

boxplot_by_site(rolling_df, 'rolling_1h_abs_deviation', 'Rolling 1h abs deviation by all sitekeys - with fliers', '05_boxplot_rolling_abs_deviation_by_site_with_fliers.png', True)
boxplot_by_site(rolling_df, 'rolling_1h_abs_deviation', 'Rolling 1h abs deviation by all sitekeys - no fliers', '06_boxplot_rolling_abs_deviation_by_site_no_fliers.png', False)
boxplot_by_site(ewm_df, 'ewm_span4_abs_deviation', 'EWM span4 abs deviation by all sitekeys - with fliers', '07_boxplot_ewm_abs_deviation_by_site_with_fliers.png', True)
boxplot_by_site(ewm_df, 'ewm_span4_abs_deviation', 'EWM span4 abs deviation by all sitekeys - no fliers', '08_boxplot_ewm_abs_deviation_by_site_no_fliers.png', False)

print('Wrote figures to', PIC_DIR)
for path in sorted(PIC_DIR.glob('*.png')):
    print(path.name)


checks = []
def add_check(name, passed, detail):
    checks.append({'check_name': name, 'passed': bool(passed), 'detail': detail})

expected_figures = [
    '01_boxplot_rolling_abs_deviation_by_hour_with_fliers.png',
    '02_boxplot_rolling_abs_deviation_by_hour_no_fliers.png',
    '03_boxplot_ewm_abs_deviation_by_hour_with_fliers.png',
    '04_boxplot_ewm_abs_deviation_by_hour_no_fliers.png',
    '05_boxplot_rolling_abs_deviation_by_site_with_fliers.png',
    '06_boxplot_rolling_abs_deviation_by_site_no_fliers.png',
    '07_boxplot_ewm_abs_deviation_by_site_with_fliers.png',
    '08_boxplot_ewm_abs_deviation_by_site_no_fliers.png',
]
missing_figures = [name for name in expected_figures if not (PIC_DIR / name).exists()]

add_check('solar row count unchanged', len(solar_feat) == len(solar), f'solar_feat={len(solar_feat)}, solar={len(solar)}')
add_check('rolling candidates nonzero', len(rolling_candidates) > 0, len(rolling_candidates))
add_check('ewm candidates nonzero', len(ewm_candidates) > 0, len(ewm_candidates))
add_check('expected figures exist', len(missing_figures) == 0, f'missing={missing_figures}')
add_check('candidate csv exists rolling', (OUT_DIR / '05_full_rolling_iqr_candidates.csv').exists(), str(OUT_DIR / '05_full_rolling_iqr_candidates.csv'))
add_check('candidate csv exists ewm', (OUT_DIR / '06_full_ewm_iqr_candidates.csv').exists(), str(OUT_DIR / '06_full_ewm_iqr_candidates.csv'))

validation = pd.DataFrame(checks)
validation.to_csv(OUT_DIR / '11_validation_checks.csv', index=False)
print(validation.to_string(index=False))
if not validation['passed'].all():
    raise RuntimeError('Validation failed')

# Recommendation text is based on method properties, not only candidate count.
recommendation = f"""
# Rolling IQR Comparison Report

## Why change from radiation-context IQR?
The previous radiation-context method joins hourly weather to 15-minute solar rows. It does not create new weather values, but four 15-minute rows within the same hour share the same weather context. To avoid that assumption, this notebook uses only the 15-minute solar generation sequence.

## Methods compared

1. Old radiation-context IQR: sitekey + hour + radiation_quantile_band.
2. Rolling IQR: IQR on absolute deviation from trailing 1-hour rolling mean, grouped by sitekey + hour.
3. EWM IQR: IQR on absolute deviation from EWM span=4 mean, grouped by sitekey + hour.

## Results

{summary_methods.to_string(index=False)}

## Overlap with old radiation-context method

{overlap_summary.to_string(index=False)}

## Recommendation

Use Rolling IQR as the main visualization/audit baseline for the current report because it:

- keeps the original 15-minute grain;
- avoids weather-hour join assumptions;
- does not interpolate or fake weather data;
- compares each point against the recent behavior of the same site;
- remains explainable with IQR and boxplots.

Use EWM IQR as a sensitivity check. EWM reacts faster to recent changes, so it can be useful for sudden transitions, but it is more model-like and less straightforward to explain than simple trailing 1-hour rolling mean.

Raw/Data Warehouse should remain unchanged. Candidate rows should be exported for review and optional training-clean experiments only.
"""
(OUT_DIR / '12_rolling_iqr_comparison_report.md').write_text(recommendation)
print(recommendation)


checksum_rows = []
for path in sorted([*OUT_DIR.glob('*'), *PIC_DIR.glob('*.png')]):
    if path.is_file():
        checksum_rows.append({'file': str(path.relative_to(ROOT)), 'sha256': hashlib.sha256(path.read_bytes()).hexdigest()})
checksums = pd.DataFrame(checksum_rows)
checksums.to_csv(OUT_DIR / '99_sha256_checksums.csv', index=False)
print(checksums.to_string(index=False))
