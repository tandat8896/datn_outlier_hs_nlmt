create schema IF not exists staging;

-- 1. Bảng Staging: stg_dim_solar_site
create table staging.stg_dim_solar_site (
  site_id VARCHAR(255),
  campus_name VARCHAR(255),
  capacity_kw VARCHAR(255),
  Number_of_panels VARCHAR(255),
  Panel VARCHAR(255),
  Inverter VARCHAR(255),
  Optimizers VARCHAR(255),
  Metric VARCHAR(255)
);

-- 2. Bảng Staging: stg_dim_geography
create table staging.stg_dim_geography (
  geo_id VARCHAR(255),
  latitude VARCHAR(255),
  longitude VARCHAR(255),
  location_name VARCHAR(255)
);

-- 3. Bảng Staging: stg_dim_date
create table staging.stg_dim_date (
  date_id VARCHAR(255),
  full_date VARCHAR(255),
  day VARCHAR(255),
  month VARCHAR(255),
  year VARCHAR(255)
);

-- 4. Bảng Staging: stg_dim_time
create table staging.stg_dim_time (
  time_id VARCHAR(255),
  time_string VARCHAR(255),
  hour VARCHAR(255),
  minute VARCHAR(255)
);

-- 5. Bảng Staging: stg_dim_weather_type
create table staging.stg_dim_weather_type (
  weather_type_id VARCHAR(255),
  weather_code VARCHAR(255),
  is_day VARCHAR(255),
  weather_condition VARCHAR(255),
  description VARCHAR(255)
);

-- 6. Bảng Staging: stg_fact_solar_energy_gen
create table staging.stg_fact_solar_energy_gen (
  gen_id VARCHAR(255),
  site_id VARCHAR(255),
  geo_id VARCHAR(255),
  date_id VARCHAR(255),
  time_id VARCHAR(255),
  energy_generated_kwh VARCHAR(255)
);

-- 7. Bảng Staging: stg_fact_weather
create table staging.stg_fact_weather (
  weather_id VARCHAR(255),
  geo_id VARCHAR(255),
  date_id VARCHAR(255),
  time_id VARCHAR(255),
  weather_type_id VARCHAR(255),
  is_day VARCHAR(255),
  shortwave_radiation VARCHAR(255),
  temperature_c VARCHAR(255),
  cloud_cover_total VARCHAR(255),
  cloud_cover_low VARCHAR(255),
  cloud_cover_mid VARCHAR(255),
  cloud_cover_high VARCHAR(255),
  Diffuse_Solar_Radiation VARCHAR(255),
  Direct_Normal_Irradiance VARCHAR(255),
  wind_speed VARCHAR(255),
  precipitation_mm VARCHAR(255),
  Sunshine_Duration VARCHAR(255)
);