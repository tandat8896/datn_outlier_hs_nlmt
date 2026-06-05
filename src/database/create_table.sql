-- ==============================================================================
-- PHẦN 1: KHỞI TẠO CÁC BẢNG DIMENSION (BẢNG CHIỀU)
-- Lưu ý: Tạo các bảng Dimension không có khóa ngoại trước.
-- ==============================================================================

-- 1. Bảng Dim_Solar_Site
CREATE TABLE dim_solar_site (
    site_id INT PRIMARY KEY,
    campus_name VARCHAR(255),
    capacity_kw FLOAT,
    Number_of_panels INT,
    Panel VARCHAR(255),
    Inverter VARCHAR(255),
    Optimizers VARCHAR(255),
    Metric VARCHAR(50)
);

-- 2. Bảng Dim_Geography
CREATE TABLE dim_geography (
    geo_id INT PRIMARY KEY,
    latitude FLOAT,
    longitude FLOAT,
    location_name VARCHAR(255),
    capacity INT
);

-- 3. Bảng Dim_Date
CREATE TABLE dim_date (
    date_id INT PRIMARY KEY,
    full_date DATE,
    day INT,
    month INT,
    year INT,
    is_holiday INT,
    is_semester INT,
    is_exam INT
);

-- 4. Bảng Dim_Time
CREATE TABLE dim_time (
    time_id INT PRIMARY KEY,
    time_string VARCHAR(50),
    hour INT,
    minute INT
);

-- 5. Bảng Dim_Weather_Type
CREATE TABLE dim_weather_type (
    weather_type_id INT PRIMARY KEY,
    weather_code INT,
    is_day INT,
    weather_condition VARCHAR(255),
    description VARCHAR(255)
);

-- ==============================================================================
-- PHẦN 2: KHỞI TẠO CÁC BẢNG FACT (BẢNG SỰ KIỆN)
-- Thiết lập Khóa chính (PK) và Khóa ngoại (FK) kết nối về các bảng Dimension
-- ==============================================================================

-- 6. Bảng Fact_Solar_Energy_Gen
CREATE TABLE fact_solar_energy_gen (
    gen_id INT PRIMARY KEY,
    site_id INT,
    geo_id INT,
    date_id INT,
    time_id INT,
    energy_generated_kwh FLOAT,
    
    -- Thiết lập các khóa ngoại
    CONSTRAINT FK_Gen_Site FOREIGN KEY (site_id) REFERENCES dim_solar_site(site_id),
    CONSTRAINT FK_Gen_Geo FOREIGN KEY (geo_id) REFERENCES dim_geography(geo_id),
    CONSTRAINT FK_Gen_Date FOREIGN KEY (date_id) REFERENCES dim_date(date_id),
    CONSTRAINT FK_Gen_Time FOREIGN KEY (time_id) REFERENCES dim_time(time_id)
);

-- 7. Bảng Fact_Weather
CREATE TABLE fact_weather (
    weather_id INT PRIMARY KEY,
    geo_id INT,
    date_id INT,
    time_id INT,
    weather_type_id INT,
    
    is_day INT,
    shortwave_radiation INT,
    temperature_c FLOAT,
    cloud_cover_total FLOAT,
    cloud_cover_low FLOAT,
    cloud_cover_mid FLOAT,
    cloud_cover_high FLOAT,
    Diffuse_Solar_Radiation INT,
    Direct_Normal_Irradiance INT,
    wind_speed FLOAT,
    precipitation_mm FLOAT,
    Sunshine_Duration FLOAT,
    
    -- Thiết lập các khóa ngoại
    CONSTRAINT FK_Weather_Geo FOREIGN KEY (geo_id) REFERENCES dim_geography(geo_id),
    CONSTRAINT FK_Weather_Date FOREIGN KEY (date_id) REFERENCES dim_date(date_id),
    CONSTRAINT FK_Weather_Time FOREIGN KEY (time_id) REFERENCES dim_time(time_id),
    CONSTRAINT FK_Weather_Type FOREIGN KEY (weather_type_id) REFERENCES dim_weather_type(weather_type_id)
);