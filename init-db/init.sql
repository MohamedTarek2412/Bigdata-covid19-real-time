USE covid_db;

DROP TABLE IF EXISTS country_rankings;
DROP TABLE IF EXISTS covid_hotspots;
DROP TABLE IF EXISTS continent_covid_stats;
DROP TABLE IF EXISTS windowed_covid_stats;
DROP TABLE IF EXISTS covid_realtime_stats;

CREATE TABLE covid_realtime_stats (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    uuid VARCHAR(255),
    continent VARCHAR(100),
    location VARCHAR(100),
    iso_code VARCHAR(10),
    date DATE,
    timestamp DATETIME,
    total_cases FLOAT,
    new_cases FLOAT,
    total_deaths FLOAT,
    new_deaths FLOAT,
    active_cases FLOAT,
    population FLOAT,
    recovery_rate FLOAT,
    death_rate FLOAT,
    cases_per_million FLOAT,
    deaths_per_million FLOAT,
    new_cases_ratio FLOAT,
    cases_to_population_ratio FLOAT,
    is_hotspot VARCHAR(10) DEFAULT 'false',
    recovery_rate_calculated FLOAT,
    fatality_rate FLOAT,
    active_cases_ratio FLOAT,
    severity_level VARCHAR(20),
    growth_rate FLOAT,
    processing_time DATETIME DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX idx_location ON covid_realtime_stats(location);
CREATE INDEX idx_date ON covid_realtime_stats(date);
CREATE INDEX idx_timestamp ON covid_realtime_stats(timestamp);
CREATE INDEX idx_hotspot ON covid_realtime_stats(is_hotspot);

CREATE TABLE windowed_covid_stats (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    window_start DATETIME,
    window_end DATETIME,
    location VARCHAR(100),
    iso_code VARCHAR(10),
    total_new_cases_window FLOAT,
    total_new_deaths_window FLOAT,
    avg_death_rate_window FLOAT,
    max_total_cases FLOAT,
    latest_active_cases FLOAT,
    processed_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE continent_covid_stats (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    continent_window_start DATETIME,
    continent_window_end DATETIME,
    continent VARCHAR(100),
    continent_new_cases FLOAT,
    continent_new_deaths FLOAT,
    continent_avg_death_rate FLOAT,
    countries_count INT,
    continent_total_cases FLOAT,
    processed_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE covid_hotspots (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    location VARCHAR(100),
    iso_code VARCHAR(10),
    total_cases FLOAT,
    new_cases FLOAT,
    death_rate FLOAT,
    active_cases FLOAT,
    timestamp DATETIME,
    detected_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE country_rankings (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    location VARCHAR(100),
    iso_code VARCHAR(10),
    max_cases_country FLOAT,
    total_new_cases_country FLOAT,
    avg_death_rate_country FLOAT,
    ranking_position INT,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE OR REPLACE VIEW covid_dashboard_summary AS
SELECT 
    DATE(timestamp) AS report_date,
    COUNT(DISTINCT location) AS countries_count,
    SUM(total_cases) AS global_total_cases,
    SUM(new_cases) AS global_new_cases,
    SUM(total_deaths) AS global_total_deaths,
    SUM(new_deaths) AS global_new_deaths,
    AVG(death_rate) AS global_avg_death_rate,
    COUNT(CASE WHEN is_hotspot = 'true' THEN 1 END) AS hotspot_count
FROM covid_realtime_stats
GROUP BY DATE(timestamp);

CREATE OR REPLACE VIEW top_10_affected_countries AS
SELECT location, iso_code, MAX(total_cases) AS total_cases
FROM covid_realtime_stats
GROUP BY location, iso_code
ORDER BY total_cases DESC
LIMIT 10;
