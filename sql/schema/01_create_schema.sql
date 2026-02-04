-- =====================================================
-- MTA Transit Data Analytics Dashboard
-- Database Schema Creation Script
-- =====================================================
-- Author: Aditya Bhuran
-- Created: 2026-02-03
-- Description: Complete PostgreSQL schema for MTA transit data
-- =====================================================

-- Create database (run separately if needed)
-- CREATE DATABASE mta_transit_db;

-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";

-- =====================================================
-- DIMENSION TABLES
-- =====================================================

-- Subway Lines Dimension Table
DROP TABLE IF EXISTS dim_subway_lines CASCADE;
CREATE TABLE dim_subway_lines (
    line_id SERIAL PRIMARY KEY,
    line_name VARCHAR(10) NOT NULL UNIQUE,
    line_color VARCHAR(20),
    line_group VARCHAR(50),
    route_type VARCHAR(50),
    service_pattern VARCHAR(20) DEFAULT 'Local',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE dim_subway_lines IS 'Dimension table containing MTA subway line information';

-- Stations Dimension Table
DROP TABLE IF EXISTS dim_stations CASCADE;
CREATE TABLE dim_stations (
    station_id SERIAL PRIMARY KEY,
    station_name VARCHAR(200) NOT NULL,
    station_complex_id VARCHAR(50),
    gtfs_stop_id VARCHAR(20),
    borough VARCHAR(50),
    latitude DECIMAL(10, 8),
    longitude DECIMAL(11, 8),
    structure_type VARCHAR(50),
    ada_accessible BOOLEAN DEFAULT FALSE,
    ada_notes TEXT,
    lines_served VARCHAR(100),
    division VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE dim_stations IS 'Dimension table containing MTA subway station information';

-- Create spatial index for geographic queries
CREATE INDEX idx_stations_coordinates ON dim_stations(latitude, longitude);
CREATE INDEX idx_stations_borough ON dim_stations(borough);
CREATE INDEX idx_stations_name ON dim_stations USING gin(station_name gin_trgm_ops);

-- Date Dimension Table
DROP TABLE IF EXISTS dim_date CASCADE;
CREATE TABLE dim_date (
    date_key INTEGER PRIMARY KEY,
    full_date DATE NOT NULL UNIQUE,
    day_of_week INTEGER,
    day_name VARCHAR(15),
    day_of_month INTEGER,
    day_of_year INTEGER,
    week_of_year INTEGER,
    month_number INTEGER,
    month_name VARCHAR(15),
    quarter INTEGER,
    year INTEGER,
    is_weekend BOOLEAN,
    is_holiday BOOLEAN DEFAULT FALSE,
    holiday_name VARCHAR(100),
    fiscal_year INTEGER,
    fiscal_quarter INTEGER
);

COMMENT ON TABLE dim_date IS 'Date dimension table for time-based analysis';

-- Time Dimension Table
DROP TABLE IF EXISTS dim_time CASCADE;
CREATE TABLE dim_time (
    time_key INTEGER PRIMARY KEY,
    full_time TIME NOT NULL,
    hour INTEGER,
    minute INTEGER,
    hour_12 INTEGER,
    am_pm VARCHAR(2),
    time_period VARCHAR(20),
    is_peak_hour BOOLEAN,
    peak_type VARCHAR(20)
);

COMMENT ON TABLE dim_time IS 'Time dimension table for hourly analysis';

-- =====================================================
-- FACT TABLES
-- =====================================================

-- Ridership Fact Table
DROP TABLE IF EXISTS fact_ridership CASCADE;
CREATE TABLE fact_ridership (
    ridership_id SERIAL PRIMARY KEY,
    date_key INTEGER REFERENCES dim_date(date_key),
    time_key INTEGER REFERENCES dim_time(time_key),
    station_id INTEGER REFERENCES dim_stations(station_id),
    line_id INTEGER REFERENCES dim_subway_lines(line_id),
    entries INTEGER DEFAULT 0,
    exits INTEGER DEFAULT 0,
    total_traffic INTEGER GENERATED ALWAYS AS (entries + exits) STORED,
    data_source VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE fact_ridership IS 'Fact table containing ridership data per station, line, date, and time';

CREATE INDEX idx_ridership_date ON fact_ridership(date_key);
CREATE INDEX idx_ridership_station ON fact_ridership(station_id);
CREATE INDEX idx_ridership_line ON fact_ridership(line_id);
CREATE INDEX idx_ridership_composite ON fact_ridership(date_key, station_id, line_id);

-- Daily Ridership Summary Table
DROP TABLE IF EXISTS fact_daily_ridership CASCADE;
CREATE TABLE fact_daily_ridership (
    daily_ridership_id SERIAL PRIMARY KEY,
    date_key INTEGER REFERENCES dim_date(date_key),
    station_id INTEGER REFERENCES dim_stations(station_id),
    line_id INTEGER REFERENCES dim_subway_lines(line_id),
    total_entries INTEGER DEFAULT 0,
    total_exits INTEGER DEFAULT 0,
    peak_hour_entries INTEGER DEFAULT 0,
    off_peak_entries INTEGER DEFAULT 0,
    avg_entries_per_hour DECIMAL(10, 2),
    data_source VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(date_key, station_id, line_id)
);

COMMENT ON TABLE fact_daily_ridership IS 'Daily aggregated ridership summary';

CREATE INDEX idx_daily_ridership_date ON fact_daily_ridership(date_key);
CREATE INDEX idx_daily_ridership_station ON fact_daily_ridership(station_id);

-- Delay Incidents Fact Table
DROP TABLE IF EXISTS fact_delays CASCADE;
CREATE TABLE fact_delays (
    delay_id SERIAL PRIMARY KEY,
    date_key INTEGER REFERENCES dim_date(date_key),
    time_key INTEGER REFERENCES dim_time(time_key),
    line_id INTEGER REFERENCES dim_subway_lines(line_id),
    station_id INTEGER REFERENCES dim_stations(station_id),
    delay_duration_minutes INTEGER,
    delay_category VARCHAR(50),
    delay_reason VARCHAR(200),
    affected_services VARCHAR(100),
    is_resolved BOOLEAN DEFAULT TRUE,
    resolution_time_minutes INTEGER,
    passenger_impact_estimate INTEGER,
    severity_level VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE fact_delays IS 'Fact table containing delay incident data';

CREATE INDEX idx_delays_date ON fact_delays(date_key);
CREATE INDEX idx_delays_line ON fact_delays(line_id);
CREATE INDEX idx_delays_severity ON fact_delays(severity_level);

-- On-Time Performance Fact Table
DROP TABLE IF EXISTS fact_performance CASCADE;
CREATE TABLE fact_performance (
    performance_id SERIAL PRIMARY KEY,
    date_key INTEGER REFERENCES dim_date(date_key),
    line_id INTEGER REFERENCES dim_subway_lines(line_id),
    scheduled_trips INTEGER,
    actual_trips INTEGER,
    on_time_trips INTEGER,
    late_trips INTEGER,
    canceled_trips INTEGER,
    on_time_percentage DECIMAL(5, 2) GENERATED ALWAYS AS (
        CASE WHEN scheduled_trips > 0 
        THEN (on_time_trips::DECIMAL / scheduled_trips * 100) 
        ELSE 0 END
    ) STORED,
    mean_distance_between_failures DECIMAL(10, 2),
    wait_assessment DECIMAL(5, 2),
    customer_journey_time_performance DECIMAL(5, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(date_key, line_id)
);

COMMENT ON TABLE fact_performance IS 'Daily on-time performance metrics by line';

CREATE INDEX idx_performance_date ON fact_performance(date_key);
CREATE INDEX idx_performance_line ON fact_performance(line_id);

-- Service Alerts Table
DROP TABLE IF EXISTS fact_service_alerts CASCADE;
CREATE TABLE fact_service_alerts (
    alert_id SERIAL PRIMARY KEY,
    date_key INTEGER REFERENCES dim_date(date_key),
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    line_id INTEGER REFERENCES dim_subway_lines(line_id),
    alert_type VARCHAR(100),
    header_text TEXT,
    description_text TEXT,
    affected_stations TEXT[],
    is_planned BOOLEAN DEFAULT FALSE,
    cause VARCHAR(100),
    effect VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE fact_service_alerts IS 'Service alerts and disruption information';

CREATE INDEX idx_alerts_date ON fact_service_alerts(date_key);
CREATE INDEX idx_alerts_line ON fact_service_alerts(line_id);
CREATE INDEX idx_alerts_type ON fact_service_alerts(alert_type);

-- =====================================================
-- STAGING TABLES (for ETL processing)
-- =====================================================

DROP TABLE IF EXISTS stg_ridership CASCADE;
CREATE TABLE stg_ridership (
    stg_id SERIAL PRIMARY KEY,
    station_name VARCHAR(200),
    line_name VARCHAR(50),
    date_value DATE,
    time_value TIME,
    entries INTEGER,
    exits INTEGER,
    raw_data JSONB,
    processed BOOLEAN DEFAULT FALSE,
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

DROP TABLE IF EXISTS stg_delays CASCADE;
CREATE TABLE stg_delays (
    stg_id SERIAL PRIMARY KEY,
    line_name VARCHAR(50),
    station_name VARCHAR(200),
    date_value DATE,
    time_value TIME,
    delay_minutes INTEGER,
    reason VARCHAR(200),
    raw_data JSONB,
    processed BOOLEAN DEFAULT FALSE,
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =====================================================
-- ETL LOGGING TABLE
-- =====================================================

DROP TABLE IF EXISTS etl_log CASCADE;
CREATE TABLE etl_log (
    log_id SERIAL PRIMARY KEY,
    job_name VARCHAR(100) NOT NULL,
    job_type VARCHAR(50),
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP,
    status VARCHAR(20),
    records_processed INTEGER DEFAULT 0,
    records_inserted INTEGER DEFAULT 0,
    records_updated INTEGER DEFAULT 0,
    records_failed INTEGER DEFAULT 0,
    error_message TEXT,
    execution_details JSONB
);

COMMENT ON TABLE etl_log IS 'ETL job execution logging';

CREATE INDEX idx_etl_log_job ON etl_log(job_name);
CREATE INDEX idx_etl_log_status ON etl_log(status);
CREATE INDEX idx_etl_log_time ON etl_log(start_time DESC);

-- =====================================================
-- FUNCTIONS AND PROCEDURES
-- =====================================================

-- Function to update timestamp on record modification
CREATE OR REPLACE FUNCTION update_modified_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Apply update trigger to dimension tables
CREATE TRIGGER trg_subway_lines_update
    BEFORE UPDATE ON dim_subway_lines
    FOR EACH ROW EXECUTE FUNCTION update_modified_timestamp();

CREATE TRIGGER trg_stations_update
    BEFORE UPDATE ON dim_stations
    FOR EACH ROW EXECUTE FUNCTION update_modified_timestamp();

-- Function to populate date dimension
CREATE OR REPLACE FUNCTION populate_date_dimension(start_date DATE, end_date DATE)
RETURNS INTEGER AS $$
DECLARE
    current_date_val DATE;
    records_inserted INTEGER := 0;
BEGIN
    current_date_val := start_date;
    
    WHILE current_date_val <= end_date LOOP
        INSERT INTO dim_date (
            date_key,
            full_date,
            day_of_week,
            day_name,
            day_of_month,
            day_of_year,
            week_of_year,
            month_number,
            month_name,
            quarter,
            year,
            is_weekend,
            fiscal_year,
            fiscal_quarter
        ) VALUES (
            TO_CHAR(current_date_val, 'YYYYMMDD')::INTEGER,
            current_date_val,
            EXTRACT(DOW FROM current_date_val),
            TO_CHAR(current_date_val, 'Day'),
            EXTRACT(DAY FROM current_date_val),
            EXTRACT(DOY FROM current_date_val),
            EXTRACT(WEEK FROM current_date_val),
            EXTRACT(MONTH FROM current_date_val),
            TO_CHAR(current_date_val, 'Month'),
            EXTRACT(QUARTER FROM current_date_val),
            EXTRACT(YEAR FROM current_date_val),
            EXTRACT(DOW FROM current_date_val) IN (0, 6),
            CASE WHEN EXTRACT(MONTH FROM current_date_val) >= 7 
                 THEN EXTRACT(YEAR FROM current_date_val) + 1 
                 ELSE EXTRACT(YEAR FROM current_date_val) END,
            CASE 
                WHEN EXTRACT(MONTH FROM current_date_val) IN (7, 8, 9) THEN 1
                WHEN EXTRACT(MONTH FROM current_date_val) IN (10, 11, 12) THEN 2
                WHEN EXTRACT(MONTH FROM current_date_val) IN (1, 2, 3) THEN 3
                ELSE 4
            END
        ) ON CONFLICT (date_key) DO NOTHING;
        
        records_inserted := records_inserted + 1;
        current_date_val := current_date_val + 1;
    END LOOP;
    
    RETURN records_inserted;
END;
$$ LANGUAGE plpgsql;

-- Function to populate time dimension
CREATE OR REPLACE FUNCTION populate_time_dimension()
RETURNS INTEGER AS $$
DECLARE
    hour_val INTEGER;
    minute_val INTEGER;
    time_key_val INTEGER;
    records_inserted INTEGER := 0;
BEGIN
    FOR hour_val IN 0..23 LOOP
        FOR minute_val IN 0..59 LOOP
            time_key_val := hour_val * 100 + minute_val;
            
            INSERT INTO dim_time (
                time_key,
                full_time,
                hour,
                minute,
                hour_12,
                am_pm,
                time_period,
                is_peak_hour,
                peak_type
            ) VALUES (
                time_key_val,
                MAKE_TIME(hour_val, minute_val, 0),
                hour_val,
                minute_val,
                CASE WHEN hour_val = 0 THEN 12 
                     WHEN hour_val > 12 THEN hour_val - 12 
                     ELSE hour_val END,
                CASE WHEN hour_val < 12 THEN 'AM' ELSE 'PM' END,
                CASE 
                    WHEN hour_val BETWEEN 7 AND 9 THEN 'Morning Rush'
                    WHEN hour_val BETWEEN 10 AND 15 THEN 'Midday'
                    WHEN hour_val BETWEEN 16 AND 19 THEN 'Evening Rush'
                    WHEN hour_val BETWEEN 20 AND 23 THEN 'Night'
                    ELSE 'Late Night'
                END,
                (hour_val BETWEEN 7 AND 9) OR (hour_val BETWEEN 16 AND 19),
                CASE 
                    WHEN hour_val BETWEEN 7 AND 9 THEN 'Morning'
                    WHEN hour_val BETWEEN 16 AND 19 THEN 'Evening'
                    ELSE NULL
                END
            ) ON CONFLICT (time_key) DO NOTHING;
            
            records_inserted := records_inserted + 1;
        END LOOP;
    END LOOP;
    
    RETURN records_inserted;
END;
$$ LANGUAGE plpgsql;

-- =====================================================
-- INITIAL DATA POPULATION
-- =====================================================

-- Insert Subway Lines
INSERT INTO dim_subway_lines (line_name, line_color, line_group, route_type, service_pattern) VALUES
    ('1', '#EE352E', 'Broadway-7th Avenue', 'Subway', 'Local'),
    ('2', '#EE352E', 'Broadway-7th Avenue', 'Subway', 'Express'),
    ('3', '#EE352E', 'Broadway-7th Avenue', 'Subway', 'Express'),
    ('4', '#00933C', 'Lexington Avenue', 'Subway', 'Express'),
    ('5', '#00933C', 'Lexington Avenue', 'Subway', 'Express'),
    ('6', '#00933C', 'Lexington Avenue', 'Subway', 'Local'),
    ('7', '#B933AD', 'Flushing', 'Subway', 'Local'),
    ('A', '#0039A6', '8th Avenue', 'Subway', 'Express'),
    ('B', '#FF6319', '6th Avenue', 'Subway', 'Express'),
    ('C', '#0039A6', '8th Avenue', 'Subway', 'Local'),
    ('D', '#FF6319', '6th Avenue', 'Subway', 'Express'),
    ('E', '#0039A6', '8th Avenue', 'Subway', 'Express'),
    ('F', '#FF6319', '6th Avenue', 'Subway', 'Local'),
    ('G', '#6CBE45', 'Crosstown', 'Subway', 'Local'),
    ('J', '#996633', 'Nassau Street', 'Subway', 'Local'),
    ('L', '#A7A9AC', 'Canarsie', 'Subway', 'Local'),
    ('M', '#FF6319', '6th Avenue', 'Subway', 'Local'),
    ('N', '#FCCC0A', 'Broadway', 'Subway', 'Express'),
    ('Q', '#FCCC0A', 'Broadway', 'Subway', 'Express'),
    ('R', '#FCCC0A', 'Broadway', 'Subway', 'Local'),
    ('S', '#808183', 'Shuttle', 'Subway', 'Shuttle'),
    ('W', '#FCCC0A', 'Broadway', 'Subway', 'Local'),
    ('Z', '#996633', 'Nassau Street', 'Subway', 'Express')
ON CONFLICT (line_name) DO NOTHING;

-- Populate date dimension for 2024-2026
SELECT populate_date_dimension('2024-01-01', '2026-12-31');

-- Populate time dimension
SELECT populate_time_dimension();

-- =====================================================
-- GRANT PERMISSIONS (adjust as needed)
-- =====================================================

-- Create read-only role for Power BI connection
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'powerbi_reader') THEN
        CREATE ROLE powerbi_reader WITH LOGIN PASSWORD 'powerbi_readonly_2024';
    END IF;
END
$$;

GRANT CONNECT ON DATABASE mta_transit_db TO powerbi_reader;
GRANT USAGE ON SCHEMA public TO powerbi_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO powerbi_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO powerbi_reader;

-- =====================================================
-- VERIFICATION QUERIES
-- =====================================================

-- Verify table creation
SELECT 
    table_name,
    pg_size_pretty(pg_total_relation_size(quote_ident(table_name))) as total_size
FROM information_schema.tables 
WHERE table_schema = 'public' 
AND table_type = 'BASE TABLE'
ORDER BY table_name;

-- Verify dimension table populations
SELECT 'dim_subway_lines' as table_name, COUNT(*) as record_count FROM dim_subway_lines
UNION ALL
SELECT 'dim_date', COUNT(*) FROM dim_date
UNION ALL
SELECT 'dim_time', COUNT(*) FROM dim_time;
