-- =====================================================
-- MTA Transit Data Analytics Dashboard
-- Analytics Queries and Views
-- =====================================================
-- Author: Aditya Bhuran
-- Description: SQL queries for data analysis and Power BI consumption
-- =====================================================

-- =====================================================
-- AGGREGATED VIEWS FOR POWER BI
-- =====================================================

-- View: Daily Ridership Summary
CREATE OR REPLACE VIEW vw_daily_ridership AS
SELECT 
    d.full_date,
    d.day_name,
    d.day_of_week,
    d.month_name,
    d.month_number,
    d.quarter,
    d.year,
    d.is_weekend,
    d.is_holiday,
    s.station_name,
    s.borough,
    s.latitude,
    s.longitude,
    l.line_name,
    l.line_color,
    l.line_group,
    SUM(r.entries) AS total_entries,
    SUM(r.exits) AS total_exits,
    SUM(r.total_traffic) AS total_traffic,
    AVG(r.entries) AS avg_entries,
    COUNT(*) AS record_count
FROM fact_ridership r
JOIN dim_date d ON r.date_key = d.date_key
LEFT JOIN dim_stations s ON r.station_id = s.station_id
LEFT JOIN dim_subway_lines l ON r.line_id = l.line_id
GROUP BY 
    d.full_date, d.day_name, d.day_of_week, d.month_name, d.month_number,
    d.quarter, d.year, d.is_weekend, d.is_holiday,
    s.station_name, s.borough, s.latitude, s.longitude,
    l.line_name, l.line_color, l.line_group;

COMMENT ON VIEW vw_daily_ridership IS 'Daily ridership aggregated by station and line';

-- View: Hourly Ridership Patterns
CREATE OR REPLACE VIEW vw_hourly_ridership AS
SELECT 
    t.hour,
    t.time_period,
    t.is_peak_hour,
    t.am_pm,
    d.day_name,
    d.is_weekend,
    l.line_name,
    l.line_color,
    s.borough,
    SUM(r.entries) AS total_entries,
    SUM(r.exits) AS total_exits,
    AVG(r.entries) AS avg_entries_per_record,
    COUNT(DISTINCT d.full_date) AS days_count
FROM fact_ridership r
JOIN dim_time t ON r.time_key = t.time_key
JOIN dim_date d ON r.date_key = d.date_key
LEFT JOIN dim_subway_lines l ON r.line_id = l.line_id
LEFT JOIN dim_stations s ON r.station_id = s.station_id
GROUP BY 
    t.hour, t.time_period, t.is_peak_hour, t.am_pm,
    d.day_name, d.is_weekend,
    l.line_name, l.line_color, s.borough;

COMMENT ON VIEW vw_hourly_ridership IS 'Hourly ridership patterns for peak hour analysis';

-- View: Station Performance Metrics
CREATE OR REPLACE VIEW vw_station_performance AS
SELECT 
    s.station_id,
    s.station_name,
    s.borough,
    s.latitude,
    s.longitude,
    s.lines_served,
    s.ada_accessible,
    COUNT(DISTINCT r.date_key) AS days_with_data,
    SUM(r.entries) AS total_entries,
    SUM(r.exits) AS total_exits,
    SUM(r.total_traffic) AS total_traffic,
    AVG(r.entries) AS avg_daily_entries,
    MAX(r.entries) AS peak_entries,
    RANK() OVER (ORDER BY SUM(r.total_traffic) DESC) AS traffic_rank,
    RANK() OVER (PARTITION BY s.borough ORDER BY SUM(r.total_traffic) DESC) AS borough_rank
FROM dim_stations s
LEFT JOIN fact_ridership r ON s.station_id = r.station_id
GROUP BY 
    s.station_id, s.station_name, s.borough, s.latitude, s.longitude,
    s.lines_served, s.ada_accessible;

COMMENT ON VIEW vw_station_performance IS 'Station-level performance and ranking metrics';

-- View: Line Performance Summary
CREATE OR REPLACE VIEW vw_line_performance AS
SELECT 
    d.full_date,
    d.day_name,
    d.month_name,
    d.year,
    d.is_weekend,
    l.line_name,
    l.line_color,
    l.line_group,
    l.service_pattern,
    p.scheduled_trips,
    p.actual_trips,
    p.on_time_trips,
    p.late_trips,
    p.canceled_trips,
    p.on_time_percentage,
    p.wait_assessment,
    p.customer_journey_time_performance,
    CASE 
        WHEN p.on_time_percentage >= 90 THEN 'Excellent'
        WHEN p.on_time_percentage >= 80 THEN 'Good'
        WHEN p.on_time_percentage >= 70 THEN 'Fair'
        ELSE 'Needs Improvement'
    END AS performance_category
FROM fact_performance p
JOIN dim_date d ON p.date_key = d.date_key
JOIN dim_subway_lines l ON p.line_id = l.line_id;

COMMENT ON VIEW vw_line_performance IS 'Daily on-time performance by subway line';

-- View: Delay Analysis
CREATE OR REPLACE VIEW vw_delay_analysis AS
SELECT 
    d.full_date,
    d.day_name,
    d.month_name,
    d.year,
    d.is_weekend,
    t.hour,
    t.time_period,
    t.is_peak_hour,
    l.line_name,
    l.line_color,
    l.line_group,
    s.station_name,
    s.borough,
    dl.delay_category,
    dl.severity_level,
    dl.delay_reason,
    dl.delay_duration_minutes,
    dl.passenger_impact_estimate,
    dl.resolution_time_minutes
FROM fact_delays dl
JOIN dim_date d ON dl.date_key = d.date_key
LEFT JOIN dim_time t ON dl.time_key = t.time_key
LEFT JOIN dim_subway_lines l ON dl.line_id = l.line_id
LEFT JOIN dim_stations s ON dl.station_id = s.station_id;

COMMENT ON VIEW vw_delay_analysis IS 'Detailed delay incident analysis';

-- View: Delay Summary by Line
CREATE OR REPLACE VIEW vw_delay_summary_by_line AS
SELECT 
    l.line_name,
    l.line_color,
    l.line_group,
    COUNT(*) AS total_delays,
    AVG(dl.delay_duration_minutes) AS avg_delay_minutes,
    MAX(dl.delay_duration_minutes) AS max_delay_minutes,
    SUM(dl.passenger_impact_estimate) AS total_passengers_affected,
    COUNT(CASE WHEN dl.severity_level = 'High' THEN 1 END) AS severe_delays,
    COUNT(CASE WHEN t.is_peak_hour THEN 1 END) AS peak_hour_delays,
    ROUND(COUNT(CASE WHEN t.is_peak_hour THEN 1 END)::DECIMAL / 
          NULLIF(COUNT(*), 0) * 100, 2) AS peak_hour_delay_pct
FROM fact_delays dl
LEFT JOIN dim_subway_lines l ON dl.line_id = l.line_id
LEFT JOIN dim_time t ON dl.time_key = t.time_key
GROUP BY l.line_name, l.line_color, l.line_group
ORDER BY total_delays DESC;

COMMENT ON VIEW vw_delay_summary_by_line IS 'Aggregated delay metrics by subway line';

-- View: Monthly Trend Summary
CREATE OR REPLACE VIEW vw_monthly_trends AS
SELECT 
    d.year,
    d.month_number,
    d.month_name,
    -- Ridership metrics
    SUM(r.entries) AS total_entries,
    SUM(r.exits) AS total_exits,
    AVG(r.entries) AS avg_daily_entries,
    -- YoY comparison setup
    LAG(SUM(r.entries)) OVER (ORDER BY d.year, d.month_number) AS prev_month_entries,
    ROUND(
        (SUM(r.entries) - LAG(SUM(r.entries)) OVER (ORDER BY d.year, d.month_number))::DECIMAL /
        NULLIF(LAG(SUM(r.entries)) OVER (ORDER BY d.year, d.month_number), 0) * 100,
        2
    ) AS mom_change_pct
FROM fact_ridership r
JOIN dim_date d ON r.date_key = d.date_key
GROUP BY d.year, d.month_number, d.month_name
ORDER BY d.year, d.month_number;

COMMENT ON VIEW vw_monthly_trends IS 'Monthly ridership trends with month-over-month changes';

-- =====================================================
-- ANALYTICAL QUERIES
-- =====================================================

-- Query: Average Delays Per Line
-- Returns the average delay duration for each subway line
SELECT 
    l.line_name,
    l.line_color,
    COUNT(*) AS delay_count,
    ROUND(AVG(dl.delay_duration_minutes), 2) AS avg_delay_minutes,
    ROUND(STDDEV(dl.delay_duration_minutes), 2) AS stddev_delay,
    MIN(dl.delay_duration_minutes) AS min_delay,
    MAX(dl.delay_duration_minutes) AS max_delay,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY dl.delay_duration_minutes) AS median_delay
FROM fact_delays dl
JOIN dim_subway_lines l ON dl.line_id = l.line_id
GROUP BY l.line_name, l.line_color
ORDER BY avg_delay_minutes DESC;

-- Query: Peak Ridership Hours Analysis
-- Identifies the busiest hours across the system
SELECT 
    t.hour,
    t.time_period,
    t.am_pm,
    SUM(r.entries) AS total_entries,
    SUM(r.exits) AS total_exits,
    AVG(r.entries) AS avg_entries,
    COUNT(DISTINCT d.full_date) AS days_measured,
    ROUND(SUM(r.entries)::DECIMAL / COUNT(DISTINCT d.full_date), 0) AS avg_hourly_entries
FROM fact_ridership r
JOIN dim_time t ON r.time_key = t.time_key
JOIN dim_date d ON r.date_key = d.date_key
GROUP BY t.hour, t.time_period, t.am_pm
ORDER BY total_entries DESC;

-- Query: On-Time Performance Rates by Line
-- Calculates OTP for each subway line
SELECT 
    l.line_name,
    l.line_group,
    l.service_pattern,
    COUNT(*) AS days_measured,
    ROUND(AVG(p.on_time_percentage), 2) AS avg_otp,
    MIN(p.on_time_percentage) AS min_otp,
    MAX(p.on_time_percentage) AS max_otp,
    ROUND(AVG(p.wait_assessment), 2) AS avg_wait_assessment,
    ROUND(AVG(p.customer_journey_time_performance), 2) AS avg_cjtp
FROM fact_performance p
JOIN dim_subway_lines l ON p.line_id = l.line_id
GROUP BY l.line_name, l.line_group, l.service_pattern
ORDER BY avg_otp DESC;

-- Query: Top 10 Busiest Stations
-- Ranks stations by total traffic
SELECT 
    s.station_name,
    s.borough,
    s.lines_served,
    SUM(r.total_traffic) AS total_traffic,
    SUM(r.entries) AS total_entries,
    SUM(r.exits) AS total_exits,
    COUNT(DISTINCT r.date_key) AS days_with_service,
    ROUND(SUM(r.total_traffic)::DECIMAL / COUNT(DISTINCT r.date_key), 0) AS avg_daily_traffic
FROM dim_stations s
JOIN fact_ridership r ON s.station_id = r.station_id
GROUP BY s.station_name, s.borough, s.lines_served
ORDER BY total_traffic DESC
LIMIT 10;

-- Query: Delay Causes Analysis
-- Groups delays by reason with impact metrics
SELECT 
    dl.delay_reason,
    COUNT(*) AS incident_count,
    ROUND(AVG(dl.delay_duration_minutes), 2) AS avg_duration,
    SUM(dl.passenger_impact_estimate) AS total_passengers_affected,
    ROUND(
        COUNT(*)::DECIMAL / (SELECT COUNT(*) FROM fact_delays) * 100, 
        2
    ) AS pct_of_all_delays
FROM fact_delays dl
GROUP BY dl.delay_reason
ORDER BY incident_count DESC;

-- Query: Weekend vs Weekday Performance
-- Compares metrics between weekends and weekdays
SELECT 
    CASE WHEN d.is_weekend THEN 'Weekend' ELSE 'Weekday' END AS day_type,
    -- Ridership
    SUM(r.entries) AS total_entries,
    AVG(r.entries) AS avg_entries,
    -- Performance
    AVG(p.on_time_percentage) AS avg_otp
FROM fact_ridership r
JOIN dim_date d ON r.date_key = d.date_key
LEFT JOIN fact_performance p ON r.date_key = p.date_key AND r.line_id = p.line_id
GROUP BY d.is_weekend;

-- Query: Borough Traffic Distribution
-- Shows traffic distribution across NYC boroughs
SELECT 
    s.borough,
    COUNT(DISTINCT s.station_id) AS station_count,
    SUM(r.total_traffic) AS total_traffic,
    ROUND(
        SUM(r.total_traffic)::DECIMAL / 
        (SELECT SUM(total_traffic) FROM fact_ridership) * 100,
        2
    ) AS traffic_share_pct,
    AVG(r.total_traffic) AS avg_traffic_per_record
FROM dim_stations s
JOIN fact_ridership r ON s.station_id = r.station_id
GROUP BY s.borough
ORDER BY total_traffic DESC;

-- Query: Time Period Performance Analysis
-- Compares performance across different time periods
SELECT 
    t.time_period,
    COUNT(DISTINCT dl.delay_id) AS delay_count,
    AVG(dl.delay_duration_minutes) AS avg_delay_minutes,
    SUM(r.entries) AS total_entries,
    ROUND(
        COUNT(DISTINCT dl.delay_id)::DECIMAL / 
        NULLIF(COUNT(DISTINCT r.ridership_id), 0) * 10000,
        2
    ) AS delays_per_10k_entries
FROM dim_time t
LEFT JOIN fact_ridership r ON t.time_key = r.time_key
LEFT JOIN fact_delays dl ON t.time_key = dl.time_key
GROUP BY t.time_period
ORDER BY 
    CASE t.time_period 
        WHEN 'Late Night' THEN 1
        WHEN 'Morning Rush' THEN 2
        WHEN 'Midday' THEN 3
        WHEN 'Evening Rush' THEN 4
        WHEN 'Night' THEN 5
    END;

-- =====================================================
-- MATERIALIZED VIEWS FOR PERFORMANCE
-- =====================================================

-- Materialized View: Daily KPIs
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_daily_kpis AS
SELECT 
    d.full_date,
    d.year,
    d.month_number,
    d.day_name,
    d.is_weekend,
    -- Ridership KPIs
    SUM(r.entries) AS total_entries,
    SUM(r.exits) AS total_exits,
    COUNT(DISTINCT r.station_id) AS active_stations,
    -- Delay KPIs
    COUNT(DISTINCT dl.delay_id) AS delay_count,
    COALESCE(AVG(dl.delay_duration_minutes), 0) AS avg_delay_minutes,
    -- Performance KPIs
    COALESCE(AVG(p.on_time_percentage), 0) AS system_avg_otp
FROM dim_date d
LEFT JOIN fact_ridership r ON d.date_key = r.date_key
LEFT JOIN fact_delays dl ON d.date_key = dl.date_key
LEFT JOIN fact_performance p ON d.date_key = p.date_key
GROUP BY d.full_date, d.year, d.month_number, d.day_name, d.is_weekend;

-- Create index for fast lookups
CREATE INDEX IF NOT EXISTS idx_mv_daily_kpis_date ON mv_daily_kpis(full_date);

-- Refresh materialized view (run periodically)
-- REFRESH MATERIALIZED VIEW mv_daily_kpis;

-- =====================================================
-- STORED PROCEDURES FOR ANALYTICS
-- =====================================================

-- Function: Get Station Performance Summary
CREATE OR REPLACE FUNCTION get_station_summary(p_station_name VARCHAR)
RETURNS TABLE (
    station_name VARCHAR,
    borough VARCHAR,
    total_entries BIGINT,
    total_exits BIGINT,
    avg_daily_entries NUMERIC,
    delay_count BIGINT,
    avg_delay_minutes NUMERIC
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        s.station_name::VARCHAR,
        s.borough::VARCHAR,
        COALESCE(SUM(r.entries), 0)::BIGINT,
        COALESCE(SUM(r.exits), 0)::BIGINT,
        ROUND(AVG(r.entries), 2),
        COUNT(dl.delay_id)::BIGINT,
        ROUND(COALESCE(AVG(dl.delay_duration_minutes), 0), 2)
    FROM dim_stations s
    LEFT JOIN fact_ridership r ON s.station_id = r.station_id
    LEFT JOIN fact_delays dl ON s.station_id = dl.station_id
    WHERE s.station_name ILIKE '%' || p_station_name || '%'
    GROUP BY s.station_name, s.borough;
END;
$$ LANGUAGE plpgsql;

-- Function: Get Line Performance for Date Range
CREATE OR REPLACE FUNCTION get_line_performance(
    p_line_name VARCHAR,
    p_start_date DATE,
    p_end_date DATE
)
RETURNS TABLE (
    line_name VARCHAR,
    date_val DATE,
    scheduled_trips INTEGER,
    on_time_trips INTEGER,
    on_time_pct NUMERIC,
    delay_count BIGINT
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        l.line_name::VARCHAR,
        d.full_date,
        p.scheduled_trips,
        p.on_time_trips,
        p.on_time_percentage,
        COUNT(dl.delay_id)::BIGINT
    FROM dim_subway_lines l
    JOIN fact_performance p ON l.line_id = p.line_id
    JOIN dim_date d ON p.date_key = d.date_key
    LEFT JOIN fact_delays dl ON l.line_id = dl.line_id AND p.date_key = dl.date_key
    WHERE l.line_name = p_line_name
      AND d.full_date BETWEEN p_start_date AND p_end_date
    GROUP BY l.line_name, d.full_date, p.scheduled_trips, p.on_time_trips, p.on_time_percentage
    ORDER BY d.full_date;
END;
$$ LANGUAGE plpgsql;

-- =====================================================
-- EXAMPLE USAGE QUERIES FOR POWER BI
-- =====================================================

-- These queries are optimized for Power BI DirectQuery or Import mode

-- KPI Card: Total System Ridership (Today)
SELECT SUM(entries + exits) AS total_ridership
FROM fact_ridership r
JOIN dim_date d ON r.date_key = d.date_key
WHERE d.full_date = CURRENT_DATE;

-- KPI Card: Average On-Time Performance
SELECT ROUND(AVG(on_time_percentage), 1) AS avg_otp
FROM fact_performance
WHERE date_key >= (SELECT date_key FROM dim_date WHERE full_date = CURRENT_DATE - INTERVAL '30 days');

-- KPI Card: Total Delays This Month
SELECT COUNT(*) AS monthly_delays
FROM fact_delays dl
JOIN dim_date d ON dl.date_key = d.date_key
WHERE d.year = EXTRACT(YEAR FROM CURRENT_DATE)
  AND d.month_number = EXTRACT(MONTH FROM CURRENT_DATE);

-- KPI Card: Busiest Station Today
SELECT s.station_name, SUM(r.total_traffic) AS traffic
FROM fact_ridership r
JOIN dim_stations s ON r.station_id = s.station_id
JOIN dim_date d ON r.date_key = d.date_key
WHERE d.full_date = CURRENT_DATE
GROUP BY s.station_name
ORDER BY traffic DESC
LIMIT 1;
