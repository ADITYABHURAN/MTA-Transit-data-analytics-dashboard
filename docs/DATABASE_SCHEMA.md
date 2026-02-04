# Database Schema Documentation

## MTA Transit Data Analytics Dashboard - Database Design

### Overview

The database uses a **star schema** design optimized for analytical queries and Power BI reporting. This design provides:

- Fast query performance for aggregations
- Simplified joins for business users
- Efficient storage with dimension/fact table separation
- Easy integration with BI tools

---

## Entity Relationship Diagram

```
                                    ┌─────────────────────┐
                                    │    dim_date         │
                                    │─────────────────────│
                                    │ date_key (PK)       │
                                    │ full_date           │
                                    │ day_name            │
                                    │ month_name          │
                                    │ year                │
                                    │ is_weekend          │
                                    └──────────┬──────────┘
                                               │
┌─────────────────────┐                        │                    ┌─────────────────────┐
│  dim_subway_lines   │                        │                    │    dim_time         │
│─────────────────────│     ┌──────────────────┼──────────────────┐ │─────────────────────│
│ line_id (PK)        │     │                  │                  │ │ time_key (PK)       │
│ line_name           │     │                  │                  │ │ hour                │
│ line_color          │     │                  │                  │ │ time_period         │
│ line_group          │     │                  │                  │ │ is_peak_hour        │
│ service_pattern     │     │                  │                  │ │ am_pm               │
└──────────┬──────────┘     │                  │                  │ └──────────┬──────────┘
           │                │                  │                  │            │
           │                ▼                  ▼                  ▼            │
           │     ┌─────────────────────────────────────────────────────┐       │
           ├────▶│              fact_ridership                         │◀──────┤
           │     │─────────────────────────────────────────────────────│       │
           │     │ ridership_id (PK)                                   │       │
           │     │ date_key (FK) ──────────────────────────────────────┼───────┘
           │     │ time_key (FK)                                       │
           │     │ station_id (FK) ────────────────────────────────────┼───────┐
           │     │ line_id (FK)                                        │       │
           │     │ entries, exits, total_traffic                       │       │
           │     └─────────────────────────────────────────────────────┘       │
           │                                                                   │
           │     ┌─────────────────────────────────────────────────────┐       │
           ├────▶│              fact_delays                            │◀──────┤
           │     │─────────────────────────────────────────────────────│       │
           │     │ delay_id (PK)                                       │       │
           │     │ date_key, time_key, line_id, station_id (FKs)       │       │
           │     │ delay_duration_minutes                              │       │
           │     │ delay_category, delay_reason                        │       │
           │     │ severity_level, passenger_impact_estimate           │       │
           │     └─────────────────────────────────────────────────────┘       │
           │                                                                   │
           │     ┌─────────────────────────────────────────────────────┐       │
           └────▶│              fact_performance                       │       │
                 │─────────────────────────────────────────────────────│       │
                 │ performance_id (PK)                                 │       │
                 │ date_key (FK), line_id (FK)                         │       │
                 │ scheduled_trips, actual_trips, on_time_trips        │       │
                 │ on_time_percentage (calculated)                     │       │
                 │ wait_assessment                                     │       │
                 └─────────────────────────────────────────────────────┘       │
                                                                               │
                                    ┌─────────────────────┐                    │
                                    │    dim_stations     │◀───────────────────┘
                                    │─────────────────────│
                                    │ station_id (PK)     │
                                    │ station_name        │
                                    │ borough             │
                                    │ latitude, longitude │
                                    │ lines_served        │
                                    │ ada_accessible      │
                                    └─────────────────────┘
```

---

## Dimension Tables

### dim_subway_lines
Contains information about NYC subway lines.

| Column | Type | Description |
|--------|------|-------------|
| line_id | SERIAL | Primary key |
| line_name | VARCHAR(10) | Line identifier (1, 2, A, B, etc.) |
| line_color | VARCHAR(20) | Official MTA color code |
| line_group | VARCHAR(50) | Line group (Broadway, Lexington, etc.) |
| route_type | VARCHAR(50) | Type of service |
| service_pattern | VARCHAR(20) | Local, Express, or Shuttle |

### dim_stations
Contains information about all 472+ subway stations.

| Column | Type | Description |
|--------|------|-------------|
| station_id | SERIAL | Primary key |
| station_name | VARCHAR(200) | Station name |
| station_complex_id | VARCHAR(50) | Complex identifier |
| gtfs_stop_id | VARCHAR(20) | GTFS reference ID |
| borough | VARCHAR(50) | NYC borough |
| latitude | DECIMAL(10,8) | Geographic coordinate |
| longitude | DECIMAL(11,8) | Geographic coordinate |
| structure_type | VARCHAR(50) | Underground, Elevated, At Grade |
| ada_accessible | BOOLEAN | ADA accessibility status |
| lines_served | VARCHAR(100) | Comma-separated line list |
| division | VARCHAR(50) | Historical division (IRT, BMT, IND) |

### dim_date
Date dimension for time-based analysis.

| Column | Type | Description |
|--------|------|-------------|
| date_key | INTEGER | Primary key (YYYYMMDD format) |
| full_date | DATE | Actual date value |
| day_of_week | INTEGER | 0-6 (Sunday-Saturday) |
| day_name | VARCHAR(15) | Full day name |
| month_number | INTEGER | 1-12 |
| month_name | VARCHAR(15) | Full month name |
| quarter | INTEGER | 1-4 |
| year | INTEGER | Four-digit year |
| is_weekend | BOOLEAN | Weekend indicator |
| is_holiday | BOOLEAN | Holiday indicator |

### dim_time
Time dimension for hourly analysis.

| Column | Type | Description |
|--------|------|-------------|
| time_key | INTEGER | Primary key (HHMM format) |
| full_time | TIME | Actual time value |
| hour | INTEGER | 0-23 |
| minute | INTEGER | 0-59 |
| time_period | VARCHAR(20) | Morning Rush, Midday, Evening Rush, Night, Late Night |
| is_peak_hour | BOOLEAN | Rush hour indicator |
| peak_type | VARCHAR(20) | Morning or Evening |

---

## Fact Tables

### fact_ridership
Stores ridership counts by station, line, date, and time.

| Column | Type | Description |
|--------|------|-------------|
| ridership_id | SERIAL | Primary key |
| date_key | INTEGER | FK to dim_date |
| time_key | INTEGER | FK to dim_time |
| station_id | INTEGER | FK to dim_stations |
| line_id | INTEGER | FK to dim_subway_lines |
| entries | INTEGER | Entry count |
| exits | INTEGER | Exit count |
| total_traffic | INTEGER | Calculated: entries + exits |
| data_source | VARCHAR(50) | Data origin (API, synthetic) |

### fact_delays
Stores delay incident information.

| Column | Type | Description |
|--------|------|-------------|
| delay_id | SERIAL | Primary key |
| date_key | INTEGER | FK to dim_date |
| time_key | INTEGER | FK to dim_time |
| line_id | INTEGER | FK to dim_subway_lines |
| station_id | INTEGER | FK to dim_stations |
| delay_duration_minutes | INTEGER | Duration of delay |
| delay_category | VARCHAR(50) | Minor, Moderate, Significant, Major, Severe |
| delay_reason | VARCHAR(200) | Cause of delay |
| severity_level | VARCHAR(20) | Low, Medium, High |
| passenger_impact_estimate | INTEGER | Estimated affected passengers |
| resolution_time_minutes | INTEGER | Time to resolve |

### fact_performance
Daily on-time performance by line.

| Column | Type | Description |
|--------|------|-------------|
| performance_id | SERIAL | Primary key |
| date_key | INTEGER | FK to dim_date |
| line_id | INTEGER | FK to dim_subway_lines |
| scheduled_trips | INTEGER | Planned trips |
| actual_trips | INTEGER | Completed trips |
| on_time_trips | INTEGER | Trips on schedule |
| late_trips | INTEGER | Delayed trips |
| canceled_trips | INTEGER | Canceled trips |
| on_time_percentage | DECIMAL(5,2) | Calculated OTP |
| wait_assessment | DECIMAL(5,2) | Wait time metric |
| customer_journey_time_performance | DECIMAL(5,2) | Journey time metric |

---

## Indexes

The schema includes the following indexes for performance:

| Table | Index | Columns | Purpose |
|-------|-------|---------|---------|
| dim_stations | idx_stations_coordinates | latitude, longitude | Geographic queries |
| dim_stations | idx_stations_borough | borough | Borough filtering |
| dim_stations | idx_stations_name | station_name (gin_trgm) | Text search |
| fact_ridership | idx_ridership_date | date_key | Date filtering |
| fact_ridership | idx_ridership_composite | date_key, station_id, line_id | Common queries |
| fact_delays | idx_delays_date | date_key | Date filtering |
| fact_delays | idx_delays_severity | severity_level | Severity analysis |
| fact_performance | idx_performance_date | date_key | Date filtering |

---

## Analytics Views

Pre-built views for Power BI and reporting:

| View | Description |
|------|-------------|
| vw_daily_ridership | Daily ridership with all dimensions joined |
| vw_hourly_ridership | Hourly patterns for peak hour analysis |
| vw_station_performance | Station-level metrics and rankings |
| vw_line_performance | Line-level OTP and performance |
| vw_delay_analysis | Detailed delay analysis with dimensions |
| vw_delay_summary_by_line | Aggregated delay metrics per line |
| vw_monthly_trends | Monthly trends with MoM calculations |

---

## Data Volume Estimates

| Table | Expected Records | Update Frequency |
|-------|------------------|------------------|
| dim_subway_lines | ~23 | Static |
| dim_stations | ~472 | Rarely |
| dim_date | ~1,095 (3 years) | Yearly |
| dim_time | ~1,440 | Static |
| fact_ridership | 100,000+ | Daily |
| fact_delays | 5,000-10,000 | Daily |
| fact_performance | 8,000+ | Daily |

---

## Maintenance

### Refresh Materialized Views
```sql
REFRESH MATERIALIZED VIEW mv_daily_kpis;
```

### Vacuum and Analyze
```sql
VACUUM ANALYZE fact_ridership;
VACUUM ANALYZE fact_delays;
VACUUM ANALYZE fact_performance;
```

### Monitor Table Sizes
```sql
SELECT 
    table_name,
    pg_size_pretty(pg_total_relation_size(quote_ident(table_name))) as size
FROM information_schema.tables 
WHERE table_schema = 'public'
ORDER BY pg_total_relation_size(quote_ident(table_name)) DESC;
```
