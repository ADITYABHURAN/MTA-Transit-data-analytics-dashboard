# Power BI Dashboard Setup Guide
## MTA Transit Data Analytics Dashboard

This guide provides step-by-step instructions for connecting Power BI to the PostgreSQL database and creating the transit analytics dashboard.

---

## Table of Contents
1. [Prerequisites](#prerequisites)
2. [Connecting to PostgreSQL](#connecting-to-postgresql)
3. [Importing Data](#importing-data)
4. [Data Model Setup](#data-model-setup)
5. [Creating Visualizations](#creating-visualizations)
6. [DAX Measures](#dax-measures)
7. [Dashboard Layout](#dashboard-layout)
8. [Publishing & Sharing](#publishing--sharing)

---

## Prerequisites

### Software Requirements
- **Power BI Desktop** (latest version) - [Download](https://powerbi.microsoft.com/desktop/)
- **PostgreSQL ODBC Driver** (Npgsql or official PostgreSQL driver)

### Database Requirements
- ETL pipeline completed with data loaded
- Read access to the `mta_transit_db` database
- Recommended user: `powerbi_reader` (created by schema script)

### Install PostgreSQL Driver

1. Download the Npgsql driver from [npgsql.org](https://www.npgsql.org/)
2. Or install via the official PostgreSQL ODBC driver
3. Restart Power BI Desktop after installation

---

## Connecting to PostgreSQL

### Step 1: Open Power BI Desktop
Launch Power BI Desktop and create a new report.

### Step 2: Get Data from PostgreSQL

1. Click **Home** → **Get Data** → **More...**
2. Search for **PostgreSQL database**
3. Click **Connect**

### Step 3: Enter Connection Details

```
Server:     localhost (or your server address)
Database:   mta_transit_db
```

**Advanced Options:**
- Data Connectivity Mode: **Import** (recommended for this dataset)
- Command timeout: 10 minutes

### Step 4: Authentication

- Select **Database** authentication
- Username: `powerbi_reader` (or your username)
- Password: `powerbi_readonly_2024` (or your password)
- Click **Connect**

### Step 5: Select Tables/Views

Select the following tables and views:

**Dimension Tables:**
- `dim_subway_lines`
- `dim_stations`
- `dim_date`
- `dim_time`

**Fact Tables:**
- `fact_ridership`
- `fact_delays`
- `fact_performance`

**Analytics Views:**
- `vw_daily_ridership`
- `vw_line_performance`
- `vw_delay_analysis`
- `vw_station_performance`

Click **Load** to import the data.

---

## Data Model Setup

### Relationships

Power BI should auto-detect most relationships. Verify these exist:

| From Table | From Column | To Table | To Column | Cardinality |
|------------|-------------|----------|-----------|-------------|
| fact_ridership | date_key | dim_date | date_key | Many-to-One |
| fact_ridership | time_key | dim_time | time_key | Many-to-One |
| fact_ridership | station_id | dim_stations | station_id | Many-to-One |
| fact_ridership | line_id | dim_subway_lines | line_id | Many-to-One |
| fact_delays | date_key | dim_date | date_key | Many-to-One |
| fact_delays | line_id | dim_subway_lines | line_id | Many-to-One |
| fact_performance | date_key | dim_date | date_key | Many-to-One |
| fact_performance | line_id | dim_subway_lines | line_id | Many-to-One |

### Date Table Configuration

Mark `dim_date` as the official date table:
1. Select `dim_date` in the model view
2. Go to **Table Tools** → **Mark as date table**
3. Select `full_date` as the date column

---

## Creating Visualizations

### Visualization 1: Subway Delays by Line (Bar Chart)

**Purpose:** Show total delays and average delay duration per subway line

**Setup:**
1. Insert a **Clustered Bar Chart**
2. Configure:
   - Y-axis: `dim_subway_lines[line_name]`
   - X-axis: `COUNT(fact_delays[delay_id])` 
   - Legend: `fact_delays[severity_level]`
3. Sort by delay count descending
4. Add data colors using subway line colors

**Formatting:**
- Title: "Delays by Subway Line"
- Enable data labels
- Use conditional formatting (gradient by severity)

---

### Visualization 2: Ridership Trends Over Time (Line Chart)

**Purpose:** Display ridership patterns over days/months

**Setup:**
1. Insert a **Line Chart**
2. Configure:
   - X-axis: `dim_date[full_date]` (set to Month hierarchy)
   - Y-axis: `SUM(fact_ridership[entries])`
   - Legend: None (or by borough)
3. Add a trend line

**Formatting:**
- Title: "Ridership Trends"
- Enable markers for data points
- Add reference line for average

**Drill-down:**
- Enable drill-through to Year → Quarter → Month → Day

---

### Visualization 3: Station Traffic Heatmap (Map)

**Purpose:** Geographic visualization of station traffic

**Setup:**
1. Insert a **Map** or **Filled Map**
2. Configure:
   - Location: `dim_stations[station_name]`
   - Latitude: `dim_stations[latitude]`
   - Longitude: `dim_stations[longitude]`
   - Size: `SUM(fact_ridership[total_traffic])`
   - Color saturation: `SUM(fact_ridership[total_traffic])`

**Formatting:**
- Title: "Station Traffic Map"
- Style: Aerial or Grayscale
- Bubble size range: 5-30

**Alternative (Matrix Heatmap):**
1. Insert a **Matrix**
2. Rows: `dim_stations[borough]`
3. Columns: `dim_date[day_name]`
4. Values: `SUM(fact_ridership[entries])`
5. Apply conditional formatting with color scale

---

### Visualization 4: On-Time Performance Gauge/KPI Cards

**Purpose:** Display key performance indicators

**Setup - Gauge:**
1. Insert a **Gauge**
2. Configure:
   - Value: `AVERAGE(fact_performance[on_time_percentage])`
   - Minimum: 0
   - Maximum: 100
   - Target: 85 (MTA target)

**Setup - KPI Cards:**
Create 4 cards with these measures:

| Card | Measure |
|------|---------|
| Total Ridership | `SUM(fact_ridership[entries])` |
| Avg On-Time % | `AVERAGE(fact_performance[on_time_percentage])` |
| Total Delays | `COUNT(fact_delays[delay_id])` |
| Active Stations | `DISTINCTCOUNT(fact_ridership[station_id])` |

**Formatting:**
- Use icons (up/down arrows) for trends
- Apply conditional formatting (red/yellow/green)

---

### Visualization 5: Peak Hours Analysis (Stacked Area Chart)

**Purpose:** Show ridership distribution across hours of day

**Setup:**
1. Insert a **Stacked Area Chart**
2. Configure:
   - X-axis: `dim_time[hour]`
   - Y-axis: `SUM(fact_ridership[entries])`
   - Legend: `dim_time[time_period]`

**Formatting:**
- Title: "Ridership by Hour"
- Colors: Morning Rush (orange), Midday (blue), Evening Rush (red), Night (gray)
- Add vertical lines at 7 AM and 5 PM to mark rush hours

---

### Visualization 6: Service Disruption Patterns (Scatter Chart)

**Purpose:** Analyze relationship between delay duration and passenger impact

**Setup:**
1. Insert a **Scatter Chart**
2. Configure:
   - X-axis: `fact_delays[delay_duration_minutes]`
   - Y-axis: `SUM(fact_delays[passenger_impact_estimate])`
   - Legend: `dim_subway_lines[line_name]`
   - Size: `COUNT(fact_delays[delay_id])`
   - Play Axis: `dim_date[full_date]` (for animation)

**Formatting:**
- Title: "Delay Impact Analysis"
- Enable analytics (trend line, clustering)
- Add reference lines for average values

---

## DAX Measures

Create these calculated measures in Power BI:

### Basic Measures

```dax
// Total Ridership
Total Ridership = SUM(fact_ridership[entries]) + SUM(fact_ridership[exits])

// Average Daily Ridership
Avg Daily Ridership = 
AVERAGEX(
    VALUES(dim_date[full_date]),
    CALCULATE(SUM(fact_ridership[entries]))
)

// Total Delays
Total Delays = COUNT(fact_delays[delay_id])

// Average Delay Duration
Avg Delay Minutes = AVERAGE(fact_delays[delay_duration_minutes])
```

### On-Time Performance Measures

```dax
// On-Time Performance Rate
OTP Rate = 
DIVIDE(
    SUM(fact_performance[on_time_trips]),
    SUM(fact_performance[scheduled_trips]),
    0
) * 100

// OTP vs Target (85%)
OTP vs Target = [OTP Rate] - 85

// OTP Status
OTP Status = 
IF([OTP Rate] >= 90, "Excellent",
IF([OTP Rate] >= 80, "Good",
IF([OTP Rate] >= 70, "Fair", "Needs Improvement")))
```

### Time Intelligence Measures

```dax
// Month-over-Month Change
MoM Ridership Change = 
VAR CurrentMonth = SUM(fact_ridership[entries])
VAR PreviousMonth = CALCULATE(
    SUM(fact_ridership[entries]),
    DATEADD(dim_date[full_date], -1, MONTH)
)
RETURN
DIVIDE(CurrentMonth - PreviousMonth, PreviousMonth, 0) * 100

// Year-to-Date Ridership
YTD Ridership = 
TOTALYTD(
    SUM(fact_ridership[entries]),
    dim_date[full_date]
)

// Rolling 7-Day Average
Rolling 7Day Avg = 
AVERAGEX(
    DATESINPERIOD(dim_date[full_date], MAX(dim_date[full_date]), -7, DAY),
    CALCULATE(SUM(fact_ridership[entries]))
)
```

### Peak Hour Analysis

```dax
// Peak Hour Ridership
Peak Hour Ridership = 
CALCULATE(
    SUM(fact_ridership[entries]),
    dim_time[is_peak_hour] = TRUE
)

// Peak Hour Percentage
Peak Hour % = 
DIVIDE(
    [Peak Hour Ridership],
    SUM(fact_ridership[entries]),
    0
) * 100

// Morning vs Evening Rush
Morning Rush Riders = 
CALCULATE(
    SUM(fact_ridership[entries]),
    dim_time[peak_type] = "Morning"
)

Evening Rush Riders = 
CALCULATE(
    SUM(fact_ridership[entries]),
    dim_time[peak_type] = "Evening"
)
```

### Delay Analysis

```dax
// Severe Delays Count
Severe Delays = 
CALCULATE(
    COUNT(fact_delays[delay_id]),
    fact_delays[severity_level] = "High"
)

// Peak Hour Delays
Peak Hour Delays = 
CALCULATE(
    COUNT(fact_delays[delay_id]),
    dim_time[is_peak_hour] = TRUE
)

// Passengers Affected
Total Passengers Affected = SUM(fact_delays[passenger_impact_estimate])

// Avg Time to Resolve
Avg Resolution Time = AVERAGE(fact_delays[resolution_time_minutes])
```

---

## Dashboard Layout

### Recommended Layout (3-Page Dashboard)

#### Page 1: Executive Overview
```
┌─────────────────────────────────────────────────────────┐
│  KPI Cards: Ridership | OTP | Delays | Stations        │
├─────────────────────────────────┬───────────────────────┤
│                                 │                       │
│   Ridership Trends              │   OTP Gauge           │
│   (Line Chart)                  │   + Line Performance  │
│                                 │                       │
├─────────────────────────────────┴───────────────────────┤
│                                                         │
│   Station Traffic Map                                   │
│                                                         │
└─────────────────────────────────────────────────────────┘
```

#### Page 2: Ridership Analysis
```
┌─────────────────────────────────────────────────────────┐
│  Slicers: Date Range | Borough | Line                   │
├─────────────────────────────────┬───────────────────────┤
│                                 │                       │
│   Peak Hours Analysis           │   Top 10 Stations     │
│   (Stacked Area)                │   (Table)             │
│                                 │                       │
├─────────────────────────────────┼───────────────────────┤
│                                 │                       │
│   Weekday vs Weekend            │   Borough Traffic     │
│   (Clustered Column)            │   (Pie/Donut)         │
│                                 │                       │
└─────────────────────────────────┴───────────────────────┘
```

#### Page 3: Delay & Performance Analysis
```
┌─────────────────────────────────────────────────────────┐
│  Slicers: Date Range | Severity | Line                  │
├─────────────────────────────────┬───────────────────────┤
│                                 │                       │
│   Delays by Line                │   Delay Causes        │
│   (Bar Chart)                   │   (Treemap)           │
│                                 │                       │
├─────────────────────────────────┼───────────────────────┤
│                                 │                       │
│   Delay Impact Scatter          │   OTP Trends          │
│   (Scatter Chart)               │   (Line Chart)        │
│                                 │                       │
└─────────────────────────────────┴───────────────────────┘
```

### Slicers to Include
- Date Range (Calendar picker)
- Borough (Dropdown)
- Subway Line (Multi-select with line colors)
- Time Period (Morning/Midday/Evening/Night)
- Weekday/Weekend (Buttons)

---

## Publishing & Sharing

### Publish to Power BI Service

1. Click **Home** → **Publish**
2. Select your workspace
3. Wait for upload completion
4. Open in Power BI Service

### Set Up Scheduled Refresh

1. Go to dataset settings in Power BI Service
2. Configure gateway connection
3. Set refresh schedule (recommended: daily at 6 AM)

### Share the Dashboard

1. Create a dashboard from report pages
2. Click **Share** to invite viewers
3. Or embed in websites using publish to web

---

## Best Practices

### Performance Optimization
- Use aggregated views instead of fact tables where possible
- Limit data to recent 12-24 months for faster loading
- Create summary tables for frequently used metrics
- Use Import mode instead of DirectQuery for this dataset size

### Visual Design
- Use consistent color scheme (MTA official colors)
- Include data refresh timestamp
- Add tooltips with additional context
- Ensure mobile-friendly responsive design

### Maintenance
- Document all DAX measures
- Create a data dictionary page
- Set up data quality alerts
- Review and optimize monthly

---

## Troubleshooting

### Connection Issues
- Verify PostgreSQL is running and accessible
- Check firewall settings for port 5432
- Ensure user has SELECT permissions on all tables

### Performance Issues
- Reduce date range in filters
- Use summarized views instead of raw tables
- Enable query reduction in options

### Data Refresh Failures
- Check gateway status
- Verify credentials haven't expired
- Review error messages in refresh history

---

## Additional Resources

- [Power BI Documentation](https://docs.microsoft.com/power-bi/)
- [DAX Reference](https://docs.microsoft.com/dax/)
- [MTA Performance Data](https://new.mta.info/transparency/metrics)
- [NYC Open Data Portal](https://opendata.cityofnewyork.us/)
