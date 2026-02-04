# MTA Transit Data Analytics Dashboard

A comprehensive data analytics pipeline for analyzing NYC Metropolitan Transportation Authority (MTA) subway transit data. This portfolio project demonstrates end-to-end data engineering skills including ETL pipeline development, database design, and business intelligence visualization.

![Python](https://img.shields.io/badge/Python-3.9+-blue.svg)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-14+-blue.svg)
![Power BI](https://img.shields.io/badge/Power%20BI-Desktop-yellow.svg)
![License](https://img.shields.io/badge/License-MIT-green.svg)

---

## 📋 Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Architecture](#architecture)
- [Project Structure](#project-structure)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Usage](#usage)
- [Data Sources](#data-sources)
- [Dashboard Visualizations](#dashboard-visualizations)
- [API Reference](#api-reference)
- [Contributing](#contributing)
- [License](#license)

---

## 🎯 Overview

This project builds a complete data analytics solution for MTA subway transit data, enabling analysis of:

- **Ridership Patterns**: Track daily, hourly, and seasonal ridership trends
- **On-Time Performance**: Monitor subway line reliability and performance
- **Delay Analysis**: Identify delay causes, patterns, and passenger impact
- **Station Metrics**: Analyze traffic distribution across 472+ stations

### Key Highlights

- ✅ Production-ready ETL pipeline with error handling and logging
- ✅ Comprehensive PostgreSQL database with star schema design
- ✅ 100,000+ realistic synthetic transit records
- ✅ Automated data cleaning and validation
- ✅ Power BI dashboard with 6 interactive visualizations
- ✅ Complete documentation and setup guides

---

## ✨ Features

### Data Collection & ETL
- Fetch data from MTA's public APIs (NYC Open Data portal)
- Generate realistic synthetic data for testing/demo
- Automated data cleaning and preprocessing
- Handle missing values, normalize data types
- Comprehensive error handling and retry logic

### Database Design
- Star schema optimized for analytics
- Dimension tables: Stations, Subway Lines, Date, Time
- Fact tables: Ridership, Delays, Performance
- Pre-built analytics views for Power BI
- Stored procedures for common queries

### Analytics & Visualization
- Daily ridership trends and patterns
- Peak hour analysis
- Station traffic heatmaps
- On-time performance metrics
- Delay cause analysis
- Service disruption patterns

---

## 🏗️ Architecture

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│   Data Sources  │     │   ETL Pipeline  │     │   PostgreSQL    │
│                 │────▶│                 │────▶│                 │
│  - MTA APIs     │     │  - Extract      │     │  - Star Schema  │
│  - NYC Open Data│     │  - Transform    │     │  - Fact Tables  │
│  - Synthetic    │     │  - Load         │     │  - Dimensions   │
└─────────────────┘     └─────────────────┘     └────────┬────────┘
                                                         │
                                                         ▼
                        ┌─────────────────┐     ┌─────────────────┐
                        │    End Users    │◀────│   Power BI      │
                        │                 │     │                 │
                        │  - Analysts     │     │  - Dashboards   │
                        │  - Operations   │     │  - Reports      │
                        │  - Executives   │     │  - KPIs         │
                        └─────────────────┘     └─────────────────┘
```

---

## 📁 Project Structure

```
MTA Transit data analytics dashboard/
├── config/
│   ├── __init__.py
│   └── settings.py              # Configuration settings
├── data/
│   ├── raw/                     # Raw data files
│   ├── processed/               # Cleaned data files
│   ├── exports/                 # Power BI exports
│   └── synthetic/               # Generated sample data
├── docs/
│   └── POWER_BI_SETUP.md        # Power BI setup guide
├── logs/                        # ETL execution logs
├── scripts/
│   ├── generate_data.py         # Synthetic data generator
│   ├── init_database.py         # Database initialization
│   └── run_etl.py               # ETL pipeline runner
├── sql/
│   ├── schema/
│   │   └── 01_create_schema.sql # Database schema
│   └── queries/
│       └── analytics_queries.sql # Analytics views & queries
├── src/
│   ├── database/
│   │   ├── __init__.py
│   │   └── connection.py        # Database connection manager
│   └── etl/
│       ├── __init__.py
│       ├── api_client.py        # MTA API client
│       ├── data_cleaning.py     # Data cleaning utilities
│       ├── data_generator.py    # Synthetic data generator
│       └── pipeline.py          # Main ETL pipeline
├── requirements.txt             # Python dependencies
└── README.md                    # This file
```

---

## 📋 Prerequisites

### Software Requirements

| Software | Version | Purpose |
|----------|---------|---------|
| Python | 3.9+ | ETL pipeline, data processing |
| PostgreSQL | 14+ | Data warehouse |
| Power BI Desktop | Latest | Visualization dashboard |

### Python Libraries

- `pandas` - Data manipulation
- `numpy` - Numerical operations
- `psycopg2` - PostgreSQL driver
- `requests` - API calls
- `python-dotenv` - Environment variables

---

## 🚀 Installation

### Step 1: Clone the Repository

```bash
git clone https://github.com/yourusername/mta-transit-dashboard.git
cd mta-transit-dashboard
```

### Step 2: Create Virtual Environment

```bash
# Windows
python -m venv venv
venv\Scripts\activate

# macOS/Linux
python3 -m venv venv
source venv/bin/activate
```

### Step 3: Install Dependencies

```bash
pip install -r requirements.txt
```

### Step 4: Set Up PostgreSQL Database

1. **Create the database:**
```sql
CREATE DATABASE mta_transit_db;
```

2. **Update configuration:**
   
   Edit `config/settings.py` with your database credentials:
```python
DATABASE_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'mta_transit_db',
    'user': 'your_username',
    'password': 'your_password',
}
```

3. **Initialize the schema:**
```bash
python scripts/init_database.py
```

### Step 5: Load Data

**Option A: Generate Synthetic Data (Recommended for Demo)**
```bash
python scripts/generate_data.py --records 100000
```

**Option B: Run Full ETL Pipeline**
```bash
python scripts/run_etl.py --synthetic --export
```

---

## 💻 Usage

### Generate Sample Data

```bash
# Generate 100K records
python scripts/generate_data.py -r 100000

# Specify date range
python scripts/generate_data.py -s 2025-01-01 -e 2025-12-31

# Custom output directory
python scripts/generate_data.py -o data/my_sample
```

### Run ETL Pipeline

```bash
# Full pipeline with synthetic data
python scripts/run_etl.py --synthetic

# Full pipeline with API data
python scripts/run_etl.py --api

# Export data for Power BI
python scripts/run_etl.py --synthetic --export
```

### Database Operations

```bash
# Initialize database schema
python scripts/init_database.py

# Test database connection
python -c "from src.database import test_connection; print(test_connection())"
```

### Query Examples

Connect to PostgreSQL and run:

```sql
-- Top 10 busiest stations
SELECT station_name, SUM(entries) as total_entries
FROM vw_daily_ridership
GROUP BY station_name
ORDER BY total_entries DESC
LIMIT 10;

-- Average delay by subway line
SELECT line_name, AVG(delay_duration_minutes) as avg_delay
FROM vw_delay_analysis
GROUP BY line_name
ORDER BY avg_delay DESC;

-- On-time performance by line
SELECT line_name, AVG(on_time_percentage) as otp
FROM vw_line_performance
GROUP BY line_name
ORDER BY otp DESC;
```

---

## 📊 Data Sources

### Primary Sources

| Source | URL | Data Type |
|--------|-----|-----------|
| NYC Open Data | https://data.ny.gov/ | Ridership, Performance |
| MTA Data Feeds | https://new.mta.info/developers | Real-time data |

### API Endpoints Used

```python
ENDPOINTS = {
    'subway_stations': 'https://data.ny.gov/resource/39hk-dx4f.json',
    'subway_ridership': 'https://data.ny.gov/resource/wujg-7c2s.json',
    'performance_subway': 'https://data.ny.gov/resource/y27x-cket.json',
}
```

### Synthetic Data

When API data is unavailable, the system generates realistic synthetic data based on:
- Actual NYC subway station locations
- Historical ridership patterns
- Real delay cause distributions
- Peak hour traffic patterns

---

## 📈 Dashboard Visualizations

The Power BI dashboard includes 6 key visualizations:

### 1. Subway Delays by Line (Bar Chart)
- Shows total delays per subway line
- Color-coded by severity level
- Identifies most problematic lines

### 2. Ridership Trends Over Time (Line Chart)
- Daily/monthly ridership patterns
- Year-over-year comparisons
- Seasonal trend analysis

### 3. Station Traffic Heatmap (Map)
- Geographic visualization of all stations
- Bubble size indicates traffic volume
- Color intensity shows utilization

### 4. On-Time Performance (Gauge/KPI)
- Real-time OTP percentage
- Target comparison (85% threshold)
- Trend indicators

### 5. Peak Hours Analysis (Stacked Area)
- Hourly ridership distribution
- Morning vs evening rush comparison
- Time period breakdown

### 6. Service Disruption Patterns (Scatter)
- Delay duration vs passenger impact
- Clustering by subway line
- Trend analysis over time

See [docs/POWER_BI_SETUP.md](docs/POWER_BI_SETUP.md) for detailed setup instructions.

---

## 🔧 API Reference

### ETL Pipeline

```python
from src.etl.pipeline import ETLPipeline

# Initialize pipeline
pipeline = ETLPipeline(use_synthetic=True, synthetic_records=100000)

# Run full ETL
success = pipeline.run(start_date='2025-01-01', end_date='2025-12-31')

# Export for Power BI
pipeline.export_for_powerbi()
```

### Data Generator

```python
from src.etl.data_generator import SyntheticDataGenerator

# Create generator
generator = SyntheticDataGenerator(target_records=100000)

# Generate all datasets
data = generator.generate_all_data('2025-01-01', '2025-12-31')

# Access individual datasets
stations = data['stations']
ridership = data['ridership']
delays = data['delays']
performance = data['performance']
```

### Database Connection

```python
from src.database import get_db, test_connection

# Test connection
if test_connection():
    db = get_db()
    
    # Execute query
    results = db.execute_query("SELECT * FROM dim_stations LIMIT 10")
    
    # Bulk insert
    db.bulk_insert('fact_ridership', columns, data)
```

---

## 🧪 Testing

### Test Database Connection
```bash
python -c "from src.database import test_connection; print('OK' if test_connection() else 'FAIL')"
```

### Test Data Generator
```bash
python src/etl/data_generator.py
```

### Test API Client
```bash
python src/etl/api_client.py
```

---

## 📝 Configuration

### Environment Variables

Create a `.env` file in the project root:

```env
# Database
DB_HOST=localhost
DB_PORT=5432
DB_NAME=mta_transit_db
DB_USER=postgres
DB_PASSWORD=your_password

# API
NYC_OPEN_DATA_TOKEN=your_token_here
```

### Customization

Edit `config/settings.py` to customize:
- Database connection settings
- API endpoints
- Data generation parameters
- Logging configuration

---

## 🤝 Contributing

Contributions are welcome! Please:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/improvement`)
3. Commit changes (`git commit -am 'Add new feature'`)
4. Push to branch (`git push origin feature/improvement`)
5. Open a Pull Request

---

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

## 👤 Author

**Aditya Bhuran**
- Portfolio: [Your Portfolio URL]
- LinkedIn: [Your LinkedIn]
- GitHub: [Your GitHub]

---

## 🙏 Acknowledgments

- MTA for providing public transit data
- NYC Open Data for API access
- Power BI community for visualization best practices

---

## 📞 Support

For questions or issues:
- Open a GitHub issue
- Contact: [your.email@example.com]
