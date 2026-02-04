"""
Configuration settings for MTA Transit Data Analytics Dashboard
"""
import os
from pathlib import Path

# Project root directory
PROJECT_ROOT = Path(__file__).parent.parent

# Database Configuration
DATABASE_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': int(os.getenv('DB_PORT', 5432)),
    'database': os.getenv('DB_NAME', 'mta_transit_db'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', 'your_password_here'),
}

# PostgreSQL Connection String
DATABASE_URL = f"postgresql://{DATABASE_CONFIG['user']}:{DATABASE_CONFIG['password']}@{DATABASE_CONFIG['host']}:{DATABASE_CONFIG['port']}/{DATABASE_CONFIG['database']}"

# MTA API Configuration
MTA_API_CONFIG = {
    # NYC Open Data API - MTA Subway Stations
    'stations_url': 'https://data.ny.gov/resource/39hk-dx4f.json',
    
    # MTA Subway Ridership Data
    'ridership_url': 'https://data.ny.gov/resource/wujg-7c2s.json',
    
    # MTA Performance Data
    'performance_url': 'https://data.ny.gov/resource/y27x-cket.json',
    
    # App Token for NYC Open Data (optional, for higher rate limits)
    'app_token': os.getenv('NYC_OPEN_DATA_TOKEN', None),
    
    # Request settings
    'timeout': 30,
    'max_retries': 3,
    'batch_size': 50000,
}

# Data Processing Configuration
DATA_CONFIG = {
    'raw_data_dir': PROJECT_ROOT / 'data' / 'raw',
    'processed_data_dir': PROJECT_ROOT / 'data' / 'processed',
    'exports_dir': PROJECT_ROOT / 'data' / 'exports',
}

# Logging Configuration
LOGGING_CONFIG = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'standard': {
            'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        },
        'detailed': {
            'format': '%(asctime)s - %(name)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s'
        },
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'level': 'INFO',
            'formatter': 'standard',
            'stream': 'ext://sys.stdout',
        },
        'file': {
            'class': 'logging.handlers.RotatingFileHandler',
            'level': 'DEBUG',
            'formatter': 'detailed',
            'filename': str(PROJECT_ROOT / 'logs' / 'etl_pipeline.log'),
            'maxBytes': 10485760,  # 10MB
            'backupCount': 5,
        },
    },
    'loggers': {
        '': {
            'handlers': ['console', 'file'],
            'level': 'DEBUG',
            'propagate': True,
        },
    },
}

# MTA Subway Lines Configuration
SUBWAY_LINES = [
    '1', '2', '3', '4', '5', '6', '7',
    'A', 'B', 'C', 'D', 'E', 'F', 'G',
    'J', 'L', 'M', 'N', 'Q', 'R', 'S', 'W', 'Z'
]

# Borough Mapping
BOROUGHS = ['Manhattan', 'Brooklyn', 'Queens', 'Bronx', 'Staten Island']

# Time Periods for Analysis
PEAK_HOURS = {
    'morning_rush': (7, 10),   # 7 AM - 10 AM
    'midday': (10, 16),        # 10 AM - 4 PM
    'evening_rush': (16, 20),  # 4 PM - 8 PM
    'night': (20, 7),          # 8 PM - 7 AM
}

# Data Generation Settings (for synthetic data)
SYNTHETIC_DATA_CONFIG = {
    'num_stations': 472,        # Actual NYC subway stations
    'days_of_data': 365,        # One year of data
    'avg_daily_riders': 3500000,  # Average daily ridership
    'target_records': 100000,   # Target number of records
}
