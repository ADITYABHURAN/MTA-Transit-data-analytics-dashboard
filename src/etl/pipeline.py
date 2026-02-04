"""
ETL Pipeline for MTA Transit Data Analytics Dashboard
Orchestrates data extraction, transformation, and loading
"""
import logging
import logging.config
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
import json
import os

import pandas as pd

import sys
sys.path.append(str(__file__).rsplit('\\', 3)[0])
from config.settings import DATA_CONFIG, LOGGING_CONFIG, PROJECT_ROOT
from src.database.connection import get_db, test_connection
from src.etl.api_client import MTADataClient, get_mta_client
from src.etl.data_cleaning import DataCleaner, get_data_cleaner
from src.etl.data_generator import SyntheticDataGenerator, get_synthetic_generator

# Set up logging
os.makedirs(PROJECT_ROOT / 'logs', exist_ok=True)
logging.config.dictConfig(LOGGING_CONFIG)
logger = logging.getLogger(__name__)


class ETLPipeline:
    """
    Complete ETL Pipeline for MTA Transit Data.
    
    Handles:
    - Data extraction from APIs or synthetic generation
    - Data cleaning and transformation
    - Loading data into PostgreSQL database
    - Job logging and error handling
    """
    
    def __init__(self, use_synthetic: bool = True, synthetic_records: int = 100000):
        """
        Initialize the ETL pipeline.
        
        Args:
            use_synthetic: If True, use synthetic data instead of API
            synthetic_records: Target number of synthetic records
        """
        self.use_synthetic = use_synthetic
        self.synthetic_records = synthetic_records
        
        self.db = None
        self.api_client = None
        self.data_cleaner = get_data_cleaner()
        self.synthetic_generator = None
        
        self.job_stats = {
            'start_time': None,
            'end_time': None,
            'status': 'initialized',
            'tables_loaded': {},
            'errors': []
        }
        
        # Ensure data directories exist
        for dir_path in DATA_CONFIG.values():
            os.makedirs(dir_path, exist_ok=True)
        
        logger.info(f"ETL Pipeline initialized (synthetic={use_synthetic})")
    
    def _log_job(self, job_name: str, job_type: str, start_time: datetime,
                 end_time: datetime = None, status: str = 'running',
                 records_processed: int = 0, records_inserted: int = 0,
                 error_message: str = None):
        """Log ETL job execution to database."""
        if self.db is None:
            return
        
        try:
            query = """
                INSERT INTO etl_log (
                    job_name, job_type, start_time, end_time, status,
                    records_processed, records_inserted, error_message,
                    execution_details
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            self.db.execute_query(
                query,
                (
                    job_name, job_type, start_time, end_time, status,
                    records_processed, records_inserted, error_message,
                    json.dumps(self.job_stats)
                ),
                fetch=False
            )
        except Exception as e:
            logger.warning(f"Failed to log job: {e}")
    
    def connect(self) -> bool:
        """
        Establish database connection.
        
        Returns:
            True if connection successful
        """
        try:
            if test_connection():
                self.db = get_db()
                logger.info("Database connection established")
                return True
            return False
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            return False
    
    def extract_data(self, start_date: str = None, end_date: str = None) -> Dict[str, pd.DataFrame]:
        """
        Extract data from source (API or synthetic generator).
        
        Args:
            start_date: Start date for data extraction
            end_date: End date for data extraction
            
        Returns:
            Dictionary of extracted DataFrames
        """
        logger.info("Starting data extraction...")
        
        if start_date is None:
            start_date = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")
        if end_date is None:
            end_date = datetime.now().strftime("%Y-%m-%d")
        
        if self.use_synthetic:
            return self._extract_synthetic(start_date, end_date)
        else:
            return self._extract_from_api(start_date, end_date)
    
    def _extract_synthetic(self, start_date: str, end_date: str) -> Dict[str, pd.DataFrame]:
        """Extract synthetic data."""
        logger.info("Generating synthetic data...")
        
        self.synthetic_generator = get_synthetic_generator(self.synthetic_records)
        data = self.synthetic_generator.generate_all_data(start_date, end_date)
        
        # Save raw data
        raw_dir = DATA_CONFIG['raw_data_dir']
        self.synthetic_generator.save_to_csv(str(raw_dir))
        
        return data
    
    def _extract_from_api(self, start_date: str, end_date: str) -> Dict[str, pd.DataFrame]:
        """Extract data from MTA APIs."""
        logger.info("Fetching data from MTA APIs...")
        
        self.api_client = get_mta_client()
        data = {}
        
        try:
            # Fetch stations
            stations_raw = self.api_client.get_subway_stations()
            if stations_raw:
                data['stations'] = pd.DataFrame(stations_raw)
            
            # Fetch ridership
            ridership_raw = self.api_client.get_ridership_data(
                start_date=start_date,
                max_records=self.synthetic_records
            )
            if ridership_raw:
                data['ridership'] = pd.DataFrame(ridership_raw)
            
            # Fetch performance
            performance_raw = self.api_client.get_performance_data(
                max_records=50000
            )
            if performance_raw:
                data['performance'] = pd.DataFrame(performance_raw)
            
            # Fetch delays
            delays_raw = self.api_client.get_delay_data(
                start_date=start_date,
                max_records=50000
            )
            if delays_raw:
                data['delays'] = pd.DataFrame(delays_raw)
            
        except Exception as e:
            logger.error(f"API extraction failed: {e}")
            logger.info("Falling back to synthetic data generation...")
            return self._extract_synthetic(start_date, end_date)
        finally:
            if self.api_client:
                self.api_client.close()
        
        # If we didn't get enough data, supplement with synthetic
        if not data or sum(len(df) for df in data.values()) < 1000:
            logger.warning("Insufficient API data, using synthetic data")
            return self._extract_synthetic(start_date, end_date)
        
        return data
    
    def transform_data(self, raw_data: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
        """
        Transform and clean extracted data.
        
        Args:
            raw_data: Dictionary of raw DataFrames
            
        Returns:
            Dictionary of cleaned DataFrames
        """
        logger.info("Starting data transformation...")
        
        cleaned_data = {}
        
        # Clean stations
        if 'stations' in raw_data:
            cleaned_data['stations'] = self.data_cleaner.clean_station_data(raw_data['stations'])
            logger.info(f"Stations cleaned: {len(cleaned_data['stations'])} records")
        
        # Clean ridership
        if 'ridership' in raw_data:
            cleaned_data['ridership'] = self.data_cleaner.clean_ridership_data(raw_data['ridership'])
            logger.info(f"Ridership cleaned: {len(cleaned_data['ridership'])} records")
        
        # Clean delays
        if 'delays' in raw_data:
            cleaned_data['delays'] = self.data_cleaner.clean_delay_data(raw_data['delays'])
            logger.info(f"Delays cleaned: {len(cleaned_data['delays'])} records")
        
        # Clean performance
        if 'performance' in raw_data:
            cleaned_data['performance'] = self.data_cleaner.clean_performance_data(raw_data['performance'])
            logger.info(f"Performance cleaned: {len(cleaned_data['performance'])} records")
        
        # Save processed data
        processed_dir = DATA_CONFIG['processed_data_dir']
        for name, df in cleaned_data.items():
            df.to_csv(f"{processed_dir}/{name}_cleaned.csv", index=False)
        
        return cleaned_data
    
    def load_stations(self, df: pd.DataFrame) -> int:
        """Load station data into database."""
        logger.info(f"Loading {len(df)} stations...")
        
        # Prepare data for insert
        columns = ['station_name', 'station_complex_id', 'gtfs_stop_id', 'borough',
                   'latitude', 'longitude', 'structure_type', 'ada_accessible',
                   'lines_served', 'division']
        
        available_cols = [c for c in columns if c in df.columns]
        
        data = []
        for _, row in df.iterrows():
            record = tuple(
                row.get(col, None) if pd.notna(row.get(col, None)) else None
                for col in available_cols
            )
            data.append(record)
        
        placeholders = ', '.join(['%s'] * len(available_cols))
        col_str = ', '.join(available_cols)
        
        query = f"""
            INSERT INTO dim_stations ({col_str})
            VALUES ({placeholders})
            ON CONFLICT (station_name) DO UPDATE SET
                borough = EXCLUDED.borough,
                latitude = EXCLUDED.latitude,
                longitude = EXCLUDED.longitude,
                updated_at = CURRENT_TIMESTAMP
        """
        
        # Add unique constraint if not exists
        try:
            self.db.execute_query(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_stations_name_unique ON dim_stations(station_name)",
                fetch=False
            )
        except:
            pass
        
        try:
            return self.db.execute_many(query, data)
        except Exception as e:
            logger.error(f"Failed to load stations: {e}")
            # Try simpler insert
            inserted = 0
            for record in data:
                try:
                    simple_query = f"INSERT INTO dim_stations ({col_str}) VALUES ({placeholders}) ON CONFLICT DO NOTHING"
                    self.db.execute_query(simple_query, record, fetch=False)
                    inserted += 1
                except:
                    pass
            return inserted
    
    def load_ridership(self, df: pd.DataFrame) -> int:
        """Load ridership data into database."""
        logger.info(f"Loading {len(df)} ridership records...")
        
        records_loaded = 0
        batch_size = 5000
        
        for i in range(0, len(df), batch_size):
            batch = df.iloc[i:i+batch_size]
            
            data = []
            for _, row in batch.iterrows():
                # Get date_key
                if 'date' in row and row['date'] is not None:
                    date_key = int(pd.Timestamp(row['date']).strftime('%Y%m%d'))
                else:
                    continue
                
                # Get time_key
                time_key = int(row.get('hour', 0)) * 100 + int(row.get('minute', 0) if 'minute' in row else 0)
                
                data.append((
                    date_key,
                    time_key,
                    row.get('station_name', ''),
                    row.get('line_name', ''),
                    int(row.get('entries', 0)),
                    int(row.get('exits', 0)),
                    'synthetic' if self.use_synthetic else 'api'
                ))
            
            query = """
                INSERT INTO fact_ridership (date_key, time_key, station_id, line_id, entries, exits, data_source)
                SELECT 
                    %s as date_key,
                    %s as time_key,
                    (SELECT station_id FROM dim_stations WHERE station_name = %s LIMIT 1),
                    (SELECT line_id FROM dim_subway_lines WHERE line_name = %s LIMIT 1),
                    %s, %s, %s
            """
            
            try:
                self.db.execute_many(query, data)
                records_loaded += len(data)
            except Exception as e:
                logger.warning(f"Batch insert failed: {e}")
                # Continue with next batch
        
        logger.info(f"Loaded {records_loaded} ridership records")
        return records_loaded
    
    def load_delays(self, df: pd.DataFrame) -> int:
        """Load delay data into database."""
        logger.info(f"Loading {len(df)} delay records...")
        
        data = []
        for _, row in df.iterrows():
            if 'date' in row and row['date'] is not None:
                date_key = int(pd.Timestamp(row['date']).strftime('%Y%m%d'))
            else:
                continue
            
            hour = 0
            if 'incident_datetime' in row and pd.notna(row['incident_datetime']):
                hour = pd.Timestamp(row['incident_datetime']).hour
            elif 'time' in row and pd.notna(row['time']):
                hour = row['time'].hour if hasattr(row['time'], 'hour') else 0
            
            time_key = hour * 100
            
            data.append((
                date_key,
                time_key,
                row.get('line_name', ''),
                row.get('station_name', ''),
                int(row.get('delay_duration_minutes', 0)),
                str(row.get('delay_category', 'Unknown')),
                str(row.get('delay_reason', '')),
                str(row.get('severity_level', 'Medium')),
                int(row.get('passenger_impact_estimate', 0))
            ))
        
        query = """
            INSERT INTO fact_delays (
                date_key, time_key, line_id, station_id,
                delay_duration_minutes, delay_category, delay_reason,
                severity_level, passenger_impact_estimate
            )
            SELECT 
                %s, %s,
                (SELECT line_id FROM dim_subway_lines WHERE line_name = %s LIMIT 1),
                (SELECT station_id FROM dim_stations WHERE station_name = %s LIMIT 1),
                %s, %s, %s, %s, %s
        """
        
        try:
            self.db.execute_many(query, data)
            logger.info(f"Loaded {len(data)} delay records")
            return len(data)
        except Exception as e:
            logger.error(f"Failed to load delays: {e}")
            return 0
    
    def load_performance(self, df: pd.DataFrame) -> int:
        """Load performance data into database."""
        logger.info(f"Loading {len(df)} performance records...")
        
        data = []
        for _, row in df.iterrows():
            if 'date' in row and row['date'] is not None:
                date_key = int(pd.Timestamp(row['date']).strftime('%Y%m%d'))
            else:
                continue
            
            data.append((
                date_key,
                row.get('line_name', ''),
                int(row.get('scheduled_trips', 0)),
                int(row.get('actual_trips', 0)),
                int(row.get('on_time_trips', 0)),
                int(row.get('late_trips', 0)),
                int(row.get('canceled_trips', 0)),
                float(row.get('mean_distance_between_failures', 0)),
                float(row.get('wait_assessment', 0)),
                float(row.get('customer_journey_time_performance', 0))
            ))
        
        query = """
            INSERT INTO fact_performance (
                date_key, line_id, scheduled_trips, actual_trips,
                on_time_trips, late_trips, canceled_trips,
                mean_distance_between_failures, wait_assessment,
                customer_journey_time_performance
            )
            SELECT 
                %s,
                (SELECT line_id FROM dim_subway_lines WHERE line_name = %s LIMIT 1),
                %s, %s, %s, %s, %s, %s, %s, %s
            ON CONFLICT (date_key, line_id) DO UPDATE SET
                scheduled_trips = EXCLUDED.scheduled_trips,
                actual_trips = EXCLUDED.actual_trips,
                on_time_trips = EXCLUDED.on_time_trips
        """
        
        try:
            self.db.execute_many(query, data)
            logger.info(f"Loaded {len(data)} performance records")
            return len(data)
        except Exception as e:
            logger.error(f"Failed to load performance: {e}")
            return 0
    
    def load_data(self, transformed_data: Dict[str, pd.DataFrame]) -> Dict[str, int]:
        """
        Load all transformed data into database.
        
        Args:
            transformed_data: Dictionary of cleaned DataFrames
            
        Returns:
            Dictionary with record counts per table
        """
        logger.info("Starting data loading...")
        
        results = {}
        
        if 'stations' in transformed_data:
            results['stations'] = self.load_stations(transformed_data['stations'])
        
        if 'ridership' in transformed_data:
            results['ridership'] = self.load_ridership(transformed_data['ridership'])
        
        if 'delays' in transformed_data:
            results['delays'] = self.load_delays(transformed_data['delays'])
        
        if 'performance' in transformed_data:
            results['performance'] = self.load_performance(transformed_data['performance'])
        
        return results
    
    def run(self, start_date: str = None, end_date: str = None) -> bool:
        """
        Execute the complete ETL pipeline.
        
        Args:
            start_date: Start date for data extraction
            end_date: End date for data extraction
            
        Returns:
            True if pipeline completed successfully
        """
        self.job_stats['start_time'] = datetime.now()
        self.job_stats['status'] = 'running'
        
        logger.info("="*60)
        logger.info("Starting MTA Transit ETL Pipeline")
        logger.info("="*60)
        
        try:
            # Step 1: Connect to database
            logger.info("\n[Step 1/4] Connecting to database...")
            if not self.connect():
                raise Exception("Database connection failed")
            
            # Log job start
            self._log_job(
                job_name="mta_transit_etl",
                job_type="full_load",
                start_time=self.job_stats['start_time'],
                status='running'
            )
            
            # Step 2: Extract data
            logger.info("\n[Step 2/4] Extracting data...")
            raw_data = self.extract_data(start_date, end_date)
            
            if not raw_data:
                raise Exception("No data extracted")
            
            logger.info(f"Extracted data: {sum(len(df) for df in raw_data.values())} total records")
            
            # Step 3: Transform data
            logger.info("\n[Step 3/4] Transforming data...")
            transformed_data = self.transform_data(raw_data)
            
            # Step 4: Load data
            logger.info("\n[Step 4/4] Loading data into database...")
            load_results = self.load_data(transformed_data)
            
            self.job_stats['tables_loaded'] = load_results
            self.job_stats['status'] = 'completed'
            self.job_stats['end_time'] = datetime.now()
            
            # Log success
            total_records = sum(load_results.values())
            self._log_job(
                job_name="mta_transit_etl",
                job_type="full_load",
                start_time=self.job_stats['start_time'],
                end_time=self.job_stats['end_time'],
                status='completed',
                records_processed=sum(len(df) for df in raw_data.values()),
                records_inserted=total_records
            )
            
            duration = (self.job_stats['end_time'] - self.job_stats['start_time']).total_seconds()
            
            logger.info("\n" + "="*60)
            logger.info("ETL Pipeline Completed Successfully!")
            logger.info("="*60)
            logger.info(f"Duration: {duration:.1f} seconds")
            logger.info(f"Records loaded:")
            for table, count in load_results.items():
                logger.info(f"  - {table}: {count:,} records")
            logger.info(f"Total: {total_records:,} records")
            
            return True
            
        except Exception as e:
            self.job_stats['status'] = 'failed'
            self.job_stats['errors'].append(str(e))
            self.job_stats['end_time'] = datetime.now()
            
            logger.error(f"ETL Pipeline failed: {e}", exc_info=True)
            
            self._log_job(
                job_name="mta_transit_etl",
                job_type="full_load",
                start_time=self.job_stats['start_time'],
                end_time=self.job_stats['end_time'],
                status='failed',
                error_message=str(e)
            )
            
            return False
        finally:
            if self.db:
                self.db.close_pool()
    
    def export_for_powerbi(self, output_dir: str = None):
        """
        Export data in formats optimized for Power BI.
        
        Args:
            output_dir: Directory to save export files
        """
        if output_dir is None:
            output_dir = str(DATA_CONFIG['exports_dir'])
        
        os.makedirs(output_dir, exist_ok=True)
        
        logger.info(f"Exporting data for Power BI to {output_dir}...")
        
        if not self.connect():
            logger.error("Cannot export - database connection failed")
            return
        
        exports = {
            'ridership_summary': """
                SELECT 
                    d.full_date,
                    d.day_name,
                    d.month_name,
                    d.year,
                    d.is_weekend,
                    s.station_name,
                    s.borough,
                    l.line_name,
                    l.line_color,
                    SUM(r.entries) as total_entries,
                    SUM(r.exits) as total_exits,
                    SUM(r.total_traffic) as total_traffic
                FROM fact_ridership r
                JOIN dim_date d ON r.date_key = d.date_key
                LEFT JOIN dim_stations s ON r.station_id = s.station_id
                LEFT JOIN dim_subway_lines l ON r.line_id = l.line_id
                GROUP BY d.full_date, d.day_name, d.month_name, d.year, d.is_weekend,
                         s.station_name, s.borough, l.line_name, l.line_color
            """,
            'delay_analysis': """
                SELECT 
                    d.full_date,
                    d.day_name,
                    d.month_name,
                    t.time_period,
                    t.is_peak_hour,
                    l.line_name,
                    l.line_color,
                    dl.delay_category,
                    dl.severity_level,
                    dl.delay_reason,
                    dl.delay_duration_minutes,
                    dl.passenger_impact_estimate
                FROM fact_delays dl
                JOIN dim_date d ON dl.date_key = d.date_key
                LEFT JOIN dim_time t ON dl.time_key = t.time_key
                LEFT JOIN dim_subway_lines l ON dl.line_id = l.line_id
            """,
            'performance_trends': """
                SELECT 
                    d.full_date,
                    d.month_name,
                    d.year,
                    d.is_weekend,
                    l.line_name,
                    l.line_color,
                    l.line_group,
                    p.scheduled_trips,
                    p.actual_trips,
                    p.on_time_trips,
                    p.late_trips,
                    p.canceled_trips,
                    p.on_time_percentage,
                    p.wait_assessment,
                    p.customer_journey_time_performance
                FROM fact_performance p
                JOIN dim_date d ON p.date_key = d.date_key
                LEFT JOIN dim_subway_lines l ON p.line_id = l.line_id
            """,
            'stations': """
                SELECT 
                    station_id,
                    station_name,
                    borough,
                    latitude,
                    longitude,
                    structure_type,
                    ada_accessible,
                    lines_served,
                    division
                FROM dim_stations
            """,
            'subway_lines': """
                SELECT * FROM dim_subway_lines
            """
        }
        
        for name, query in exports.items():
            try:
                result = self.db.execute_query(query)
                if result:
                    df = pd.DataFrame(result)
                    df.to_csv(f"{output_dir}/{name}.csv", index=False)
                    logger.info(f"Exported {name}.csv ({len(df)} records)")
            except Exception as e:
                logger.error(f"Failed to export {name}: {e}")
        
        logger.info("Power BI export completed")


def main():
    """Main entry point for the ETL pipeline."""
    import argparse
    
    parser = argparse.ArgumentParser(description='MTA Transit ETL Pipeline')
    parser.add_argument('--synthetic', action='store_true', default=True,
                       help='Use synthetic data (default: True)')
    parser.add_argument('--api', action='store_true',
                       help='Use MTA API data')
    parser.add_argument('--records', type=int, default=100000,
                       help='Target number of records (default: 100000)')
    parser.add_argument('--start-date', type=str, default='2025-01-01',
                       help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end-date', type=str, default='2025-12-31',
                       help='End date (YYYY-MM-DD)')
    parser.add_argument('--export', action='store_true',
                       help='Export data for Power BI after ETL')
    
    args = parser.parse_args()
    
    use_synthetic = not args.api
    
    pipeline = ETLPipeline(use_synthetic=use_synthetic, synthetic_records=args.records)
    
    success = pipeline.run(start_date=args.start_date, end_date=args.end_date)
    
    if success and args.export:
        pipeline.export_for_powerbi()
    
    return 0 if success else 1


if __name__ == "__main__":
    exit(main())
