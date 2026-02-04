"""
Data Cleaning and Preprocessing Module for MTA Transit Data
"""
import logging
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime, timedelta
import re

import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)


class DataCleaner:
    """
    Handles data cleaning, validation, and preprocessing for MTA transit data.
    """
    
    def __init__(self):
        """Initialize the data cleaner."""
        self.cleaning_stats = {
            'total_records': 0,
            'cleaned_records': 0,
            'dropped_records': 0,
            'nulls_filled': 0,
            'duplicates_removed': 0
        }
        logger.info("DataCleaner initialized")
    
    def reset_stats(self):
        """Reset cleaning statistics."""
        for key in self.cleaning_stats:
            self.cleaning_stats[key] = 0
    
    def get_stats(self) -> Dict[str, int]:
        """Get cleaning statistics."""
        return self.cleaning_stats.copy()
    
    def clean_station_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean and normalize station data.
        
        Args:
            df: Raw station DataFrame
            
        Returns:
            Cleaned DataFrame
        """
        self.reset_stats()
        self.cleaning_stats['total_records'] = len(df)
        
        logger.info(f"Cleaning station data: {len(df)} records")
        
        # Make a copy to avoid modifying original
        df = df.copy()
        
        # Standardize column names
        df.columns = df.columns.str.lower().str.replace(' ', '_').str.replace('-', '_')
        
        # Handle station name cleaning
        if 'stop_name' in df.columns:
            df['station_name'] = df['stop_name'].str.strip().str.title()
        elif 'station_name' in df.columns:
            df['station_name'] = df['station_name'].str.strip().str.title()
        
        # Clean borough names
        if 'borough' in df.columns:
            df['borough'] = df['borough'].str.strip().str.title()
            # Standardize borough names
            borough_mapping = {
                'M': 'Manhattan',
                'Mn': 'Manhattan',
                'Bk': 'Brooklyn',
                'Q': 'Queens',
                'Qn': 'Queens',
                'Bx': 'Bronx',
                'Si': 'Staten Island'
            }
            df['borough'] = df['borough'].replace(borough_mapping)
        
        # Clean coordinates
        for col in ['latitude', 'gtfs_latitude', 'lat']:
            if col in df.columns:
                df['latitude'] = pd.to_numeric(df[col], errors='coerce')
                break
        
        for col in ['longitude', 'gtfs_longitude', 'lon', 'long']:
            if col in df.columns:
                df['longitude'] = pd.to_numeric(df[col], errors='coerce')
                break
        
        # Validate coordinate ranges (NYC bounds)
        if 'latitude' in df.columns:
            valid_lat = (df['latitude'] >= 40.4) & (df['latitude'] <= 41.0)
            df.loc[~valid_lat, 'latitude'] = np.nan
        
        if 'longitude' in df.columns:
            valid_lon = (df['longitude'] >= -74.3) & (df['longitude'] <= -73.7)
            df.loc[~valid_lon, 'longitude'] = np.nan
        
        # Clean line information
        if 'daytime_routes' in df.columns:
            df['lines_served'] = df['daytime_routes'].str.replace(' ', ',')
        elif 'line' in df.columns:
            df['lines_served'] = df['line']
        
        # Remove duplicates
        initial_count = len(df)
        if 'station_name' in df.columns:
            df = df.drop_duplicates(subset=['station_name'], keep='first')
        self.cleaning_stats['duplicates_removed'] = initial_count - len(df)
        
        # Drop rows with missing critical fields
        critical_cols = ['station_name']
        before_drop = len(df)
        df = df.dropna(subset=[c for c in critical_cols if c in df.columns])
        self.cleaning_stats['dropped_records'] = before_drop - len(df)
        
        self.cleaning_stats['cleaned_records'] = len(df)
        
        logger.info(f"Station data cleaned: {len(df)} records remaining")
        logger.info(f"Cleaning stats: {self.cleaning_stats}")
        
        return df
    
    def clean_ridership_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean and normalize ridership data.
        
        Args:
            df: Raw ridership DataFrame
            
        Returns:
            Cleaned DataFrame
        """
        self.reset_stats()
        self.cleaning_stats['total_records'] = len(df)
        
        logger.info(f"Cleaning ridership data: {len(df)} records")
        
        df = df.copy()
        
        # Standardize column names
        df.columns = df.columns.str.lower().str.replace(' ', '_').str.replace('-', '_')
        
        # Parse datetime fields
        datetime_cols = ['transit_timestamp', 'date', 'datetime', 'timestamp']
        for col in datetime_cols:
            if col in df.columns:
                df['datetime'] = pd.to_datetime(df[col], errors='coerce')
                df['date'] = df['datetime'].dt.date
                df['time'] = df['datetime'].dt.time
                df['hour'] = df['datetime'].dt.hour
                break
        
        # Clean numeric fields
        numeric_cols = ['entries', 'exits', 'ridership', 'count', 'total_entries', 'total_exits']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
                # Remove negative values
                df.loc[df[col] < 0, col] = np.nan
        
        # Handle entries/exits columns
        if 'entries' not in df.columns:
            for col in ['total_entries', 'entry_count', 'entry']:
                if col in df.columns:
                    df['entries'] = df[col]
                    break
        
        if 'exits' not in df.columns:
            for col in ['total_exits', 'exit_count', 'exit']:
                if col in df.columns:
                    df['exits'] = df[col]
                    break
        
        # Fill missing values
        if 'entries' in df.columns:
            nulls_before = df['entries'].isna().sum()
            df['entries'] = df['entries'].fillna(0).astype(int)
            self.cleaning_stats['nulls_filled'] += nulls_before
        
        if 'exits' in df.columns:
            nulls_before = df['exits'].isna().sum()
            df['exits'] = df['exits'].fillna(0).astype(int)
            self.cleaning_stats['nulls_filled'] += nulls_before
        
        # Clean station names
        if 'station' in df.columns:
            df['station_name'] = df['station'].str.strip().str.title()
        elif 'station_name' in df.columns:
            df['station_name'] = df['station_name'].str.strip().str.title()
        
        # Clean line names
        if 'line' in df.columns:
            df['line_name'] = df['line'].str.strip().str.upper()
        elif 'line_name' in df.columns:
            df['line_name'] = df['line_name'].str.strip().str.upper()
        
        # Remove outliers (entries/exits > 3 std from mean)
        for col in ['entries', 'exits']:
            if col in df.columns and len(df) > 0:
                mean_val = df[col].mean()
                std_val = df[col].std()
                if std_val > 0:
                    upper_limit = mean_val + 3 * std_val
                    df.loc[df[col] > upper_limit, col] = upper_limit
        
        # Remove duplicates
        initial_count = len(df)
        dedup_cols = [c for c in ['datetime', 'station_name', 'line_name'] if c in df.columns]
        if dedup_cols:
            df = df.drop_duplicates(subset=dedup_cols, keep='first')
        self.cleaning_stats['duplicates_removed'] = initial_count - len(df)
        
        # Drop rows with missing datetime
        before_drop = len(df)
        if 'datetime' in df.columns:
            df = df.dropna(subset=['datetime'])
        self.cleaning_stats['dropped_records'] = before_drop - len(df)
        
        self.cleaning_stats['cleaned_records'] = len(df)
        
        logger.info(f"Ridership data cleaned: {len(df)} records remaining")
        
        return df
    
    def clean_delay_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean and normalize delay incident data.
        
        Args:
            df: Raw delay DataFrame
            
        Returns:
            Cleaned DataFrame
        """
        self.reset_stats()
        self.cleaning_stats['total_records'] = len(df)
        
        logger.info(f"Cleaning delay data: {len(df)} records")
        
        df = df.copy()
        
        # Standardize column names
        df.columns = df.columns.str.lower().str.replace(' ', '_').str.replace('-', '_')
        
        # Parse datetime fields
        for col in ['start_date', 'incident_date', 'date', 'timestamp']:
            if col in df.columns:
                df['incident_datetime'] = pd.to_datetime(df[col], errors='coerce')
                df['date'] = df['incident_datetime'].dt.date
                df['time'] = df['incident_datetime'].dt.time
                break
        
        # Parse end datetime if available
        for col in ['end_date', 'resolution_date']:
            if col in df.columns:
                df['resolution_datetime'] = pd.to_datetime(df[col], errors='coerce')
                break
        
        # Calculate delay duration
        if 'delay_duration_minutes' not in df.columns:
            if 'delay_minutes' in df.columns:
                df['delay_duration_minutes'] = pd.to_numeric(df['delay_minutes'], errors='coerce')
            elif 'duration' in df.columns:
                df['delay_duration_minutes'] = pd.to_numeric(df['duration'], errors='coerce')
            elif 'incident_datetime' in df.columns and 'resolution_datetime' in df.columns:
                duration = (df['resolution_datetime'] - df['incident_datetime']).dt.total_seconds() / 60
                df['delay_duration_minutes'] = duration.clip(lower=0)
        
        # Clean line names
        for col in ['line', 'subway_line', 'route']:
            if col in df.columns:
                df['line_name'] = df[col].str.strip().str.upper()
                break
        
        # Clean reason/cause
        for col in ['reason', 'cause', 'delay_reason', 'description']:
            if col in df.columns:
                df['delay_reason'] = df[col].str.strip()
                break
        
        # Categorize delays
        if 'delay_duration_minutes' in df.columns:
            df['delay_category'] = pd.cut(
                df['delay_duration_minutes'],
                bins=[0, 5, 15, 30, 60, float('inf')],
                labels=['Minor', 'Moderate', 'Significant', 'Major', 'Severe']
            )
            
            df['severity_level'] = pd.cut(
                df['delay_duration_minutes'],
                bins=[0, 10, 30, float('inf')],
                labels=['Low', 'Medium', 'High']
            )
        
        # Fill missing delay durations with median
        if 'delay_duration_minutes' in df.columns:
            median_delay = df['delay_duration_minutes'].median()
            nulls_before = df['delay_duration_minutes'].isna().sum()
            df['delay_duration_minutes'] = df['delay_duration_minutes'].fillna(median_delay)
            self.cleaning_stats['nulls_filled'] += nulls_before
        
        # Remove duplicates
        initial_count = len(df)
        dedup_cols = [c for c in ['incident_datetime', 'line_name', 'delay_reason'] if c in df.columns]
        if dedup_cols:
            df = df.drop_duplicates(subset=dedup_cols, keep='first')
        self.cleaning_stats['duplicates_removed'] = initial_count - len(df)
        
        self.cleaning_stats['cleaned_records'] = len(df)
        
        logger.info(f"Delay data cleaned: {len(df)} records remaining")
        
        return df
    
    def clean_performance_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean and normalize performance metric data.
        
        Args:
            df: Raw performance DataFrame
            
        Returns:
            Cleaned DataFrame
        """
        self.reset_stats()
        self.cleaning_stats['total_records'] = len(df)
        
        logger.info(f"Cleaning performance data: {len(df)} records")
        
        df = df.copy()
        
        # Standardize column names
        df.columns = df.columns.str.lower().str.replace(' ', '_').str.replace('-', '_')
        
        # Parse date/period fields
        if 'period_month' in df.columns and 'period_year' in df.columns:
            df['date'] = pd.to_datetime(
                df['period_year'].astype(str) + '-' + 
                df['period_month'].astype(str).str.zfill(2) + '-01',
                errors='coerce'
            )
        elif 'period' in df.columns:
            df['date'] = pd.to_datetime(df['period'], errors='coerce')
        
        # Clean line names
        for col in ['line', 'subway_line', 'indicator_line', 'route']:
            if col in df.columns:
                df['line_name'] = df[col].str.strip().str.upper()
                break
        
        # Clean percentage fields
        pct_cols = ['on_time_percentage', 'otp', 'on_time_rate', 
                    'wait_assessment', 'customer_journey_time_performance']
        
        for col in pct_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
                # Ensure percentages are in 0-100 range
                if df[col].max() <= 1.0:
                    df[col] = df[col] * 100
        
        # Clean trip counts
        trip_cols = ['scheduled_trips', 'actual_trips', 'on_time_trips', 
                     'late_trips', 'canceled_trips']
        
        for col in trip_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
                nulls_before = df[col].isna().sum()
                df[col] = df[col].fillna(0).astype(int)
                self.cleaning_stats['nulls_filled'] += nulls_before
        
        # Remove duplicates
        initial_count = len(df)
        dedup_cols = [c for c in ['date', 'line_name'] if c in df.columns]
        if dedup_cols:
            df = df.drop_duplicates(subset=dedup_cols, keep='last')
        self.cleaning_stats['duplicates_removed'] = initial_count - len(df)
        
        self.cleaning_stats['cleaned_records'] = len(df)
        
        logger.info(f"Performance data cleaned: {len(df)} records remaining")
        
        return df
    
    def validate_data_quality(self, df: pd.DataFrame, 
                              required_cols: List[str] = None) -> Tuple[bool, Dict[str, Any]]:
        """
        Validate data quality and return a report.
        
        Args:
            df: DataFrame to validate
            required_cols: List of required column names
            
        Returns:
            Tuple of (is_valid, quality_report)
        """
        report = {
            'total_records': len(df),
            'total_columns': len(df.columns),
            'missing_required_cols': [],
            'null_percentages': {},
            'duplicate_count': 0,
            'issues': []
        }
        
        # Check required columns
        if required_cols:
            for col in required_cols:
                if col not in df.columns:
                    report['missing_required_cols'].append(col)
                    report['issues'].append(f"Missing required column: {col}")
        
        # Calculate null percentages
        for col in df.columns:
            null_pct = (df[col].isna().sum() / len(df)) * 100 if len(df) > 0 else 0
            if null_pct > 0:
                report['null_percentages'][col] = round(null_pct, 2)
                if null_pct > 50:
                    report['issues'].append(f"High null rate ({null_pct:.1f}%) in column: {col}")
        
        # Check for duplicates
        if len(df) > 0:
            report['duplicate_count'] = len(df) - len(df.drop_duplicates())
        
        # Determine if data is valid
        is_valid = (
            len(report['missing_required_cols']) == 0 and
            len(report['issues']) == 0
        )
        
        return is_valid, report


def get_data_cleaner() -> DataCleaner:
    """Factory function to create DataCleaner instance."""
    return DataCleaner()


if __name__ == "__main__":
    # Test the data cleaner
    logging.basicConfig(level=logging.INFO)
    
    cleaner = get_data_cleaner()
    
    # Create sample data
    sample_df = pd.DataFrame({
        'station_name': ['Times Square', ' penn station ', 'GRAND CENTRAL'],
        'borough': ['M', 'Mn', 'Manhattan'],
        'latitude': [40.756, 40.750, 40.752],
        'longitude': [-73.987, -73.992, -73.977],
        'entries': [1000, None, 1500],
        'exits': [900, 800, -100]  # Include invalid value
    })
    
    print("Original data:")
    print(sample_df)
    
    cleaned = cleaner.clean_station_data(sample_df)
    
    print("\nCleaned data:")
    print(cleaned)
    
    print("\nCleaning stats:")
    print(cleaner.get_stats())
