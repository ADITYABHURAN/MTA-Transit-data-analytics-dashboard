"""
Synthetic Data Generator for MTA Transit Dashboard
Generates realistic transit data for testing and demonstration
"""
import logging
from typing import Dict, List, Tuple, Optional
from datetime import datetime, timedelta, time
import random
import hashlib

import pandas as pd
import numpy as np

import sys
sys.path.append(str(__file__).rsplit('\\', 3)[0])
from config.settings import SUBWAY_LINES, BOROUGHS, PEAK_HOURS, SYNTHETIC_DATA_CONFIG

logger = logging.getLogger(__name__)

# Seed for reproducibility
np.random.seed(42)
random.seed(42)


class SyntheticDataGenerator:
    """
    Generates realistic synthetic MTA transit data for testing and demonstration.
    """
    
    # Real NYC Subway Stations (sample of major stations)
    MAJOR_STATIONS = [
        ("Times Square-42nd St", "Manhattan", 40.756, -73.987, "A,C,E,N,Q,R,S,W,1,2,3,7"),
        ("Grand Central-42nd St", "Manhattan", 40.752, -73.977, "4,5,6,7,S"),
        ("Penn Station", "Manhattan", 40.750, -73.992, "1,2,3,A,C,E"),
        ("Union Square-14th St", "Manhattan", 40.735, -73.990, "L,N,Q,R,W,4,5,6"),
        ("Fulton Street", "Manhattan", 40.710, -74.008, "A,C,J,Z,2,3,4,5"),
        ("Herald Square-34th St", "Manhattan", 40.749, -73.988, "B,D,F,M,N,Q,R,W"),
        ("Lexington Av-59th St", "Manhattan", 40.762, -73.967, "4,5,6,N,R,W"),
        ("Columbus Circle-59th St", "Manhattan", 40.768, -73.982, "A,B,C,D,1"),
        ("Canal Street", "Manhattan", 40.720, -74.000, "A,C,E,J,Z,N,Q,R,W,6"),
        ("Chambers Street", "Manhattan", 40.715, -74.009, "A,C,1,2,3"),
        ("Atlantic Av-Barclays", "Brooklyn", 40.684, -73.978, "B,D,N,Q,R,2,3,4,5"),
        ("Jay Street-MetroTech", "Brooklyn", 40.692, -73.986, "A,C,F,R"),
        ("Bedford Av", "Brooklyn", 40.717, -73.957, "L"),
        ("Williamsburg Bridge", "Brooklyn", 40.714, -73.958, "J,M,Z"),
        ("DeKalb Av", "Brooklyn", 40.691, -73.982, "B,D,N,Q,R"),
        ("Borough Hall", "Brooklyn", 40.693, -73.990, "2,3,4,5,R"),
        ("Flushing-Main St", "Queens", 40.759, -73.830, "7"),
        ("Jackson Heights-Roosevelt", "Queens", 40.746, -73.891, "E,F,M,R,7"),
        ("Jamaica-179th St", "Queens", 40.712, -73.784, "F"),
        ("Astoria-Ditmars Blvd", "Queens", 40.775, -73.912, "N,W"),
        ("Court Square", "Queens", 40.747, -73.946, "E,G,M,7"),
        ("Forest Hills-71st Av", "Queens", 40.722, -73.844, "E,F,M,R"),
        ("161st St-Yankee Stadium", "Bronx", 40.828, -73.926, "4,B,D"),
        ("3rd Ave-149th St", "Bronx", 40.816, -73.918, "2,5"),
        ("Pelham Parkway", "Bronx", 40.858, -73.867, "2,5"),
        ("Fordham Road", "Bronx", 40.861, -73.888, "4,B,D"),
        ("Kingsbridge Road", "Bronx", 40.867, -73.897, "4,B,D"),
        ("St George", "Staten Island", 40.644, -74.074, "SIR"),
    ]
    
    DELAY_REASONS = [
        "Signal problems", "Switch problems", "Track maintenance",
        "Sick passenger", "Police investigation", "Debris on tracks",
        "Overcrowding", "Train traffic", "Mechanical problems",
        "Weather conditions", "Unattended package", "Track fire",
        "Medical emergency", "Door problems", "Power outage",
        "Earlier incident", "Crew availability", "Bridge operations"
    ]
    
    def __init__(self, target_records: int = 100000):
        """
        Initialize the synthetic data generator.
        
        Args:
            target_records: Target number of records to generate
        """
        self.target_records = target_records
        self.stations_df = None
        self.ridership_df = None
        self.delays_df = None
        self.performance_df = None
        
        logger.info(f"SyntheticDataGenerator initialized for {target_records} records")
    
    def _generate_station_id(self, station_name: str) -> str:
        """Generate a consistent station ID from name."""
        return hashlib.md5(station_name.encode()).hexdigest()[:8].upper()
    
    def generate_stations(self, num_stations: int = 472) -> pd.DataFrame:
        """
        Generate synthetic station data.
        
        Args:
            num_stations: Number of stations to generate
            
        Returns:
            DataFrame with station data
        """
        logger.info(f"Generating {num_stations} stations...")
        
        stations = []
        
        # Add real major stations first
        for station_data in self.MAJOR_STATIONS:
            stations.append({
                'station_id': self._generate_station_id(station_data[0]),
                'station_name': station_data[0],
                'station_complex_id': f"SC{len(stations) + 1:03d}",
                'gtfs_stop_id': f"STOP{len(stations) + 1:03d}",
                'borough': station_data[1],
                'latitude': station_data[2] + np.random.normal(0, 0.0001),
                'longitude': station_data[3] + np.random.normal(0, 0.0001),
                'structure_type': np.random.choice(['Underground', 'Elevated', 'At Grade'], p=[0.7, 0.25, 0.05]),
                'ada_accessible': np.random.choice([True, False], p=[0.3, 0.7]),
                'lines_served': station_data[4],
                'division': np.random.choice(['IRT', 'BMT', 'IND'], p=[0.33, 0.33, 0.34]),
            })
        
        # Generate additional stations
        street_names = ['Street', 'Avenue', 'Boulevard', 'Road', 'Place', 'Parkway']
        directions = ['', 'North', 'South', 'East', 'West']
        
        while len(stations) < num_stations:
            borough = random.choice(BOROUGHS)
            
            # Generate random station name
            if random.random() < 0.5:
                # Number-based street name
                street_num = random.randint(1, 200)
                suffix = 'th'
                if street_num % 10 == 1 and street_num != 11:
                    suffix = 'st'
                elif street_num % 10 == 2 and street_num != 12:
                    suffix = 'nd'
                elif street_num % 10 == 3 and street_num != 13:
                    suffix = 'rd'
                station_name = f"{street_num}{suffix} {random.choice(street_names)}"
            else:
                # Named station
                names = ['Park', 'Central', 'Main', 'Church', 'Broadway', 'Atlantic', 
                         'Ocean', 'Metropolitan', 'Northern', 'Southern', 'Eastern', 
                         'Western', 'Grand', 'Liberty', 'Washington', 'Lincoln']
                station_name = f"{random.choice(names)} {random.choice(street_names)}"
            
            # Borough-specific coordinate ranges
            coord_ranges = {
                'Manhattan': (40.70, 40.88, -74.02, -73.93),
                'Brooklyn': (40.57, 40.74, -74.04, -73.83),
                'Queens': (40.65, 40.80, -73.96, -73.70),
                'Bronx': (40.80, 40.92, -73.93, -73.75),
                'Staten Island': (40.50, 40.65, -74.25, -74.05)
            }
            lat_min, lat_max, lon_min, lon_max = coord_ranges.get(borough, coord_ranges['Manhattan'])
            
            # Generate random lines served (1-4 lines)
            num_lines = random.choices([1, 2, 3, 4], weights=[0.4, 0.35, 0.2, 0.05])[0]
            lines = random.sample(SUBWAY_LINES, min(num_lines, len(SUBWAY_LINES)))
            
            stations.append({
                'station_id': self._generate_station_id(station_name + str(len(stations))),
                'station_name': station_name,
                'station_complex_id': f"SC{len(stations) + 1:03d}",
                'gtfs_stop_id': f"STOP{len(stations) + 1:03d}",
                'borough': borough,
                'latitude': random.uniform(lat_min, lat_max),
                'longitude': random.uniform(lon_min, lon_max),
                'structure_type': np.random.choice(['Underground', 'Elevated', 'At Grade'], p=[0.6, 0.3, 0.1]),
                'ada_accessible': np.random.choice([True, False], p=[0.25, 0.75]),
                'lines_served': ','.join(lines),
                'division': np.random.choice(['IRT', 'BMT', 'IND'], p=[0.33, 0.33, 0.34]),
            })
        
        self.stations_df = pd.DataFrame(stations)
        logger.info(f"Generated {len(self.stations_df)} stations")
        
        return self.stations_df
    
    def generate_ridership_data(self, start_date: str = "2025-01-01",
                                 end_date: str = "2025-12-31",
                                 records_per_day: int = None) -> pd.DataFrame:
        """
        Generate synthetic ridership data.
        
        Args:
            start_date: Start date for data generation
            end_date: End date for data generation
            records_per_day: Records per day (auto-calculated if None)
            
        Returns:
            DataFrame with ridership data
        """
        if self.stations_df is None:
            self.generate_stations()
        
        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")
        total_days = (end - start).days + 1
        
        if records_per_day is None:
            records_per_day = max(50, self.target_records // total_days)
        
        logger.info(f"Generating ridership data from {start_date} to {end_date}...")
        logger.info(f"Target: {records_per_day} records per day, ~{records_per_day * total_days} total")
        
        records = []
        current_date = start
        
        # Station popularity weights (based on real ridership patterns)
        major_station_names = [s[0] for s in self.MAJOR_STATIONS]
        station_weights = []
        
        for _, row in self.stations_df.iterrows():
            if row['station_name'] in major_station_names:
                weight = random.uniform(3, 10)  # Major stations have higher weight
            elif row['borough'] == 'Manhattan':
                weight = random.uniform(1.5, 4)
            else:
                weight = random.uniform(0.5, 2)
            station_weights.append(weight)
        
        station_weights = np.array(station_weights)
        station_weights = station_weights / station_weights.sum()
        
        while current_date <= end:
            is_weekend = current_date.weekday() >= 5
            is_holiday = current_date.month == 12 and current_date.day in [24, 25, 31]
            
            # Fewer records on weekends/holidays
            day_records = records_per_day
            if is_weekend:
                day_records = int(day_records * 0.6)
            if is_holiday:
                day_records = int(day_records * 0.4)
            
            # Select stations for this day
            day_stations = np.random.choice(
                len(self.stations_df), 
                size=min(day_records, len(self.stations_df)),
                replace=True,
                p=station_weights
            )
            
            for station_idx in day_stations:
                station = self.stations_df.iloc[station_idx]
                
                # Generate hour (peak hours have more records)
                hour_weights = np.ones(24)
                hour_weights[7:10] = 3.0   # Morning rush
                hour_weights[17:20] = 3.5  # Evening rush
                hour_weights[10:17] = 1.5  # Midday
                hour_weights[0:6] = 0.3    # Late night
                hour_weights = hour_weights / hour_weights.sum()
                
                hour = np.random.choice(24, p=hour_weights)
                minute = random.randint(0, 59)
                
                # Base ridership varies by hour
                if 7 <= hour <= 9 or 17 <= hour <= 19:
                    base_entries = random.randint(500, 5000)
                elif 10 <= hour <= 16:
                    base_entries = random.randint(200, 2000)
                else:
                    base_entries = random.randint(50, 500)
                
                # Adjust for weekend
                if is_weekend:
                    base_entries = int(base_entries * 0.5)
                
                # Adjust for station importance
                if station['station_name'] in major_station_names:
                    base_entries = int(base_entries * random.uniform(2, 5))
                
                # Get a random line from this station
                lines = station['lines_served'].split(',')
                line = random.choice(lines).strip()
                
                records.append({
                    'date': current_date.date(),
                    'time': time(hour, minute),
                    'hour': hour,
                    'station_id': station['station_id'],
                    'station_name': station['station_name'],
                    'line_name': line,
                    'entries': base_entries,
                    'exits': int(base_entries * random.uniform(0.85, 1.15)),
                    'is_weekend': is_weekend,
                    'is_peak_hour': 7 <= hour <= 9 or 17 <= hour <= 19
                })
            
            current_date += timedelta(days=1)
        
        self.ridership_df = pd.DataFrame(records)
        
        # Add datetime column
        self.ridership_df['datetime'] = pd.to_datetime(
            self.ridership_df['date'].astype(str) + ' ' + 
            self.ridership_df['time'].astype(str)
        )
        
        logger.info(f"Generated {len(self.ridership_df)} ridership records")
        
        return self.ridership_df
    
    def generate_delay_data(self, start_date: str = "2025-01-01",
                             end_date: str = "2025-12-31",
                             avg_delays_per_day: int = 15) -> pd.DataFrame:
        """
        Generate synthetic delay incident data.
        
        Args:
            start_date: Start date for data generation
            end_date: End date for data generation
            avg_delays_per_day: Average number of delays per day
            
        Returns:
            DataFrame with delay data
        """
        logger.info(f"Generating delay data from {start_date} to {end_date}...")
        
        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")
        
        records = []
        current_date = start
        
        while current_date <= end:
            # Number of delays varies by day
            is_weekend = current_date.weekday() >= 5
            num_delays = np.random.poisson(avg_delays_per_day)
            
            if is_weekend:
                num_delays = int(num_delays * 0.6)
            
            # Weather effect (more delays in winter)
            if current_date.month in [12, 1, 2, 3]:
                num_delays = int(num_delays * 1.3)
            
            for _ in range(num_delays):
                # Peak hours have more delays
                hour = np.random.choice(
                    24,
                    p=np.array([0.02]*6 + [0.08]*3 + [0.04]*7 + [0.08]*3 + [0.03]*5)
                )
                minute = random.randint(0, 59)
                
                line = random.choice(SUBWAY_LINES)
                
                # Delay duration (most delays are short)
                delay_minutes = int(np.random.exponential(15) + 2)
                delay_minutes = min(delay_minutes, 120)  # Cap at 2 hours
                
                if self.stations_df is not None:
                    station = self.stations_df.sample(1).iloc[0]
                    station_name = station['station_name']
                else:
                    station_name = random.choice([s[0] for s in self.MAJOR_STATIONS])
                
                reason = random.choice(self.DELAY_REASONS)
                
                # Categorize severity
                if delay_minutes <= 5:
                    severity = 'Low'
                    category = 'Minor'
                elif delay_minutes <= 15:
                    severity = 'Medium'
                    category = 'Moderate'
                elif delay_minutes <= 30:
                    severity = 'Medium'
                    category = 'Significant'
                else:
                    severity = 'High'
                    category = 'Major' if delay_minutes <= 60 else 'Severe'
                
                # Estimate passenger impact
                if 7 <= hour <= 9 or 17 <= hour <= 19:
                    passenger_impact = delay_minutes * random.randint(100, 500)
                else:
                    passenger_impact = delay_minutes * random.randint(20, 150)
                
                records.append({
                    'date': current_date.date(),
                    'time': time(hour, minute),
                    'incident_datetime': datetime.combine(current_date.date(), time(hour, minute)),
                    'line_name': line,
                    'station_name': station_name,
                    'delay_duration_minutes': delay_minutes,
                    'delay_category': category,
                    'delay_reason': reason,
                    'severity_level': severity,
                    'passenger_impact_estimate': passenger_impact,
                    'is_resolved': True,
                    'resolution_time_minutes': delay_minutes + random.randint(5, 30)
                })
            
            current_date += timedelta(days=1)
        
        self.delays_df = pd.DataFrame(records)
        logger.info(f"Generated {len(self.delays_df)} delay records")
        
        return self.delays_df
    
    def generate_performance_data(self, start_date: str = "2025-01-01",
                                   end_date: str = "2025-12-31") -> pd.DataFrame:
        """
        Generate synthetic daily performance metrics by line.
        
        Args:
            start_date: Start date for data generation
            end_date: End date for data generation
            
        Returns:
            DataFrame with performance data
        """
        logger.info(f"Generating performance data from {start_date} to {end_date}...")
        
        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")
        
        records = []
        current_date = start
        
        # Base performance varies by line
        line_base_performance = {}
        for line in SUBWAY_LINES:
            line_base_performance[line] = random.uniform(0.75, 0.92)
        
        while current_date <= end:
            is_weekend = current_date.weekday() >= 5
            
            for line in SUBWAY_LINES:
                base_otp = line_base_performance[line]
                
                # Add daily variation
                daily_variation = np.random.normal(0, 0.03)
                
                # Weekend performance is typically better
                if is_weekend:
                    daily_variation += 0.02
                
                # Winter months have worse performance
                if current_date.month in [12, 1, 2, 3]:
                    daily_variation -= 0.02
                
                otp = max(0.5, min(1.0, base_otp + daily_variation))
                
                # Calculate trip numbers
                if is_weekend:
                    scheduled = random.randint(150, 250)
                else:
                    scheduled = random.randint(300, 450)
                
                on_time = int(scheduled * otp)
                late = int(scheduled * (1 - otp) * 0.7)
                canceled = scheduled - on_time - late
                
                records.append({
                    'date': current_date.date(),
                    'line_name': line,
                    'scheduled_trips': scheduled,
                    'actual_trips': scheduled - canceled,
                    'on_time_trips': on_time,
                    'late_trips': late,
                    'canceled_trips': max(0, canceled),
                    'on_time_percentage': round(otp * 100, 2),
                    'mean_distance_between_failures': random.uniform(50000, 150000),
                    'wait_assessment': round(random.uniform(70, 95), 2),
                    'customer_journey_time_performance': round(random.uniform(75, 95), 2)
                })
            
            current_date += timedelta(days=1)
        
        self.performance_df = pd.DataFrame(records)
        logger.info(f"Generated {len(self.performance_df)} performance records")
        
        return self.performance_df
    
    def generate_all_data(self, start_date: str = "2025-01-01",
                          end_date: str = "2025-12-31") -> Dict[str, pd.DataFrame]:
        """
        Generate all synthetic datasets.
        
        Args:
            start_date: Start date for data generation
            end_date: End date for data generation
            
        Returns:
            Dictionary containing all generated DataFrames
        """
        logger.info("Generating all synthetic data...")
        
        self.generate_stations()
        self.generate_ridership_data(start_date, end_date)
        self.generate_delay_data(start_date, end_date)
        self.generate_performance_data(start_date, end_date)
        
        total_records = (
            len(self.stations_df) + 
            len(self.ridership_df) + 
            len(self.delays_df) + 
            len(self.performance_df)
        )
        
        logger.info(f"Generated {total_records} total records across all tables")
        
        return {
            'stations': self.stations_df,
            'ridership': self.ridership_df,
            'delays': self.delays_df,
            'performance': self.performance_df
        }
    
    def save_to_csv(self, output_dir: str):
        """
        Save all generated data to CSV files.
        
        Args:
            output_dir: Directory to save CSV files
        """
        import os
        os.makedirs(output_dir, exist_ok=True)
        
        if self.stations_df is not None:
            self.stations_df.to_csv(f"{output_dir}/stations.csv", index=False)
            logger.info(f"Saved stations.csv")
        
        if self.ridership_df is not None:
            self.ridership_df.to_csv(f"{output_dir}/ridership.csv", index=False)
            logger.info(f"Saved ridership.csv")
        
        if self.delays_df is not None:
            self.delays_df.to_csv(f"{output_dir}/delays.csv", index=False)
            logger.info(f"Saved delays.csv")
        
        if self.performance_df is not None:
            self.performance_df.to_csv(f"{output_dir}/performance.csv", index=False)
            logger.info(f"Saved performance.csv")
        
        logger.info(f"All data saved to {output_dir}")


def get_synthetic_generator(target_records: int = 100000) -> SyntheticDataGenerator:
    """Factory function to create SyntheticDataGenerator instance."""
    return SyntheticDataGenerator(target_records)


if __name__ == "__main__":
    # Test the generator
    logging.basicConfig(level=logging.INFO)
    
    generator = get_synthetic_generator(100000)
    
    print("\n=== Generating Synthetic MTA Transit Data ===\n")
    
    data = generator.generate_all_data("2025-01-01", "2025-12-31")
    
    print("\nGenerated Data Summary:")
    print(f"  Stations:    {len(data['stations']):,} records")
    print(f"  Ridership:   {len(data['ridership']):,} records")
    print(f"  Delays:      {len(data['delays']):,} records")
    print(f"  Performance: {len(data['performance']):,} records")
    print(f"  Total:       {sum(len(df) for df in data.values()):,} records")
    
    print("\nSample Station:")
    print(data['stations'].head(1).to_string())
    
    print("\nSample Ridership:")
    print(data['ridership'].head(1).to_string())
