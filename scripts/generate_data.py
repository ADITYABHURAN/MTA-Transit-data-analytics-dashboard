"""
Standalone script to generate synthetic MTA transit data
Can be run without database connection to create sample datasets
"""
import os
import sys
import argparse
import logging
from datetime import datetime

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.etl.data_generator import SyntheticDataGenerator

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def main():
    """Generate synthetic MTA transit data."""
    parser = argparse.ArgumentParser(
        description='Generate synthetic MTA transit data for testing and demonstration'
    )
    parser.add_argument(
        '--records', '-r',
        type=int,
        default=100000,
        help='Target number of ridership records (default: 100000)'
    )
    parser.add_argument(
        '--start-date', '-s',
        type=str,
        default='2025-01-01',
        help='Start date for data generation (YYYY-MM-DD)'
    )
    parser.add_argument(
        '--end-date', '-e',
        type=str,
        default='2025-12-31',
        help='End date for data generation (YYYY-MM-DD)'
    )
    parser.add_argument(
        '--output', '-o',
        type=str,
        default='data/synthetic',
        help='Output directory for CSV files'
    )
    parser.add_argument(
        '--stations',
        type=int,
        default=472,
        help='Number of stations to generate (default: 472)'
    )
    
    args = parser.parse_args()
    
    print("="*60)
    print("MTA Transit Data - Synthetic Data Generator")
    print("="*60)
    print(f"\nConfiguration:")
    print(f"  Target records:  {args.records:,}")
    print(f"  Date range:      {args.start_date} to {args.end_date}")
    print(f"  Stations:        {args.stations}")
    print(f"  Output:          {args.output}")
    print()
    
    # Create output directory
    os.makedirs(args.output, exist_ok=True)
    
    # Initialize generator
    generator = SyntheticDataGenerator(target_records=args.records)
    
    # Generate all data
    print("Generating data...")
    start_time = datetime.now()
    
    # Generate stations
    stations_df = generator.generate_stations(num_stations=args.stations)
    print(f"  ✓ Generated {len(stations_df):,} stations")
    
    # Generate ridership
    ridership_df = generator.generate_ridership_data(
        start_date=args.start_date,
        end_date=args.end_date
    )
    print(f"  ✓ Generated {len(ridership_df):,} ridership records")
    
    # Generate delays
    delays_df = generator.generate_delay_data(
        start_date=args.start_date,
        end_date=args.end_date
    )
    print(f"  ✓ Generated {len(delays_df):,} delay records")
    
    # Generate performance
    performance_df = generator.generate_performance_data(
        start_date=args.start_date,
        end_date=args.end_date
    )
    print(f"  ✓ Generated {len(performance_df):,} performance records")
    
    # Save to CSV
    print(f"\nSaving files to {args.output}/...")
    generator.save_to_csv(args.output)
    
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    # Calculate total records
    total_records = (
        len(stations_df) +
        len(ridership_df) +
        len(delays_df) +
        len(performance_df)
    )
    
    # Print summary
    print("\n" + "="*60)
    print("Generation Complete!")
    print("="*60)
    print(f"\nSummary:")
    print(f"  Stations:        {len(stations_df):>10,} records")
    print(f"  Ridership:       {len(ridership_df):>10,} records")
    print(f"  Delays:          {len(delays_df):>10,} records")
    print(f"  Performance:     {len(performance_df):>10,} records")
    print(f"  {'─'*35}")
    print(f"  Total:           {total_records:>10,} records")
    print(f"\nDuration: {duration:.1f} seconds")
    print(f"Rate: {total_records/duration:,.0f} records/second")
    
    # Print file sizes
    print(f"\nGenerated files:")
    for filename in ['stations.csv', 'ridership.csv', 'delays.csv', 'performance.csv']:
        filepath = os.path.join(args.output, filename)
        if os.path.exists(filepath):
            size_mb = os.path.getsize(filepath) / (1024 * 1024)
            print(f"  {filename}: {size_mb:.2f} MB")
    
    print(f"\n✓ Data saved to: {os.path.abspath(args.output)}")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
