"""
Script to run the complete ETL pipeline
"""
import os
import sys
import argparse
import logging

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.etl.pipeline import ETLPipeline

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)


def main():
    """Run the ETL pipeline."""
    parser = argparse.ArgumentParser(
        description='Run the MTA Transit ETL Pipeline'
    )
    parser.add_argument(
        '--synthetic', '-s',
        action='store_true',
        default=True,
        help='Use synthetic data (default: True)'
    )
    parser.add_argument(
        '--api',
        action='store_true',
        help='Use MTA API data instead of synthetic'
    )
    parser.add_argument(
        '--records', '-r',
        type=int,
        default=100000,
        help='Target number of records (default: 100000)'
    )
    parser.add_argument(
        '--start-date',
        type=str,
        default='2025-01-01',
        help='Start date (YYYY-MM-DD)'
    )
    parser.add_argument(
        '--end-date',
        type=str,
        default='2025-12-31',
        help='End date (YYYY-MM-DD)'
    )
    parser.add_argument(
        '--export',
        action='store_true',
        help='Export data for Power BI after ETL'
    )
    parser.add_argument(
        '--export-only',
        action='store_true',
        help='Only export data (skip ETL)'
    )
    
    args = parser.parse_args()
    
    # Determine data source
    use_synthetic = not args.api
    
    print("="*60)
    print("MTA Transit ETL Pipeline")
    print("="*60)
    print(f"\nConfiguration:")
    print(f"  Data source:     {'Synthetic' if use_synthetic else 'MTA API'}")
    print(f"  Target records:  {args.records:,}")
    print(f"  Date range:      {args.start_date} to {args.end_date}")
    print(f"  Export for BI:   {'Yes' if args.export else 'No'}")
    print()
    
    # Create pipeline
    pipeline = ETLPipeline(
        use_synthetic=use_synthetic,
        synthetic_records=args.records
    )
    
    if args.export_only:
        # Just export existing data
        print("Exporting data for Power BI...")
        if pipeline.connect():
            pipeline.export_for_powerbi()
            print("Export complete!")
            return 0
        else:
            print("Failed to connect to database")
            return 1
    
    # Run full pipeline
    success = pipeline.run(
        start_date=args.start_date,
        end_date=args.end_date
    )
    
    if success and args.export:
        print("\nExporting data for Power BI...")
        pipeline.export_for_powerbi()
    
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
