"""
Script to initialize the database with schema and reference data
"""
import os
import sys
import argparse
import logging

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.database.connection import get_db, test_connection

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def read_sql_file(filepath: str) -> str:
    """Read SQL file content."""
    with open(filepath, 'r', encoding='utf-8') as f:
        return f.read()


def execute_sql_statements(db, sql_content: str) -> int:
    """Execute SQL statements from content."""
    # Split by semicolons but be careful with functions
    statements = []
    current_statement = []
    in_function = False
    
    for line in sql_content.split('\n'):
        stripped = line.strip()
        
        # Track function/procedure blocks
        if 'CREATE OR REPLACE FUNCTION' in line.upper() or 'CREATE FUNCTION' in line.upper():
            in_function = True
        
        current_statement.append(line)
        
        if in_function and '$$ LANGUAGE' in line:
            in_function = False
            statements.append('\n'.join(current_statement))
            current_statement = []
        elif not in_function and stripped.endswith(';') and not stripped.startswith('--'):
            statements.append('\n'.join(current_statement))
            current_statement = []
    
    # Add any remaining statement
    if current_statement:
        remaining = '\n'.join(current_statement).strip()
        if remaining and not remaining.startswith('--'):
            statements.append(remaining)
    
    executed = 0
    errors = 0
    
    for statement in statements:
        statement = statement.strip()
        if not statement or statement.startswith('--'):
            continue
        
        # Skip comments-only blocks
        lines = [l for l in statement.split('\n') if l.strip() and not l.strip().startswith('--')]
        if not lines:
            continue
        
        try:
            db.execute_query(statement, fetch=False)
            executed += 1
        except Exception as e:
            error_msg = str(e).lower()
            # Ignore certain non-critical errors
            if 'already exists' in error_msg:
                logger.debug(f"Object already exists, skipping")
            elif 'does not exist' in error_msg and 'drop' in statement.lower():
                logger.debug(f"Object doesn't exist for drop, skipping")
            else:
                logger.warning(f"Statement failed: {str(e)[:100]}")
                errors += 1
    
    return executed


def main():
    """Initialize the database."""
    parser = argparse.ArgumentParser(
        description='Initialize the MTA Transit database with schema'
    )
    parser.add_argument(
        '--schema-only',
        action='store_true',
        help='Only create schema, skip analytics views'
    )
    parser.add_argument(
        '--force',
        action='store_true',
        help='Force recreation of tables (drops existing data)'
    )
    
    args = parser.parse_args()
    
    print("="*60)
    print("MTA Transit Database Initialization")
    print("="*60)
    
    # Test connection
    print("\nTesting database connection...")
    if not test_connection():
        print("✗ Database connection failed!")
        print("\nPlease ensure:")
        print("  1. PostgreSQL is running")
        print("  2. Database 'mta_transit_db' exists")
        print("  3. Connection settings in config/settings.py are correct")
        return 1
    
    print("✓ Database connection successful")
    
    # Get database connection
    db = get_db()
    
    # Find SQL files
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    schema_file = os.path.join(project_root, 'sql', 'schema', '01_create_schema.sql')
    queries_file = os.path.join(project_root, 'sql', 'queries', 'analytics_queries.sql')
    
    # Execute schema
    if os.path.exists(schema_file):
        print(f"\nExecuting schema creation...")
        sql_content = read_sql_file(schema_file)
        executed = execute_sql_statements(db, sql_content)
        print(f"✓ Executed {executed} schema statements")
    else:
        print(f"✗ Schema file not found: {schema_file}")
        return 1
    
    # Execute analytics views
    if not args.schema_only and os.path.exists(queries_file):
        print(f"\nCreating analytics views...")
        sql_content = read_sql_file(queries_file)
        executed = execute_sql_statements(db, sql_content)
        print(f"✓ Executed {executed} analytics statements")
    
    # Verify tables
    print("\nVerifying database objects...")
    
    tables_to_check = [
        'dim_subway_lines',
        'dim_stations',
        'dim_date',
        'dim_time',
        'fact_ridership',
        'fact_delays',
        'fact_performance',
        'etl_log'
    ]
    
    for table in tables_to_check:
        if db.table_exists(table):
            count = db.get_table_row_count(table)
            print(f"  ✓ {table}: {count:,} records")
        else:
            print(f"  ✗ {table}: NOT FOUND")
    
    # Close connection
    db.close_pool()
    
    print("\n" + "="*60)
    print("Database initialization complete!")
    print("="*60)
    print("\nNext steps:")
    print("  1. Run ETL pipeline: python scripts/run_etl.py")
    print("  2. Or generate sample data: python scripts/generate_data.py")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
