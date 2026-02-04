"""
Database connection and utility functions for MTA Transit Dashboard
"""
import logging
from contextlib import contextmanager
from typing import Optional, Generator, Any

import psycopg2
from psycopg2 import pool, extras
from psycopg2.extensions import connection

import sys
sys.path.append(str(__file__).rsplit('\\', 2)[0])
from config.settings import DATABASE_CONFIG

logger = logging.getLogger(__name__)


class DatabaseConnection:
    """
    Manages PostgreSQL database connections with connection pooling.
    """
    
    _instance: Optional['DatabaseConnection'] = None
    _pool: Optional[pool.ThreadedConnectionPool] = None
    
    def __new__(cls) -> 'DatabaseConnection':
        """Singleton pattern to ensure single pool instance."""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self, min_connections: int = 2, max_connections: int = 10):
        """
        Initialize the database connection pool.
        
        Args:
            min_connections: Minimum number of connections in the pool
            max_connections: Maximum number of connections in the pool
        """
        if self._pool is None:
            try:
                self._pool = pool.ThreadedConnectionPool(
                    min_connections,
                    max_connections,
                    host=DATABASE_CONFIG['host'],
                    port=DATABASE_CONFIG['port'],
                    database=DATABASE_CONFIG['database'],
                    user=DATABASE_CONFIG['user'],
                    password=DATABASE_CONFIG['password']
                )
                logger.info(f"Database connection pool created successfully. "
                           f"Database: {DATABASE_CONFIG['database']}")
            except psycopg2.Error as e:
                logger.error(f"Failed to create database connection pool: {e}")
                raise
    
    @contextmanager
    def get_connection(self) -> Generator[connection, None, None]:
        """
        Get a connection from the pool as a context manager.
        
        Yields:
            psycopg2 connection object
        """
        conn = None
        try:
            conn = self._pool.getconn()
            yield conn
        except psycopg2.Error as e:
            logger.error(f"Database connection error: {e}")
            raise
        finally:
            if conn:
                self._pool.putconn(conn)
    
    @contextmanager
    def get_cursor(self, cursor_factory=None) -> Generator[Any, None, None]:
        """
        Get a cursor with automatic transaction management.
        
        Args:
            cursor_factory: Optional cursor factory (e.g., RealDictCursor)
            
        Yields:
            psycopg2 cursor object
        """
        with self.get_connection() as conn:
            cursor = conn.cursor(cursor_factory=cursor_factory)
            try:
                yield cursor
                conn.commit()
            except psycopg2.Error as e:
                conn.rollback()
                logger.error(f"Database operation failed, rolling back: {e}")
                raise
            finally:
                cursor.close()
    
    def execute_query(self, query: str, params: tuple = None, 
                      fetch: bool = True) -> Optional[list]:
        """
        Execute a single query and optionally fetch results.
        
        Args:
            query: SQL query string
            params: Query parameters
            fetch: Whether to fetch and return results
            
        Returns:
            Query results if fetch=True, else None
        """
        with self.get_cursor(cursor_factory=extras.RealDictCursor) as cursor:
            cursor.execute(query, params)
            if fetch:
                return cursor.fetchall()
            return None
    
    def execute_many(self, query: str, data: list) -> int:
        """
        Execute a query for multiple rows efficiently.
        
        Args:
            query: SQL query string with placeholders
            data: List of tuples containing row data
            
        Returns:
            Number of rows affected
        """
        with self.get_cursor() as cursor:
            extras.execute_batch(cursor, query, data, page_size=1000)
            return cursor.rowcount
    
    def bulk_insert(self, table: str, columns: list, data: list,
                    on_conflict: str = None) -> int:
        """
        Bulk insert data using COPY command for maximum performance.
        
        Args:
            table: Target table name
            columns: List of column names
            data: List of tuples containing row data
            on_conflict: ON CONFLICT clause (optional)
            
        Returns:
            Number of rows inserted
        """
        if not data:
            return 0
        
        columns_str = ', '.join(columns)
        placeholders = ', '.join(['%s'] * len(columns))
        
        query = f"INSERT INTO {table} ({columns_str}) VALUES ({placeholders})"
        
        if on_conflict:
            query += f" {on_conflict}"
        
        with self.get_cursor() as cursor:
            extras.execute_batch(cursor, query, data, page_size=5000)
            logger.info(f"Bulk inserted {len(data)} rows into {table}")
            return len(data)
    
    def copy_from_dataframe(self, df, table: str, columns: list = None) -> int:
        """
        Copy data from pandas DataFrame using PostgreSQL COPY.
        
        Args:
            df: pandas DataFrame
            table: Target table name
            columns: Column names (defaults to DataFrame columns)
            
        Returns:
            Number of rows copied
        """
        import io
        
        if columns is None:
            columns = list(df.columns)
        
        buffer = io.StringIO()
        df[columns].to_csv(buffer, index=False, header=False, sep='\t', na_rep='\\N')
        buffer.seek(0)
        
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.copy_from(buffer, table, columns=columns, sep='\t', null='\\N')
                conn.commit()
                logger.info(f"Copied {len(df)} rows to {table}")
                return len(df)
    
    def table_exists(self, table_name: str) -> bool:
        """Check if a table exists in the database."""
        query = """
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = %s
            );
        """
        result = self.execute_query(query, (table_name,))
        return result[0]['exists'] if result else False
    
    def get_table_row_count(self, table_name: str) -> int:
        """Get the row count for a table."""
        query = f"SELECT COUNT(*) as count FROM {table_name};"
        result = self.execute_query(query)
        return result[0]['count'] if result else 0
    
    def close_pool(self):
        """Close all connections in the pool."""
        if self._pool:
            self._pool.closeall()
            logger.info("Database connection pool closed")
            DatabaseConnection._pool = None
            DatabaseConnection._instance = None


# Convenience function for quick database operations
def get_db() -> DatabaseConnection:
    """Get the database connection instance."""
    return DatabaseConnection()


def test_connection() -> bool:
    """
    Test database connectivity.
    
    Returns:
        True if connection successful, False otherwise
    """
    try:
        db = get_db()
        result = db.execute_query("SELECT version();")
        if result:
            logger.info(f"Database connection successful: {result[0]['version'][:50]}...")
            return True
        return False
    except Exception as e:
        logger.error(f"Database connection test failed: {e}")
        return False


if __name__ == "__main__":
    # Test the connection
    logging.basicConfig(level=logging.INFO)
    
    if test_connection():
        print("✓ Database connection successful!")
        
        db = get_db()
        
        # Test table check
        tables = ['dim_subway_lines', 'dim_stations', 'fact_ridership']
        for table in tables:
            exists = db.table_exists(table)
            print(f"  Table '{table}': {'exists' if exists else 'not found'}")
        
        db.close_pool()
    else:
        print("✗ Database connection failed!")
