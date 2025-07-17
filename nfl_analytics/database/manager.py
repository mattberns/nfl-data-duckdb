"""
Database Manager for NFL Analytics
Handles DuckDB database operations with proper data types and NULL handling
"""

import duckdb
import pandas as pd
import numpy as np
from pathlib import Path
from typing import List, Optional, Dict, Any, Tuple
import logging
from datetime import datetime, date
import re

from .schema_generator import SchemaGenerator

logger = logging.getLogger(__name__)


class DatabaseManager:
    """Database manager with proper data type handling and NULL support"""
    
    def __init__(self, db_path: str = "nfl_analytics.duckdb"):
        """
        Initialize database manager
        
        Args:
            db_path: Path to DuckDB database file
        """
        self.db_path = Path(db_path)
        self.conn = None
        self.schema_generator = SchemaGenerator()
        self._initialize_database()
    
    def _initialize_database(self):
        """Initialize database connection and create schema"""
        try:
            self.conn = duckdb.connect(str(self.db_path))
            self._create_schema()
            logger.info(f"Database initialized at {self.db_path}")
        except Exception as e:
            logger.error(f"Failed to initialize database: {e}")
            raise
    
    def _create_schema(self):
        """Create minimal database schema - let tables be created dynamically"""
        try:
            # Only create essential system tables, let data tables be created automatically
            essential_schemas = {
                'data_refresh_log': """
                CREATE TABLE IF NOT EXISTS data_refresh_log (
                    id INTEGER PRIMARY KEY,
                    table_name TEXT NOT NULL,
                    season INTEGER NOT NULL,
                    week INTEGER,
                    season_type TEXT NOT NULL,
                    refresh_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    status TEXT NOT NULL CHECK (status IN ('SUCCESS', 'FAILED', 'IN_PROGRESS')),
                    error_message TEXT,
                    records_processed INTEGER DEFAULT 0
                );
                """
            }
            
            for table_name, schema_sql in essential_schemas.items():
                logger.info(f"Creating essential table: {table_name}")
                self.conn.execute(schema_sql)
            
            logger.info("Essential database schema created - data tables will be created automatically")
            
        except Exception as e:
            logger.error(f"Failed to create schema: {e}")
            raise
    
    def _convert_dataframe_types(self, df: pd.DataFrame, table_name: str) -> pd.DataFrame:
        """
        Convert DataFrame columns to appropriate types before insertion
        
        Args:
            df: DataFrame to convert
            table_name: Target table name for context
            
        Returns:
            DataFrame with converted types
        """
        if df.empty:
            return df
        
        df_converted = df.copy()
        
        # Define conversion functions that handle NaN/NULL properly
        def safe_int_convert(series):
            """Convert to integer, keeping NaN as NaN"""
            if series.dtype == 'object':
                # For text columns, try to convert to numeric first
                series = pd.to_numeric(series, errors='coerce')
            # Convert numpy types to pandas nullable types
            if series.dtype in ['int64', 'int32', 'int16', 'int8']:
                return series.astype('Int64')  # Nullable integer
            return series.astype('Int64')  # Nullable integer
        
        def safe_float_convert(series):
            """Convert to float, keeping NaN as NaN"""
            if series.dtype == 'object':
                # For text columns, try to convert to numeric first
                series = pd.to_numeric(series, errors='coerce')
            # Convert numpy types to pandas nullable types
            if series.dtype in ['float64', 'float32', 'float16']:
                return series.astype('Float64')  # Nullable float
            return series.astype('Float64')  # Nullable float
        
        def safe_date_convert(series):
            """Convert to date, keeping invalid dates as NaT"""
            if series.dtype == 'object':
                return pd.to_datetime(series, errors='coerce').dt.date
            return series
        
        def safe_datetime_convert(series):
            """Convert to datetime, keeping invalid dates as NaT"""
            if series.dtype == 'object':
                return pd.to_datetime(series, errors='coerce')
            return series
        
        def safe_bool_convert(series):
            """Convert to boolean, handling various representations"""
            if series.dtype == 'object':
                # Handle common boolean representations
                series = series.astype(str).str.lower()
                series = series.replace({
                    'true': True, 'false': False,
                    '1': True, '0': False,
                    'yes': True, 'no': False,
                    'y': True, 'n': False,
                    'nan': np.nan, 'none': np.nan, '': np.nan
                })
                return series.astype('boolean')  # Nullable boolean
            return series.astype('boolean')
        
        # Apply type conversions based on column names and overrides
        for col in df_converted.columns:
            if col in ['created_at', 'updated_at']:
                continue  # Skip metadata columns
            
            # Get expected SQL type for this column
            expected_type = self.schema_generator.get_sql_type(df_converted[col].dtype, col)
            
            try:
                if expected_type == 'INTEGER':
                    df_converted[col] = safe_int_convert(df_converted[col])
                elif expected_type == 'REAL':
                    df_converted[col] = safe_float_convert(df_converted[col])
                elif expected_type == 'DATE':
                    df_converted[col] = safe_date_convert(df_converted[col])
                elif expected_type == 'TIMESTAMP':
                    df_converted[col] = safe_datetime_convert(df_converted[col])
                elif expected_type == 'TIME':
                    if df_converted[col].dtype == 'object':
                        # Keep as string for time values
                        df_converted[col] = df_converted[col].astype(str)
                        df_converted[col] = df_converted[col].replace('nan', np.nan)
                elif expected_type == 'BOOLEAN':
                    df_converted[col] = safe_bool_convert(df_converted[col])
                elif expected_type == 'TEXT':
                    # For text columns, convert to string and handle NaN
                    df_converted[col] = df_converted[col].astype(str)
                    df_converted[col] = df_converted[col].replace('nan', np.nan)
                    df_converted[col] = df_converted[col].replace('None', np.nan)
                    df_converted[col] = df_converted[col].replace('', np.nan)
                    
            except Exception as e:
                logger.warning(f"Failed to convert column {col} to {expected_type}: {e}")
                # Keep original column if conversion fails
                continue
        
        # Final check: ensure no int64 columns remain
        for col in df_converted.columns:
            if str(df_converted[col].dtype) == 'int64':
                logger.info(f"Converting remaining int64 column {col} to Int64")
                df_converted[col] = df_converted[col].astype('Int64')
        
        return df_converted
    
    def insert_dataframe(self, df: pd.DataFrame, table_name: str, 
                        on_conflict: str = "REPLACE") -> int:
        """
        Insert DataFrame into specified table with proper type handling
        
        Args:
            df: DataFrame to insert
            table_name: Target table name
            on_conflict: How to handle conflicts (REPLACE, IGNORE)
            
        Returns:
            Number of rows inserted
        """
        try:
            if df.empty:
                logger.warning(f"No data to insert into {table_name}")
                return 0
            
            # Make a clean copy to avoid modification issues
            df_clean = df.copy()
            
            # Handle column renames for reserved keywords
            reserved_keyword_renames = {
                'desc': 'play_description',
                'order': 'play_order',
                'time': 'game_time',
                'date': 'game_date_field'
            }
            
            for old_col, new_col in reserved_keyword_renames.items():
                if old_col in df_clean.columns:
                    df_clean = df_clean.rename(columns={old_col: new_col})
            
            # Reset index to avoid int64 index issues with DuckDB
            df_clean = df_clean.reset_index(drop=True)
            
            # Add timestamp columns if they don't exist
            if 'created_at' not in df_clean.columns:
                df_clean['created_at'] = pd.Timestamp.now()
            if 'updated_at' not in df_clean.columns:
                df_clean['updated_at'] = pd.Timestamp.now()
            
            # Check if table exists, if not let DuckDB create it automatically
            table_exists = self.conn.execute(f"""
                SELECT COUNT(*) FROM information_schema.tables 
                WHERE table_name = '{table_name}'
            """).fetchone()[0] > 0
            
            if not table_exists:
                logger.info(f"Creating table {table_name} automatically from DataFrame structure")
                # Let DuckDB create the table automatically with proper types from the data
                self.conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df_clean WHERE 1=0")
            
            # Handle conflicts by clearing existing data if needed
            if on_conflict == "REPLACE":
                # For lookup tables, delete all existing data
                if table_name in ['teams', 'players']:
                    self.conn.execute(f"DELETE FROM {table_name}")
                    logger.info(f"Cleared existing data from {table_name}")
                elif table_name in ['weekly_stats', 'seasonal_stats', 'pbp_data', 'rosters', 'injuries']:
                    # For data tables, delete by season to avoid duplicate data
                    if 'season' in df_clean.columns:
                        seasons = df_clean['season'].dropna().unique()
                        for season in seasons:
                            if table_name == 'weekly_stats' and 'week' in df_clean.columns:
                                weeks = df_clean[df_clean['season'] == season]['week'].dropna().unique()
                                for week in weeks:
                                    self.conn.execute(f"DELETE FROM {table_name} WHERE season = ? AND week = ?", [int(season), int(week)])
                            else:
                                self.conn.execute(f"DELETE FROM {table_name} WHERE season = ?", [int(season)])
                        logger.info(f"Cleared existing data from {table_name} for seasons {seasons}")
            
            # Use DuckDB's direct DataFrame insertion - it handles types automatically
            self.conn.execute(f"INSERT INTO {table_name} SELECT * FROM df_clean")
            
            rows_inserted = len(df_clean)
            logger.info(f"Successfully inserted {rows_inserted} rows into {table_name}")
            return rows_inserted
            
        except Exception as e:
            logger.error(f"Failed to insert data into {table_name}: {e}")
            raise
    
    def query(self, sql: str, params: Optional[List[Any]] = None) -> pd.DataFrame:
        """
        Execute SQL query and return results as DataFrame
        
        Args:
            sql: SQL query string
            params: Optional parameters for query
            
        Returns:
            DataFrame with query results
        """
        try:
            if params:
                result = self.conn.execute(sql, params).fetchdf()
            else:
                result = self.conn.execute(sql).fetchdf()
            
            logger.info(f"Query executed successfully, returned {len(result)} rows")
            return result
            
        except Exception as e:
            logger.error(f"Failed to execute query: {e}")
            raise
    
    def get_table_info(self, table_name: str) -> pd.DataFrame:
        """
        Get information about a table's structure
        
        Args:
            table_name: Name of the table
            
        Returns:
            DataFrame with table structure info
        """
        try:
            result = self.conn.execute(f"DESCRIBE {table_name}").fetchdf()
            return result
        except Exception as e:
            logger.error(f"Failed to get table info for {table_name}: {e}")
            raise
    
    def get_last_refresh(self, table_name: str, season: int, 
                        week: Optional[int] = None) -> Optional[pd.Timestamp]:
        """
        Get the last refresh timestamp for a table/season/week combination
        
        Args:
            table_name: Table name to check
            season: Season to check
            week: Optional week to check
            
        Returns:
            Last refresh timestamp or None if not found
        """
        try:
            sql = """
            SELECT MAX(refresh_date) as last_refresh
            FROM data_refresh_log
            WHERE table_name = ? AND season = ? AND status = 'SUCCESS'
            """
            params = [table_name, season]
            
            if week is not None:
                sql += " AND week = ?"
                params.append(week)
            
            result = self.conn.execute(sql, params).fetchone()
            
            if result and result[0]:
                return pd.Timestamp(result[0])
            return None
            
        except Exception as e:
            logger.error(f"Failed to get last refresh info: {e}")
            return None
    
    def log_refresh(self, table_name: str, season: int, week: Optional[int],
                   season_type: str, status: str, error_message: str = None,
                   records_processed: int = 0):
        """
        Log a data refresh operation
        
        Args:
            table_name: Table being refreshed
            season: Season being refreshed
            week: Week being refreshed (optional)
            season_type: Type of season (REG, POST, PRE)
            status: Status of refresh (SUCCESS, FAILED, IN_PROGRESS)
            error_message: Error message if failed
            records_processed: Number of records processed
        """
        try:
            # Get next ID value
            try:
                max_id_result = self.conn.execute("SELECT COALESCE(MAX(id), 0) + 1 FROM data_refresh_log").fetchone()
                next_id = max_id_result[0] if max_id_result else 1
            except:
                next_id = 1
                
            sql = """
            INSERT INTO data_refresh_log 
            (id, table_name, season, week, season_type, status, error_message, records_processed)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """
            
            self.conn.execute(sql, [
                next_id, table_name, season, week, season_type, 
                status, error_message, records_processed
            ])
            
        except Exception as e:
            logger.error(f"Failed to log refresh: {e}")
    
    def validate_data_quality(self, table_name: str) -> Dict[str, Any]:
        """
        Validate data quality for a table
        
        Args:
            table_name: Name of the table to validate
            
        Returns:
            Dictionary with validation results
        """
        try:
            results = {}
            
            # Get table info
            table_info = self.get_table_info(table_name)
            results['columns'] = len(table_info)
            
            # Get row count
            row_count = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            results['rows'] = row_count
            
            # Check for NULL values in key columns
            null_checks = {}
            for _, row in table_info.iterrows():
                col_name = row['column_name']
                col_type = row['column_type']
                
                if col_type in ['INTEGER', 'REAL'] and col_name not in ['created_at', 'updated_at']:
                    null_count = self.conn.execute(
                        f"SELECT COUNT(*) FROM {table_name} WHERE {col_name} IS NULL"
                    ).fetchone()[0]
                    null_checks[col_name] = {
                        'null_count': null_count,
                        'null_percentage': (null_count / row_count * 100) if row_count > 0 else 0
                    }
            
            results['null_checks'] = null_checks
            
            # Check for duplicate rows (if applicable)
            if table_name in ['teams', 'players', 'schedules']:
                try:
                    # Use COLUMNS(*) to get all columns for DuckDB
                    duplicate_count = self.conn.execute(f"""
                        SELECT COUNT(*) - COUNT(DISTINCT (COLUMNS(*))) as duplicates
                        FROM {table_name}
                    """).fetchone()[0]
                    results['duplicates'] = duplicate_count
                except:
                    results['duplicates'] = 0
            
            return results
            
        except Exception as e:
            logger.error(f"Failed to validate data quality for {table_name}: {e}")
            return {}
    
    def get_database_stats(self) -> Dict[str, Any]:
        """
        Get comprehensive database statistics
        
        Returns:
            Dictionary with database statistics
        """
        try:
            stats = {}
            
            # Get all tables
            tables = self.conn.execute("""
                SELECT table_name FROM information_schema.tables 
                WHERE table_schema = 'main'
            """).fetchall()
            
            table_names = [t[0] for t in tables]
            stats['tables'] = table_names
            
            # Get record counts
            record_counts = {}
            for table in table_names:
                try:
                    count = self.conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                    record_counts[table] = count
                except:
                    record_counts[table] = 0
            
            stats['record_counts'] = record_counts
            
            # Get season coverage
            season_coverage = {}
            for table in ['weekly_stats', 'seasonal_stats', 'pbp_data', 'schedules']:
                if table in table_names:
                    try:
                        seasons = self.conn.execute(
                            f"SELECT DISTINCT season FROM {table} ORDER BY season"
                        ).fetchall()
                        season_coverage[table] = [s[0] for s in seasons]
                    except:
                        season_coverage[table] = []
            
            stats['season_coverage'] = season_coverage
            
            return stats
            
        except Exception as e:
            logger.error(f"Failed to get database stats: {e}")
            return {}
    
    def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
            logger.info("Database connection closed")
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
