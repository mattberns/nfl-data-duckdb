#!/usr/bin/env python3
"""
NFL Analytics Main Interface
Command-line interface for NFL data extraction with proper data types
"""

import argparse
import logging
import sys
from pathlib import Path
from typing import List

from nfl_analytics.database.manager import DatabaseManager
from nfl_analytics.extractors.data_extractor import NFLDataExtractor
from nfl_analytics.extractors.ecr_extractor import ECRExtractor
from nfl_analytics.models.fantasy_points import FantasyPointsCalculator

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('nfl_analytics.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)


def extract_all_data(args):
    """Extract all NFL data for specified seasons"""
    try:
        logger.info(f"Starting data extraction for seasons: {args.seasons}")
        
        with DatabaseManager(args.database) as db:
            # Create extractor with database manager
            extractor = NFLDataExtractor(db)
            
            results = extractor.extract_all_data(
                seasons=args.seasons,
                max_workers=args.workers
            )
            
            print("\n=== Extraction Results ===")
            total_records = 0
            for table, records in results.items():
                print(f"{table}: {records:,} records")
                total_records += records
            
            print(f"\nTotal records extracted: {total_records:,}")
            
            # Validate data quality
            print("\n=== Data Quality Validation ===")
            for table in results.keys():
                quality = db.validate_data_quality(table)
                if quality:
                    print(f"{table}: {quality.get('rows', 0)} rows, {quality.get('columns', 0)} columns")
                    if 'null_checks' in quality:
                        high_null_cols = [
                            col for col, info in quality['null_checks'].items()
                            if info['null_percentage'] > 50
                        ]
                        if high_null_cols:
                            print(f"  High NULL columns: {', '.join(high_null_cols)}")
            
        logger.info("Data extraction completed successfully")
        
    except Exception as e:
        logger.error(f"Failed to extract data: {e}")
        sys.exit(1)


def refresh_season(args):
    """Refresh data for a specific season"""
    try:
        logger.info(f"Refreshing season {args.season} data")
        
        with DatabaseManager(args.database) as db:
            # Create extractor with database manager
            extractor = NFLDataExtractor(db)
            
            results = extractor.refresh_season_data(
                season=args.season,
                data_types=args.data_types
            )
            
            print(f"\n=== Season {args.season} Refresh Results ===")
            for table, records in results.items():
                print(f"{table}: {records:,} records")
            
            # Validate data quality after refresh
            print(f"\n=== Data Quality After Refresh ===")
            for table in results.keys():
                quality = db.validate_data_quality(table)
                if quality:
                    print(f"{table}: {quality.get('rows', 0)} rows, {quality.get('columns', 0)} columns")
                    
        logger.info("Season refresh completed successfully")
        
    except Exception as e:
        logger.error(f"Failed to refresh season data: {e}")
        sys.exit(1)


def validate_database(args):
    """Validate database data quality"""
    try:
        logger.info("Starting database validation")
        
        with DatabaseManager(args.database) as db:
            stats = db.get_database_stats()
            
            print("\n=== Database Overview ===")
            print(f"Tables: {len(stats.get('tables', []))}")
            
            print("\n=== Record Counts ===")
            for table, count in stats.get('record_counts', {}).items():
                print(f"{table}: {count:,} records")
            
            print("\n=== Season Coverage ===")
            for table, seasons in stats.get('season_coverage', {}).items():
                if seasons:
                    print(f"{table}: {seasons}")
            
            print("\n=== Data Quality Validation ===")
            for table in stats.get('tables', []):
                if table != 'data_refresh_log':
                    quality = db.validate_data_quality(table)
                    if quality:
                        print(f"\n{table.upper()}:")
                        print(f"  Rows: {quality.get('rows', 0):,}")
                        print(f"  Columns: {quality.get('columns', 0)}")
                        
                        if 'duplicates' in quality:
                            print(f"  Duplicates: {quality['duplicates']}")
                        
                        if 'null_checks' in quality:
                            high_null_cols = [
                                f"{col} ({info['null_percentage']:.1f}%)"
                                for col, info in quality['null_checks'].items()
                                if info['null_percentage'] > 10
                            ]
                            if high_null_cols:
                                print(f"  High NULL columns: {', '.join(high_null_cols)}")
            
    except Exception as e:
        logger.error(f"Failed to validate database: {e}")
        sys.exit(1)


def show_schema(args):
    """Show database schema information"""
    try:
        with DatabaseManager(args.database) as db:
            stats = db.get_database_stats()
            
            print("\n=== Database Schema ===")
            for table in stats.get('tables', []):
                print(f"\n{table.upper()}:")
                try:
                    table_info = db.get_table_info(table)
                    for _, row in table_info.iterrows():
                        print(f"  {row['column_name']}: {row['column_type']}")
                except Exception as e:
                    print(f"  Error getting schema: {e}")
                    
    except Exception as e:
        logger.error(f"Failed to show schema: {e}")
        sys.exit(1)


def query_data(args):
    """Execute a SQL query against the database"""
    try:
        with DatabaseManager(args.database) as db:
            if args.file:
                # Read SQL from file
                with open(args.file, 'r') as f:
                    sql = f.read()
            else:
                sql = args.sql
            
            result = db.query(sql)
            
            if args.output:
                # Save to file
                if args.output.endswith('.csv'):
                    result.to_csv(args.output, index=False)
                elif args.output.endswith('.parquet'):
                    result.to_parquet(args.output, index=False)
                else:
                    result.to_json(args.output, orient='records')
                print(f"Results saved to {args.output}")
            else:
                # Print to console
                print(result.to_string())
                
    except Exception as e:
        logger.error(f"Failed to execute query: {e}")
        sys.exit(1)


def refresh_raw_ecr(args):
    """Refresh raw ECR rankings data"""
    try:
        logger.info("Starting ECR data refresh")
        
        with DatabaseManager(args.database) as db:
            # Create ECR extractor
            ecr_extractor = ECRExtractor(db)
            
            # Refresh the data
            results = ecr_extractor.refresh_raw_ecr()
            
            if 'error' in results:
                print(f"Error: {results['error']}")
                sys.exit(1)
            
            print("\n=== ECR Refresh Results ===")
            print(f"Total records: {results['total_records']:,}")
            print(f"Processed files: {results['processed_files']}")
            print(f"Failed files: {results['failed_files']}")
            
            # Display verification results
            verification = results.get('verification', {})
            if isinstance(verification, dict) and 'year_coverage' in verification:
                print("\n=== Year Coverage ===")
                for year_data in verification['year_coverage']:
                    year = year_data['year']
                    total = year_data['total_records']
                    before = year_data['before_preseason_records']
                    after = year_data['after_preseason_records']
                    print(f"{year}: {total:,} total ({before:,} pre-preseason, {after:,} preseason)")
            
            if isinstance(verification, dict) and 'data_quality' in verification:
                quality = verification['data_quality']
                print(f"\n=== Data Quality ===")
                print(f"Year range: {quality['min_year']} - {quality['max_year']}")
                print(f"Unique years: {quality['unique_years']}")
                print(f"Years with pre-preseason data: {quality['years_with_prepreseason']}")
                print(f"Years with preseason data: {quality['years_with_preseason']}")
            
        logger.info("ECR refresh completed successfully")
        
    except Exception as e:
        logger.error(f"Failed to refresh ECR data: {e}")
        sys.exit(1)





def main():
    """Main CLI interface"""
    parser = argparse.ArgumentParser(
        description='NFL Analytics Data Management System',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s extract --seasons 2022 2023 2024 --database nfl.duckdb
  %(prog)s refresh-season --season 2024 --database nfl.duckdb
  %(prog)s refresh-raw-ecr --database nfl.duckdb
  %(prog)s validate --database nfl.duckdb
  %(prog)s schema --database nfl.duckdb
  %(prog)s query --sql "SELECT * FROM weekly_stats WHERE season = 2024 LIMIT 10" --database nfl.duckdb
        """
    )
    
    parser.add_argument(
        '--database', '-d',
        default='nfl_analytics.duckdb',
        help='Path to DuckDB database file (default: nfl_analytics.duckdb)'
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Extract command
    extract_parser = subparsers.add_parser('extract', help='Extract NFL data')
    extract_parser.add_argument(
        'seasons', nargs='+', type=int,
        help='Seasons to extract (e.g., 2022 2023 2024)'
    )
    extract_parser.add_argument(
        '--workers', '-w', type=int, default=4,
        help='Number of concurrent workers (default: 4)'
    )
    extract_parser.set_defaults(func=extract_all_data)
    
    # Refresh season command
    refresh_season_parser = subparsers.add_parser('refresh-season', help='Refresh season data')
    refresh_season_parser.add_argument(
        'season', type=int,
        help='Season to refresh'
    )
    refresh_season_parser.add_argument(
        '--data-types', nargs='+',
        choices=['pbp_data', 'weekly_stats', 'seasonal_stats', 'rosters', 'injuries'],
        help='Specific data types to refresh'
    )
    refresh_season_parser.set_defaults(func=refresh_season)
    
    # Refresh ECR command
    refresh_ecr_parser = subparsers.add_parser('refresh-raw-ecr', help='Refresh Expert Consensus Rankings data')
    refresh_ecr_parser.set_defaults(func=refresh_raw_ecr)
    
    # Validate command
    validate_parser = subparsers.add_parser('validate', help='Validate database data quality')
    validate_parser.set_defaults(func=validate_database)
    
    # Schema command
    schema_parser = subparsers.add_parser('schema', help='Show database schema')
    schema_parser.set_defaults(func=show_schema)
    
    # Query command
    query_parser = subparsers.add_parser('query', help='Execute SQL query')
    query_group = query_parser.add_mutually_exclusive_group(required=True)
    query_group.add_argument(
        '--sql', '-s',
        help='SQL query to execute'
    )
    query_group.add_argument(
        '--file', '-f',
        help='File containing SQL query'
    )
    query_parser.add_argument(
        '--output', '-o',
        help='Output file (CSV, JSON, or Parquet)'
    )
    query_parser.set_defaults(func=query_data)
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        sys.exit(1)
    
    args.func(args)


if __name__ == '__main__':
    main()
