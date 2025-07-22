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
from summarizers import create_smry_season_table, run_all_tests

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


def refresh_transformed_ecr(args):
    """Transform raw ECR data into ECR rankings with player IDs"""
    try:
        logger.info("Starting ECR transformation with player ID matching")
        
        with DatabaseManager(args.database) as db:
            # Create ECR extractor
            ecr_extractor = ECRExtractor(db)
            
            # Transform the data
            results = ecr_extractor.create_ecr_rankings_with_player_ids()
            
            if 'error' in results:
                print(f"Error: {results['error']}")
                sys.exit(1)
            
            print("\n=== ECR Transformation Results ===")
            print(f"Total records processed: {results.get('total_records', 0):,}")
            print(f"Successfully matched: {results.get('matched_records', 0):,}")
            print(f"Unmatched records: {results.get('unmatched_records', 0):,}")
            
            # Display match statistics if available
            if 'match_stats' in results:
                stats = results['match_stats']
                print(f"\n=== Match Statistics ===")
                print(f"Exact matches: {stats.get('exact_matches', 0):,}")
                print(f"Partial matches: {stats.get('partial_matches', 0):,}")
                print(f"No matches: {stats.get('no_matches', 0):,}")
            
            # Display verification results if available
            if 'verification' in results:
                verification = results['verification']
                print(f"\n=== Data Verification ===")
                print(f"Final table records: {verification.get('final_count', 0):,}")
                print(f"Duplicate records removed: {verification.get('duplicates_removed', 0):,}")
            
        logger.info("ECR transformation completed successfully")
        
    except Exception as e:
        logger.error(f"Failed to transform ECR data: {e}")
        sys.exit(1)


def refresh_summary_tables(args):
    """Refresh all summary (smry_) tables"""
    try:
        logger.info("Starting summary tables refresh")
        
        # Dictionary to track summary table functions
        summary_functions = {
            'smry_season': create_smry_season_table
        }
        
        # Dictionary to track results
        results = {}
        
        print("\n=== Summary Tables Refresh ===")
        
        # Create each summary table
        for table_name, create_func in summary_functions.items():
            logger.info(f"Creating {table_name} table...")
            print(f"Creating {table_name}...")
            
            try:
                create_func(args.database)
                
                # Get record count
                with DatabaseManager(args.database) as db:
                    count_result = db.query(f"SELECT COUNT(*) as count FROM {table_name}")
                    record_count = count_result.iloc[0]['count']
                    results[table_name] = record_count
                    print(f"  ✅ {table_name}: {record_count:,} records")
                    
            except Exception as e:
                logger.error(f"Failed to create {table_name}: {e}")
                print(f"  ❌ {table_name}: Failed - {e}")
                results[table_name] = 0
        
        # Run tests if requested
        if args.run_tests:
            print("\n=== Running Tests ===")
            test_results = run_all_tests(args.database)
            
            passed = sum(test_results.values())
            total = len(test_results)
            
            print(f"\nTest Summary: {passed}/{total} tests passed")
            
            if passed != total:
                logger.warning(f"Some tests failed: {passed}/{total} passed")
            else:
                logger.info("All tests passed successfully")
        
        # Summary
        print(f"\n=== Summary Tables Refresh Complete ===")
        total_records = sum(results.values())
        successful_tables = len([v for v in results.values() if v > 0])
        total_tables = len(results)
        
        print(f"Tables created: {successful_tables}/{total_tables}")
        print(f"Total records: {total_records:,}")
        
        for table_name, count in results.items():
            status = "✅" if count > 0 else "❌"
            print(f"  {status} {table_name}: {count:,} records")
        
        if successful_tables != total_tables:
            logger.error(f"Some summary tables failed to create: {successful_tables}/{total_tables}")
            sys.exit(1)
        
        logger.info("Summary tables refresh completed successfully")
        
    except Exception as e:
        logger.error(f"Failed to refresh summary tables: {e}")
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
  %(prog)s refresh-transformed-ecr --database nfl.duckdb
  %(prog)s refresh-summary --database nfl.duckdb --run-tests
  %(prog)s validate --database nfl.duckdb
  %(prog)s schema --database nfl.duckdb
  %(prog)s query --sql "SELECT * FROM weekly_stats WHERE season = 2024 LIMIT 10" --database nfl.duckdb
        """
    )
    
    parser.add_argument(
        '--database', '-d',
        default='prod_nfl.duckdb',
        help='Path to DuckDB database file (default: prod_nfl.duckdb)'
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
    
    # Refresh transformed ECR command
    refresh_transformed_ecr_parser = subparsers.add_parser('refresh-transformed-ecr', help='Transform raw ECR data into ECR rankings with player IDs')
    refresh_transformed_ecr_parser.set_defaults(func=refresh_transformed_ecr)
    
    # Refresh summary tables command
    refresh_summary_parser = subparsers.add_parser('refresh-summary', help='Refresh all summary (smry_) tables')
    refresh_summary_parser.add_argument(
        '--run-tests', action='store_true',
        help='Run validation tests after creating summary tables'
    )
    refresh_summary_parser.set_defaults(func=refresh_summary_tables)
    
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
