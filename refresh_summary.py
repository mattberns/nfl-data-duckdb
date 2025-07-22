#!/usr/bin/env python3
"""
Summary Tables Refresh Script
Command-line interface for refreshing NFL summary tables
"""

import argparse
import logging
import sys
import duckdb
from pathlib import Path

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


def refresh_summary_tables(db_path: str, run_tests: bool = False):
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
                create_func(db_path)
                
                # Get record count
                with duckdb.connect(db_path) as conn:
                    count_result = conn.execute(f"SELECT COUNT(*) as count FROM {table_name}").fetchone()
                    record_count = count_result[0]
                    results[table_name] = record_count
                    print(f"  ✅ {table_name}: {record_count:,} records")
                    
            except Exception as e:
                logger.error(f"Failed to create {table_name}: {e}")
                print(f"  ❌ {table_name}: Failed - {e}")
                results[table_name] = 0
        
        # Run tests if requested
        if run_tests:
            print("\n=== Running Tests ===")
            test_results = run_all_tests(db_path)
            
            passed = sum(test_results.values())
            total = len(test_results)
            
            print(f"\nTest Summary: {passed}/{total} tests passed")
            
            if passed != total:
                logger.warning(f"Some tests failed: {passed}/{total} passed")
                return False
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
            return False
        
        logger.info("Summary tables refresh completed successfully")
        return True
        
    except Exception as e:
        logger.error(f"Failed to refresh summary tables: {e}")
        return False


def main():
    """Main CLI interface"""
    parser = argparse.ArgumentParser(
        description='NFL Analytics Summary Tables Refresh',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --database prod_nfl.duckdb
  %(prog)s --database prod_nfl.duckdb --run-tests
        """
    )
    
    parser.add_argument(
        '--database', '-d',
        default='prod_nfl.duckdb',
        help='Path to DuckDB database file (default: prod_nfl.duckdb)'
    )
    
    parser.add_argument(
        '--run-tests', action='store_true',
        help='Run validation tests after creating summary tables'
    )
    
    args = parser.parse_args()
    
    # Check if database file exists
    if not Path(args.database).exists():
        logger.error(f"Database file not found: {args.database}")
        sys.exit(1)
    
    success = refresh_summary_tables(args.database, args.run_tests)
    
    if not success:
        sys.exit(1)


if __name__ == '__main__':
    main()
