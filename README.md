# NFL Fantasy Football Analytics Database

## Project Motivation

Fantasy football analytics requires easy access to comprehensive NFL data for player evaluation, matchup analysis, and performance prediction. While the `nfl_data_py` package provides excellent data access, it requires online connectivity and real-time API calls. This project creates a **persistent, easily queryable database** that consolidates NFL data and Expert Consensus Rankings (ECR) for offline fantasy football analysis.

The system enables analysts to:
- Perform complex historical analysis without repeated API calls
- Combine official NFL statistics with fantasy expert rankings
- Run sophisticated queries for player evaluation and lineup optimization
- Access consistent data structures for reproducible analysis

## Technical Overview

This system extracts NFL data using the `nfl_data_py` package and stores it in a high-performance DuckDB database. It combines official NFL statistics with Expert Consensus Rankings (ECR) data, providing a comprehensive foundation for fantasy football analytics.

### Architecture

```
nfl_data_py API → Data Extraction → Schema Generation → DuckDB Storage → Analytics
                ↓
ECR Data Files → Raw Processing → Player Matching → Transformed Tables
```

### Key Components

1. **NFL Data Extraction**: Pulls comprehensive NFL data including play-by-play, player stats, rosters, and injuries
2. **ECR Integration**: Processes Expert Consensus Rankings and links them to official player IDs
3. **Fantasy Points Calculation**: Supports multiple scoring systems (Standard, Half PPR, Full PPR)
4. **Persistent Storage**: DuckDB provides fast analytical queries on large datasets
5. **Data Quality Management**: Comprehensive validation and error handling

### Data Tables

**Core NFL Tables:**
- `pbp_data` - Play-by-play data with detailed game information
- `weekly_stats` - Weekly player statistics with fantasy points
- `seasonal_stats` - Seasonal player aggregations
- `rosters` - Weekly roster data with player-team assignments
- `schedules` - Game schedules and results
- `teams` - Team information and metadata
- `players` - Player information and metadata
- `injuries` - Injury reports and player status

**ECR Tables:**
- `raw_ecr_rankings` - Unprocessed Expert Consensus Rankings
- `ecr_rankings` - ECR data linked to official player IDs

**Summary Tables:**
- `smry_season` - Aggregated seasonal analytics for downstream analysis

## Command Line Interface

### Core Data Management

**Refresh NFL Season Data**
```bash
python main.py --database <*.duckdb> refresh-season <year>
```
Refreshes all NFL data for the specified season by replacing existing contents with current data from `nfl_data_py`.

**Refresh Raw ECR Data**
```bash
python main.py --database <*.duckdb> refresh-raw-ecr
```
Extracts raw ECR data from the data folder and recreates the `raw_ecr_rankings` table.

**Transform ECR Data**
```bash
python main.py --database <*.duckdb> refresh-transformed-ecr
```
Updates the `ecr_rankings` table to include links to the `nfl_data_py` `player_id` field, enabling joins between ECR rankings and official statistics.

**Refresh Summary Tables**
```bash
python main.py --database <*.duckdb> refresh-summary --run-tests
```
Updates the `smry_` tables used for downstream analyses and optionally runs validation tests.

### Additional Commands

**Extract Multiple Seasons**
```bash
python main.py --database <*.duckdb> extract 2022 2023 2024
```

**Query Data**
```bash
python main.py --database <*.duckdb> query --sql "SELECT * FROM weekly_stats WHERE season = 2024 LIMIT 10"
```

**Database Validation**
```bash
python main.py --database <*.duckdb> validate
```

**Show Schema**
```bash
python main.py --database <*.duckdb> schema
```

## Installation

1. **Clone the repository:**
   ```bash
   git clone https://github.com/mattberns/nfl-data-duckdb
   cd nfl-data-duckdb
   ```

2. **Create and activate virtual environment:**
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   ```

3. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   pip install -e .
   ```

## Quick Start

1. **Extract NFL data for analysis:**
   ```bash
   python main.py --database nfl_analytics.duckdb extract 2023 2024
   ```

2. **Add ECR rankings:**
   ```bash
   python main.py --database nfl_analytics.duckdb refresh-raw-ecr
   python main.py --database nfl_analytics.duckdb refresh-transformed-ecr
   ```

3. **Generate summary tables:**
   ```bash
   python main.py --database nfl_analytics.duckdb refresh-summary --run-tests
   ```

4. **Query combined data:**
   ```bash
   python main.py --database nfl_analytics.duckdb query --sql "
   SELECT 
       w.player_name,
       w.position,
       w.team,
       w.fantasy_points_ppr,
       e.overall_rank as ecr_rank
   FROM weekly_stats w
   LEFT JOIN ecr_rankings e ON w.player_id = e.player_id 
       AND w.season = e.year 
       AND w.week = e.week
   WHERE w.season = 2024 AND w.week = 1
   ORDER BY w.fantasy_points_ppr DESC
   LIMIT 20
   "
   ```

## Fantasy Points Calculation

The system calculates fantasy points using three standard scoring systems:

**Standard (STD)**
- Passing: 1 point per 25 yards, 4 points per TD, -2 per INT
- Rushing: 1 point per 10 yards, 6 points per TD
- Receiving: 1 point per 10 yards, 6 points per TD, 0 points per reception
- Fumbles: -2 points per fumble lost

**Half PPR**
- Same as Standard + 0.5 points per reception

**Full PPR**
- Same as Standard + 1 point per reception

## Data Quality and Performance

### Data Quality Features
- Comprehensive validation of extracted data
- NULL percentage analysis for key columns
- Duplicate detection and removal
- Data type validation and conversion
- Refresh operation logging

### Performance Characteristics
- **Database Size**: ~50MB per NFL season
- **Query Performance**: Sub-second response times for most analytics queries
- **Memory Usage**: Efficient processing of large datasets
- **Concurrent Processing**: Configurable workers for data extraction

## Troubleshooting

**Common Issues:**

1. **API Rate Limiting**: Reduce concurrent workers if experiencing timeouts
2. **Memory Issues**: Process fewer seasons at once for large extractions
3. **Missing ECR Data**: Ensure ECR files are present in the data directory before running refresh commands

**Logging**: All operations are logged to `nfl_analytics.log` for debugging:
```bash
tail -f nfl_analytics.log
```

## Acknowledgments

This project builds upon excellent open-source tools and data sources:

- **[nfl_data_py](https://github.com/cooperdff/nfl_data_py)**: Comprehensive NFL data package that provides the foundation for all official statistics
- **[DuckDB](https://duckdb.org/)**: High-performance analytical database that enables fast querying of large datasets
- **NFL Community**: For providing comprehensive, well-structured data that makes projects like this possible
- **Fantasy Football Community**: For establishing standard scoring systems and consensus ranking methodologies

## License

This project is licensed under the MIT License. See LICENSE file for details.

## Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Update documentation
5. Submit a pull request

For issues and questions, please open a GitHub issue with detailed information including system specs and error messages.
