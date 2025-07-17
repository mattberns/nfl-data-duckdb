# NFL Analytics System

A comprehensive NFL data analytics system that extracts, stores, and analyzes NFL data using `nfl_data_py` and DuckDB. This system provides persistent storage for offline analysis and supports multiple fantasy football scoring systems.

## Features

- ✅ Complete NFL data extraction from `nfl_data_py` package
- ✅ Persistent DuckDB storage for offline analysis
- ✅ Individual week and season refresh capabilities
- ✅ Fantasy points calculation (STD, HALF PPR, FULL PPR)
- ✅ Comprehensive CLI interface
- ✅ Concurrent data extraction for performance
- ✅ Data refresh tracking and logging
- ✅ Flexible SQL querying capabilities

## Installation

1. **Clone the repository:**
   ```bash
   git clone <repository-url>
   cd nfl-analytics
   ```

2. **Create and activate virtual environment:**
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   ```

3. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

4. **Install the package:**
   ```bash
   pip install -e .
   ```

## Quick Start

### 1. Extract NFL Data

Extract all available NFL data for specific seasons:

```bash
python main.py extract 2023 2024 --database nfl_analytics.duckdb
```

This will extract:
- Play-by-play data
- Weekly player statistics
- Seasonal player statistics
- Team schedules
- Player information
- Team information
- Roster data
- Injury reports

### 2. Query the Data

Execute SQL queries against the database:

```bash
# Get top 10 fantasy performers in 2023
python main.py query --sql "
SELECT player_name, position, team, fantasy_points_std, fantasy_points_full_ppr
FROM weekly_stats 
WHERE season = 2023 
ORDER BY fantasy_points_std DESC 
LIMIT 10
" --database nfl_analytics.duckdb
```

### 3. Show Database Statistics

Check what data is available:

```bash
python main.py stats --database nfl_analytics.duckdb
```

## Data Tables

The system creates the following tables in DuckDB:

### Core Tables

- **`pbp_data`**: Play-by-play data with detailed game information
- **`weekly_stats`**: Weekly player statistics with fantasy points
- **`seasonal_stats`**: Seasonal player statistics with fantasy points
- **`schedules`**: Game schedules and results
- **`teams`**: Team information and metadata
- **`players`**: Player information and metadata
- **`rosters`**: Weekly roster data
- **`injuries`**: Injury reports and player status

### System Tables

- **`data_refresh_log`**: Tracks data refresh operations

## Fantasy Points Calculation

The system calculates fantasy points using three scoring systems:

### Standard (STD)
- Passing: 1 point per 25 yards, 4 points per TD, -2 per INT
- Rushing: 1 point per 10 yards, 6 points per TD
- Receiving: 1 point per 10 yards, 6 points per TD, 0 points per reception
- Fumbles: -2 points per fumble lost

### Half PPR
- Same as Standard + 0.5 points per reception

### Full PPR
- Same as Standard + 1 point per reception

## CLI Commands

### Extract Data

```bash
# Extract data for multiple seasons
python main.py extract 2020 2021 2022 2023 2024

# Extract with custom database location
python main.py extract 2023 --database /path/to/custom.duckdb

# Extract with more concurrent workers
python main.py extract 2023 --workers 8
```

### Refresh Data

```bash
# Refresh all data for a season
python main.py refresh-season 2024

# Refresh specific data types for a season
python main.py refresh-season 2024 --data-types weekly_stats seasonal_stats

# Refresh a specific week
python main.py refresh-week 2024 5
```

### Query Data

```bash
# Execute SQL query
python main.py query --sql "SELECT * FROM teams"

# Execute SQL from file
python main.py query --file query.sql

# Save results to CSV
python main.py query --sql "SELECT * FROM weekly_stats WHERE season = 2023" --output results.csv

# Save results to Parquet
python main.py query --sql "SELECT * FROM weekly_stats WHERE season = 2023" --output results.parquet
```

## Usage Examples

### Python API Usage

```python
from nfl_analytics import DatabaseManager, NFLDataExtractor, FantasyPointsCalculator

# Initialize database
with DatabaseManager('nfl_analytics.duckdb') as db:
    # Extract data
    extractor = NFLDataExtractor(db)
    results = extractor.extract_all_data([2023, 2024])
    
    # Query data
    top_qbs = db.query("""
        SELECT player_name, team, fantasy_points_std
        FROM weekly_stats
        WHERE position = 'QB' AND season = 2023
        ORDER BY fantasy_points_std DESC
        LIMIT 10
    """)
    
    # Calculate custom fantasy points
    fantasy_calc = FantasyPointsCalculator()
    player_stats = {
        'passing_yards': 300,
        'passing_tds': 2,
        'rushing_yards': 50,
        'rushing_tds': 1
    }
    points = fantasy_calc.calculate_player_fantasy_points(player_stats, 'std')
```

### Common SQL Queries

#### Top Fantasy Performers by Position

```sql
SELECT 
    player_name, 
    position, 
    team, 
    AVG(fantasy_points_std) as avg_fantasy_points
FROM weekly_stats
WHERE season = 2023 AND position = 'RB'
GROUP BY player_name, position, team
HAVING COUNT(*) >= 10  -- At least 10 games
ORDER BY avg_fantasy_points DESC
LIMIT 20;
```

#### Best Matchups by Team Defense

```sql
SELECT 
    w.opponent_team as defense,
    w.position,
    AVG(w.fantasy_points_std) as avg_points_allowed
FROM weekly_stats w
WHERE w.season = 2023 AND w.position IN ('QB', 'RB', 'WR', 'TE')
GROUP BY w.opponent_team, w.position
ORDER BY w.opponent_team, avg_points_allowed DESC;
```

#### Player Performance Trends

```sql
SELECT 
    player_name,
    week,
    fantasy_points_std,
    LAG(fantasy_points_std) OVER (PARTITION BY player_name ORDER BY week) as prev_week_points,
    fantasy_points_std - LAG(fantasy_points_std) OVER (PARTITION BY player_name ORDER BY week) as week_change
FROM weekly_stats
WHERE season = 2023 AND player_name = 'Josh Allen'
ORDER BY week;
```

## Data Refresh Strategy

### Full Season Refresh
- Run at the end of each season
- Updates all data types for the season
- Useful for final statistics and corrections

### Weekly Refresh
- Run after each game week
- Updates only the current week's data
- Fast and efficient for real-time analysis

### Incremental Updates
- The system tracks what data has been refreshed
- Only updates changed data when possible
- Logs all refresh operations for audit trail

## Performance Considerations

### Concurrent Extraction
- Default: 4 concurrent workers
- Increase for better performance (if API allows)
- Monitor API rate limits

### Database Size
- Full season data: ~50MB per season
- Multiple seasons: Scale accordingly
- DuckDB handles large datasets efficiently

### Memory Usage
- Large datasets loaded into memory during processing
- Monitor system memory during extraction
- Consider processing seasons individually if needed

## Troubleshooting

### Common Issues

1. **API Rate Limiting**
   - Reduce concurrent workers
   - Add delays between requests
   - Monitor nfl_data_py logs

2. **Memory Issues**
   - Process fewer seasons at once
   - Reduce concurrent workers
   - Monitor system resources

3. **Database Corruption**
   - DuckDB files are resilient
   - Backup database before major operations
   - Use database repair tools if needed

### Logging

All operations are logged to `nfl_analytics.log`:

```bash
# View recent logs
tail -f nfl_analytics.log

# Search for errors
grep ERROR nfl_analytics.log
```

## Development

### Project Structure

```
nfl_analytics/
├── __init__.py
├── database/
│   ├── __init__.py
│   └── manager.py          # Database operations
├── extractors/
│   ├── __init__.py
│   └── data_extractor.py   # Data extraction logic
├── models/
│   ├── __init__.py
│   └── fantasy_points.py   # Fantasy points calculation
└── utils/
    └── __init__.py
```

### Testing

```bash
# Run basic system test
python -c "
from nfl_analytics import DatabaseManager
with DatabaseManager('test.duckdb') as db:
    print('Database connection successful')
"

# Test fantasy points calculation
python -c "
from nfl_analytics import FantasyPointsCalculator
calc = FantasyPointsCalculator()
points = calc.calculate_player_fantasy_points({'passing_yards': 300, 'passing_tds': 2}, 'std')
print(f'Fantasy points: {points}')
"
```

### Adding New Features

1. **New Data Sources**: Add extraction methods to `NFLDataExtractor`
2. **Custom Scoring**: Extend `FantasyPointsCalculator` with new systems
3. **New Queries**: Add query templates to documentation
4. **New Tables**: Update database schema in `DatabaseManager`

## Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Update documentation
5. Submit a pull request

## License

This project is licensed under the MIT License. See LICENSE file for details.

## Support

For issues and questions:
1. Check the troubleshooting section
2. Review the logs for error details
3. Open an issue with detailed information
4. Include system information and error messages

## Acknowledgments

- **nfl_data_py**: Excellent NFL data package
- **DuckDB**: High-performance analytical database
- **NFL community**: For providing comprehensive data
