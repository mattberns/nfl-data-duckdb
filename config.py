"""
Configuration settings for NFL Analytics system
"""

import os
from pathlib import Path

# Database settings
DEFAULT_DB_PATH = "nfl_analytics.duckdb"
DB_PATH = os.getenv("NFL_DB_PATH", DEFAULT_DB_PATH)

# Data extraction settings
DEFAULT_SEASONS = [2023, 2024]
MAX_WORKERS = int(os.getenv("NFL_MAX_WORKERS", "4"))
DEFAULT_SEASON_TYPES = ['REG', 'POST', 'PRE']

# Logging settings
LOG_LEVEL = os.getenv("NFL_LOG_LEVEL", "INFO")
LOG_FILE = os.getenv("NFL_LOG_FILE", "nfl_analytics.log")

# API settings
API_TIMEOUT = int(os.getenv("NFL_API_TIMEOUT", "30"))
API_RETRIES = int(os.getenv("NFL_API_RETRIES", "3"))

# Fantasy scoring configurations
FANTASY_SCORING_SYSTEMS = {
    'std': {
        'passing_yards': 0.04,
        'passing_tds': 4.0,
        'interceptions': -2.0,
        'rushing_yards': 0.1,
        'rushing_tds': 6.0,
        'receptions': 0.0,
        'receiving_yards': 0.1,
        'receiving_tds': 6.0,
        'fumbles_lost': -2.0,
        'two_point_conversions': 2.0
    },
    'half_ppr': {
        'passing_yards': 0.04,
        'passing_tds': 4.0,
        'interceptions': -2.0,
        'rushing_yards': 0.1,
        'rushing_tds': 6.0,
        'receptions': 0.5,
        'receiving_yards': 0.1,
        'receiving_tds': 6.0,
        'fumbles_lost': -2.0,
        'two_point_conversions': 2.0
    },
    'full_ppr': {
        'passing_yards': 0.04,
        'passing_tds': 4.0,
        'interceptions': -2.0,
        'rushing_yards': 0.1,
        'rushing_tds': 6.0,
        'receptions': 1.0,
        'receiving_yards': 0.1,
        'receiving_tds': 6.0,
        'fumbles_lost': -2.0,
        'two_point_conversions': 2.0
    }
}

# Data refresh settings
REFRESH_BATCH_SIZE = 1000
REFRESH_DELAY = 1  # seconds between batches

# File paths
PROJECT_ROOT = Path(__file__).parent
DATA_DIR = PROJECT_ROOT / "data"
LOGS_DIR = PROJECT_ROOT / "logs"
QUERIES_DIR = PROJECT_ROOT / "queries"

# Create directories if they don't exist
DATA_DIR.mkdir(exist_ok=True)
LOGS_DIR.mkdir(exist_ok=True)
QUERIES_DIR.mkdir(exist_ok=True)
