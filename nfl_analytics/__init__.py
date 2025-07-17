"""
NFL Analytics Package

A comprehensive system for extracting, storing, and analyzing NFL data
using nfl_data_py and DuckDB.
"""

from .database.manager import DatabaseManager
from .extractors.data_extractor import NFLDataExtractor
from .models.fantasy_points import FantasyPointsCalculator

__version__ = "0.1.0"
__all__ = ["DatabaseManager", "NFLDataExtractor", "FantasyPointsCalculator"]
