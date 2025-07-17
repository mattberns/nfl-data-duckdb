"""
NFL Data Extractor
Handles extraction of data from nfl_data_py package
"""

import nfl_data_py as nfl
import pandas as pd
from typing import List, Optional, Dict, Any, Tuple
import logging
from tqdm import tqdm
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from ..database.manager import DatabaseManager
from ..models.fantasy_points import FantasyPointsCalculator

logger = logging.getLogger(__name__)


class NFLDataExtractor:
    """Extracts NFL data from nfl_data_py and stores in DuckDB"""
    
    def __init__(self, db_manager: DatabaseManager):
        """
        Initialize data extractor
        
        Args:
            db_manager: Database manager instance
        """
        self.db_manager = db_manager
        self.fantasy_calc = FantasyPointsCalculator()
    
    def extract_all_data(self, seasons: List[int], 
                        season_types: List[str] = ['REG', 'POST', 'PRE'],
                        max_workers: int = 4) -> Dict[str, int]:
        """
        Extract all available NFL data for specified seasons
        
        Args:
            seasons: List of seasons to extract
            season_types: List of season types to extract
            max_workers: Maximum number of concurrent workers
            
        Returns:
            Dictionary with extraction results
        """
        logger.info(f"Starting extraction for seasons: {seasons}")
        
        results = {}
        
        # Extract data in parallel where possible
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = []
            
            # Submit extraction tasks
            futures.append(executor.submit(self._extract_teams))
            futures.append(executor.submit(self._extract_players))
            futures.append(executor.submit(self._extract_schedules, seasons))
            
            for season in seasons:
                futures.append(executor.submit(self._extract_pbp_data, [season]))
                futures.append(executor.submit(self._extract_weekly_data, [season]))
                futures.append(executor.submit(self._extract_seasonal_data, [season]))
                futures.append(executor.submit(self._extract_rosters, [season]))
                futures.append(executor.submit(self._extract_injuries, [season]))
            
            # Collect results
            for future in tqdm(as_completed(futures), total=len(futures), 
                             desc="Extracting NFL data"):
                try:
                    table_name, records = future.result()
                    results[table_name] = records
                except Exception as e:
                    logger.error(f"Extraction task failed: {e}")
        
        logger.info(f"Extraction complete. Results: {results}")
        return results
    
    def _extract_teams(self) -> Tuple[str, int]:
        """Extract team information"""
        try:
            logger.info("Extracting team data...")
            teams_df = nfl.import_team_desc()
            
            # Clean and standardize team data
            teams_df = nfl.clean_nfl_data(teams_df)
            
            records = self.db_manager.insert_dataframe(teams_df, 'teams')
            
            self.db_manager.log_refresh(
                'teams', 0, None, 'ALL', 'SUCCESS', 
                records_processed=records
            )
            
            return 'teams', records
            
        except Exception as e:
            error_msg = f"Failed to extract team data: {type(e).__name__}: {str(e)}"
            logger.error(error_msg)
            self.db_manager.log_refresh(
                'teams', 0, None, 'ALL', 'FAILED', error_msg
            )
            raise
    
    def _extract_players(self) -> Tuple[str, int]:
        """Extract player information"""
        try:
            logger.info("Extracting player data...")
            players_df = nfl.import_players()
            
            # Clean and standardize player data
            players_df = nfl.clean_nfl_data(players_df)
            
            records = self.db_manager.insert_dataframe(players_df, 'players')
            
            self.db_manager.log_refresh(
                'players', 0, None, 'ALL', 'SUCCESS',
                records_processed=records
            )
            
            return 'players', records
            
        except Exception as e:
            error_msg = f"Failed to extract player data: {type(e).__name__}: {str(e)}"
            logger.error(error_msg)
            self.db_manager.log_refresh(
                'players', 0, None, 'ALL', 'FAILED', error_msg
            )
            raise
    
    def _extract_schedules(self, seasons: List[int]) -> Tuple[str, int]:
        """Extract schedule data"""
        try:
            logger.info(f"Extracting schedule data for seasons: {seasons}")
            schedules_df = nfl.import_schedules(seasons)
            
            # Clean and standardize schedule data
            schedules_df = nfl.clean_nfl_data(schedules_df)
            
            records = self.db_manager.insert_dataframe(schedules_df, 'schedules')
            
            for season in seasons:
                self.db_manager.log_refresh(
                    'schedules', season, None, 'ALL', 'SUCCESS',
                    records_processed=records
                )
            
            return 'schedules', records
            
        except Exception as e:
            error_msg = f"Failed to extract schedule data: {type(e).__name__}: {str(e)}"
            logger.error(error_msg)
            for season in seasons:
                self.db_manager.log_refresh(
                    'schedules', season, None, 'ALL', 'FAILED', error_msg
                )
            raise
    
    def _extract_pbp_data(self, seasons: List[int]) -> Tuple[str, int]:
        """Extract play-by-play data"""
        try:
            logger.info(f"Extracting PBP data for seasons: {seasons}")
            
            # Handle the nfl_data_py library bug where 'Error' is not defined
            # We need to monkey-patch the Error class before importing PBP data
            import builtins
            if not hasattr(builtins, 'Error'):
                builtins.Error = Exception
            
            pbp_df = nfl.import_pbp_data(seasons)
            logger.info(f"Successfully extracted PBP data with {len(pbp_df)} records")
            
            # Clean and standardize PBP data
            pbp_df = nfl.clean_nfl_data(pbp_df)
            
            # Rename 'desc' column to avoid SQL reserved keyword conflict
            if 'desc' in pbp_df.columns:
                pbp_df = pbp_df.rename(columns={'desc': 'play_description'})
            
            records = self.db_manager.insert_dataframe(pbp_df, 'pbp_data')
            
            for season in seasons:
                self.db_manager.log_refresh(
                    'pbp_data', season, None, 'ALL', 'SUCCESS',
                    records_processed=records
                )
            
            return 'pbp_data', records
            
        except Exception as e:
            # Provide more detailed error information
            error_msg = f"Failed to extract PBP data: {type(e).__name__}: {str(e)}"
            logger.error(error_msg)
            for season in seasons:
                self.db_manager.log_refresh(
                    'pbp_data', season, None, 'ALL', 'FAILED', error_msg
                )
            raise
    
    def _extract_weekly_data(self, seasons: List[int]) -> Tuple[str, int]:
        """Extract weekly player stats"""
        try:
            logger.info(f"Extracting weekly data for seasons: {seasons}")
            
            weekly_df = nfl.import_weekly_data(seasons)
            
            # Clean and standardize weekly data
            weekly_df = nfl.clean_nfl_data(weekly_df)
            
            # Add our custom fantasy points calculations
            # NFL data already has fantasy_points and fantasy_points_ppr
            # We'll add our custom calculations as additional columns
            weekly_df = self.fantasy_calc.calculate_fantasy_points(weekly_df)
            
            records = self.db_manager.insert_dataframe(weekly_df, 'weekly_stats')
            
            for season in seasons:
                self.db_manager.log_refresh(
                    'weekly_stats', season, None, 'ALL', 'SUCCESS',
                    records_processed=records
                )
            
            return 'weekly_stats', records
            
        except Exception as e:
            error_msg = f"Failed to extract weekly data: {type(e).__name__}: {str(e)}"
            logger.error(error_msg)
            for season in seasons:
                self.db_manager.log_refresh(
                    'weekly_stats', season, None, 'ALL', 'FAILED', error_msg
                )
            raise
    
    def _extract_seasonal_data(self, seasons: List[int]) -> Tuple[str, int]:
        """Extract seasonal player stats"""
        try:
            logger.info(f"Extracting seasonal data for seasons: {seasons}")
            
            seasonal_df = nfl.import_seasonal_data(seasons)
            
            # Clean and standardize seasonal data
            seasonal_df = nfl.clean_nfl_data(seasonal_df)
            
            # Add our custom fantasy points calculations
            # NFL data already has fantasy_points and fantasy_points_ppr
            # We'll add our custom calculations as additional columns
            seasonal_df = self.fantasy_calc.calculate_fantasy_points(seasonal_df)
            
            records = self.db_manager.insert_dataframe(seasonal_df, 'seasonal_stats')
            
            for season in seasons:
                self.db_manager.log_refresh(
                    'seasonal_stats', season, None, 'ALL', 'SUCCESS',
                    records_processed=records
                )
            
            return 'seasonal_stats', records
            
        except Exception as e:
            error_msg = f"Failed to extract seasonal data: {type(e).__name__}: {str(e)}"
            logger.error(error_msg)
            for season in seasons:
                self.db_manager.log_refresh(
                    'seasonal_stats', season, None, 'ALL', 'FAILED', error_msg
                )
            raise
    
    def _extract_rosters(self, seasons: List[int]) -> Tuple[str, int]:
        """Extract roster data"""
        try:
            logger.info(f"Extracting roster data for seasons: {seasons}")
            
            rosters_df = nfl.import_weekly_rosters(seasons)
            
            # Clean and standardize roster data
            rosters_df = nfl.clean_nfl_data(rosters_df)
            
            records = self.db_manager.insert_dataframe(rosters_df, 'rosters')
            
            for season in seasons:
                self.db_manager.log_refresh(
                    'rosters', season, None, 'ALL', 'SUCCESS',
                    records_processed=records
                )
            
            return 'rosters', records
            
        except Exception as e:
            error_msg = f"Failed to extract roster data: {type(e).__name__}: {str(e)}"
            logger.error(error_msg)
            for season in seasons:
                self.db_manager.log_refresh(
                    'rosters', season, None, 'ALL', 'FAILED', error_msg
                )
            raise
    
    def _extract_injuries(self, seasons: List[int]) -> Tuple[str, int]:
        """Extract injury data"""
        try:
            logger.info(f"Extracting injury data for seasons: {seasons}")
            
            injuries_df = nfl.import_injuries(seasons)
            
            # Clean and standardize injury data
            injuries_df = nfl.clean_nfl_data(injuries_df)
            
            records = self.db_manager.insert_dataframe(injuries_df, 'injuries')
            
            for season in seasons:
                self.db_manager.log_refresh(
                    'injuries', season, None, 'ALL', 'SUCCESS',
                    records_processed=records
                )
            
            return 'injuries', records
            
        except Exception as e:
            error_msg = f"Failed to extract injury data: {type(e).__name__}: {str(e)}"
            logger.error(error_msg)
            for season in seasons:
                self.db_manager.log_refresh(
                    'injuries', season, None, 'ALL', 'FAILED', error_msg
                )
            raise
    
    def refresh_season_data(self, season: int, 
                           data_types: Optional[List[str]] = None) -> Dict[str, int]:
        """
        Refresh data for a specific season
        
        Args:
            season: Season to refresh
            data_types: Optional list of data types to refresh
            
        Returns:
            Dictionary with refresh results
        """
        if data_types is None:
            data_types = ['pbp_data', 'weekly_stats', 'seasonal_stats', 
                         'rosters', 'injuries']
        
        results = {}
        
        for data_type in data_types:
            try:
                if data_type == 'pbp_data':
                    _, records = self._extract_pbp_data([season])
                elif data_type == 'weekly_stats':
                    _, records = self._extract_weekly_data([season])
                elif data_type == 'seasonal_stats':
                    _, records = self._extract_seasonal_data([season])
                elif data_type == 'rosters':
                    _, records = self._extract_rosters([season])
                elif data_type == 'injuries':
                    _, records = self._extract_injuries([season])
                
                results[data_type] = records
                
            except Exception as e:
                error_msg = f"Failed to refresh {data_type} for season {season}: {type(e).__name__}: {str(e)}"
                logger.error(error_msg)
                results[data_type] = 0
        
        return results
    
    def refresh_week_data(self, season: int, week: int) -> Dict[str, int]:
        """
        Refresh data for a specific week
        
        Args:
            season: Season 
            week: Week number
            
        Returns:
            Dictionary with refresh results
        """
        results = {}
        
        try:
            # Get week-specific data from weekly stats
            weekly_df = nfl.import_weekly_data([season])
            week_data = weekly_df[weekly_df['week'] == week].copy()
            
            if not week_data.empty:
                # Clean and calculate fantasy points
                week_data = nfl.clean_nfl_data(week_data)
                week_data = self.fantasy_calc.calculate_fantasy_points(week_data)
                
                # Remove existing week data
                self.db_manager.query(
                    "DELETE FROM weekly_stats WHERE season = ? AND week = ?",
                    {"season": season, "week": week}
                )
                
                # Insert new week data
                records = self.db_manager.insert_dataframe(week_data, 'weekly_stats')
                results['weekly_stats'] = records
                
                self.db_manager.log_refresh(
                    'weekly_stats', season, week, 'REG', 'SUCCESS',
                    records_processed=records
                )
            
        except Exception as e:
            error_msg = f"Failed to refresh week {week} data for season {season}: {type(e).__name__}: {str(e)}"
            logger.error(error_msg)
            self.db_manager.log_refresh(
                'weekly_stats', season, week, 'REG', 'FAILED', error_msg
            )
            results['weekly_stats'] = 0
        
        return results
