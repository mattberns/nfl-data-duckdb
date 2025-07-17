"""
Fantasy Points Calculator
Calculates fantasy points for different scoring systems (STD, HALF PPR, FULL PPR)
"""

import pandas as pd
from typing import Dict, Any
import logging

logger = logging.getLogger(__name__)


class FantasyPointsCalculator:
    """Calculate fantasy points for different scoring systems"""
    
    def __init__(self):
        """Initialize fantasy points calculator with scoring systems"""
        self.scoring_systems = {
            'std': {
                'passing_yards': 0.04,  # 1 point per 25 yards
                'passing_tds': 4.0,
                'interceptions': -2.0,
                'rushing_yards': 0.1,   # 1 point per 10 yards
                'rushing_tds': 6.0,
                'receptions': 0.0,      # No PPR
                'receiving_yards': 0.1, # 1 point per 10 yards
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
                'receptions': 0.5,      # Half PPR
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
                'receptions': 1.0,      # Full PPR
                'receiving_yards': 0.1,
                'receiving_tds': 6.0,
                'fumbles_lost': -2.0,
                'two_point_conversions': 2.0
            }
        }
    
    def calculate_fantasy_points(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate fantasy points for all scoring systems
        
        Args:
            df: DataFrame with player stats
            
        Returns:
            DataFrame with fantasy points columns added
        """
        try:
            # Make a copy to avoid modifying original
            result_df = df.copy()
            
            # Ensure numeric columns exist and are filled
            stat_columns = [
                'passing_yards', 'passing_tds', 'interceptions',
                'rushing_yards', 'rushing_tds', 'receptions',
                'receiving_yards', 'receiving_tds', 'fumbles_lost'
            ]
            
            # Fill missing stat columns with 0
            for col in stat_columns:
                if col not in result_df.columns:
                    result_df[col] = 0.0  # Use float to avoid int64 creation
                else:
                    result_df[col] = pd.to_numeric(result_df[col], errors='coerce').fillna(0.0)
            
            # Calculate fantasy points for each scoring system
            for system_name, scoring in self.scoring_systems.items():
                points = pd.Series(0.0, index=result_df.index)
                
                for stat, multiplier in scoring.items():
                    if stat in result_df.columns:
                        stat_values = pd.to_numeric(result_df[stat], errors='coerce').fillna(0.0)
                        points += stat_values * multiplier
                
                # Round to 2 decimal places
                result_df[f'fantasy_points_{system_name}'] = points.round(2)
            
            logger.info(f"Calculated fantasy points for {len(result_df)} records")
            return result_df
            
        except Exception as e:
            logger.error(f"Failed to calculate fantasy points: {e}")
            # Return original dataframe with empty fantasy points columns
            for system_name in self.scoring_systems.keys():
                df[f'fantasy_points_{system_name}'] = 0.0
            return df
    
    def get_scoring_system(self, system_name: str) -> Dict[str, float]:
        """
        Get scoring system configuration
        
        Args:
            system_name: Name of scoring system (std, half_ppr, full_ppr)
            
        Returns:
            Dictionary with scoring multipliers
        """
        return self.scoring_systems.get(system_name, {})
    
    def calculate_player_fantasy_points(self, player_stats: Dict[str, Any], 
                                      system: str = 'std') -> float:
        """
        Calculate fantasy points for a single player
        
        Args:
            player_stats: Dictionary with player statistics
            system: Scoring system to use
            
        Returns:
            Fantasy points for the player
        """
        try:
            scoring = self.scoring_systems.get(system, self.scoring_systems['std'])
            
            points = 0.0
            for stat, multiplier in scoring.items():
                stat_value = player_stats.get(stat, 0)
                if stat_value is not None:
                    points += float(stat_value) * multiplier
            
            return round(points, 2)
            
        except Exception as e:
            logger.error(f"Failed to calculate fantasy points for player: {e}")
            return 0.0
    
    def update_scoring_system(self, system_name: str, 
                            scoring_rules: Dict[str, float]):
        """
        Update or add a custom scoring system
        
        Args:
            system_name: Name of the scoring system
            scoring_rules: Dictionary with stat names and multipliers
        """
        self.scoring_systems[system_name] = scoring_rules
        logger.info(f"Updated scoring system: {system_name}")
    
    def get_top_performers(self, df: pd.DataFrame, 
                          system: str = 'std', 
                          position: str = None,
                          top_n: int = 10) -> pd.DataFrame:
        """
        Get top fantasy performers for a scoring system
        
        Args:
            df: DataFrame with player stats and fantasy points
            system: Scoring system to use
            position: Optional position filter
            top_n: Number of top performers to return
            
        Returns:
            DataFrame with top performers
        """
        try:
            result_df = df.copy()
            
            # Filter by position if specified
            if position:
                result_df = result_df[result_df['position'] == position]
            
            # Sort by fantasy points for the system
            fantasy_col = f'fantasy_points_{system}'
            if fantasy_col in result_df.columns:
                result_df = result_df.sort_values(fantasy_col, ascending=False)
                result_df = result_df.head(top_n)
            
            return result_df
            
        except Exception as e:
            logger.error(f"Failed to get top performers: {e}")
            return pd.DataFrame()
    
    def compare_scoring_systems(self, df: pd.DataFrame, 
                               player_id: str = None) -> pd.DataFrame:
        """
        Compare fantasy points across different scoring systems
        
        Args:
            df: DataFrame with player stats
            player_id: Optional specific player to compare
            
        Returns:
            DataFrame with fantasy points comparison
        """
        try:
            result_df = df.copy()
            
            # Filter by player if specified
            if player_id:
                result_df = result_df[result_df['player_id'] == player_id]
            
            # Select relevant columns
            compare_cols = ['player_id', 'player_name', 'position', 'team']
            fantasy_cols = [col for col in result_df.columns if col.startswith('fantasy_points_')]
            
            available_cols = [col for col in compare_cols if col in result_df.columns]
            available_cols.extend(fantasy_cols)
            
            if available_cols:
                result_df = result_df[available_cols]
                
                # Calculate differences between systems
                if 'fantasy_points_std' in result_df.columns and 'fantasy_points_full_ppr' in result_df.columns:
                    result_df['ppr_advantage'] = (
                        result_df['fantasy_points_full_ppr'] - result_df['fantasy_points_std']
                    ).round(2)
            
            return result_df
            
        except Exception as e:
            logger.error(f"Failed to compare scoring systems: {e}")
            return pd.DataFrame()
