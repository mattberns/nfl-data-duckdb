"""
Schema Generator for NFL Analytics
Creates database schemas with proper data types based on nfl_data_py structure
"""

import nfl_data_py as nfl
import pandas as pd
from typing import Dict, List, Any, Optional
import logging

logger = logging.getLogger(__name__)


class SchemaGenerator:
    """Generates database schemas with proper data types for NFL data"""
    
    def __init__(self):
        # Enhanced type mapping that prioritizes numeric types
        self.type_mapping = {
            'object': 'TEXT',
            'int32': 'INTEGER',
            'int64': 'INTEGER', 
            'float32': 'REAL',
            'float64': 'REAL',
            'bool': 'BOOLEAN',
            'boolean': 'BOOLEAN',
            'datetime64[ns]': 'TIMESTAMP',
            'Int64': 'INTEGER',
            'Float64': 'REAL'
        }
        
        # Define specific column type overrides for known numeric/date columns
        self.column_type_overrides = {
            # Core identification columns
            'season': 'INTEGER',
            'week': 'INTEGER',
            'game_id': 'TEXT',
            'play_id': 'TEXT',
            'player_id': 'TEXT',
            'team': 'TEXT',
            'opponent_team': 'TEXT',
            
            # Date/time columns
            'game_date': 'DATE',
            'gameday': 'DATE',
            'gametime': 'TIME',
            'datetime': 'TIMESTAMP',
            'report_date': 'DATE',
            'report_primary_injury': 'TEXT',
            'report_secondary_injury': 'TEXT',
            'report_status': 'TEXT',
            
            # Numeric statistics - passing
            'completions': 'INTEGER',
            'attempts': 'INTEGER',
            'passing_yards': 'REAL',
            'passing_tds': 'INTEGER',
            'interceptions': 'REAL',
            'sacks': 'REAL',
            'sack_yards': 'REAL',
            'sack_fumbles': 'INTEGER',
            'sack_fumbles_lost': 'INTEGER',
            'passing_air_yards': 'REAL',
            'passing_yards_after_catch': 'REAL',
            'passing_first_downs': 'REAL',
            'passing_epa': 'REAL',
            'passing_2pt_conversions': 'INTEGER',
            'pacr': 'REAL',
            'dakota': 'REAL',
            
            # Numeric statistics - rushing
            'carries': 'INTEGER',
            'rushing_yards': 'REAL',
            'rushing_tds': 'INTEGER',
            'rushing_fumbles': 'REAL',
            'rushing_fumbles_lost': 'REAL',
            'rushing_first_downs': 'REAL',
            'rushing_epa': 'REAL',
            'rushing_2pt_conversions': 'INTEGER',
            
            # Numeric statistics - receiving
            'receptions': 'INTEGER',
            'targets': 'INTEGER',
            'receiving_yards': 'REAL',
            'receiving_tds': 'INTEGER',
            'receiving_fumbles': 'REAL',
            'receiving_fumbles_lost': 'REAL',
            'receiving_air_yards': 'REAL',
            'receiving_yards_after_catch': 'REAL',
            'receiving_first_downs': 'REAL',
            'receiving_epa': 'REAL',
            'receiving_2pt_conversions': 'INTEGER',
            'racr': 'REAL',
            'target_share': 'REAL',
            'air_yards_share': 'REAL',
            'wopr': 'REAL',
            
            # Other numeric columns
            'special_teams_tds': 'REAL',
            'fantasy_points': 'REAL',
            'fantasy_points_ppr': 'REAL',
            'down': 'INTEGER',
            'yards_gained': 'REAL',
            'quarter': 'INTEGER',
            'play_clock': 'INTEGER',
            'drive': 'INTEGER',
            'sp': 'BOOLEAN',
            'qtr': 'INTEGER',
            'drive_play_count': 'INTEGER',
            'drive_time_of_possession': 'REAL',
            'drive_first_downs': 'INTEGER',
            'drive_inside20': 'BOOLEAN',
            'drive_ended_with_score': 'BOOLEAN',
            'drive_quarter_start': 'INTEGER',
            'drive_quarter_end': 'INTEGER',
            'drive_yards_penalized': 'INTEGER',
            'drive_start_transition': 'TEXT',
            'drive_end_transition': 'TEXT',
            'drive_game_clock_start': 'TEXT',
            'drive_game_clock_end': 'TEXT',
            'drive_start_yard_line': 'INTEGER',
            'drive_end_yard_line': 'INTEGER',
            'series_success': 'BOOLEAN',
            'series_result': 'TEXT',
            'play_order': 'INTEGER',
            'order_sequence': 'INTEGER',
            'play_type': 'TEXT',
            'play_type_nfl': 'TEXT',
            'fixed_drive': 'INTEGER',
            'fixed_drive_result': 'TEXT',
            'drive_real_start_time': 'REAL',
            'series': 'INTEGER',
            'series_success': 'BOOLEAN',
            'yards_to_go': 'INTEGER',
            'goal_to_go': 'BOOLEAN',
            'first_down_rush': 'BOOLEAN',
            'first_down_pass': 'BOOLEAN',
            'first_down_penalty': 'BOOLEAN',
            'third_down_converted': 'BOOLEAN',
            'third_down_failed': 'BOOLEAN',
            'fourth_down_converted': 'BOOLEAN',
            'fourth_down_failed': 'BOOLEAN',
            'incomplete_pass': 'BOOLEAN',
            'touchback': 'BOOLEAN',
            'interception': 'BOOLEAN',
            'punt_blocked': 'BOOLEAN',
            'first_down': 'BOOLEAN',
            'fumble': 'BOOLEAN',
            'complete_pass': 'BOOLEAN',
            'assist_tackle': 'BOOLEAN',
            'lateral_reception': 'BOOLEAN',
            'lateral_rush': 'BOOLEAN',
            'lateral_return': 'BOOLEAN',
            'lateral_recovery': 'BOOLEAN',
            'passer_player_id': 'TEXT',
            'passer_player_name': 'TEXT',
            'receiver_player_id': 'TEXT',
            'receiver_player_name': 'TEXT',
            'rusher_player_id': 'TEXT',
            'rusher_player_name': 'TEXT',
            'lateral_receiver_player_id': 'TEXT',
            'lateral_receiver_player_name': 'TEXT',
            'lateral_rusher_player_id': 'TEXT',
            'lateral_rusher_player_name': 'TEXT',
            'lateral_sack_player_id': 'TEXT',
            'lateral_sack_player_name': 'TEXT',
            'interception_player_id': 'TEXT',
            'interception_player_name': 'TEXT',
            'lateral_interception_player_id': 'TEXT',
            'lateral_interception_player_name': 'TEXT',
            'punt_returner_player_id': 'TEXT',
            'punt_returner_player_name': 'TEXT',
            'lateral_punt_returner_player_id': 'TEXT',
            'lateral_punt_returner_player_name': 'TEXT',
            'kickoff_returner_player_name': 'TEXT',
            'kickoff_returner_player_id': 'TEXT',
            'lateral_kickoff_returner_player_id': 'TEXT',
            'lateral_kickoff_returner_player_name': 'TEXT',
            'punter_player_id': 'TEXT',
            'punter_player_name': 'TEXT',
            'kicker_player_name': 'TEXT',
            'kicker_player_id': 'TEXT',
            'own_kickoff_recovery_player_id': 'TEXT',
            'own_kickoff_recovery_player_name': 'TEXT',
            'blocked_player_id': 'TEXT',
            'blocked_player_name': 'TEXT',
            'tackle_for_loss_1_player_id': 'TEXT',
            'tackle_for_loss_1_player_name': 'TEXT',
            'tackle_for_loss_2_player_id': 'TEXT',
            'tackle_for_loss_2_player_name': 'TEXT',
            'qb_hit_1_player_id': 'TEXT',
            'qb_hit_1_player_name': 'TEXT',
            'qb_hit_2_player_id': 'TEXT',
            'qb_hit_2_player_name': 'TEXT',
            'forced_fumble_player_1_team': 'TEXT',
            'forced_fumble_player_1_player_id': 'TEXT',
            'forced_fumble_player_1_player_name': 'TEXT',
            'forced_fumble_player_2_team': 'TEXT',
            'forced_fumble_player_2_player_id': 'TEXT',
            'forced_fumble_player_2_player_name': 'TEXT',
            'solo_tackle_1_team': 'TEXT',
            'solo_tackle_1_player_id': 'TEXT',
            'solo_tackle_1_player_name': 'TEXT',
            'solo_tackle_2_team': 'TEXT',
            'solo_tackle_2_player_id': 'TEXT',
            'solo_tackle_2_player_name': 'TEXT',
            'assist_tackle_1_player_id': 'TEXT',
            'assist_tackle_1_player_name': 'TEXT',
            'assist_tackle_1_team': 'TEXT',
            'assist_tackle_2_player_id': 'TEXT',
            'assist_tackle_2_player_name': 'TEXT',
            'assist_tackle_2_team': 'TEXT',
            'assist_tackle_3_player_id': 'TEXT',
            'assist_tackle_3_player_name': 'TEXT',
            'assist_tackle_3_team': 'TEXT',
            'assist_tackle_4_player_id': 'TEXT',
            'assist_tackle_4_player_name': 'TEXT',
            'assist_tackle_4_team': 'TEXT',
            'pass_defense_1_player_id': 'TEXT',
            'pass_defense_1_player_name': 'TEXT',
            'pass_defense_2_player_id': 'TEXT',
            'pass_defense_2_player_name': 'TEXT',
            'fumbled_1_team': 'TEXT',
            'fumbled_1_player_id': 'TEXT',
            'fumbled_1_player_name': 'TEXT',
            'fumbled_2_team': 'TEXT',
            'fumbled_2_player_id': 'TEXT',
            'fumbled_2_player_name': 'TEXT',
            'fumble_recovery_1_team': 'TEXT',
            'fumble_recovery_1_player_id': 'TEXT',
            'fumble_recovery_1_player_name': 'TEXT',
            'fumble_recovery_2_team': 'TEXT',
            'fumble_recovery_2_player_id': 'TEXT',
            'fumble_recovery_2_player_name': 'TEXT',
            'fumble_recovery_1_yards': 'REAL',
            'fumble_recovery_2_yards': 'REAL',
            'return_yards': 'REAL',
            'penalty_yards': 'REAL',
            'replay_or_challenge': 'BOOLEAN',
            'replay_or_challenge_result': 'TEXT',
            'penalty_type': 'TEXT',
            'penalty_player_id': 'TEXT',
            'penalty_player_name': 'TEXT',
            'penalty_player_team': 'TEXT',
            'tackle_with_assist': 'BOOLEAN',
            'tackle_with_assist_1_player_id': 'TEXT',
            'tackle_with_assist_1_player_name': 'TEXT',
            'tackle_with_assist_1_team': 'TEXT',
            'tackle_with_assist_2_player_id': 'TEXT',
            'tackle_with_assist_2_player_name': 'TEXT',
            'tackle_with_assist_2_team': 'TEXT',
            'pass_defense_1_player_id': 'TEXT',
            'pass_defense_1_player_name': 'TEXT',
            'pass_defense_2_player_id': 'TEXT',
            'pass_defense_2_player_name': 'TEXT',
            'fumbled_1_forced': 'BOOLEAN',
            'fumbled_2_forced': 'BOOLEAN',
            'fumbled_1_not_forced': 'BOOLEAN',
            'fumbled_2_not_forced': 'BOOLEAN',
            'fumble_out_of_bounds': 'BOOLEAN',
            'safety_player_name': 'TEXT',
            'safety_player_id': 'TEXT',
            'season_type': 'TEXT',
            'week': 'INTEGER',
            'posteam': 'TEXT',
            'posteam_type': 'TEXT',
            'defteam': 'TEXT',
            'side_of_field': 'TEXT',
            'yardline_100': 'INTEGER',
            'game_date': 'DATE',
            'quarter_seconds_remaining': 'INTEGER',
            'half_seconds_remaining': 'INTEGER',
            'game_seconds_remaining': 'INTEGER',
            'game_half': 'TEXT',
            'quarter_end': 'BOOLEAN',
            'drive': 'INTEGER',
            'sp': 'BOOLEAN',
            'qtr': 'INTEGER',
            'down': 'INTEGER',
            'goal_to_go': 'BOOLEAN',
            'game_time': 'TEXT',
            'yrdln': 'TEXT',
            'ydstogo': 'INTEGER',
            'ydsnet': 'INTEGER',
            'play_description': 'TEXT',
            'play_type': 'TEXT',
            'yards_gained': 'REAL',
            'shotgun': 'BOOLEAN',
            'no_huddle': 'BOOLEAN',
            'qb_dropback': 'BOOLEAN',
            'qb_kneel': 'BOOLEAN',
            'qb_spike': 'BOOLEAN',
            'qb_scramble': 'BOOLEAN',
            'pass_length': 'TEXT',
            'pass_location': 'TEXT',
            'air_yards': 'REAL',
            'yards_after_catch': 'REAL',
            'run_location': 'TEXT',
            'run_gap': 'TEXT',
            'field_goal_result': 'TEXT',
            'kick_distance': 'INTEGER',
            'extra_point_result': 'TEXT',
            'two_point_conv_result': 'TEXT',
            'home_timeouts_remaining': 'INTEGER',
            'away_timeouts_remaining': 'INTEGER',
            'timeout': 'BOOLEAN',
            'timeout_team': 'TEXT',
            'td_team': 'TEXT',
            'td_player_name': 'TEXT',
            'td_player_id': 'TEXT',
            'posteam_timeouts_remaining': 'INTEGER',
            'defteam_timeouts_remaining': 'INTEGER',
            'total_home_score': 'INTEGER',
            'total_away_score': 'INTEGER',
            'posteam_score': 'INTEGER',
            'defteam_score': 'INTEGER',
            'score_differential': 'INTEGER',
            'posteam_score_post': 'INTEGER',
            'defteam_score_post': 'INTEGER',
            'score_differential_post': 'INTEGER',
            'no_score_prob': 'REAL',
            'opp_fg_prob': 'REAL',
            'opp_safety_prob': 'REAL',
            'opp_td_prob': 'REAL',
            'fg_prob': 'REAL',
            'safety_prob': 'REAL',
            'td_prob': 'REAL',
            'extra_point_prob': 'REAL',
            'two_point_conversion_prob': 'REAL',
            'ep': 'REAL',
            'epa': 'REAL',
            'total_home_epa': 'REAL',
            'total_away_epa': 'REAL',
            'total_home_rush_epa': 'REAL',
            'total_away_rush_epa': 'REAL',
            'total_home_pass_epa': 'REAL',
            'total_away_pass_epa': 'REAL',
            'air_epa': 'REAL',
            'yac_epa': 'REAL',
            'comp_air_epa': 'REAL',
            'comp_yac_epa': 'REAL',
            'total_home_comp_air_epa': 'REAL',
            'total_away_comp_air_epa': 'REAL',
            'total_home_comp_yac_epa': 'REAL',
            'total_away_comp_yac_epa': 'REAL',
            'total_home_raw_air_epa': 'REAL',
            'total_away_raw_air_epa': 'REAL',
            'total_home_raw_yac_epa': 'REAL',
            'total_away_raw_yac_epa': 'REAL',
            'wp': 'REAL',
            'def_wp': 'REAL',
            'home_wp': 'REAL',
            'away_wp': 'REAL',
            'wpa': 'REAL',
            'vegas_wpa': 'REAL',
            'vegas_home_wpa': 'REAL',
            'home_wp_post': 'REAL',
            'away_wp_post': 'REAL',
            'vegas_wp': 'REAL',
            'vegas_home_wp': 'REAL',
            'total_home_rush_wpa': 'REAL',
            'total_away_rush_wpa': 'REAL',
            'total_home_pass_wpa': 'REAL',
            'total_away_pass_wpa': 'REAL',
            'air_wpa': 'REAL',
            'yac_wpa': 'REAL',
            'comp_air_wpa': 'REAL',
            'comp_yac_wpa': 'REAL',
            'total_home_comp_air_wpa': 'REAL',
            'total_away_comp_air_wpa': 'REAL',
            'total_home_comp_yac_wpa': 'REAL',
            'total_away_comp_yac_wpa': 'REAL',
            'total_home_raw_air_wpa': 'REAL',
            'total_away_raw_air_wpa': 'REAL',
            'total_home_raw_yac_wpa': 'REAL',
            'total_away_raw_yac_wpa': 'REAL',
            'punt_blocked': 'BOOLEAN',
            'first_down_rush': 'BOOLEAN',
            'first_down_pass': 'BOOLEAN',
            'first_down_penalty': 'BOOLEAN',
            'third_down_converted': 'BOOLEAN',
            'third_down_failed': 'BOOLEAN',
            'fourth_down_converted': 'BOOLEAN',
            'fourth_down_failed': 'BOOLEAN',
            'incomplete_pass': 'BOOLEAN',
            'touchback': 'BOOLEAN',
            'interception': 'BOOLEAN',
            'punt_blocked': 'BOOLEAN',
            'first_down': 'BOOLEAN',
            'fumble': 'BOOLEAN',
            'complete_pass': 'BOOLEAN',
            'assist_tackle': 'BOOLEAN',
            'lateral_reception': 'BOOLEAN',
            'lateral_rush': 'BOOLEAN',
            'lateral_return': 'BOOLEAN',
            'lateral_recovery': 'BOOLEAN',
            'fantasy_player_name': 'TEXT',
            'fantasy_player_id': 'TEXT',
            'fantasy_position': 'TEXT',
            'racr': 'REAL',
            'target_share': 'REAL',
            'air_yards_share': 'REAL',
            'wopr': 'REAL',
            'pacr': 'REAL',
            'dakota': 'REAL',
            'cpoe': 'REAL',
            'gsis_id': 'TEXT',
            'pff_id': 'TEXT',
            'pfr_id': 'TEXT',
            'sleeper_id': 'TEXT',
            'nfl_id': 'TEXT',
            'espn_id': 'TEXT',
            'yahoo_id': 'TEXT',
            'rotowire_id': 'TEXT',
            'sportradar_id': 'TEXT',
            'stats_id': 'TEXT',
            'stats_global_id': 'TEXT',
            'fantasy_data_id': 'TEXT',
            'first_name': 'TEXT',
            'last_name': 'TEXT',
            'position': 'TEXT',
            'position_group': 'TEXT',
            'jersey_number': 'INTEGER',
            'height': 'TEXT',
            'weight': 'INTEGER',
            'college': 'TEXT',
            'high_school': 'TEXT',
            'birth_date': 'DATE',
            'entry_year': 'INTEGER',
            'rookie_year': 'INTEGER',
            'draft_club': 'TEXT',
            'draft_number': 'INTEGER',
            'draft_round': 'INTEGER',
            'draft_position': 'INTEGER',
            'status': 'TEXT',
            'headshot_url': 'TEXT',
            'ngs_position': 'TEXT',
            'depth_chart_position': 'TEXT',
            'years_exp': 'INTEGER',
            'status_description_abbr': 'TEXT',
            'status_short_description': 'TEXT',
            'gsis_it_id': 'TEXT',
            'smart_id': 'TEXT',
            'full_name': 'TEXT',
            'team_name': 'TEXT',
            'team_logo_espn': 'TEXT',
            'team_logo_wikipedia': 'TEXT',
            'team_wordmark': 'TEXT',
            'team_color': 'TEXT',
            'team_color2': 'TEXT',
            'team_color3': 'TEXT',
            'team_color4': 'TEXT',
            'nfl_api_id': 'TEXT',
            'team_nick': 'TEXT',
            'team_abbr': 'TEXT',
            'team_id': 'TEXT',
            'team_id_pfr': 'TEXT',
            'team_conf': 'TEXT',
            'team_division': 'TEXT',
            'team_location': 'TEXT',
            'team_name_raw': 'TEXT',
            'gameday': 'DATE',
            'weekday': 'TEXT',
            'gametime': 'TIME',
            'away_team': 'TEXT',
            'away_score': 'INTEGER',
            'home_team': 'TEXT',
            'home_score': 'INTEGER',
            'location': 'TEXT',
            'result': 'INTEGER',
            'total': 'INTEGER',
            'overtime': 'BOOLEAN',
            'old_game_id': 'TEXT',
            'gsis_id': 'TEXT',
            'nfl_detail_id': 'TEXT',
            'pfr_game_id': 'TEXT',
            'pff_game_id': 'TEXT',
            'espn_game_id': 'TEXT',
            'ftn_game_id': 'TEXT',
            'away_rest': 'INTEGER',
            'home_rest': 'INTEGER',
            'away_moneyline': 'INTEGER',
            'home_moneyline': 'INTEGER',
            'spread_line': 'REAL',
            'away_spread_odds': 'INTEGER',
            'home_spread_odds': 'INTEGER',
            'total_line': 'REAL',
            'under_odds': 'INTEGER',
            'over_odds': 'INTEGER',
            'div_game': 'BOOLEAN',
            'roof': 'TEXT',
            'surface': 'TEXT',
            'temp': 'INTEGER',
            'wind': 'INTEGER',
            'away_qb_id': 'TEXT',
            'home_qb_id': 'TEXT',
            'away_qb_name': 'TEXT',
            'home_qb_name': 'TEXT',
            'away_coach': 'TEXT',
            'home_coach': 'TEXT',
            'referee': 'TEXT',
            'stadium_id': 'TEXT',
            'stadium': 'TEXT',
            'away_qb_epa': 'REAL',
            'home_qb_epa': 'REAL',
            'away_qb_qbr': 'REAL',
            'home_qb_qbr': 'REAL',
            'pfr_game_id': 'TEXT',
            'pff_game_id': 'TEXT',
            'espn_game_id': 'TEXT',
            'ftn_game_id': 'TEXT',
            'away_rest': 'INTEGER',
            'home_rest': 'INTEGER',
            'away_moneyline': 'INTEGER',
            'home_moneyline': 'INTEGER',
            'spread_line': 'REAL',
            'away_spread_odds': 'INTEGER',
            'home_spread_odds': 'INTEGER',
            'total_line': 'REAL',
            'under_odds': 'INTEGER',
            'over_odds': 'INTEGER',
            'div_game': 'BOOLEAN',
            'roof': 'TEXT',
            'surface': 'TEXT',
            'temp': 'INTEGER',
            'wind': 'INTEGER',
            'away_qb_id': 'TEXT',
            'home_qb_id': 'TEXT',
            'away_qb_name': 'TEXT',
            'home_qb_name': 'TEXT',
            'away_coach': 'TEXT',
            'home_coach': 'TEXT',
            'referee': 'TEXT',
            'stadium_id': 'TEXT',
            'stadium': 'TEXT',
            'away_qb_epa': 'REAL',
            'home_qb_epa': 'REAL',
            'away_qb_qbr': 'REAL',
            'home_qb_qbr': 'REAL',
            'nfl_api_id': 'TEXT',
            'team_nick': 'TEXT',
            'team_abbr': 'TEXT',
            'team_id': 'TEXT',
            'team_id_pfr': 'TEXT',
            'team_conf': 'TEXT',
            'team_division': 'TEXT',
            'team_location': 'TEXT',
            'team_name_raw': 'TEXT',
            'game_type': 'TEXT',
            'week': 'INTEGER',
            'season': 'INTEGER',
            'season_type': 'TEXT',
            'depth_chart_position': 'TEXT',
            'jersey_number': 'INTEGER',
            'status': 'TEXT',
            'full_name': 'TEXT',
            'first_name': 'TEXT',
            'last_name': 'TEXT',
            'team': 'TEXT',
            'position': 'TEXT',
            'height': 'TEXT',
            'weight': 'INTEGER',
            'birth_date': 'DATE',
            'college': 'TEXT',
            'gsis_id': 'TEXT',
            'espn_id': 'TEXT',
            'pfr_id': 'TEXT',
            'pff_id': 'TEXT',
            'sleeper_id': 'TEXT',
            'nfl_id': 'TEXT',
            'yahoo_id': 'TEXT',
            'rotowire_id': 'TEXT',
            'sportradar_id': 'TEXT',
            'stats_id': 'TEXT',
            'fantasy_data_id': 'TEXT',
            'player_id': 'TEXT',
            'player_name': 'TEXT',
            'player_display_name': 'TEXT',
            'position_group': 'TEXT',
            'headshot_url': 'TEXT',
            'recent_team': 'TEXT',
            'report_primary_injury': 'TEXT',
            'report_secondary_injury': 'TEXT',
            'report_status': 'TEXT',
            'report_date': 'DATE',
            'practice_status': 'TEXT',
            'date_modified': 'TIMESTAMP',
            'gsis_id': 'TEXT',
            'pfr_id': 'TEXT',
            'pff_id': 'TEXT',
            'sleeper_id': 'TEXT'
        }
    
    def get_sql_type(self, pandas_type: str, column_name: str) -> str:
        """
        Map pandas dtype to SQL type with column-specific overrides
        
        Args:
            pandas_type: The pandas dtype as string
            column_name: The column name for type override lookup
            
        Returns:
            SQL type string
        """
        # Check for specific column override first
        if column_name in self.column_type_overrides:
            return self.column_type_overrides[column_name]
        
        # Fall back to general type mapping
        return self.type_mapping.get(str(pandas_type), 'TEXT')
    
    def generate_table_schema(self, df: pd.DataFrame, table_name: str, 
                             primary_key: List[str] = None) -> str:
        """
        Generate CREATE TABLE statement from DataFrame with improved type handling
        
        Args:
            df: DataFrame to analyze
            table_name: Name of the table to create
            primary_key: Optional list of primary key columns
            
        Returns:
            CREATE TABLE SQL statement
        """
        columns = []
        
        for col_name, dtype in df.dtypes.items():
            # Handle special cases for date columns
            if col_name in ['game_date', 'gameday', 'birth_date', 'report_date']:
                sql_type = 'DATE'
            elif col_name in ['gametime']:
                sql_type = 'TIME'
            elif col_name in ['datetime', 'date_modified']:
                sql_type = 'TIMESTAMP'
            else:
                sql_type = self.get_sql_type(dtype, col_name)
            
            columns.append(f"    {col_name} {sql_type}")
        
        # Add metadata columns
        columns.append("    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
        columns.append("    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
        
        schema = f"CREATE TABLE IF NOT EXISTS {table_name} (\n"
        schema += ",\n".join(columns)
        
        if primary_key:
            # Filter primary key columns to only include those that exist
            existing_pk = [pk for pk in primary_key if pk in df.columns]
            if existing_pk:
                schema += f",\n    PRIMARY KEY ({', '.join(existing_pk)})"
        
        schema += "\n);"
        
        return schema
    
    def create_data_conversion_functions(self) -> List[str]:
        """
        Create SQL functions for data type conversion and NULL handling
        
        Returns:
            List of SQL function definitions
        """
        functions = [
            """
            -- Function to safely convert to integer, returning NULL for invalid values
            CREATE OR REPLACE FUNCTION safe_int(value TEXT) 
            RETURNS INTEGER AS $$
            BEGIN
                RETURN CAST(value AS INTEGER);
            EXCEPTION
                WHEN OTHERS THEN
                    RETURN NULL;
            END;
            $$ LANGUAGE plpgsql;
            """,
            """
            -- Function to safely convert to real, returning NULL for invalid values
            CREATE OR REPLACE FUNCTION safe_real(value TEXT) 
            RETURNS REAL AS $$
            BEGIN
                RETURN CAST(value AS REAL);
            EXCEPTION
                WHEN OTHERS THEN
                    RETURN NULL;
            END;
            $$ LANGUAGE plpgsql;
            """,
            """
            -- Function to safely convert to date, returning NULL for invalid values
            CREATE OR REPLACE FUNCTION safe_date(value TEXT) 
            RETURNS DATE AS $$
            BEGIN
                RETURN CAST(value AS DATE);
            EXCEPTION
                WHEN OTHERS THEN
                    RETURN NULL;
            END;
            $$ LANGUAGE plpgsql;
            """
        ]
        return functions
    
    def generate_all_schemas(self) -> Dict[str, str]:
        """
        Generate schemas for all NFL data tables with improved type handling
        
        Returns:
            Dictionary mapping table names to CREATE TABLE statements
        """
        schemas = {}
        
        try:
            logger.info("Generating improved schemas from sample data...")
            
            # Teams
            teams_df = nfl.import_team_desc()
            schemas['teams'] = self.generate_table_schema(
                teams_df, 'teams', ['team_abbr']
            )
            
            # Players  
            players_df = nfl.import_players()
            schemas['players'] = self.generate_table_schema(
                players_df, 'players', None  # No primary key since gsis_id can be NULL
            )
            
            # Schedules
            schedules_df = nfl.import_schedules([2023])
            schemas['schedules'] = self.generate_table_schema(
                schedules_df, 'schedules', ['game_id']
            )
            
            # Weekly stats
            weekly_df = nfl.import_weekly_data([2023])
            schemas['weekly_stats'] = self.generate_table_schema(
                weekly_df, 'weekly_stats', ['player_id', 'season', 'week', 'season_type']
            )
            
            # Seasonal stats
            seasonal_df = nfl.import_seasonal_data([2023])
            schemas['seasonal_stats'] = self.generate_table_schema(
                seasonal_df, 'seasonal_stats', ['player_id', 'season', 'season_type']
            )
            
            # Rosters
            rosters_df = nfl.import_weekly_rosters([2023])
            schemas['rosters'] = self.generate_table_schema(
                rosters_df, 'rosters', ['player_id', 'season', 'week', 'team']
            )
            
            # PBP data (get full schema but limit rows for performance)
            pbp_df = nfl.import_pbp_data([2023])
            # Rename columns to avoid SQL reserved keyword conflicts
            reserved_keyword_renames = {
                'desc': 'play_description',
                'order': 'play_order',
                'time': 'game_time',
                'date': 'game_date_field'
            }
            for old_col, new_col in reserved_keyword_renames.items():
                if old_col in pbp_df.columns:
                    pbp_df = pbp_df.rename(columns={old_col: new_col})
            schemas['pbp_data'] = self.generate_table_schema(
                pbp_df, 'pbp_data', ['game_id', 'play_id']
            )
            
            # Injuries
            injuries_df = nfl.import_injuries([2023])
            schemas['injuries'] = self.generate_table_schema(
                injuries_df, 'injuries', ['player_id', 'season', 'week', 'report_date']
            )
            
            # Add system tables
            schemas['data_refresh_log'] = """
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
            
            return schemas
            
        except Exception as e:
            logger.error(f"Failed to generate improved schemas: {e}")
            raise
    
    def create_indexes(self) -> List[str]:
        """
        Generate optimized index creation statements
        
        Returns:
            List of CREATE INDEX statements
        """
        indexes = [
            # PBP data indexes
            "CREATE INDEX IF NOT EXISTS idx_pbp_game_id ON pbp_data(game_id);",
            "CREATE INDEX IF NOT EXISTS idx_pbp_season_week ON pbp_data(season, week);",
            "CREATE INDEX IF NOT EXISTS idx_pbp_season_type ON pbp_data(season_type);",
            "CREATE INDEX IF NOT EXISTS idx_pbp_posteam ON pbp_data(posteam);",
            "CREATE INDEX IF NOT EXISTS idx_pbp_defteam ON pbp_data(defteam);",
            "CREATE INDEX IF NOT EXISTS idx_pbp_play_type ON pbp_data(play_type);",
            "CREATE INDEX IF NOT EXISTS idx_pbp_down ON pbp_data(down);",
            "CREATE INDEX IF NOT EXISTS idx_pbp_quarter ON pbp_data(qtr);",
            "CREATE INDEX IF NOT EXISTS idx_pbp_game_date ON pbp_data(game_date);",
            
            # Weekly stats indexes
            "CREATE INDEX IF NOT EXISTS idx_weekly_player_season ON weekly_stats(player_id, season, week);",
            "CREATE INDEX IF NOT EXISTS idx_weekly_season_type ON weekly_stats(season_type);",
            "CREATE INDEX IF NOT EXISTS idx_weekly_position ON weekly_stats(position);",
            "CREATE INDEX IF NOT EXISTS idx_weekly_team ON weekly_stats(recent_team);",
            "CREATE INDEX IF NOT EXISTS idx_weekly_opponent ON weekly_stats(opponent_team);",
            "CREATE INDEX IF NOT EXISTS idx_weekly_fantasy_points ON weekly_stats(fantasy_points);",
            
            # Seasonal stats indexes
            "CREATE INDEX IF NOT EXISTS idx_seasonal_player_season ON seasonal_stats(player_id, season);",
            "CREATE INDEX IF NOT EXISTS idx_seasonal_season_type ON seasonal_stats(season_type);",
            "CREATE INDEX IF NOT EXISTS idx_seasonal_position ON seasonal_stats(position);",
            "CREATE INDEX IF NOT EXISTS idx_seasonal_team ON seasonal_stats(recent_team);",
            "CREATE INDEX IF NOT EXISTS idx_seasonal_fantasy_points ON seasonal_stats(fantasy_points);",
            
            # Schedules indexes
            "CREATE INDEX IF NOT EXISTS idx_schedules_season_week ON schedules(season, week);",
            "CREATE INDEX IF NOT EXISTS idx_schedules_game_date ON schedules(gameday);",
            "CREATE INDEX IF NOT EXISTS idx_schedules_teams ON schedules(home_team, away_team);",
            "CREATE INDEX IF NOT EXISTS idx_schedules_season_type ON schedules(season_type);",
            
            # Rosters indexes
            "CREATE INDEX IF NOT EXISTS idx_rosters_player_season ON rosters(player_id, season, week);",
            "CREATE INDEX IF NOT EXISTS idx_rosters_team ON rosters(team);",
            "CREATE INDEX IF NOT EXISTS idx_rosters_position ON rosters(position);",
            "CREATE INDEX IF NOT EXISTS idx_rosters_status ON rosters(status);",
            
            # Players indexes
            "CREATE INDEX IF NOT EXISTS idx_players_position ON players(position);",
            "CREATE INDEX IF NOT EXISTS idx_players_team ON players(team);",
            "CREATE INDEX IF NOT EXISTS idx_players_status ON players(status);",
            "CREATE INDEX IF NOT EXISTS idx_players_college ON players(college);",
            "CREATE INDEX IF NOT EXISTS idx_players_draft_year ON players(entry_year);",
            
            # Teams indexes
            "CREATE INDEX IF NOT EXISTS idx_teams_conf_div ON teams(team_conf, team_division);",
            
            # Injuries indexes
            "CREATE INDEX IF NOT EXISTS idx_injuries_player_season ON injuries(player_id, season, week);",
            "CREATE INDEX IF NOT EXISTS idx_injuries_report_date ON injuries(report_date);",
            "CREATE INDEX IF NOT EXISTS idx_injuries_status ON injuries(report_status);",
            
            # Data refresh log indexes
            "CREATE INDEX IF NOT EXISTS idx_refresh_log_table ON data_refresh_log(table_name, season, week);",
            "CREATE INDEX IF NOT EXISTS idx_refresh_log_status ON data_refresh_log(status);",
            "CREATE INDEX IF NOT EXISTS idx_refresh_log_date ON data_refresh_log(refresh_date);"
        ]
        
        return indexes


def generate_improved_schema_file():
    """Generate an improved schema file with all table definitions"""
    
    generator = ImprovedSchemaGenerator()
    schemas = generator.generate_all_schemas()
    indexes = generator.create_indexes()
    
    schema_content = "-- Auto-generated NFL Analytics Database Schema (Improved)\n"
    schema_content += "-- Generated with proper data types for numeric and date columns\n\n"
    
    for table_name, schema in schemas.items():
        schema_content += f"-- {table_name.upper()} TABLE\n"
        schema_content += schema + "\n\n"
    
    schema_content += "-- INDEXES\n"
    for index in indexes:
        schema_content += index + "\n"
    
    with open('nfl_schema_improved.sql', 'w') as f:
        f.write(schema_content)
    
    logger.info("Improved schema file generated: nfl_schema_improved.sql")
    return schemas


if __name__ == '__main__':
    generate_improved_schema_file()
