"""
Seasonal summary table creation script.

Creates the smry_season table with comprehensive player statistics including:
- All seasonal_stats fields
- Player info (position, years experience, draft position, weight, height)
- Per-game statistics
- Fantasy point rankings by position
- ECR rankings data
"""

import duckdb
import logging
from typing import List

logger = logging.getLogger(__name__)

def get_numeric_columns() -> List[str]:
    """Return list of numeric columns that should have per-game versions."""
    return [
        'completions', 'attempts', 'passing_yards', 'passing_tds', 'interceptions',
        'sacks', 'sack_yards', 'sack_fumbles', 'sack_fumbles_lost', 
        'passing_air_yards', 'passing_yards_after_catch', 'passing_first_downs',
        'passing_epa', 'passing_2pt_conversions', 'carries', 'rushing_yards',
        'rushing_tds', 'rushing_first_downs', 'rushing_epa', 'rushing_2pt_conversions',
        'receptions', 'targets', 'receiving_yards', 'receiving_tds', 
        'receiving_air_yards', 'receiving_yards_after_catch', 'receiving_first_downs',
        'receiving_epa', 'receiving_2pt_conversions', 'racr', 'target_share',
        'air_yards_share', 'wopr_x', 'special_teams_tds', 'fantasy_points_std',
        'fantasy_points_half_ppr', 'fantasy_points_full_ppr', 'fantasy_points_ppr',
        'fumbles_lost'
    ]

def create_per_game_columns() -> str:
    """Generate SQL for per-game columns."""
    numeric_cols = get_numeric_columns()
    per_game_sql = []
    
    for col in numeric_cols:
        per_game_sql.append(f"""
            CASE 
                WHEN games > 0 THEN CAST({col} AS FLOAT) / games 
                ELSE 0.0 
            END AS {col}_per_game""")
    
    return ',\n    '.join(per_game_sql)

def create_ranking_columns() -> str:
    """Generate SQL for ranking columns."""
    fantasy_cols = ['fantasy_points_std', 'fantasy_points_half_ppr', 'fantasy_points_full_ppr']
    ranking_sql = []
    
    # Total rankings
    for col in fantasy_cols:
        suffix = col.replace('fantasy_points_', '').replace('_', '')
        ranking_sql.append(f"""
            RANK() OVER (
                PARTITION BY bd.season, bd.position 
                ORDER BY bd.{col} DESC NULLS LAST
            ) AS {suffix}_tot_rnk""")
    
    # Per-game rankings
    for col in fantasy_cols:
        suffix = col.replace('fantasy_points_', '').replace('_', '')
        ranking_sql.append(f"""
            RANK() OVER (
                PARTITION BY bd.season, bd.position 
                ORDER BY (CASE WHEN bd.games > 0 THEN bd.{col} / bd.games ELSE 0 END) DESC NULLS LAST
            ) AS {suffix}_ppg_rnk""")
    
    return ',\n    '.join(ranking_sql)

def create_smry_season_table(db_path: str):
    """Create the smry_season table."""
    
    logger.info("Creating smry_season table...")
    
    per_game_cols = create_per_game_columns()
    ranking_cols = create_ranking_columns()
    
    drop_table_sql = "DROP TABLE IF EXISTS smry_season;"
    
    create_table_sql = f"""
    CREATE TABLE smry_season AS
    WITH roster_summary AS (
        SELECT 
            player_id, 
            season,
            ANY_VALUE(player_name) as player_name,
            ANY_VALUE(position) as position,
            ANY_VALUE(years_exp) as years_exp,
            ANY_VALUE(draft_number) as draft_number,
            ANY_VALUE(weight) as weight,
            ANY_VALUE(height) as height,
            ANY_VALUE(rookie_year) as rookie_year
        FROM rosters 
        GROUP BY player_id, season
    ),
    base_data AS (
        SELECT 
            s.*,
            -- Player info from rosters
            r.player_name,
            r.position,
            r.years_exp,
            r.draft_number AS draft_position,
            r.weight,
            r.height,
            -- Rookie indicator
            CASE 
                WHEN r.rookie_year IS NOT NULL AND s.season = CAST(r.rookie_year AS INTEGER) THEN 1
                ELSE 0
            END AS is_rookie,
            
            -- Per-game columns
            {per_game_cols}
            
        FROM seasonal_stats s
        LEFT JOIN roster_summary r ON s.player_id = r.player_id AND s.season = r.season
    ),
    
    ranked_data AS (
        SELECT 
            bd.*,
            
            -- Ranking columns (only for relevant positions)
            CASE 
                WHEN bd.position IN ('QB', 'RB', 'WR', 'TE', 'K') THEN
                    RANK() OVER (
                        PARTITION BY bd.season, bd.position 
                        ORDER BY bd.fantasy_points_std DESC NULLS LAST
                    )
                ELSE NULL
            END AS std_tot_rnk,
            CASE 
                WHEN bd.position IN ('QB', 'RB', 'WR', 'TE', 'K') THEN
                    RANK() OVER (
                        PARTITION BY bd.season, bd.position 
                        ORDER BY bd.fantasy_points_half_ppr DESC NULLS LAST
                    )
                ELSE NULL
            END AS halfppr_tot_rnk,
            CASE 
                WHEN bd.position IN ('QB', 'RB', 'WR', 'TE', 'K') THEN
                    RANK() OVER (
                        PARTITION BY bd.season, bd.position 
                        ORDER BY bd.fantasy_points_full_ppr DESC NULLS LAST
                    )
                ELSE NULL
            END AS fullppr_tot_rnk,
            CASE 
                WHEN bd.position IN ('QB', 'RB', 'WR', 'TE', 'K') THEN
                    RANK() OVER (
                        PARTITION BY bd.season, bd.position 
                        ORDER BY (CASE WHEN bd.games > 0 THEN bd.fantasy_points_std / bd.games ELSE 0 END) DESC NULLS LAST
                    )
                ELSE NULL
            END AS std_ppg_rnk,
            CASE 
                WHEN bd.position IN ('QB', 'RB', 'WR', 'TE', 'K') THEN
                    RANK() OVER (
                        PARTITION BY bd.season, bd.position 
                        ORDER BY (CASE WHEN bd.games > 0 THEN bd.fantasy_points_half_ppr / bd.games ELSE 0 END) DESC NULLS LAST
                    )
                ELSE NULL
            END AS halfppr_ppg_rnk,
            CASE 
                WHEN bd.position IN ('QB', 'RB', 'WR', 'TE', 'K') THEN
                    RANK() OVER (
                        PARTITION BY bd.season, bd.position 
                        ORDER BY (CASE WHEN bd.games > 0 THEN bd.fantasy_points_full_ppr / bd.games ELSE 0 END) DESC NULLS LAST
                    )
                ELSE NULL
            END AS fullppr_ppg_rnk
            
        FROM base_data bd
    ),
    
    ecr_summary AS (
        SELECT 
            player_id,
            year as season,
            ANY_VALUE(CASE WHEN before_preseason = FALSE THEN rank END) AS ecr_preszn_rank,
            ANY_VALUE(CASE WHEN before_preseason = FALSE THEN best_rank END) AS ecr_preszn_best_rank,
            ANY_VALUE(CASE WHEN before_preseason = FALSE THEN worst_rank END) AS ecr_preszn_worst_rank,
            ANY_VALUE(CASE WHEN before_preseason = FALSE THEN avg_rank END) AS ecr_preszn_avg_rank,
            ANY_VALUE(CASE WHEN before_preseason = FALSE THEN stddev_rank END) AS ecr_preszn_stddev_rank,
            ANY_VALUE(CASE WHEN before_preseason = FALSE THEN adp END) AS ecr_preszn_adp,
            ANY_VALUE(CASE WHEN before_preseason = FALSE THEN vs_adp END) AS ecr_preszn_vs_adp,
            ANY_VALUE(CASE WHEN before_preseason = FALSE THEN position_rank END) AS ecr_preszn_position_rank,
            ANY_VALUE(CASE WHEN before_preseason = TRUE THEN rank END) AS ecr_rank,
            ANY_VALUE(CASE WHEN before_preseason = TRUE THEN best_rank END) AS ecr_best_rank,
            ANY_VALUE(CASE WHEN before_preseason = TRUE THEN worst_rank END) AS ecr_worst_rank,
            ANY_VALUE(CASE WHEN before_preseason = TRUE THEN avg_rank END) AS ecr_avg_rank,
            ANY_VALUE(CASE WHEN before_preseason = TRUE THEN stddev_rank END) AS ecr_stddev_rank,
            ANY_VALUE(CASE WHEN before_preseason = TRUE THEN adp END) AS ecr_adp,
            ANY_VALUE(CASE WHEN before_preseason = TRUE THEN vs_adp END) AS ecr_vs_adp,
            ANY_VALUE(CASE WHEN before_preseason = TRUE THEN position_rank END) AS ecr_position_rank
        FROM ecr_rankings
        GROUP BY player_id, year
    )
    
    SELECT 
        rd.*,
        
        -- ECR pre-season data (before_preseason = FALSE)
        ecr.ecr_preszn_rank,
        ecr.ecr_preszn_best_rank,
        ecr.ecr_preszn_worst_rank,
        ecr.ecr_preszn_avg_rank,
        ecr.ecr_preszn_stddev_rank,
        ecr.ecr_preszn_adp,
        ecr.ecr_preszn_vs_adp,
        ecr.ecr_preszn_position_rank,
        
        -- ECR data (before_preseason = TRUE)
        ecr.ecr_rank,
        ecr.ecr_best_rank,
        ecr.ecr_worst_rank,
        ecr.ecr_avg_rank,
        ecr.ecr_stddev_rank,
        ecr.ecr_adp,
        ecr.ecr_vs_adp,
        ecr.ecr_position_rank
        
    FROM ranked_data rd
    
    -- Join ECR data
    LEFT JOIN ecr_summary ecr 
        ON rd.player_id = ecr.player_id 
        AND rd.season = ecr.season;
    """
    
    with duckdb.connect(db_path) as conn:
        conn.execute(drop_table_sql)
        conn.execute(create_table_sql)
        
        # Get count for verification
        result = conn.execute("SELECT COUNT(*) FROM smry_season").fetchone()
        logger.info(f"Created smry_season table with {result[0]} rows")
        
        # Show sample data
        sample = conn.execute("""
            SELECT player_id, season, position, games, 
                   fantasy_points_std, std_tot_rnk, std_ppg_rnk,
                   ecr_rank, ecr_preszn_rank
            FROM smry_season 
            WHERE fantasy_points_std > 0 
            ORDER BY fantasy_points_std DESC 
            LIMIT 5
        """).fetchall()
        
        logger.info("Sample data from smry_season:")
        for row in sample:
            logger.info(f"  {row}")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    create_smry_season_table("../prod_nfl.duckdb")
