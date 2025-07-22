"""
Test script for smry_season table validation.

Tests:
1. Compare record counts between seasonal_stats and smry_season
2. Check for duplicate player_id and season combinations
3. Verify Cooper Kupp #1 WR HALF PPR tot_rnk 2021
4. Verify Davante Adams #2 WR FULL PPR ppg_rnk 2021
5. Verify Russell Wilson #1 QB 2017, Cam Newton #2
"""

import duckdb
import logging
from typing import Dict, Any

logger = logging.getLogger(__name__)

def run_test_query(conn: duckdb.DuckDBPyConnection, query: str, test_name: str) -> Any:
    """Run a test query and return the result."""
    logger.info(f"Running {test_name}...")
    try:
        result = conn.execute(query).fetchall()
        return result
    except Exception as e:
        logger.error(f"Error in {test_name}: {e}")
        return None

def test_record_counts(conn: duckdb.DuckDBPyConnection) -> bool:
    """Test 1: Compare record counts between seasonal_stats and smry_season."""
    
    seasonal_count = conn.execute("SELECT COUNT(*) FROM seasonal_stats").fetchone()[0]
    smry_count = conn.execute("SELECT COUNT(*) FROM smry_season").fetchone()[0]
    
    logger.info(f"seasonal_stats count: {seasonal_count}")
    logger.info(f"smry_season count: {smry_count}")
    
    if seasonal_count == smry_count:
        logger.info("✅ TEST 1 PASSED: Record counts match")
        return True
    else:
        logger.error(f"❌ TEST 1 FAILED: Record counts don't match ({seasonal_count} vs {smry_count})")
        return False

def test_no_duplicates(conn: duckdb.DuckDBPyConnection) -> bool:
    """Test 2: Check for duplicate player_id and season combinations."""
    
    query = """
    SELECT COUNT(*) as total_records,
           COUNT(DISTINCT player_id || '_' || season) as unique_combinations
    FROM smry_season
    """
    
    result = conn.execute(query).fetchone()
    total_records, unique_combinations = result
    
    logger.info(f"Total records: {total_records}")
    logger.info(f"Unique player_id/season combinations: {unique_combinations}")
    
    if total_records == unique_combinations:
        logger.info("✅ TEST 2 PASSED: No duplicate player_id/season combinations")
        return True
    else:
        logger.error(f"❌ TEST 2 FAILED: Found duplicates ({total_records} vs {unique_combinations})")
        return False

def test_cooper_kupp_2021(conn: duckdb.DuckDBPyConnection) -> bool:
    """Test 3: Verify Cooper Kupp #1 WR HALF PPR tot_rnk 2021."""
    
    query = """
    SELECT player_name, halfppr_tot_rnk, fantasy_points_half_ppr
    FROM smry_season 
    WHERE season = 2021 
      AND position = 'WR' 
      AND halfppr_tot_rnk = 1
    """
    
    result = conn.execute(query).fetchall()
    
    if result and len(result) == 1:
        player_name, rank, points = result[0]
        logger.info(f"2021 #1 WR HALF PPR: {player_name} (rank: {rank}, points: {points})")
        
        if 'KUPP' in player_name.upper():
            logger.info("✅ TEST 3 PASSED: Cooper Kupp is #1 WR HALF PPR 2021")
            return True
        else:
            logger.error(f"❌ TEST 3 FAILED: Expected Cooper Kupp, got {player_name}")
            return False
    else:
        logger.error(f"❌ TEST 3 FAILED: Expected 1 result, got {len(result) if result else 0}")
        return False

def test_davante_adams_2021(conn: duckdb.DuckDBPyConnection) -> bool:
    """Test 4: Verify Davante Adams #2 WR FULL PPR ppg_rnk 2021."""
    
    query = """
    SELECT player_name, fullppr_ppg_rnk, 
           fantasy_points_full_ppr / games AS ppg_points,
           fantasy_points_full_ppr, games
    FROM smry_season 
    WHERE season = 2021 
      AND position = 'WR' 
      AND fullppr_ppg_rnk = 2
    """
    
    result = conn.execute(query).fetchall()
    
    if result and len(result) == 1:
        player_name, rank, ppg_points, total_points, games = result[0]
        logger.info(f"2021 #2 WR FULL PPR PPG: {player_name} (rank: {rank}, ppg: {ppg_points:.2f}, total: {total_points}, games: {games})")
        
        if 'ADAMS' in player_name.upper():
            logger.info("✅ TEST 4 PASSED: Davante Adams is #2 WR FULL PPR PPG 2021")
            return True
        else:
            logger.error(f"❌ TEST 4 FAILED: Expected Davante Adams, got {player_name}")
            return False
    else:
        logger.error(f"❌ TEST 4 FAILED: Expected 1 result, got {len(result) if result else 0}")
        return False

def test_qb_rankings_2017(conn: duckdb.DuckDBPyConnection) -> bool:
    """Test 5: Verify Russell Wilson #1 QB 2017, Cam Newton #2."""
    
    query = """
    SELECT player_name, std_tot_rnk, fantasy_points_std
    FROM smry_season 
    WHERE season = 2017 
      AND position = 'QB' 
      AND std_tot_rnk IN (1, 2)
    ORDER BY std_tot_rnk
    """
    
    result = conn.execute(query).fetchall()
    
    if result and len(result) == 2:
        rank1_player, rank1_pos, rank1_points = result[0]
        rank2_player, rank2_pos, rank2_points = result[1]
        
        logger.info(f"2017 #1 QB: {rank1_player} (points: {rank1_points})")
        logger.info(f"2017 #2 QB: {rank2_player} (points: {rank2_points})")
        
        wilson_check = 'WILSON' in rank1_player.upper()
        newton_check = 'NEWTON' in rank2_player.upper()
        
        if wilson_check and newton_check:
            logger.info("✅ TEST 5 PASSED: Russell Wilson #1, Cam Newton #2 QB 2017")
            return True
        else:
            logger.error(f"❌ TEST 5 FAILED: Expected Wilson #1, Newton #2. Got {rank1_player} #1, {rank2_player} #2")
            return False
    else:
        logger.error(f"❌ TEST 5 FAILED: Expected 2 results, got {len(result) if result else 0}")
        return False

def debug_rankings_2021_wr(conn: duckdb.DuckDBPyConnection):
    """Debug function to check 2021 WR rankings."""
    logger.info("\n=== DEBUG: 2021 WR Rankings ===")
    
    # Half PPR total rankings
    query = """
    SELECT player_name, halfppr_tot_rnk, fantasy_points_half_ppr
    FROM smry_season 
    WHERE season = 2021 AND position = 'WR' 
    ORDER BY halfppr_tot_rnk
    LIMIT 5
    """
    
    result = conn.execute(query).fetchall()
    logger.info("Top 5 WR HALF PPR Total Rankings 2021:")
    for row in result:
        logger.info(f"  {row[0]} - Rank {row[1]} - {row[2]} points")
    
    # Full PPR per-game rankings
    query = """
    SELECT player_name, fullppr_ppg_rnk, 
           fantasy_points_full_ppr / games AS ppg_points,
           fantasy_points_full_ppr, games
    FROM smry_season 
    WHERE season = 2021 AND position = 'WR' 
    ORDER BY fullppr_ppg_rnk
    LIMIT 5
    """
    
    result = conn.execute(query).fetchall()
    logger.info("\nTop 5 WR FULL PPR Per-Game Rankings 2021:")
    for row in result:
        logger.info(f"  {row[0]} - Rank {row[1]} - {row[2]:.2f} PPG - {row[3]} total pts - {row[4]} games")

def debug_rankings_2017_qb(conn: duckdb.DuckDBPyConnection):
    """Debug function to check 2017 QB rankings."""
    logger.info("\n=== DEBUG: 2017 QB Rankings ===")
    
    query = """
    SELECT player_name, std_tot_rnk, fantasy_points_std
    FROM smry_season 
    WHERE season = 2017 AND position = 'QB' 
    ORDER BY std_tot_rnk
    LIMIT 5
    """
    
    result = conn.execute(query).fetchall()
    logger.info("Top 5 QB Standard Total Rankings 2017:")
    for row in result:
        logger.info(f"  {row[0]} - Rank {row[1]} - {row[2]} points")

def run_all_tests(db_path: str) -> Dict[str, bool]:
    """Run all tests and return results."""
    
    logger.info("Starting smry_season table tests...")
    
    results = {}
    
    with duckdb.connect(db_path) as conn:
        # Run all tests
        results["record_counts"] = test_record_counts(conn)
        results["no_duplicates"] = test_no_duplicates(conn)
        results["cooper_kupp_2021"] = test_cooper_kupp_2021(conn)
        results["davante_adams_2021"] = test_davante_adams_2021(conn)
        results["qb_rankings_2017"] = test_qb_rankings_2017(conn)
        
        # Run debug functions for failed tests
        if not results["cooper_kupp_2021"] or not results["davante_adams_2021"]:
            debug_rankings_2021_wr(conn)
            
        if not results["qb_rankings_2017"]:
            debug_rankings_2017_qb(conn)
    
    # Summary
    passed = sum(results.values())
    total = len(results)
    
    logger.info(f"\n=== TEST SUMMARY ===")
    logger.info(f"Tests passed: {passed}/{total}")
    
    for test_name, passed in results.items():
        status = "✅ PASSED" if passed else "❌ FAILED"
        logger.info(f"{test_name}: {status}")
    
    return results

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
    run_all_tests("../prod_nfl.duckdb")
