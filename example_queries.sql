-- Example SQL Queries for NFL Analytics Database

-- =======================
-- FANTASY FOOTBALL QUERIES
-- =======================

-- Top 20 fantasy players by position (Standard scoring)
SELECT 
    player_name, 
    position, 
    team, 
    COUNT(*) as games_played,
    AVG(fantasy_points_std) as avg_fantasy_points,
    SUM(fantasy_points_std) as total_fantasy_points
FROM weekly_stats
WHERE season = 2023 AND position IN ('QB', 'RB', 'WR', 'TE')
GROUP BY player_name, position, team
HAVING games_played >= 10
ORDER BY position, avg_fantasy_points DESC;

-- PPR vs Standard scoring comparison
SELECT 
    player_name,
    position,
    team,
    AVG(fantasy_points_std) as avg_std,
    AVG(fantasy_points_full_ppr) as avg_ppr,
    AVG(fantasy_points_full_ppr - fantasy_points_std) as ppr_advantage
FROM weekly_stats
WHERE season = 2023 AND position IN ('RB', 'WR', 'TE')
GROUP BY player_name, position, team
HAVING COUNT(*) >= 10
ORDER BY ppr_advantage DESC;

-- Weekly fantasy point volatility
SELECT 
    player_name,
    position,
    team,
    AVG(fantasy_points_std) as avg_points,
    STDDEV(fantasy_points_std) as volatility,
    MIN(fantasy_points_std) as min_points,
    MAX(fantasy_points_std) as max_points
FROM weekly_stats
WHERE season = 2023 AND position = 'RB'
GROUP BY player_name, position, team
HAVING COUNT(*) >= 10
ORDER BY volatility;

-- Best matchups by opposing team defense
SELECT 
    opponent_team as defense,
    position,
    COUNT(*) as games,
    AVG(fantasy_points_std) as avg_points_allowed,
    AVG(passing_yards) as avg_pass_yards,
    AVG(rushing_yards) as avg_rush_yards,
    AVG(receiving_yards) as avg_rec_yards
FROM weekly_stats
WHERE season = 2023 AND position IN ('QB', 'RB', 'WR', 'TE')
GROUP BY opponent_team, position
ORDER BY position, avg_points_allowed DESC;

-- =======================
-- PLAYER PERFORMANCE ANALYSIS
-- =======================

-- Red zone efficiency
SELECT 
    p.player_name,
    p.position,
    p.team,
    COUNT(CASE WHEN p.touchdown = true THEN 1 END) as touchdowns,
    COUNT(CASE WHEN p.yardline_100 <= 20 THEN 1 END) as redzone_plays,
    ROUND(
        COUNT(CASE WHEN p.touchdown = true THEN 1 END) * 100.0 / 
        NULLIF(COUNT(CASE WHEN p.yardline_100 <= 20 THEN 1 END), 0), 2
    ) as redzone_td_rate
FROM pbp_data p
WHERE p.season = 2023 
  AND p.yardline_100 <= 20
  AND (p.passer_player_name IS NOT NULL OR p.rusher_player_name IS NOT NULL OR p.receiver_player_name IS NOT NULL)
GROUP BY p.player_name, p.position, p.team
HAVING redzone_plays >= 10
ORDER BY redzone_td_rate DESC;

-- Quarterback efficiency metrics
SELECT 
    ws.player_name,
    ws.team,
    SUM(ws.attempts) as total_attempts,
    SUM(ws.completions) as total_completions,
    ROUND(SUM(ws.completions) * 100.0 / SUM(ws.attempts), 2) as completion_pct,
    SUM(ws.passing_yards) as total_yards,
    ROUND(SUM(ws.passing_yards) * 1.0 / SUM(ws.attempts), 2) as yards_per_attempt,
    SUM(ws.passing_tds) as total_tds,
    SUM(ws.interceptions) as total_ints,
    ROUND(SUM(ws.passing_tds) * 1.0 / SUM(ws.interceptions), 2) as td_int_ratio
FROM weekly_stats ws
WHERE ws.season = 2023 AND ws.position = 'QB'
GROUP BY ws.player_name, ws.team
HAVING total_attempts >= 100
ORDER BY yards_per_attempt DESC;

-- Running back workload analysis
SELECT 
    ws.player_name,
    ws.team,
    COUNT(*) as games_played,
    SUM(ws.carries) as total_carries,
    ROUND(SUM(ws.carries) * 1.0 / COUNT(*), 2) as carries_per_game,
    SUM(ws.targets) as total_targets,
    ROUND(SUM(ws.targets) * 1.0 / COUNT(*), 2) as targets_per_game,
    SUM(ws.carries + ws.targets) as total_touches,
    ROUND(SUM(ws.carries + ws.targets) * 1.0 / COUNT(*), 2) as touches_per_game
FROM weekly_stats ws
WHERE ws.season = 2023 AND ws.position = 'RB'
GROUP BY ws.player_name, ws.team
HAVING games_played >= 10
ORDER BY touches_per_game DESC;

-- =======================
-- TEAM ANALYSIS
-- =======================

-- Team offensive efficiency
SELECT 
    t.team_name,
    t.team_division,
    COUNT(DISTINCT s.game_id) as games_played,
    SUM(CASE WHEN s.home_team = t.team_abbr THEN s.home_score 
             WHEN s.away_team = t.team_abbr THEN s.away_score END) as total_points,
    ROUND(SUM(CASE WHEN s.home_team = t.team_abbr THEN s.home_score 
                   WHEN s.away_team = t.team_abbr THEN s.away_score END) * 1.0 / 
          COUNT(DISTINCT s.game_id), 2) as points_per_game,
    SUM(CASE WHEN (s.home_team = t.team_abbr AND s.home_score > s.away_score) OR
                  (s.away_team = t.team_abbr AND s.away_score > s.home_score) 
             THEN 1 ELSE 0 END) as wins,
    ROUND(SUM(CASE WHEN (s.home_team = t.team_abbr AND s.home_score > s.away_score) OR
                        (s.away_team = t.team_abbr AND s.away_score > s.home_score) 
                   THEN 1 ELSE 0 END) * 100.0 / COUNT(DISTINCT s.game_id), 2) as win_pct
FROM teams t
LEFT JOIN schedules s ON (s.home_team = t.team_abbr OR s.away_team = t.team_abbr)
WHERE s.season = 2023 AND s.season_type = 'REG'
GROUP BY t.team_name, t.team_division, t.team_abbr
ORDER BY points_per_game DESC;

-- Division standings
SELECT 
    t.team_division,
    t.team_name,
    COUNT(DISTINCT s.game_id) as games_played,
    SUM(CASE WHEN (s.home_team = t.team_abbr AND s.home_score > s.away_score) OR
                  (s.away_team = t.team_abbr AND s.away_score > s.home_score) 
             THEN 1 ELSE 0 END) as wins,
    SUM(CASE WHEN (s.home_team = t.team_abbr AND s.home_score < s.away_score) OR
                  (s.away_team = t.team_abbr AND s.away_score < s.home_score) 
             THEN 1 ELSE 0 END) as losses,
    ROUND(SUM(CASE WHEN (s.home_team = t.team_abbr AND s.home_score > s.away_score) OR
                        (s.away_team = t.team_abbr AND s.away_score > s.home_score) 
                   THEN 1 ELSE 0 END) * 100.0 / COUNT(DISTINCT s.game_id), 2) as win_pct
FROM teams t
LEFT JOIN schedules s ON (s.home_team = t.team_abbr OR s.away_team = t.team_abbr)
WHERE s.season = 2023 AND s.season_type = 'REG'
GROUP BY t.team_division, t.team_name, t.team_abbr
ORDER BY t.team_division, win_pct DESC;

-- =======================
-- ADVANCED ANALYTICS
-- =======================

-- Player consistency (coefficient of variation)
SELECT 
    player_name,
    position,
    team,
    COUNT(*) as games,
    AVG(fantasy_points_std) as avg_points,
    STDDEV(fantasy_points_std) as std_dev,
    ROUND(STDDEV(fantasy_points_std) / AVG(fantasy_points_std), 3) as coefficient_of_variation
FROM weekly_stats
WHERE season = 2023 AND position IN ('QB', 'RB', 'WR', 'TE')
GROUP BY player_name, position, team
HAVING games >= 10 AND avg_points > 5
ORDER BY coefficient_of_variation;

-- Breakout candidate analysis (improvement over previous season)
WITH current_season AS (
    SELECT 
        player_name,
        position,
        team,
        AVG(fantasy_points_std) as avg_2023
    FROM weekly_stats
    WHERE season = 2023
    GROUP BY player_name, position, team
    HAVING COUNT(*) >= 10
),
previous_season AS (
    SELECT 
        player_name,
        position,
        team,
        AVG(fantasy_points_std) as avg_2022
    FROM weekly_stats
    WHERE season = 2022
    GROUP BY player_name, position, team
    HAVING COUNT(*) >= 10
)
SELECT 
    c.player_name,
    c.position,
    c.team,
    p.avg_2022,
    c.avg_2023,
    ROUND(c.avg_2023 - p.avg_2022, 2) as improvement,
    ROUND((c.avg_2023 - p.avg_2022) / p.avg_2022 * 100, 2) as improvement_pct
FROM current_season c
JOIN previous_season p ON c.player_name = p.player_name
WHERE c.avg_2023 > p.avg_2022
ORDER BY improvement_pct DESC;

-- Home/Away performance splits
SELECT 
    ws.player_name,
    ws.position,
    ws.team,
    COUNT(CASE WHEN s.home_team = ws.team THEN 1 END) as home_games,
    ROUND(AVG(CASE WHEN s.home_team = ws.team THEN ws.fantasy_points_std END), 2) as home_avg,
    COUNT(CASE WHEN s.away_team = ws.team THEN 1 END) as away_games,
    ROUND(AVG(CASE WHEN s.away_team = ws.team THEN ws.fantasy_points_std END), 2) as away_avg,
    ROUND(AVG(CASE WHEN s.home_team = ws.team THEN ws.fantasy_points_std END) - 
          AVG(CASE WHEN s.away_team = ws.team THEN ws.fantasy_points_std END), 2) as home_advantage
FROM weekly_stats ws
JOIN schedules s ON ws.team = s.home_team OR ws.team = s.away_team
WHERE ws.season = 2023 AND s.season = 2023 AND ws.position = 'QB'
GROUP BY ws.player_name, ws.position, ws.team
HAVING home_games >= 5 AND away_games >= 5
ORDER BY home_advantage DESC;

-- =======================
-- INJURY IMPACT ANALYSIS
-- =======================

-- Player performance before/after injury
SELECT 
    ws.player_name,
    ws.position,
    ws.team,
    AVG(CASE WHEN ws.week < i.week THEN ws.fantasy_points_std END) as pre_injury_avg,
    AVG(CASE WHEN ws.week > i.week THEN ws.fantasy_points_std END) as post_injury_avg,
    COUNT(CASE WHEN ws.week < i.week THEN 1 END) as pre_injury_games,
    COUNT(CASE WHEN ws.week > i.week THEN 1 END) as post_injury_games
FROM weekly_stats ws
JOIN injuries i ON ws.player_name = i.player_name AND ws.season = i.season
WHERE ws.season = 2023 AND i.injury_designation != 'Healthy'
GROUP BY ws.player_name, ws.position, ws.team
HAVING pre_injury_games >= 3 AND post_injury_games >= 3
ORDER BY (pre_injury_avg - post_injury_avg) DESC;

-- =======================
-- SEASON TRENDS
-- =======================

-- Player performance by month
SELECT 
    ws.player_name,
    ws.position,
    ws.team,
    CASE 
        WHEN ws.week <= 4 THEN 'September'
        WHEN ws.week <= 8 THEN 'October'
        WHEN ws.week <= 12 THEN 'November'
        WHEN ws.week <= 16 THEN 'December'
        ELSE 'January'
    END as month,
    COUNT(*) as games,
    AVG(ws.fantasy_points_std) as avg_points
FROM weekly_stats ws
WHERE ws.season = 2023 AND ws.position = 'RB'
GROUP BY ws.player_name, ws.position, ws.team, month
HAVING games >= 2
ORDER BY ws.player_name, month;

-- Weekly league scoring trends
SELECT 
    week,
    COUNT(*) as player_performances,
    AVG(fantasy_points_std) as avg_std_points,
    AVG(fantasy_points_full_ppr) as avg_ppr_points,
    MIN(fantasy_points_std) as min_points,
    MAX(fantasy_points_std) as max_points
FROM weekly_stats
WHERE season = 2023 AND position IN ('QB', 'RB', 'WR', 'TE')
GROUP BY week
ORDER BY week;
