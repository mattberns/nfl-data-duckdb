"""
Expert Consensus Rankings (ECR) Data Extractor
Processes FantasyPros ECR spreadsheets with varying formats across years
"""

import logging
import pandas as pd
import re
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple
import numpy as np

logger = logging.getLogger(__name__)


class ECRExtractor:
    """Extracts and processes Expert Consensus Rankings data from FantasyPros files"""
    
    def __init__(self, db_manager):
        """
        Initialize ECR extractor
        
        Args:
            db_manager: DatabaseManager instance
        """
        self.db = db_manager
        self.data_path = Path("data/raw_ecr")
    
    def extract_file_metadata(self, filename: str) -> Tuple[int, bool]:
        """
        Extract year and before_preseason flag from filename
        
        Args:
            filename: Name of the ECR file
            
        Returns:
            Tuple of (year, before_preseason)
        """
        # Extract year from filename
        year_match = re.search(r'(\d{4})', filename)
        if not year_match:
            raise ValueError(f"Could not extract year from filename: {filename}")
        
        year = int(year_match.group(1))
        
        # Determine if this is before preseason
        before_preseason = 'prepreseason' in filename.lower()
        
        return year, before_preseason
    
    def normalize_column_names(self, df: pd.DataFrame, year: int) -> pd.DataFrame:
        """
        Normalize column names across different file formats
        
        Args:
            df: Input DataFrame
            year: Year of the data
            
        Returns:
            DataFrame with normalized column names
        """
        df_norm = df.copy()
        
        # Create mapping of common column variations to standard names
        column_mapping = {
            # Player name variations
            'player name': 'player_name',
            'overall (team)': 'player_name',
            'player': 'player_name',
            'player (team)': 'player_name',
            
            # Position variations
            'pos': 'position',
            'position': 'position',
            
            # Ranking variations
            'rk': 'rank',
            'rank': 'rank',
            
            # Best rank variations
            'best': 'best_rank',
            'best rank': 'best_rank',
            
            # Worst rank variations
            'worst': 'worst_rank',
            'worst rank': 'worst_rank',
            
            # Average rank variations
            'avg.': 'avg_rank',
            'avg': 'avg_rank',
            'average': 'avg_rank',
            'ave rank': 'avg_rank',
            
            # Standard deviation variations
            'std.dev': 'stddev_rank',
            'std dev': 'stddev_rank',
            'stddev': 'stddev_rank',
            'standard deviation': 'stddev_rank',
            
            # ADP variations
            'adp': 'adp',
            
            # vs ADP variations
            'ecr vs. adp': 'vs_adp',
            'vs. adp': 'vs_adp',
            'vs adp': 'vs_adp',
            'versus adp': 'vs_adp'
        }
        
        # Normalize column names to lowercase and map
        new_columns = []
        for col in df_norm.columns:
            col_lower = str(col).lower().strip()
            new_col = column_mapping.get(col_lower, col_lower)
            new_columns.append(new_col)
        
        df_norm.columns = new_columns
        
        return df_norm
    
    def extract_player_info(self, player_name_col: str) -> Tuple[str, str]:
        """
        Extract clean player name and position from combined string
        
        Args:
            player_name_col: Raw player name column value
            
        Returns:
            Tuple of (clean_player_name, position)
        """
        if pd.isna(player_name_col) or not isinstance(player_name_col, str):
            return "", ""
        
        # Pattern to match player name with team in parentheses
        # e.g., "Christian McCaffrey (SF)" or "Christian McCaffrey CAR"
        
        # First try parentheses format
        paren_match = re.match(r'^(.+?)\s*\(([A-Z]{2,4})\)$', player_name_col.strip())
        if paren_match:
            return paren_match.group(1).strip(), ""
        
        # Try space-separated format (name + team)
        space_match = re.match(r'^(.+?)\s+([A-Z]{2,4})$', player_name_col.strip())
        if space_match:
            return space_match.group(1).strip(), ""
        
        # If no pattern matches, return as-is
        return player_name_col.strip(), ""
    
    def calculate_adp_from_vs_adp(self, rank: int, vs_adp: float) -> Optional[float]:
        """
        Calculate ADP from rank and vs_adp difference
        
        Args:
            rank: Current ECR rank
            vs_adp: Difference between ECR and ADP
            
        Returns:
            Calculated ADP or None if cannot calculate
        """
        if pd.isna(rank) or pd.isna(vs_adp):
            return None
        
        # Handle string values in vs_adp (some files have "N/A" or other text)
        try:
            vs_adp_num = float(vs_adp)
            rank_num = float(rank)
            # vs_adp = ECR - ADP, so ADP = ECR - vs_adp
            return float(rank_num - vs_adp_num)
        except (ValueError, TypeError):
            return None
    
    def process_ecr_file(self, file_path: Path) -> pd.DataFrame:
        """
        Process a single ECR file into normalized format
        
        Args:
            file_path: Path to the ECR file
            
        Returns:
            Normalized DataFrame
        """
        logger.info(f"Processing ECR file: {file_path}")
        
        try:
            # Extract metadata from filename
            year, before_preseason = self.extract_file_metadata(file_path.name)
            
            # Read the Excel file
            if file_path.suffix.lower() == '.xls':
                # Handle older .xls files - 2014-2016 are actually TSV files
                try:
                    # Check if it's a TSV file (tab-separated text)
                    with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                        content = f.read(500)
                        if content.count('\t') > content.count(',') and '\t' in content:
                            logger.info(f"Detected TSV format in {file_path}, reading as TSV")
                            # Skip header rows and read as TSV
                            df = pd.read_csv(file_path, sep='\t', skiprows=4)
                        else:
                            # Try reading as actual Excel file
                            df = pd.read_excel(file_path, engine='xlrd')
                except Exception as e:
                    logger.warning(f"Could not read {file_path}: {e}")
                    return pd.DataFrame()
            else:
                df = pd.read_excel(file_path)
            
            if df.empty:
                logger.warning(f"Empty file: {file_path}")
                return pd.DataFrame()
            
            # Handle 2017 files with weird header structure
            if year == 2017:
                # Skip the first few rows that contain headers/metadata
                if df.shape[0] > 5 and df.iloc[0].isna().all():
                    # Find the actual header row
                    for i in range(min(5, len(df))):
                        if 'Rank' in str(df.iloc[i].values) or 'RK' in str(df.iloc[i].values):
                            df.columns = df.iloc[i]
                            df = df.iloc[i+1:].reset_index(drop=True)
                            break
                    else:
                        # If no proper header found, use row 1 as headers
                        df.columns = df.iloc[1]
                        df = df.iloc[2:].reset_index(drop=True)
            
            # Normalize column names
            df = self.normalize_column_names(df, year)
            
            # Initialize result DataFrame with the correct length
            num_rows = len(df)
            result_df = pd.DataFrame(index=range(num_rows))
            
            # Add metadata columns
            result_df['year'] = year
            result_df['before_preseason'] = before_preseason
            
            # Extract player names and positions
            if 'player_name' in df.columns:
                player_info = df['player_name'].apply(self.extract_player_info)
                result_df['player_name'] = [info[0] for info in player_info]
                
                # Try to get position from dedicated position column first
                if 'position' in df.columns:
                    result_df['position'] = df['position'].fillna("")
                else:
                    # Extract from player name if no dedicated position column
                    result_df['position'] = [info[1] for info in player_info]
            else:
                result_df['player_name'] = ""
                result_df['position'] = ""
            
            # Map ranking columns
            # For rank, use the rank column or create sequence
            if 'rank' in df.columns:
                rank_series = pd.to_numeric(df['rank'], errors='coerce')
                # Fill NaN values with sequential numbers
                result_df['rank'] = [rank_series.iloc[i] if not pd.isna(rank_series.iloc[i]) else i+1 
                                   for i in range(len(rank_series))]
            else:
                result_df['rank'] = list(range(1, len(df) + 1))
                
            result_df['best_rank'] = pd.to_numeric(df.get('best_rank'), errors='coerce') if 'best_rank' in df.columns else None
            result_df['worst_rank'] = pd.to_numeric(df.get('worst_rank'), errors='coerce') if 'worst_rank' in df.columns else None
            result_df['avg_rank'] = pd.to_numeric(df.get('avg_rank'), errors='coerce') if 'avg_rank' in df.columns else None
            result_df['stddev_rank'] = pd.to_numeric(df.get('stddev_rank'), errors='coerce') if 'stddev_rank' in df.columns else None
            
            # Handle ADP and vs_ADP
            if 'adp' in df.columns:
                result_df['adp'] = df['adp']
                result_df['vs_adp'] = df.get('vs_adp', None)
            elif 'vs_adp' in df.columns:
                # Calculate ADP from rank and vs_adp
                result_df['vs_adp'] = df['vs_adp']
                result_df['adp'] = [
                    self.calculate_adp_from_vs_adp(rank, vs_adp)
                    for rank, vs_adp in zip(result_df['rank'], result_df['vs_adp'])
                ]
            else:
                result_df['adp'] = None
                result_df['vs_adp'] = None
            
            # Filter out rows with empty player names (but be more lenient)
            if len(result_df) > 0:
                result_df = result_df[
                    (result_df['player_name'].notna()) & 
                    (result_df['player_name'].astype(str).str.strip() != "") &
                    (result_df['player_name'].astype(str).str.strip() != "nan")
                ]
            
            logger.info(f"Processed {len(result_df)} records from {file_path}")
            
            # Debug: if no records, show what we got
            if len(result_df) == 0 and len(df) > 0:
                logger.warning(f"No valid records after filtering from {file_path}")
                logger.warning(f"Original columns: {list(df.columns)}")
                logger.warning(f"Sample original data: {df.head(3).to_dict()}")
            
            return result_df
            
        except Exception as e:
            logger.error(f"Error processing file {file_path}: {e}")
            return pd.DataFrame()
    
    def refresh_raw_ecr(self) -> Dict[str, int]:
        """
        Refresh the raw_ecr_rankings table with all ECR data
        
        Returns:
            Dictionary with processing results
        """
        logger.info("Starting ECR data refresh")
        
        try:
            # Drop existing table if it exists
            logger.info("Dropping existing raw_ecr_rankings table if exists")
            self.db.conn.execute("DROP TABLE IF EXISTS raw_ecr_rankings")
            
            # Verify table was dropped
            table_check = self.db.conn.execute("""
                SELECT COUNT(*) FROM information_schema.tables 
                WHERE table_name = 'raw_ecr_rankings'
            """).fetchone()[0]
            logger.info(f"Table exists after drop: {table_check > 0}")
            
            # Find all ECR files
            ecr_files = list(self.data_path.glob("FantasyPros_*.xl*"))
            
            if not ecr_files:
                logger.warning(f"No ECR files found in {self.data_path}")
                return {"error": "No ECR files found"}
            
            logger.info(f"Found {len(ecr_files)} ECR files to process")
            
            # Process all files and combine
            all_data = []
            processed_files = 0
            failed_files = 0
            
            for file_path in sorted(ecr_files):
                try:
                    df = self.process_ecr_file(file_path)
                    if not df.empty:
                        all_data.append(df)
                        processed_files += 1
                    else:
                        failed_files += 1
                        logger.warning(f"No data extracted from {file_path}")
                except Exception as e:
                    failed_files += 1
                    logger.error(f"Failed to process {file_path}: {e}")
            
            if not all_data:
                logger.error("No data was successfully processed from any files")
                return {"error": "No data processed"}
            
            # Combine all DataFrames
            combined_df = pd.concat(all_data, ignore_index=True)
            
            # Create table with proper schema
            logger.info(f"Creating raw_ecr_rankings table with {len(combined_df)} total records")
            
            # Let the database manager handle table creation and type inference
            self.db.insert_dataframe(combined_df, 'raw_ecr_rankings')
            
            # Verify the data was inserted
            verification_results = self.verify_ecr_data()
            
            results = {
                'total_records': len(combined_df),
                'processed_files': processed_files,
                'failed_files': failed_files,
                'verification': verification_results
            }
            
            logger.info(f"ECR refresh completed: {results}")
            return results
            
        except Exception as e:
            logger.error(f"Failed to refresh ECR data: {e}")
            raise
    
    def verify_ecr_data(self) -> Dict[str, Any]:
        """
        Verify the accuracy and completeness of loaded ECR data
        
        Returns:
            Dictionary with verification results
        """
        try:
            # Check total record count
            total_count = self.db.query("SELECT COUNT(*) as count FROM raw_ecr_rankings").iloc[0]['count']
            
            # Check year coverage
            year_coverage = self.db.query("""
                SELECT year, 
                       COUNT(*) as total_records,
                       SUM(CASE WHEN before_preseason THEN 1 ELSE 0 END) as before_preseason_records,
                       SUM(CASE WHEN before_preseason THEN 0 ELSE 1 END) as after_preseason_records
                FROM raw_ecr_rankings 
                GROUP BY year 
                ORDER BY year
            """)
            
            # Check for missing data
            missing_data = self.db.query("""
                SELECT 
                    SUM(CASE WHEN player_name IS NULL OR player_name = '' THEN 1 ELSE 0 END) as missing_player_names,
                    SUM(CASE WHEN position IS NULL OR position = '' THEN 1 ELSE 0 END) as missing_positions,
                    SUM(CASE WHEN rank IS NULL THEN 1 ELSE 0 END) as missing_ranks,
                    SUM(CASE WHEN avg_rank IS NULL THEN 1 ELSE 0 END) as missing_avg_ranks
                FROM raw_ecr_rankings
            """).iloc[0]
            
            # Check data quality
            data_quality = self.db.query("""
                SELECT 
                    MIN(year) as min_year,
                    MAX(year) as max_year,
                    COUNT(DISTINCT year) as unique_years,
                    COUNT(DISTINCT CASE WHEN before_preseason THEN year END) as years_with_prepreseason,
                    COUNT(DISTINCT CASE WHEN NOT before_preseason THEN year END) as years_with_preseason
                FROM raw_ecr_rankings
            """).iloc[0]
            
            return {
                'total_records': int(total_count),
                'year_coverage': year_coverage.to_dict('records'),
                'missing_data': missing_data.to_dict(),
                'data_quality': data_quality.to_dict()
            }
            
        except Exception as e:
            logger.error(f"Error verifying ECR data: {e}")
            return {'error': str(e)}
