#!/usr/bin/env python3
"""
Bulk Data Loader for movies_stage schema
Loads staged data from JSON files into Iceberg tables
"""

import os
import json
import logging
import subprocess
from pathlib import Path
from typing import List, Dict, Any
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("logs/stage_data_loading.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class StageDataLoader:
    """Bulk loads staged data into movies_stage schema tables"""
    
    def __init__(self):
        self.container_name = "trino"
        self.server = "localhost:8080"
        self.catalog = "iceberg"
        self.schema = "movies_stage"
        
        # Create logs directory
        os.makedirs("logs", exist_ok=True)
    
    def load_tmdb_movies(self, data: List[Dict[str, Any]]) -> bool:
        """Load TMDB movies data into tmdb_movies table"""
        logger.info(f"Loading {len(data)} TMDB movies into {self.schema}.tmdb_movies")
        
        if not data:
            logger.warning("No TMDB movies data to load")
            return True
        
        # Process data in batches for better performance
        batch_size = 100
        success_count = 0
        
        for i in range(0, len(data), batch_size):
            batch = data[i:i + batch_size]
            batch_num = (i // batch_size) + 1
            total_batches = (len(data) + batch_size - 1) // batch_size
            
            logger.info(f"Processing batch {batch_num}/{total_batches} ({len(batch)} records)")
            
            if self._insert_tmdb_movies_batch(batch):
                success_count += len(batch)
                logger.info(f"Batch {batch_num} loaded successfully")
            else:
                logger.error(f"Batch {batch_num} failed to load")
        
        logger.info(f"TMDB movies loading completed: {success_count}/{len(data)} records loaded")
        return success_count == len(data)
    
    def _insert_tmdb_movies_batch(self, batch: List[Dict[str, Any]]) -> bool:
        """Insert a batch of TMDB movies"""
        if not batch:
            return True
        
        # Build INSERT statement
        columns = [
            'tmdb_id', 'imdb_id', 'title', 'original_title', 'release_date', 'year',
            'overview', 'tagline', 'status', 'runtime', 'budget', 'revenue',
            'popularity', 'vote_average', 'vote_count', 'genres', 'genre_ids',
            'original_language', 'production_companies', 'production_countries',
            'spoken_languages', 'cast_data', 'crew_data', 'backdrop_path', 'poster_path',
            'homepage', 'external_ids', 'data_source', 'created_at', 'updated_at'
        ]
        
        values_list = []
        for record in batch:
            values = [
                self._format_value(record.get(col)) for col in columns
            ]
            values_list.append(f"({', '.join(values)})")
        
        insert_sql = f"""
        INSERT INTO {self.catalog}.{self.schema}.tmdb_movies 
        ({', '.join(columns)})
        VALUES {', '.join(values_list)}
        """
        
        return self._execute_trino_command(insert_sql, f"Inserting {len(batch)} TMDB movies")
    
    def load_omdb_movies(self, data: List[Dict[str, Any]]) -> bool:
        """Load OMDb movies data into omdb_movies table"""
        logger.info(f"Loading {len(data)} OMDb movies into {self.schema}.omdb_movies")
        
        if not data:
            logger.warning("No OMDb movies data to load")
            return True
        
        # Process data in batches
        batch_size = 100
        success_count = 0
        
        for i in range(0, len(data), batch_size):
            batch = data[i:i + batch_size]
            batch_num = (i // batch_size) + 1
            total_batches = (len(data) + batch_size - 1) // batch_size
            
            logger.info(f"Processing batch {batch_num}/{total_batches} ({len(batch)} records)")
            
            if self._insert_omdb_movies_batch(batch):
                success_count += len(batch)
                logger.info(f"Batch {batch_num} loaded successfully")
            else:
                logger.error(f"Batch {batch_num} failed to load")
        
        logger.info(f"OMDb movies loading completed: {success_count}/{len(data)} records loaded")
        return success_count == len(data)
    
    def _insert_omdb_movies_batch(self, batch: List[Dict[str, Any]]) -> bool:
        """Insert a batch of OMDb movies"""
        if not batch:
            return True
        
        columns = [
            'imdb_id', 'title', 'year', 'omdb_title', 'rated', 'released', 'runtime',
            'genre', 'director', 'writer', 'actors', 'plot', 'language', 'country',
            'awards', 'poster', 'ratings', 'imdb_rating', 'imdb_votes', 'metascore',
            'box_office', 'production', 'website', 'data_source', 'created_at', 'updated_at'
        ]
        
        values_list = []
        for record in batch:
            values = [
                self._format_value(record.get(col)) for col in columns
            ]
            values_list.append(f"({', '.join(values)})")
        
        insert_sql = f"""
        INSERT INTO {self.catalog}.{self.schema}.omdb_movies 
        ({', '.join(columns)})
        VALUES {', '.join(values_list)}
        """
        
        return self._execute_trino_command(insert_sql, f"Inserting {len(batch)} OMDb movies")
    
    def load_metacritic_ratings(self, data: List[Dict[str, Any]]) -> bool:
        """Load Metacritic ratings data into metacritic_ratings table"""
        logger.info(f"Loading {len(data)} Metacritic ratings into {self.schema}.metacritic_ratings")
        
        if not data:
            logger.warning("No Metacritic ratings data to load")
            return True
        
        # Process data in batches
        batch_size = 100
        success_count = 0
        
        for i in range(0, len(data), batch_size):
            batch = data[i:i + batch_size]
            batch_num = (i // batch_size) + 1
            total_batches = (len(data) + batch_size - 1) // batch_size
            
            logger.info(f"Processing batch {batch_num}/{total_batches} ({len(batch)} records)")
            
            if self._insert_metacritic_ratings_batch(batch):
                success_count += len(batch)
                logger.info(f"Batch {batch_num} loaded successfully")
            else:
                logger.error(f"Batch {batch_num} failed to load")
        
        logger.info(f"Metacritic ratings loading completed: {success_count}/{len(data)} records loaded")
        return success_count == len(data)
    
    def _insert_metacritic_ratings_batch(self, batch: List[Dict[str, Any]]) -> bool:
        """Insert a batch of Metacritic ratings"""
        if not batch:
            return True
        
        columns = [
            'tmdb_id', 'imdb_id', 'title', 'year', 'critic_score', 'critic_count',
            'user_score', 'user_count', 'data_source', 'created_at'
        ]
        
        values_list = []
        for record in batch:
            values = [
                self._format_value(record.get(col)) for col in columns
            ]
            values_list.append(f"({', '.join(values)})")
        
        insert_sql = f"""
        INSERT INTO {self.catalog}.{self.schema}.metacritic_ratings 
        ({', '.join(columns)})
        VALUES {', '.join(values_list)}
        """
        
        return self._execute_trino_command(insert_sql, f"Inserting {len(batch)} Metacritic ratings")
    
    def load_rotten_tomatoes_ratings(self, data: List[Dict[str, Any]]) -> bool:
        """Load Rotten Tomatoes ratings data into rotten_tomatoes_ratings table"""
        logger.info(f"Loading {len(data)} Rotten Tomatoes ratings into {self.schema}.rotten_tomatoes_ratings")
        
        if not data:
            logger.warning("No Rotten Tomatoes ratings data to load")
            return True
        
        # Process data in batches
        batch_size = 100
        success_count = 0
        
        for i in range(0, len(data), batch_size):
            batch = data[i:i + batch_size]
            batch_num = (i // batch_size) + 1
            total_batches = (len(data) + batch_size - 1) // batch_size
            
            logger.info(f"Processing batch {batch_num}/{total_batches} ({len(batch)} records)")
            
            if self._insert_rotten_tomatoes_ratings_batch(batch):
                success_count += len(batch)
                logger.info(f"Batch {batch_num} loaded successfully")
            else:
                logger.error(f"Batch {batch_num} failed to load")
        
        logger.info(f"Rotten Tomatoes ratings loading completed: {success_count}/{len(data)} records loaded")
        return success_count == len(data)
    
    def _insert_rotten_tomatoes_ratings_batch(self, batch: List[Dict[str, Any]]) -> bool:
        """Insert a batch of Rotten Tomatoes ratings"""
        if not batch:
            return True
        
        columns = [
            'tmdb_id', 'imdb_id', 'title', 'year', 'critic_score', 'critic_count',
            'user_score', 'user_count', 'data_source', 'created_at'
        ]
        
        values_list = []
        for record in batch:
            values = [
                self._format_value(record.get(col)) for col in columns
            ]
            values_list.append(f"({', '.join(values)})")
        
        insert_sql = f"""
        INSERT INTO {self.catalog}.{self.schema}.rotten_tomatoes_ratings 
        ({', '.join(columns)})
        VALUES {', '.join(values_list)}
        """
        
        return self._execute_trino_command(insert_sql, f"Inserting {len(batch)} Rotten Tomatoes ratings")
    
    def _format_value(self, value: Any) -> str:
        """Format a value for SQL insertion"""
        if value is None:
            return 'NULL'
        
        if isinstance(value, (int, float)):
            return str(value)
        
        if isinstance(value, bool):
            return str(value).lower()
        
        if isinstance(value, list):
            # Format as Trino array
            if not value:
                return 'ARRAY[]'
            formatted_values = []
            for item in value:
                if isinstance(item, str):
                    escaped = item.replace(chr(39), chr(39)+chr(39))
                    formatted_values.append(f"'{escaped}'")
                else:
                    formatted_values.append(str(item))
            return f"ARRAY[{', '.join(formatted_values)}]"
        
        if isinstance(value, dict):
            # Convert to JSON string and escape single quotes
            json_str = json.dumps(value, ensure_ascii=False)
            return f"'{json_str.replace(chr(39), chr(39)+chr(39))}'"
        
        if isinstance(value, str):
            # Escape single quotes and handle special characters
            escaped = value.replace(chr(39), chr(39)+chr(39))
            return f"'{escaped}'"
        
        # Default: convert to string and escape
        str_value = str(value)
        escaped = str_value.replace(chr(39), chr(39)+chr(39))
        return f"'{escaped}'"
    
    def _execute_trino_command(self, command: str, description: str) -> bool:
        """Execute a Trino command"""
        try:
            result = subprocess.run([
                'docker', 'exec', self.container_name, 'trino',
                '--server', self.server,
                '--execute', command
            ], capture_output=True, text=True, check=True)
            
            return True
            
        except subprocess.CalledProcessError as e:
            logger.error(f"Error executing Trino command: {description}")
            if e.stderr:
                logger.error(f"Error details: {e.stderr}")
            return False
    
    def load_all_staged_data(self) -> bool:
        """Load all staged data from JSON files"""
        logger.info("Loading all staged data into movies_stage schema")
        logger.info("=" * 60)
        
        # Find staged data files
        data_dir = Path("data/raw")
        if not data_dir.exists():
            logger.error("Data directory not found!")
            return False
        
        data_files = list(data_dir.glob("*.json"))
        if not data_files:
            logger.error("No data files found in data directory!")
            return False
        
        logger.info(f"Found {len(data_files)} data files:")
        for f in data_files:
            logger.info(f"  - {f.name}")
        
        success = True
        
        # Load each file type
        for data_file in data_files:
            try:
                logger.info(f"\nLoading {data_file.name}...")
                
                with open(data_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                logger.info(f"  Loaded {len(data)} records")
                
                # Determine table type from filename and load accordingly
                if 'tmdb_movies' in data_file.name:
                    if not self.load_tmdb_movies(data):
                        success = False
                        
                elif 'omdb_movies' in data_file.name:
                    if not self.load_omdb_movies(data):
                        success = False
                        
                elif 'metacritic_ratings' in data_file.name:
                    if not self.load_metacritic_ratings(data):
                        success = False
                        
                elif 'rotten_tomatoes_ratings' in data_file.name:
                    if not self.load_rotten_tomatoes_ratings(data):
                        success = False
                        
                else:
                    logger.warning(f"  Unknown file type: {data_file.name}")
                    
            except Exception as e:
                logger.error(f"  ✗ Error loading {data_file.name}: {e}")
                success = False
        
        if success:
            logger.info("\n" + "=" * 60)
            logger.info("All staged data loaded successfully!")
            logger.info("=" * 60)
        else:
            logger.error("\n" + "=" * 60)
            logger.error("Some data failed to load. Check logs for details.")
            logger.error("=" * 60)
        
        return success
    
    def verify_data_loaded(self) -> bool:
        """Verify that data was loaded into the tables"""
        logger.info("Verifying data load...")
        
        try:
            # Check table counts
            tables = ['tmdb_movies', 'omdb_movies', 'metacritic_ratings', 'rotten_tomatoes_ratings']
            
            for table in tables:
                count_sql = f"SELECT COUNT(*) FROM {self.catalog}.{self.schema}.{table};"
                
                result = subprocess.run([
                    'docker', 'exec', self.container_name, 'trino',
                    '--server', self.server,
                    '--execute', count_sql
                ], capture_output=True, text=True, check=True)
                
                count = result.stdout.strip().strip('"')
                logger.info(f"  {table}: {count} records")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to verify data load: {e}")
            return False


def main():
    """Main function for testing"""
    try:
        loader = StageDataLoader()
        
        if loader.load_all_staged_data():
            print("\nData loading completed successfully!")
            loader.verify_data_loaded()
        else:
            print("\nData loading failed. Check logs for details.")
            
    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    main()
