#!/usr/bin/env python3
"""
Create movies_stage schema and tables for bulk data ingestion
"""

import subprocess
import sys
from pathlib import Path

# Add project root to path for imports
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from config.settings import *

class StageSchemaCreator:
    """Creates the movies_stage schema and tables"""
    
    def __init__(self):
        self.container_name = "trino"
        self.server = "localhost:8080"
        self.catalog = "iceberg"
        self.schema = "movies_stage"
    
    def create_schema(self) -> bool:
        """Create the movies_stage schema"""
        print(f"Creating schema: {self.catalog}.{self.schema}")
        
        schema_cmd = f"CREATE SCHEMA IF NOT EXISTS {self.catalog}.{self.schema};"
        return self._execute_trino_command(schema_cmd, f"Creating {self.schema} schema")
    
    def create_tmdb_movies_table(self) -> bool:
        """Create TMDB movies table"""
        print("Creating TMDB movies table...")
        
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS {self.catalog}.{self.schema}.tmdb_movies (
            tmdb_id INTEGER,
            imdb_id VARCHAR,
            title VARCHAR,
            original_title VARCHAR,
            release_date VARCHAR,
            year INTEGER,
            overview VARCHAR,
            tagline VARCHAR,
            status VARCHAR,
            runtime INTEGER,
            budget INTEGER,
            revenue INTEGER,
            popularity DECIMAL(10,4),
            vote_average DECIMAL(10,3),
            vote_count INTEGER,
            genres ARRAY(VARCHAR),
            genre_ids ARRAY(INTEGER),
            original_language VARCHAR,
            production_companies ARRAY(VARCHAR),
            production_countries ARRAY(VARCHAR),
            spoken_languages ARRAY(VARCHAR),
            cast_data VARCHAR,
            crew_data VARCHAR,
            backdrop_path VARCHAR,
            poster_path VARCHAR,
            homepage VARCHAR,
            external_ids VARCHAR,
            data_source VARCHAR,
            created_at VARCHAR,
            updated_at VARCHAR
        ) WITH (
            format = 'PARQUET',
            partitioning = ARRAY['year']
        )
        """
        
        return self._execute_trino_command(create_sql, "Creating tmdb_movies table")
    
    def create_omdb_movies_table(self) -> bool:
        """Create OMDb movies table"""
        print("Creating OMDb movies table...")
        
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS {self.catalog}.{self.schema}.omdb_movies (
            imdb_id VARCHAR,
            title VARCHAR,
            year INTEGER,
            omdb_title VARCHAR,
            rated VARCHAR,
            released VARCHAR,
            runtime VARCHAR,
            genre VARCHAR,
            director VARCHAR,
            writer VARCHAR,
            actors VARCHAR,
            plot VARCHAR,
            language VARCHAR,
            country VARCHAR,
            awards VARCHAR,
            poster VARCHAR,
            ratings VARCHAR,
            imdb_rating DECIMAL(10,1),
            imdb_votes INTEGER,
            metascore INTEGER,
            box_office VARCHAR,
            production VARCHAR,
            website VARCHAR,
            data_source VARCHAR,
            created_at VARCHAR,
            updated_at VARCHAR
        ) WITH (
            format = 'PARQUET',
            partitioning = ARRAY['year']
        )
        """
        
        return self._execute_trino_command(create_sql, "Creating omdb_movies table")
    
    def create_metacritic_ratings_table(self) -> bool:
        """Create Metacritic ratings table"""
        print("Creating Metacritic ratings table...")
        
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS {self.catalog}.{self.schema}.metacritic_ratings (
            tmdb_id INTEGER,
            imdb_id VARCHAR,
            title VARCHAR,
            year INTEGER,
            critic_score DECIMAL(10,1),
            critic_count INTEGER,
            user_score DECIMAL(10,1),
            user_count INTEGER,
            data_source VARCHAR,
            created_at VARCHAR
        ) WITH (
            format = 'PARQUET',
            partitioning = ARRAY['year']
        )
        """
        
        return self._execute_trino_command(create_sql, "Creating metacritic_ratings table")
    
    def create_rotten_tomatoes_ratings_table(self) -> bool:
        """Create Rotten Tomatoes ratings table"""
        print("Creating Rotten Tomatoes ratings table...")
        
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS {self.catalog}.{self.schema}.rotten_tomatoes_ratings (
            tmdb_id INTEGER,
            imdb_id VARCHAR,
            title VARCHAR,
            year INTEGER,
            critic_score DECIMAL(10,1),
            critic_count INTEGER,
            user_score DECIMAL(10,1),
            user_count VARCHAR,
            data_source VARCHAR,
            created_at VARCHAR
        ) WITH (
            format = 'PARQUET',
            partitioning = ARRAY['year']
        )
        """
        
        return self._execute_trino_command(create_sql, "Creating rotten_tomatoes_ratings table")
    

    
    def _execute_trino_command(self, command: str, description: str) -> bool:
        """Execute a Trino command"""
        print(f"Executing: {description}")
        
        try:
            subprocess.run([
                'docker', 'exec', self.container_name, 'trino',
                '--server', self.server,
                '--execute', command
            ], capture_output=True, text=True, check=True)
            
            print(f"Success: {description}")
            return True
            
        except subprocess.CalledProcessError as e:
            print(f"✗ Error: {description}")
            if e.stderr:
                print(f"Error details: {e.stderr}")
            return False
    
    def verify_tables(self) -> bool:
        """Verify that all tables were created"""
        print("\nVerifying table creation...")
        
        try:
            result = subprocess.run([
                'docker', 'exec', self.container_name, 'trino',
                '--server', self.server,
                '--catalog', self.catalog,
                '--schema', self.schema,
                '--execute', 'SHOW TABLES;'
            ], capture_output=True, text=True, check=True)
            
            print("Available tables:")
            print(result.stdout)
            return True
            
        except subprocess.CalledProcessError as e:
            print(f"Failed to verify tables: {e}")
            return False
    
    def create_all_tables(self) -> bool:
        """Create all tables in the movies_stage schema"""
        print("Creating movies_stage schema and tables...")
        print("=" * 50)
        
        # Create schema
        if not self.create_schema():
            return False
        
        # Create individual tables
        if not self.create_tmdb_movies_table():
            return False
        
        if not self.create_omdb_movies_table():
            return False
        
        if not self.create_metacritic_ratings_table():
            return False
        
        if not self.create_rotten_tomatoes_ratings_table():
            return False
        
        # Verify tables
        if not self.verify_tables():
            return False
        
        print("\n" + "=" * 50)
        print("All tables created successfully!")
        print("=" * 50)
        
        return True


def main():
    """Main function"""
    creator = StageSchemaCreator()
    
    if creator.create_all_tables():
        print("\nSchema and tables creation completed successfully!")
        print("The movies_stage schema is ready for data ingestion.")
        sys.exit(0)
    else:
        print("\nSchema and tables creation failed. Please check the logs and try again.")
        sys.exit(1)


if __name__ == "__main__":
    main()
