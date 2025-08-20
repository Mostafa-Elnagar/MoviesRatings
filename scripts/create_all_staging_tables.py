#!/usr/bin/env python3
"""
Create All Staging Tables from JSON Data
This script creates staging tables for TMDB, OMDb, Metacritic, and Rotten Tomatoes data
"""

import subprocess
import json
from pathlib import Path

class AllStagingCreator:
    """Creates staging tables for all data sources"""
    
    def __init__(self):
        self.container_name = "trino"
        self.server = "localhost:8080"
        self.catalog = "iceberg"
        self.schema = "staging"
    
    def create_staging_schema(self) -> bool:
        """Create the staging schema"""
        print("Creating staging schema...")
        
        create_sql = f"CREATE SCHEMA IF NOT EXISTS {self.catalog}.{self.schema};"
        return self._execute_trino_command(create_sql, "Creating staging schema")
    
    def create_omdb_tables(self) -> bool:
        """Create OMDb staging tables"""
        print("Creating OMDb staging tables...")
        
        # Create OMDb movies table
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS {self.catalog}.{self.schema}.omdb_movies (
            imdb_id VARCHAR,
            title VARCHAR,
            year INTEGER,
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
            metascore VARCHAR,
            imdb_rating DECIMAL(3,1),
            imdb_votes INTEGER,
            type VARCHAR,
            dvd VARCHAR,
            box_office VARCHAR,
            production VARCHAR,
            website VARCHAR,
            data_source VARCHAR,
            created_at VARCHAR,
            updated_at VARCHAR
        ) WITH (
            format = 'PARQUET'
        )
        """
        
        if not self._execute_trino_command(create_sql, "Creating OMDb movies table"):
            return False
        
        # Load and insert OMDb data
        json_file = Path("data/raw/omdb_movies_20250820_204113.json")
        if not json_file.exists():
            print(f"OMDb JSON file not found: {json_file}")
            return False
        
        with open(json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Insert each OMDb record
        for record in data:
            insert_sql = f"""
            INSERT INTO {self.catalog}.{self.schema}.omdb_movies (
                imdb_id, title, year, rated, released, runtime, genre, director, writer,
                actors, plot, language, country, awards, poster, ratings, metascore,
                imdb_rating, imdb_votes, type, dvd, box_office, production, website,
                data_source, created_at, updated_at
            ) VALUES (
                '{record.get('imdb_id', '').replace("'", "''")}',
                '{record.get('title', '').replace("'", "''")}',
                {record.get('year', 'NULL')},
                '{record.get('rated', '').replace("'", "''")}',
                '{record.get('released', '').replace("'", "''")}',
                '{record.get('runtime', '').replace("'", "''")}',
                '{record.get('genre', '').replace("'", "''")}',
                '{record.get('director', '').replace("'", "''")}',
                '{record.get('writer', '').replace("'", "''")}',
                '{record.get('actors', '').replace("'", "''")}',
                '{record.get('plot', '').replace("'", "''")}',
                '{record.get('language', '').replace("'", "''")}',
                '{record.get('country', '').replace("'", "''")}',
                '{record.get('awards', '').replace("'", "''")}',
                '{record.get('poster', '').replace("'", "''")}',
                '{json.dumps(record.get('ratings', []), ensure_ascii=False).replace("'", "''")}',
                '{str(record.get('metascore', '')).replace("'", "''")}',
                {record.get('imdb_rating', 'NULL')},
                {record.get('imdb_votes', 'NULL')},
                '{record.get('type', '').replace("'", "''")}',
                '{record.get('dvd', '').replace("'", "''")}',
                '{record.get('box_office', '').replace("'", "''")}',
                '{record.get('production', '').replace("'", "''")}',
                '{record.get('website', '').replace("'", "''")}',
                '{record.get('data_source', '').replace("'", "''")}',
                '{record.get('created_at', '').replace("'", "''")}',
                '{record.get('updated_at', '').replace("'", "''")}'
            )
            """
            
            if not self._execute_trino_command(insert_sql, f"Inserting OMDb movie {record.get('title', 'Unknown')}"):
                return False
        
        print(f"Loaded {len(data)} OMDb movies")
        return True
    
    def create_metacritic_tables(self) -> bool:
        """Create Metacritic staging tables"""
        print("Creating Metacritic staging tables...")
        
        # Create Metacritic ratings table
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS {self.catalog}.{self.schema}.metacritic_ratings (
            imdb_id VARCHAR,
            title VARCHAR,
            year INTEGER,
            metacritic_score INTEGER,
            metacritic_url VARCHAR,
            data_source VARCHAR,
            created_at VARCHAR
        ) WITH (
            format = 'PARQUET'
        )
        """
        
        if not self._execute_trino_command(create_sql, "Creating Metacritic ratings table"):
            return False
        
        # Load and insert Metacritic data
        json_file = Path("data/raw/metacritic_ratings_20250820_204113.json")
        if not json_file.exists():
            print(f"Metacritic JSON file not found: {json_file}")
            return False
        
        with open(json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Insert each Metacritic record
        for record in data:
            insert_sql = f"""
            INSERT INTO {self.catalog}.{self.schema}.metacritic_ratings (
                imdb_id, title, year, metacritic_score, metacritic_url, data_source, created_at
            ) VALUES (
                '{record.get('imdb_id', '').replace("'", "''")}',
                '{record.get('title', '').replace("'", "''")}',
                {record.get('year', 'NULL')},
                {record.get('metacritic_score', 'NULL')},
                '{record.get('metacritic_url', '').replace("'", "''")}',
                '{record.get('data_source', '').replace("'", "''")}',
                '{record.get('created_at', '').replace("'", "''")}'
            )
            """
            
            if not self._execute_trino_command(insert_sql, f"Inserting Metacritic rating for {record.get('title', 'Unknown')}"):
                return False
        
        print(f"Loaded {len(data)} Metacritic ratings")
        return True
    
    def create_rotten_tomatoes_tables(self) -> bool:
        """Create Rotten Tomatoes staging tables"""
        print("Creating Rotten Tomatoes staging tables...")
        
        # Create Rotten Tomatoes ratings table
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS {self.catalog}.{self.schema}.rotten_tomatoes_ratings (
            imdb_id VARCHAR,
            title VARCHAR,
            year INTEGER,
            tomatometer_score INTEGER,
            tomatometer_count INTEGER,
            audience_score INTEGER,
            audience_count INTEGER,
            critic_count INTEGER,
            user_score DECIMAL(10,1),
            user_count VARCHAR,
            data_source VARCHAR,
            created_at VARCHAR
        ) WITH (
            format = 'PARQUET'
        )
        """
        
        if not self._execute_trino_command(create_sql, "Creating Rotten Tomatoes ratings table"):
            return False
        
        # Load and insert Rotten Tomatoes data
        json_file = Path("data/raw/rotten_tomatoes_ratings_20250820_204113.json")
        if not json_file.exists():
            print(f"Rotten Tomatoes JSON file not found: {json_file}")
            return False
        
        with open(json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Insert each Rotten Tomatoes record
        for record in data:
            insert_sql = f"""
            INSERT INTO {self.catalog}.{self.schema}.rotten_tomatoes_ratings (
                imdb_id, title, year, tomatometer_score, tomatometer_count, audience_score,
                audience_count, critic_count, user_score, user_count, data_source, created_at
            ) VALUES (
                '{record.get('imdb_id', '').replace("'", "''")}',
                '{record.get('title', '').replace("'", "''")}',
                {record.get('year', 'NULL')},
                {record.get('tomatometer_score', 'NULL')},
                {record.get('tomatometer_count', 'NULL')},
                {record.get('audience_score', 'NULL')},
                {record.get('audience_count', 'NULL')},
                {record.get('critic_count', 'NULL')},
                {record.get('user_score', 'NULL')},
                '{record.get('user_count', '').replace("'", "''")}',
                '{record.get('data_source', '').replace("'", "''")}',
                '{record.get('created_at', '').replace("'", "''")}'
            )
            """
            
            if not self._execute_trino_command(insert_sql, f"Inserting Rotten Tomatoes rating for {record.get('title', 'Unknown')}"):
                return False
        
        print(f"Loaded {len(data)} Rotten Tomatoes ratings")
        return True
    
    def _execute_trino_command(self, command: str, description: str) -> bool:
        """Execute a Trino command"""
        print(f"Executing: {description}")
        
        try:
            result = subprocess.run([
                'docker', 'exec', self.container_name, 'trino',
                '--server', self.server,
                '--execute', command
            ], capture_output=True, text=True, check=True)
            
            print(f"Success: {description}")
            return True
            
        except subprocess.CalledProcessError as e:
            print(f"Error: {description}")
            if e.stderr:
                print(f"Error details: {e.stderr}")
            return False
    
    def create_all_tables(self) -> bool:
        """Create all staging tables"""
        print("Creating all staging tables...")
        print("=" * 50)
        
        # Create staging schema
        if not self.create_staging_schema():
            return False
        
        # Create OMDb tables
        if not self.create_omdb_tables():
            return False
        
        # Create Metacritic tables
        if not self.create_metacritic_tables():
            return False
        
        # Create Rotten Tomatoes tables
        if not self.create_rotten_tomatoes_tables():
            return False
        
        print("\n" + "=" * 50)
        print("All staging tables created successfully!")
        print("=" * 50)
        
        return True


def main():
    """Main function"""
    creator = AllStagingCreator()
    
    if creator.create_all_tables():
        print("\nAll staging tables creation completed successfully!")
        print("The staging schema is ready with all data sources.")
        return True
    else:
        print("\nStaging tables creation failed. Please check the logs and try again.")
        return False


if __name__ == "__main__":
    main()
