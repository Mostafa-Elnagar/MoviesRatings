#!/usr/bin/env python3
"""
Create Simple Staging Tables from JSON Data
This script creates the final tables directly from JSON data
"""

import subprocess
import json
from pathlib import Path

class SimpleStagingCreator:
    """Creates simple staging tables from JSON data"""
    
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
    
    def create_movies_table(self) -> bool:
        """Create movies table directly from JSON data"""
        print("Creating movies table...")
        
        # Create the movies table
        create_sql = f"""
        CREATE TABLE {self.catalog}.{self.schema}.movies (
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
            original_language VARCHAR,
            backdrop_path VARCHAR,
            poster_path VARCHAR,
            homepage VARCHAR
        ) WITH (
            format = 'PARQUET',
            partitioning = ARRAY['year']
        )
        """
        
        if not self._execute_trino_command(create_sql, "Creating movies table"):
            return False
        
        # Load and insert data
        json_file = Path("data/raw/tmdb_movies_20250820_204113.json")
        if not json_file.exists():
            print(f"JSON file not found: {json_file}")
            return False
        
        with open(json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Insert each record
        for record in data:
            insert_sql = f"""
            INSERT INTO {self.catalog}.{self.schema}.movies (
                tmdb_id, imdb_id, title, original_title, release_date, year,
                overview, tagline, status, runtime, budget, revenue,
                popularity, vote_average, vote_count, original_language,
                backdrop_path, poster_path, homepage
            ) VALUES (
                {record.get('tmdb_id', 'NULL')},
                '{record.get('imdb_id', '').replace("'", "''")}',
                '{record.get('title', '').replace("'", "''")}',
                '{record.get('original_title', '').replace("'", "''")}',
                '{record.get('release_date', '').replace("'", "''")}',
                {record.get('year', 'NULL')},
                '{record.get('overview', '').replace("'", "''")}',
                '{record.get('tagline', '').replace("'", "''")}',
                '{record.get('status', '').replace("'", "''")}',
                {record.get('runtime', 'NULL')},
                {record.get('budget', 'NULL')},
                {record.get('revenue', 'NULL')},
                {record.get('popularity', 'NULL')},
                {record.get('vote_average', 'NULL')},
                {record.get('vote_count', 'NULL')},
                '{record.get('original_language', '').replace("'", "''")}',
                '{record.get('backdrop_path', '').replace("'", "''")}',
                '{record.get('poster_path', '').replace("'", "''")}',
                '{record.get('homepage', '').replace("'", "''")}'
            )
            """
            
            if not self._execute_trino_command(insert_sql, f"Inserting movie {record.get('title', 'Unknown')}"):
                return False
        
        print(f"Loaded {len(data)} movies")
        return True
    
    def create_genres_table(self) -> bool:
        """Create genres table from JSON data"""
        print("Creating genres table...")
        
        # Create the genres table
        create_sql = f"""
        CREATE TABLE {self.catalog}.{self.schema}.movie_genres (
            tmdb_id INTEGER,
            genre VARCHAR
        ) WITH (
            format = 'PARQUET'
        )
        """
        
        if not self._execute_trino_command(create_sql, "Creating movie_genres table"):
            return False
        
        # Load and insert data
        json_file = Path("data/raw/tmdb_movies_20250820_204113.json")
        with open(json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Insert genres for each movie
        for record in data:
            tmdb_id = record.get('tmdb_id')
            genres = record.get('genres', [])
            
            for genre in genres:
                insert_sql = f"""
                INSERT INTO {self.catalog}.{self.schema}.movie_genres (tmdb_id, genre)
                VALUES ({tmdb_id}, '{genre.replace("'", "''")}')
                """
                
                if not self._execute_trino_command(insert_sql, f"Inserting genre {genre} for movie {tmdb_id}"):
                    return False
        
        print(f"Loaded genres for {len(data)} movies")
        return True
    
    def create_cast_table(self) -> bool:
        """Create cast table from JSON data"""
        print("Creating cast table...")
        
        # Create the cast table
        create_sql = f"""
        CREATE TABLE {self.catalog}.{self.schema}.movie_cast (
            tmdb_id INTEGER,
            cast_id INTEGER,
            cast_name VARCHAR,
            character VARCHAR
        ) WITH (
            format = 'PARQUET'
        )
        """
        
        if not self._execute_trino_command(create_sql, "Creating movie_cast table"):
            return False
        
        # Load and insert data
        json_file = Path("data/raw/tmdb_movies_20250820_204113.json")
        with open(json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Insert cast for each movie
        for record in data:
            tmdb_id = record.get('tmdb_id')
            cast = record.get('cast', [])
            
            for person in cast:
                insert_sql = f"""
                INSERT INTO {self.catalog}.{self.schema}.movie_cast (tmdb_id, cast_id, cast_name, character)
                VALUES (
                    {tmdb_id},
                    {person.get('id', 'NULL')},
                    '{person.get('name', '').replace("'", "''")}',
                    '{person.get('character', '').replace("'", "''")}'
                )
                """
                
                if not self._execute_trino_command(insert_sql, f"Inserting cast member {person.get('name', 'Unknown')} for movie {tmdb_id}"):
                    return False
        
        print(f"Loaded cast for {len(data)} movies")
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
        print("Creating simple staging tables...")
        print("=" * 50)
        
        # Create staging schema
        if not self.create_staging_schema():
            return False
        
        # Create movies table
        if not self.create_movies_table():
            return False
        
        # Create genres table
        if not self.create_genres_table():
            return False
        
        # Create cast table
        if not self.create_cast_table():
            return False
        
        print("\n" + "=" * 50)
        print("All tables created successfully!")
        print("=" * 50)
        
        return True


def main():
    """Main function"""
    creator = SimpleStagingCreator()
    
    if creator.create_all_tables():
        print("\nSimple staging tables creation completed successfully!")
        print("The staging schema is ready with normalized tables.")
        return True
    else:
        print("\nSimple staging tables creation failed. Please check the logs and try again.")
        return False


if __name__ == "__main__":
    main()
