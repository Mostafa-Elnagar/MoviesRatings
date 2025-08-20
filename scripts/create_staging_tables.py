#!/usr/bin/env python3
"""
Create Staging Tables with JSON Data and Normalized Tables using UNNEST
This script creates the proper staging approach for the movie data
"""

import subprocess
import json
from pathlib import Path

class StagingTableCreator:
    """Creates staging tables and normalized tables using UNNEST"""
    
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
    
    def create_tmdb_staging_table(self) -> bool:
        """Create TMDB staging table with JSON data"""
        print("Creating TMDB staging table...")
        
        # Create table with JSON column
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS {self.catalog}.{self.schema}.tmdb_raw (
            data VARCHAR
        ) WITH (
            format = 'PARQUET'
        )
        """
        
        if not self._execute_trino_command(create_sql, "Creating TMDB raw table"):
            return False
        
        # Load JSON data
        json_file = Path("data/raw/tmdb_movies_20250820_204113.json")
        if not json_file.exists():
            print(f"JSON file not found: {json_file}")
            return False
        
        with open(json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Insert each record as JSON
        for record in data:
            json_str = json.dumps(record, ensure_ascii=False)
            # Escape single quotes for SQL but don't double-escape
            escaped_json = json_str.replace("'", "''")
            insert_sql = f"INSERT INTO {self.catalog}.{self.schema}.tmdb_raw (data) VALUES ('{escaped_json}');"
            
            if not self._execute_trino_command(insert_sql, f"Inserting TMDB record {record.get('title', 'Unknown')}"):
                return False
        
        print(f"Loaded {len(data)} TMDB records")
        return True
    
    def create_normalized_tables(self) -> bool:
        """Create normalized tables using CREATE TABLE AS SELECT with UNNEST"""
        print("Creating normalized tables...")
        
        # Create movies table
        movies_sql = f"""
        CREATE TABLE {self.catalog}.{self.schema}.movies AS
        SELECT 
            CAST(JSON_EXTRACT_SCALAR(CAST(data AS JSON), '$.tmdb_id') AS INTEGER) as tmdb_id,
            JSON_EXTRACT_SCALAR(CAST(data AS JSON), '$.imdb_id') as imdb_id,
            JSON_EXTRACT_SCALAR(CAST(data AS JSON), '$.title') as title,
            JSON_EXTRACT_SCALAR(CAST(data AS JSON), '$.original_title') as original_title,
            JSON_EXTRACT_SCALAR(CAST(data AS JSON), '$.release_date') as release_date,
            CAST(JSON_EXTRACT_SCALAR(CAST(data AS JSON), '$.year') AS INTEGER) as year,
            JSON_EXTRACT_SCALAR(CAST(data AS JSON), '$.overview') as overview,
            JSON_EXTRACT_SCALAR(CAST(data AS JSON), '$.tagline') as tagline,
            JSON_EXTRACT_SCALAR(CAST(data AS JSON), '$.status') as status,
            CAST(JSON_EXTRACT_SCALAR(CAST(data AS JSON), '$.runtime') AS INTEGER) as runtime,
            CAST(JSON_EXTRACT_SCALAR(CAST(data AS JSON), '$.budget') AS INTEGER) as budget,
            CAST(JSON_EXTRACT_SCALAR(CAST(data AS JSON), '$.revenue') AS INTEGER) as revenue,
            CAST(JSON_EXTRACT_SCALAR(CAST(data AS JSON), '$.popularity') AS DECIMAL(10,4)) as popularity,
            CAST(JSON_EXTRACT_SCALAR(CAST(data AS JSON), '$.vote_average') AS DECIMAL(10,3)) as vote_average,
            CAST(JSON_EXTRACT_SCALAR(CAST(data AS JSON), '$.vote_count') AS INTEGER) as vote_count,
            JSON_EXTRACT_SCALAR(CAST(data AS JSON), '$.original_language') as original_language,
            JSON_EXTRACT_SCALAR(CAST(data AS JSON), '$.backdrop_path') as backdrop_path,
            JSON_EXTRACT_SCALAR(CAST(data AS JSON), '$.poster_path') as poster_path,
            JSON_EXTRACT_SCALAR(CAST(data AS JSON), '$.homepage') as homepage
        FROM {self.catalog}.{self.schema}.tmdb_raw
        """
        
        if not self._execute_trino_command(movies_sql, "Creating movies table"):
            return False
        
        # Create genres table using UNNEST
        genres_sql = f"""
        CREATE TABLE {self.catalog}.{self.schema}.movie_genres AS
        SELECT 
            CAST(JSON_EXTRACT_SCALAR(CAST(data AS JSON), '$.tmdb_id') AS INTEGER) as tmdb_id,
            g as genre
        FROM {self.catalog}.{self.schema}.tmdb_raw
        CROSS JOIN UNNEST(CAST(JSON_EXTRACT(CAST(data AS JSON), '$.genres') AS ARRAY(VARCHAR))) AS t(g)
        """
        
        if not self._execute_trino_command(genres_sql, "Creating movie_genres table"):
            return False
        
        # Create cast table using UNNEST
        cast_sql = f"""
        CREATE TABLE {self.catalog}.{self.schema}.movie_cast AS
        SELECT 
            CAST(JSON_EXTRACT_SCALAR(CAST(data AS JSON), '$.tmdb_id') AS INTEGER) as tmdb_id,
            CAST(JSON_EXTRACT_SCALAR(c, '$.id') AS INTEGER) as cast_id,
            JSON_EXTRACT_SCALAR(c, '$.name') as cast_name,
            JSON_EXTRACT_SCALAR(c, '$.character') as character
        FROM {self.catalog}.{self.schema}.tmdb_raw
        CROSS JOIN UNNEST(CAST(JSON_EXTRACT(CAST(data AS JSON), '$.cast') AS ARRAY(JSON))) AS t(c)
        """
        
        if not self._execute_trino_command(cast_sql, "Creating movie_cast table"):
            return False
        
        # Create crew table using UNNEST
        crew_sql = f"""
        CREATE TABLE {self.catalog}.{self.schema}.movie_crew AS
        SELECT 
            CAST(JSON_EXTRACT_SCALAR(CAST(data AS JSON), '$.tmdb_id') AS INTEGER) as tmdb_id,
            CAST(JSON_EXTRACT_SCALAR(c, '$.id') AS INTEGER) as crew_id,
            JSON_EXTRACT_SCALAR(c, '$.name') as crew_name,
            JSON_EXTRACT_SCALAR(c, '$.job') as job
        FROM {self.catalog}.{self.schema}.tmdb_raw
        CROSS JOIN UNNEST(CAST(JSON_EXTRACT(CAST(data AS JSON), '$.crew') AS ARRAY(JSON))) AS t(c)
        """
        
        if not self._execute_trino_command(crew_sql, "Creating movie_crew table"):
            return False
        
        print("All normalized tables created successfully!")
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
        """Create all staging and normalized tables"""
        print("Creating staging tables and normalized tables...")
        print("=" * 50)
        
        # Create staging schema
        if not self.create_staging_schema():
            return False
        
        # Create TMDB staging table
        if not self.create_tmdb_staging_table():
            return False
        
        # Create normalized tables
        if not self.create_normalized_tables():
            return False
        
        print("\n" + "=" * 50)
        print("All tables created successfully!")
        print("=" * 50)
        
        return True


def main():
    """Main function"""
    creator = StagingTableCreator()
    
    if creator.create_all_tables():
        print("\nStaging tables creation completed successfully!")
        print("The staging schema is ready with normalized tables.")
        return True
    else:
        print("\nStaging tables creation failed. Please check the logs and try again.")
        return False


if __name__ == "__main__":
    main()
