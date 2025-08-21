#!/usr/bin/env python3
"""
Multi-Table Database Inserter for Movie Ratings Pipeline
Handles insertion into multiple Iceberg tables based on data source
"""

import logging
import subprocess
import tempfile
import os
from pathlib import Path
from typing import List, Dict, Any, Optional
from datetime import datetime

logger = logging.getLogger(__name__)

class MultiTableInserter:
    """Handles insertion of movie data into multiple Iceberg tables"""
    
    def __init__(self, host: str = "localhost", port: int = 8080, 
                 catalog: str = "iceberg", schema: str = "movies_stage"):
        self.host = host
        self.port = port
        self.catalog = catalog
        self.schema = schema
        self.container_name = "trino"
        
        # Table schemas for data transformation
        self.table_schemas = {
            'tmdb_movies': [
                'tmdb_id', 'imdb_id', 'title', 'original_title', 'release_date', 'year',
                'overview', 'tagline', 'status', 'runtime', 'budget', 'revenue',
                'popularity', 'vote_average', 'vote_count', 'genres', 'genre_ids',
                'original_language', 'production_companies', 'production_countries',
                'spoken_languages', 'cast_data', 'crew_data', 'backdrop_path',
                'poster_path', 'homepage', 'external_ids', 'data_source', 'created_at', 'updated_at'
            ],
            'omdb_movies': [
                'imdb_id', 'title', 'year', 'omdb_title', 'rated', 'released', 'runtime',
                'genre', 'director', 'writer', 'actors', 'plot', 'language', 'country',
                'awards', 'poster', 'ratings', 'imdb_rating', 'imdb_votes', 'metascore',
                'box_office', 'production', 'website', 'data_source', 'created_at', 'updated_at'
            ],
            'metacritic_ratings': [
                'tmdb_id', 'imdb_id', 'title', 'year', 'critic_score', 'critic_count',
                'user_score', 'user_count', 'data_source', 'created_at'
            ],
            'rotten_tomatoes_ratings': [
                'tmdb_id', 'imdb_id', 'title', 'year', 'critic_score', 'critic_count',
                'user_score', 'user_count', 'data_source', 'created_at'
            ]
        }
    
    def test_connection(self) -> bool:
        """Test database connection"""
        try:
            result = self._execute_trino_command("SELECT 1", "Testing connection")
            return result
        except Exception as e:
            logger.error(f"Database connection test failed: {e}")
            return False
    
    def transform_for_tmdb_table(self, movie: Dict[str, Any]) -> Dict[str, Any]:
        """Transform movie data for tmdb_movies table"""
        transformed = {}
        
        field_mapping = {
            'tmdb_id': movie.get('tmdb_id'),
            'imdb_id': movie.get('imdb_id'),
            'title': movie.get('title'),
            'original_title': movie.get('original_title'),
            'release_date': movie.get('release_date'),
            'year': movie.get('year'),
            'overview': self._truncate_text(movie.get('overview'), 1000),
            'tagline': movie.get('tagline'),
            'status': movie.get('status'),
            'runtime': movie.get('runtime'),
            'budget': movie.get('budget'),
            'revenue': movie.get('revenue'),
            'popularity': movie.get('popularity'),
            'vote_average': movie.get('vote_average'),
            'vote_count': movie.get('vote_count'),
            'genres': movie.get('genres'),
            'genre_ids': movie.get('genre_ids'),
            'original_language': movie.get('original_language'),
            'production_companies': movie.get('production_companies'),
            'production_countries': movie.get('production_countries'),
            'spoken_languages': movie.get('spoken_languages'),
            'cast_data': movie.get('cast_data'),
            'crew_data': movie.get('crew_data'),
            'backdrop_path': movie.get('backdrop_path'),
            'poster_path': movie.get('poster_path'),
            'homepage': movie.get('homepage'),
            'external_ids': movie.get('external_ids'),
            'data_source': 'tmdb',
            'created_at': datetime.now().isoformat(),
            'updated_at': datetime.now().isoformat()
        }
        
        for field in self.table_schemas['tmdb_movies']:
            transformed[field] = field_mapping.get(field)
        
        return transformed
    
    def transform_for_omdb_table(self, movie: Dict[str, Any]) -> Dict[str, Any]:
        """Transform movie data for omdb_movies table"""
        transformed = {}
        
        field_mapping = {
            'imdb_id': movie.get('imdb_id'),
            'title': movie.get('title'),
            'year': movie.get('year'),
            'omdb_title': movie.get('omdb_title'),
            'rated': movie.get('omdb_rated'),
            'released': movie.get('omdb_released'),
            'runtime': movie.get('omdb_runtime'),
            'genre': movie.get('omdb_genre'),
            'director': movie.get('omdb_director'),
            'writer': movie.get('omdb_writer'),
            'actors': movie.get('omdb_actors'),
            'plot': self._truncate_text(movie.get('omdb_plot'), 1000),
            'language': movie.get('omdb_language'),
            'country': movie.get('omdb_country'),
            'awards': movie.get('omdb_awards'),
            'poster': movie.get('omdb_poster'),
            'ratings': str(movie.get('omdb_ratings', [])),
            'imdb_rating': movie.get('omdb_imdb_rating'),
            'imdb_votes': movie.get('omdb_imdb_votes'),
            'metascore': movie.get('omdb_metascore'),
            'box_office': movie.get('omdb_box_office'),
            'production': movie.get('omdb_production'),
            'website': movie.get('omdb_website'),
            'data_source': 'omdb',
            'created_at': datetime.now().isoformat(),
            'updated_at': datetime.now().isoformat()
        }
        
        for field in self.table_schemas['omdb_movies']:
            transformed[field] = field_mapping.get(field)
        
        return transformed
    
    def transform_for_metacritic_table(self, movie: Dict[str, Any]) -> Dict[str, Any]:
        """Transform movie data for metacritic_ratings table"""
        transformed = {}
        
        field_mapping = {
            'tmdb_id': movie.get('tmdb_id'),
            'imdb_id': movie.get('imdb_id'),
            'title': movie.get('title'),
            'year': movie.get('year'),
            'critic_score': movie.get('metacritic_critic_score'),
            'critic_count': movie.get('metacritic_critic_count'),
            'user_score': movie.get('metacritic_user_score'),
            'user_count': movie.get('metacritic_user_count'),
            'data_source': 'metacritic',
            'created_at': datetime.now().isoformat()
        }
        
        for field in self.table_schemas['metacritic_ratings']:
            transformed[field] = field_mapping.get(field)
        
        return transformed
    
    def transform_for_rotten_tomatoes_table(self, movie: Dict[str, Any]) -> Dict[str, Any]:
        """Transform movie data for rotten_tomatoes_ratings table"""
        transformed = {}
        
        field_mapping = {
            'tmdb_id': movie.get('tmdb_id'),
            'imdb_id': movie.get('imdb_id'),
            'title': movie.get('title'),
            'year': movie.get('year'),
            'critic_score': movie.get('rt_critic_score'),
            'critic_count': movie.get('rt_critic_count'),
            'user_score': movie.get('rt_user_score'),
            'user_count': movie.get('rt_user_count'),
            'data_source': 'rottentomatoes',
            'created_at': datetime.now().isoformat()
        }
        
        for field in self.table_schemas['rotten_tomatoes_ratings']:
            transformed[field] = field_mapping.get(field)
        
        return transformed
    
    def _truncate_text(self, text: Any, max_length: int) -> Optional[str]:
        """Truncate text to specified length"""
        if not text:
            return None
        text_str = str(text)
        if len(text_str) <= max_length:
            return text_str
        return text_str[:max_length-3] + "..."
    
    def create_insert_sql(self, table_name: str, data: List[Dict[str, Any]]) -> str:
        """Create INSERT SQL for a specific table"""
        if not data:
            return ""
        
        schema = self.table_schemas.get(table_name)
        if not schema:
            raise ValueError(f"Unknown table: {table_name}")
        
        columns = ', '.join(schema)
        values_list = []
        
        for row in data:
            row_values = []
            for field in schema:
                value = row.get(field)
                if value is None:
                    row_values.append('NULL')
                elif isinstance(value, (int, float)):
                    row_values.append(str(value))
                elif isinstance(value, bool):
                    row_values.append('TRUE' if value else 'FALSE')
                elif isinstance(value, list):
                    # Handle arrays
                    if value:
                        array_values = []
                        for item in value:
                            if isinstance(item, str):
                                escaped_item = str(item).replace("'", "''")
                                array_values.append(f"'{escaped_item}'")
                            else:
                                array_values.append(str(item))
                        row_values.append(f"ARRAY[{', '.join(array_values)}]")
                    else:
                        row_values.append('ARRAY[]')
                else:
                    # Escape single quotes and wrap in quotes
                    escaped_value = str(value).replace("'", "''")
                    row_values.append(f"'{escaped_value}'")
            
            values_list.append(f"({', '.join(row_values)})")
        
        sql = f"INSERT INTO {self.catalog}.{self.schema}.{table_name} ({columns}) VALUES {', '.join(values_list)};"
        return sql
    
    def insert_tmdb_data(self, movies: List[Dict[str, Any]]) -> bool:
        """Insert TMDB data into tmdb_movies table"""
        try:
            transformed_movies = [self.transform_for_tmdb_table(movie) for movie in movies]
            insert_sql = self.create_insert_sql('tmdb_movies', transformed_movies)
            
            if not insert_sql:
                return False
            
            success = self._execute_trino_sql_file(insert_sql, f"Inserting {len(movies)} movies into tmdb_movies")
            if success:
                logger.info(f"Successfully inserted {len(movies)} movies into tmdb_movies table")
            
            return success
            
        except Exception as e:
            logger.error(f"Error inserting TMDB data: {e}")
            return False
    
    def insert_omdb_data(self, movies: List[Dict[str, Any]]) -> bool:
        """Insert OMDb data into omdb_movies table"""
        try:
            transformed_movies = [self.transform_for_omdb_table(movie) for movie in movies]
            insert_sql = self.create_insert_sql('omdb_movies', transformed_movies)
            
            if not insert_sql:
                return False
            
            success = self._execute_trino_sql_file(insert_sql, f"Inserting {len(movies)} movies into omdb_movies")
            if success:
                logger.info(f"Successfully inserted {len(movies)} movies into omdb_movies table")
            
            return success
            
        except Exception as e:
            logger.error(f"Error inserting OMDb data: {e}")
            return False
    
    def insert_metacritic_data(self, movies: List[Dict[str, Any]]) -> bool:
        """Insert Metacritic data into metacritic_ratings table"""
        try:
            metacritic_movies = [movie for movie in movies if movie.get('metacritic_critic_score') is not None]
            
            if not metacritic_movies:
                logger.info("No Metacritic data to insert")
                return True
            
            transformed_movies = [self.transform_for_metacritic_table(movie) for movie in metacritic_movies]
            insert_sql = self.create_insert_sql('metacritic_ratings', transformed_movies)
            
            if not insert_sql:
                return False
            
            success = self._execute_trino_sql_file(insert_sql, f"Inserting {len(transformed_movies)} ratings into metacritic_ratings")
            if success:
                logger.info(f"Successfully inserted {len(transformed_movies)} ratings into metacritic_ratings table")
            
            return success
            
        except Exception as e:
            logger.error(f"Error inserting Metacritic data: {e}")
            return False
    
    def insert_rotten_tomatoes_data(self, movies: List[Dict[str, Any]]) -> bool:
        """Insert Rotten Tomatoes data into rotten_tomatoes_ratings table"""
        try:
            rt_movies = [movie for movie in movies if movie.get('rt_critic_score') is not None]
            
            if not rt_movies:
                logger.info("No Rotten Tomatoes data to insert")
                return True
            
            transformed_movies = [self.transform_for_rotten_tomatoes_table(movie) for movie in rt_movies]
            insert_sql = self.create_insert_sql('rotten_tomatoes_ratings', transformed_movies)
            
            if not insert_sql:
                return False
            
            success = self._execute_trino_sql_file(insert_sql, f"Inserting {len(transformed_movies)} ratings into rotten_tomatoes_ratings")
            if success:
                logger.info(f"Successfully inserted {len(transformed_movies)} ratings into rotten_tomatoes_ratings table")
            
            return success
            
        except Exception as e:
            logger.error(f"Error inserting Rotten Tomatoes data: {e}")
            return False
    
    def insert_all_data(self, movies: List[Dict[str, Any]]) -> Dict[str, bool]:
        """Insert data into all appropriate tables"""
        results = {}
        
        try:
            results['tmdb_movies'] = self.insert_tmdb_data(movies)
            results['omdb_movies'] = self.insert_omdb_data(movies)
            results['metacritic_ratings'] = self.insert_metacritic_data(movies)
            results['rotten_tomatoes_ratings'] = self.insert_rotten_tomatoes_data(movies)
            
            return results
            
        except Exception as e:
            logger.error(f"Error in multi-table insertion: {e}")
            return {table: False for table in ['tmdb_movies', 'omdb_movies', 'metacritic_ratings', 'rotten_tomatoes_ratings']}
    
    def _execute_trino_command(self, command: str, description: str) -> bool:
        """Execute a Trino command via Docker exec"""
        try:
            logger.info(f"Executing: {description}")
            
            docker_cmd = [
                'docker', 'exec', self.container_name, 'trino',
                '--server', f'{self.host}:{self.port}',
                '--catalog', self.catalog,
                '--schema', self.schema,
                '--execute', command
            ]
            
            result = subprocess.run(docker_cmd, capture_output=True, text=True, timeout=60)
            
            if result.returncode == 0:
                logger.info(f"Command completed successfully: {description}")
                return True
            else:
                logger.error(f"Command failed with return code {result.returncode}: {description}")
                if result.stderr:
                    logger.error(f"Error output: {result.stderr}")
                return False
                
        except subprocess.TimeoutExpired:
            logger.error(f"Command timed out: {description}")
            return False
        except Exception as e:
            logger.error(f"Command failed: {description} - {e}")
            return False
    
    def _execute_trino_sql_file(self, sql: str, description: str) -> bool:
        """Execute SQL via temporary file to avoid command length limits"""
        try:
            with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False, encoding='utf-8') as f:
                f.write(sql)
                temp_file_path = f.name
            
            try:
                # Copy file to Docker container
                copy_cmd = ['docker', 'cp', temp_file_path, f'{self.container_name}:/tmp/temp_insert.sql']
                result = subprocess.run(copy_cmd, capture_output=True, text=True, timeout=30)
                
                if result.returncode != 0:
                    logger.error(f"Failed to copy SQL file to container: {result.stderr}")
                    return False
                
                # Execute SQL file in container
                exec_cmd = [
                    'docker', 'exec', self.container_name, 'trino',
                    '--server', f'{self.host}:{self.port}',
                    '--catalog', self.catalog,
                    '--schema', self.schema,
                    '--file', '/tmp/temp_insert.sql'
                ]
                
                result = subprocess.run(exec_cmd, capture_output=True, text=True, timeout=60)
                
                if result.returncode == 0:
                    logger.info(f"SQL execution completed successfully: {description}")
                    return True
                else:
                    logger.error(f"SQL execution failed with return code {result.returncode}: {description}")
                    if result.stderr:
                        logger.error(f"Error output: {result.stderr}")
                    return False
                    
            finally:
                os.unlink(temp_file_path)
                
        except Exception as e:
            logger.error(f"SQL execution failed: {description} - {e}")
            return False
    
    def disconnect(self):
        """No connection to close with Docker exec approach"""
        logger.info("Docker exec connection closed")
