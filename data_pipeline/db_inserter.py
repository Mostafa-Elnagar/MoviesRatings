#!/usr/bin/env python3
"""
Database inserter utility for bulk inserting enhanced movie data into Iceberg tables
"""

import json
import logging
import subprocess
import tempfile
import os
from pathlib import Path
from typing import List, Dict, Any, Optional
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DatabaseInserter:
    """Handles bulk insertion of enhanced movie data into Iceberg tables using Docker exec"""
    
    def __init__(self, host: str = "localhost", port: int = 8080, 
                 catalog: str = "iceberg", schema: str = "movies_stage"):
        self.host = host
        self.port = port
        self.catalog = catalog
        self.schema = schema
        self.container_name = "trino"
        
    def connect(self) -> bool:
        """Test connection to Trino via Docker"""
        try:
            # Test connection by running a simple query
            result = self._execute_trino_command("SELECT 1", "Testing connection")
            return result
        except Exception as e:
            logger.error(f"❌ Failed to connect to Trino: {e}")
            return False
    
    def disconnect(self):
        """No connection to close with Docker exec approach"""
        logger.info("🔌 Docker exec connection closed")
    
    def transform_movie_data(self, movie: Dict[str, Any]) -> Dict[str, Any]:
        """Transform movie data to match the omdb_movies table schema"""
        # Extract year from release_date if available
        year = None
        if movie.get('release_date'):
            try:
                year = int(movie['release_date'][:4])
            except (ValueError, TypeError):
                pass
        
        # Transform the data to match table schema
        transformed = {
            'imdb_id': movie.get('imdb_id'),
            'title': movie.get('title'),
            'year': year,
            'omdb_title': movie.get('omdb_title'),
            'rated': movie.get('omdb_rated'),
            'released': movie.get('omdb_released'),
            'runtime': movie.get('omdb_runtime'),
            'genre': movie.get('omdb_genre'),
            'director': movie.get('omdb_director'),
            'writer': movie.get('omdb_writer'),
            'actors': movie.get('omdb_actors'),
            'plot': movie.get('omdb_plot'),
            'language': movie.get('omdb_language'),
            'country': movie.get('omdb_country'),
            'awards': movie.get('omdb_awards'),
            'poster': movie.get('omdb_poster'),
            'ratings': json.dumps(movie.get('omdb_ratings', [])) if movie.get('omdb_ratings') else None,
            'imdb_rating': movie.get('omdb_imdb_rating'),
            'imdb_votes': movie.get('omdb_imdb_votes'),
            'metascore': movie.get('omdb_metascore'),
            'box_office': movie.get('omdb_box_office'),
            'production': movie.get('omdb_production'),
            'website': movie.get('omdb_website'),
            'data_source': movie.get('data_source', 'tmdb'),
            'created_at': movie.get('omdb_enhanced_at') or datetime.now().isoformat(),
            'updated_at': datetime.now().isoformat()
        }
        
        return transformed
    
    def create_insert_sql(self, movies: List[Dict[str, Any]]) -> str:
        """Create INSERT SQL statement for bulk insertion"""
        if not movies:
            return ""
        
        # Get column names from the first movie
        columns = list(movies[0].keys())
        columns_str = ', '.join(columns)
        
        # Create VALUES clause
        values_list = []
        for movie in movies:
            values = []
            for col in columns:
                value = movie.get(col)
                if value is None:
                    values.append('NULL')
                elif isinstance(value, (int, float)):
                    values.append(str(value))
                elif isinstance(value, bool):
                    values.append('TRUE' if value else 'FALSE')
                else:
                    # Escape single quotes and wrap in quotes
                    escaped_value = str(value).replace("'", "''")
                    # Truncate very long values to avoid SQL issues
                    if len(escaped_value) > 1000:
                        escaped_value = escaped_value[:1000] + "..."
                    values.append(f"'{escaped_value}'")
            
            values_list.append(f"({', '.join(values)})")
        
        values_str = ',\n'.join(values_list)
        
        sql = f"""INSERT INTO {self.catalog}.{self.schema}.omdb_movies 
({columns_str})
VALUES
{values_str};"""
        
        return sql
    
    def bulk_insert(self, movies: List[Dict[str, Any]], batch_size: int = 50) -> bool:
        """Bulk insert movies into the omdb_movies table"""
        try:
            # Transform all movies
            transformed_movies = [self.transform_movie_data(movie) for movie in movies]
            logger.info(f"🔄 Transformed {len(transformed_movies)} movies for database insertion")
            
            # Process in batches
            total_inserted = 0
            for i in range(0, len(transformed_movies), batch_size):
                batch = transformed_movies[i:i + batch_size]
                batch_num = (i // batch_size) + 1
                total_batches = (len(transformed_movies) + batch_size - 1) // batch_size
                
                logger.info(f"📦 Processing batch {batch_num}/{total_batches} ({len(batch)} movies)")
                
                # Create and execute INSERT SQL
                insert_sql = self.create_insert_sql(batch)
                if not insert_sql:
                    logger.warning("⚠️ No SQL generated for batch, skipping")
                    continue
                
                try:
                    # Execute via Docker exec using file input
                    success = self._execute_trino_sql_file(insert_sql, f"Inserting batch {batch_num}")
                    if success:
                        total_inserted += len(batch)
                        logger.info(f"✅ Batch {batch_num} inserted successfully ({len(batch)} movies)")
                    else:
                        logger.error(f"❌ Failed to insert batch {batch_num}")
                        return False
                    
                except Exception as e:
                    logger.error(f"❌ Unexpected error in batch {batch_num}: {e}")
                    return False
            
            logger.info(f"🎉 Successfully inserted {total_inserted} movies into database")
            return True
            
        except Exception as e:
            logger.error(f"❌ Bulk insert failed: {e}")
            return False
    
    def _execute_trino_command(self, command: str, description: str) -> bool:
        """Execute a Trino command via Docker exec"""
        try:
            logger.info(f"🔄 {description}")
            
            # Prepare the Docker exec command
            docker_cmd = [
                'docker', 'exec', self.container_name, 'trino',
                '--server', f'{self.host}:{self.port}',
                '--catalog', self.catalog,
                '--schema', self.schema,
                '--execute', command
            ]
            
            # Execute the command
            result = subprocess.run(
                docker_cmd,
                capture_output=True,
                text=True,
                timeout=60
            )
            
            if result.returncode == 0:
                logger.info(f"✅ {description} completed successfully")
                return True
            else:
                logger.error(f"❌ {description} failed with return code {result.returncode}")
                if result.stderr:
                    logger.error(f"Error output: {result.stderr}")
                return False
                
        except subprocess.TimeoutExpired:
            logger.error(f"❌ {description} timed out")
            return False
        except Exception as e:
            logger.error(f"❌ {description} failed: {e}")
            return False
    
    def _execute_trino_sql_file(self, sql: str, description: str) -> bool:
        """Execute a Trino SQL command via Docker exec using a file input"""
        try:
            logger.info(f"🔄 {description}")
            
            # Create a temporary SQL file
            with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False, encoding='utf-8') as f:
                f.write(sql)
                temp_file = f.name
            
            try:
                # Copy the SQL file to the Docker container
                copy_cmd = [
                    'docker', 'cp', temp_file, f'{self.container_name}:/tmp/insert.sql'
                ]
                
                result = subprocess.run(copy_cmd, capture_output=True, text=True, timeout=30)
                if result.returncode != 0:
                    logger.error(f"❌ Failed to copy SQL file to container: {result.stderr}")
                    return False
                
                # Execute the SQL file in the container
                docker_cmd = [
                    'docker', 'exec', self.container_name, 'trino',
                    '--server', f'{self.host}:{self.port}',
                    '--catalog', self.catalog,
                    '--schema', self.schema,
                    '--file', '/tmp/insert.sql'
                ]
                
                result = subprocess.run(
                    docker_cmd,
                    capture_output=True,
                    text=True,
                    timeout=120
                )
                
                if result.returncode == 0:
                    logger.info(f"✅ {description} completed successfully")
                    return True
                else:
                    logger.error(f"❌ {description} failed with return code {result.returncode}")
                    if result.stderr:
                        logger.error(f"Error output: {result.stderr}")
                    return False
                    
            finally:
                # Clean up temporary file
                try:
                    os.unlink(temp_file)
                except:
                    pass
                
        except subprocess.TimeoutExpired:
            logger.error(f"❌ {description} timed out")
            return False
        except Exception as e:
            logger.error(f"❌ {description} failed: {e}")
            return False
    
    def test_connection(self) -> bool:
        """Test database connection and table access"""
        try:
            result = self._execute_trino_command("SELECT 1", "Testing connection")
            return result
        except Exception as e:
            logger.error(f"❌ Database connection test failed: {e}")
            return False
    
    def get_table_info(self) -> Optional[Dict[str, Any]]:
        """Get information about the omdb_movies table"""
        try:
            # Execute DESCRIBE command
            result = subprocess.run([
                'docker', 'exec', self.container_name, 'trino',
                '--server', f'{self.host}:{self.port}',
                '--catalog', self.catalog,
                '--schema', self.schema,
                '--execute', f'DESCRIBE {self.catalog}.{self.schema}.omdb_movies'
            ], capture_output=True, text=True, timeout=30)
            
            if result.returncode == 0:
                # Parse the output to get column count
                lines = result.stdout.strip().split('\n')
                column_count = len([line for line in lines if line.strip() and not line.startswith('Column')])
                
                table_info = {
                    'table_name': f"{self.catalog}.{self.schema}.omdb_movies",
                    'columns': [line.split()[0] for line in lines if line.strip() and not line.startswith('Column')],
                    'column_count': column_count
                }
                
                logger.info(f"📋 Table info: {table_info['column_count']} columns")
                return table_info
            else:
                logger.error(f"❌ Failed to get table info: {result.stderr}")
                return None
                
        except Exception as e:
            logger.error(f"❌ Failed to get table info: {e}")
            return None
