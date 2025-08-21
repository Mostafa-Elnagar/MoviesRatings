#!/usr/bin/env python3
"""
Configuration Module for Movie Ratings Data Pipeline
Centralizes all configuration settings and constants
"""

import os
from pathlib import Path
from typing import Dict, Any
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class PipelineConfig:
    """Configuration class for the movie ratings data pipeline"""
    
    # API Configuration
    OMDB_API_KEY = os.getenv('OMDB_API_KEY')
    OMDB_BASE_URL = "http://www.omdbapi.com/"
    
    # Database Configuration
    DATABASE_CONFIG = {
        'host': os.getenv('TRINO_HOST', 'localhost'),
        'port': int(os.getenv('TRINO_PORT', '8080')),
        'catalog': os.getenv('TRINO_CATALOG', 'iceberg'),
        'schema': os.getenv('TRINO_SCHEMA', 'movies_stage'),
        'container_name': 'trino'
    }
    
    # Directory Configuration
    DIRECTORIES = {
        'raw': Path("data/raw"),
        'processed': Path("data/processed"),
        'analysis': Path("data/analysis"),
        'logs': Path("logs")
    }
    
    # Rate Limiting
    RATE_LIMITS = {
        'omdb_api': 0.1,  # 100ms between API calls
        'scraper': 1.0,   # 1 second between scraping calls
        'database': 0.05  # 50ms between database operations
    }
    
    # Processing Configuration
    PROCESSING = {
        'default_batch_size': 50,
        'max_movies_per_file': None,  # None means process all
        'enable_metacritic': True,
        'enable_rotten_tomatoes': True,
        'headless_scraping': True
    }
    
    # Data Quality
    DATA_QUALITY = {
        'max_plot_length': 1000,
        'min_rating': 0.0,
        'max_rating': 10.0,
        'min_year': 1900,
        'max_year': 2030
    }
    
    # Output Configuration
    OUTPUT = {
        'file_format': 'json',
        'indent': 2,
        'ensure_ascii': False,
        'timestamp_format': '%Y%m%d_%H%M%S'
    }
    
    # Logging Configuration
    LOGGING = {
        'level': 'INFO',
        'format': '%(asctime)s %(levelname)s %(message)s',
        'file': 'logs/pipeline.log',
        'max_file_size': 10 * 1024 * 1024,  # 10MB
        'backup_count': 5
    }
    
    # Error Handling
    ERROR_HANDLING = {
        'max_retries': 3,
        'retry_delay': 1.0,
        'continue_on_error': True,
        'log_errors': True
    }
    
    @classmethod
    def validate_config(cls) -> bool:
        """Validate the configuration"""
        errors = []
        
        # Check required API key
        if not cls.OMDB_API_KEY:
            errors.append("OMDB_API_KEY is required")
        
        # Check directories exist or can be created
        for name, path in cls.DIRECTORIES.items():
            try:
                path.mkdir(parents=True, exist_ok=True)
            except Exception as e:
                errors.append(f"Cannot create directory {name}: {path} - {e}")
        
        # Check database connection
        try:
            host = cls.DATABASE_CONFIG['host']
            port = cls.DATABASE_CONFIG['port']
            if not host or not port:
                errors.append("Database host and port must be specified")
        except Exception as e:
            errors.append(f"Database configuration error: {e}")
        
        if errors:
            print("❌ Configuration validation failed:")
            for error in errors:
                print(f"   - {error}")
            return False
        
        print("✅ Configuration validation passed")
        return True
    
    @classmethod
    def get_database_inserter_config(cls) -> Dict[str, Any]:
        """Get configuration for database inserter"""
        return {
            'host': cls.DATABASE_CONFIG['host'],
            'port': cls.DATABASE_CONFIG['port'],
            'catalog': cls.DATABASE_CONFIG['catalog'],
            'schema': cls.DATABASE_CONFIG['schema']
        }
    
    @classmethod
    def get_scraper_config(cls) -> Dict[str, Any]:
        """Get configuration for scrapers"""
        return {
            'headless': cls.PROCESSING['headless_scraping'],
            'rate_limit': cls.RATE_LIMITS['scraper'],
            'max_retries': cls.ERROR_HANDLING['max_retries']
        }
    
    @classmethod
    def get_ingestor_config(cls) -> Dict[str, Any]:
        """Get configuration for ingestor"""
        return {
            'omdb_api_key': cls.OMDB_API_KEY,
            'omdb_base_url': cls.OMDB_BASE_URL,
            'rate_limit': cls.RATE_LIMITS['omdb_api'],
            'batch_size': cls.PROCESSING['default_batch_size'],
            'max_movies': cls.PROCESSING['max_movies_per_file']
        }
    
    @classmethod
    def print_config_summary(cls):
        """Print a summary of the current configuration"""
        print("🔧 PIPELINE CONFIGURATION SUMMARY")
        print("=" * 50)
        
        print(f"📡 OMDb API: {'✅ Configured' if cls.OMDB_API_KEY else '❌ Not configured'}")
        print(f"🗄️ Database: {cls.DATABASE_CONFIG['host']}:{cls.DATABASE_CONFIG['port']}")
        print(f"📁 Raw data: {cls.DIRECTORIES['raw']}")
        print(f"📁 Processed: {cls.DIRECTORIES['processed']}")
        print(f"📁 Analysis: {cls.DIRECTORIES['analysis']}")
        
        print(f"\n⚙️ Processing:")
        print(f"   - Batch size: {cls.PROCESSING['default_batch_size']}")
        print(f"   - Metacritic: {'✅' if cls.PROCESSING['enable_metacritic'] else '❌'}")
        print(f"   - Rotten Tomatoes: {'✅' if cls.PROCESSING['enable_rotten_tomatoes'] else '❌'}")
        print(f"   - Headless scraping: {'✅' if cls.PROCESSING['headless_scraping'] else '❌'}")
        
        print(f"\n⏱️ Rate limits:")
        print(f"   - OMDb API: {cls.RATE_LIMITS['omdb_api']}s")
        print(f"   - Scrapers: {cls.RATE_LIMITS['scraper']}s")
        print(f"   - Database: {cls.RATE_LIMITS['database']}s")
        
        print("=" * 50)

# Global configuration instance
config = PipelineConfig()
