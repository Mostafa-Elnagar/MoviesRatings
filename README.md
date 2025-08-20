# Movie Ratings Data Lakehouse

A complete data lakehouse solution for ingesting, storing, and analyzing movie ratings data using Apache Iceberg, Polaris, Trino, and MinIO.

## Quick Start

### 1. Setup Environment
```bash
# Run the complete pipeline (infrastructure + data ingestion + schema creation + data loading)
python scripts/run_complete_pipeline.py
```

### 2. Check Status
```bash
# Verify everything is working
python scripts/status.py
```

## What This Does

The complete pipeline automatically:
- Starts Docker services (MinIO, Polaris, Trino)
- Ingests movie data from TMDB API (by IMDb ID)
- Scrapes ratings from Metacritic and Rotten Tomatoes
- Creates `movies_stage` schema with optimized tables
- Loads all data into the data lakehouse

## Architecture

- **MinIO**: S3-compatible object storage
- **Polaris**: Iceberg catalog server
- **Trino**: SQL query engine
- **Apache Iceberg**: Table format for data lakes
- **Data Pipeline**: Modular ingestion and scraping system

## Data Sources

- **TMDB API**: Movie metadata and details
- **OMDb API**: Additional movie information and ratings
- **Metacritic**: Critic and user scores
- **Rotten Tomatoes**: Critic and audience ratings

## Tables Created

The `movies_stage` schema contains:
- `tmdb_movies`: Raw movie data from TMDB
- `omdb_movies`: Enriched movie data from OMDb
- `metacritic_ratings`: Ratings from Metacritic
- `rotten_tomatoes_ratings`: Ratings from Rotten Tomatoes

## Usage

### Run Complete Pipeline
```bash
python scripts/run_complete_pipeline.py
```

### Individual Steps
```bash
# Setup infrastructure only
python scripts/setup.py

# Create schema and tables
python scripts/create_stage_schema.py

# Load staged data
python data_pipeline/stage_data_loader.py
```

### Check Status
```bash
python scripts/status.py
```

### Query Data
```bash
# Connect to Trino
docker exec -it trino trino --server localhost:8080 --catalog iceberg --schema movies_stage

# Show tables
SHOW TABLES FROM iceberg.movies_stage;

# Query data
SELECT * FROM iceberg.movies_stage.tmdb_movies LIMIT 5;
```

## Configuration

All configuration is centralized in `config/settings.py`:
- Service URLs and ports
- Container names
- Database settings
- Timeouts and credentials
- Table names and formats

Other configuration files:
- **Docker Compose**: `infrastructure/docker-compose.yml`
- **Trino Config**: `infrastructure/trino/`
- **Environment**: `.env` file for API keys

## Data Pipeline

The data pipeline consists of:
- `comprehensive_ingestor.py`: Orchestrates data ingestion from all sources
- `stage_data_loader.py`: Loads staged data into Iceberg tables
- `scrappers/`: Web scraping modules for Metacritic and Rotten Tomatoes

## Troubleshooting

If you encounter issues:

1. **Check status**: `python scripts/status.py`
2. **Restart services**: `docker-compose -f infrastructure/docker-compose.yml restart`
3. **View logs**: `docker-compose -f infrastructure/docker-compose.yml logs -f [service]`
4. **Re-run setup**: `python scripts/setup.py`

## Requirements

- Docker and Docker Compose
- Python 3.8+
- 4GB+ RAM available for Docker
- TMDB and OMDb API keys in `.env` file

## Next Steps

After setup, you can:
1. Run analytical queries on the loaded data
2. Build additional data models and views
3. Create data pipelines for new data sources
4. Build dashboards and reports
