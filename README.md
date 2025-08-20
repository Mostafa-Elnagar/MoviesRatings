# Movie Ratings Data Lakehouse

A complete data lakehouse solution for ingesting, storing, and analyzing movie ratings data using Apache Iceberg, Polaris, Trino, and MinIO.

## Quick Start

### 1. Setup Environment
```bash
# Run the complete setup (infrastructure + tables)
python scripts/setup.py
```

### 2. Check Status
```bash
# Verify everything is working
python scripts/status.py
```

## What This Does

The setup script automatically:
- Starts Docker services (MinIO, Polaris, Trino)
- Configures Polaris catalog and permissions
- Creates Iceberg tables with proper partitioning
- Verifies the complete setup

## Architecture

- **MinIO**: S3-compatible object storage
- **Polaris**: Iceberg catalog server
- **Trino**: SQL query engine
- **Apache Iceberg**: Table format for data lakes

## Tables Created

- `raw_movies`: Raw movie data from TMDB
- `enriched_movies`: Enriched movie data with ratings
- `movie_ratings`: Movie ratings from various sources

## Usage

### Check Status
```bash
python scripts/status.py
```

### Complete Setup
```bash
python scripts/setup.py
```

### Query Data
```bash
# Connect to Trino
docker exec -it trino trino --server localhost:8080

# Show tables
SHOW TABLES FROM iceberg.movies;

# Query data
SELECT * FROM iceberg.movies.raw_movies LIMIT 5;
```

## Configuration

- **Docker Compose**: `infrastructure/docker-compose.yml`
- **Trino Config**: `infrastructure/trino/`
- **Pipeline Config**: `config/pipeline_config.yaml`

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

## Next Steps

After setup, you can:
1. Ingest movie data from APIs
2. Run analytics queries
3. Build data pipelines
4. Create dashboards and reports
