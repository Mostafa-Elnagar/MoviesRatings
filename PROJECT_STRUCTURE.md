# Project Structure

```
MovieRatings/
├── README.md                    # Project documentation
├── requirements.txt             # Python dependencies
├── .gitignore                  # Git ignore rules
├── PROJECT_STRUCTURE.md        # This file
├── .env                        # Environment variables (API keys)
│
├── infrastructure/             # Docker infrastructure
│   ├── docker-compose.yml     # Docker services configuration
│   └── trino/                 # Trino configuration
│       ├── config.properties  # Trino server configuration
│       ├── jvm.config         # JVM settings
│       ├── node.properties    # Node configuration
│       └── catalog/           # Catalog configurations
│           └── iceberg.properties # Iceberg catalog config
│
├── scripts/                    # Main orchestration scripts
│   ├── setup.py               # Infrastructure setup
│   ├── create_stage_schema.py # Schema and table creation
│   ├── run_complete_pipeline.py # Complete pipeline runner
│   ├── run_top_rated_pipeline.py # Top-rated movies pipeline runner
│   ├── run_full_top_rated_pipeline.py # Full top-rated pipeline (all pages)
│   ├── test_top_rated_data.py # Top-rated data analysis
│   ├── status.py              # Service status checker
│   └── verify_current_status.py # Current status verification
│
├── data_pipeline/             # Data processing modules
│   ├── comprehensive_ingestor.py # Main data ingestion orchestrator
│   ├── top_rated_ingestor.py # Top-rated movies pipeline
│   ├── stage_data_loader.py   # Data loader for Iceberg tables
│   └── scrappers/             # Web scraping modules
│       ├── __init__.py
│       ├── metacritic_scraper.py
│       └── rotten_tomatoes_scraper.py
│
├── data/                      # Data storage
│   ├── raw/                   # Raw ingested data (JSON files)
│   └── processed/             # Processed data (future use)
│
├── config/                    # Configuration files
│   ├── settings.py            # Centralized settings
│   ├── top_rated_pipeline_config.yaml # Top-rated pipeline configuration
│   └── env.example            # Environment template
│
└── docs/                      # Additional documentation
```

## Key Components

### Infrastructure
- **MinIO**: S3-compatible object storage
- **Polaris**: Iceberg catalog server
- **Trino**: SQL query engine (no Spark dependencies)

### Data Pipeline
- **Ingestion**: TMDB API, OMDb API, web scraping
- **Staging**: JSON files in data/raw/
- **Loading**: Bulk insertion into Iceberg tables

### Schema
- **movies_stage**: Main schema with 4 tables
- **Partitioning**: By year for performance
- **Format**: PARQUET with Iceberg metadata

## Data Flow

1. **Setup**: Docker infrastructure starts
2. **Ingestion**: APIs and scrapers collect data
3. **Staging**: Data saved to JSON files
4. **Schema**: Tables created in movies_stage
5. **Loading**: Data inserted into Iceberg tables
6. **Query**: Data accessible via Trino SQL

## Architecture Notes

- **No Spark**: This project uses Trino directly for SQL queries
- **Pure SQL**: All data processing is done through SQL queries
- **Lightweight**: Minimal dependencies, focused on core data lakehouse functionality
