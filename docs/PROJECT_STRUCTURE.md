# Project Structure Documentation

## Overview

The Movie Ratings Data Lakehouse project has been reorganized for better maintainability, clarity, and efficiency. The new structure follows modern Python project conventions and separates concerns logically.

## Directory Structure

```
MovieRatings/
├── config/                     # Configuration files
│   ├── pipeline_config.yaml   # Main pipeline configuration
│   └── env.example            # Environment variables template
├── data_pipeline/             # Data processing modules
│   ├── __init__.py
│   ├── tmdb_ingestor.py      # TMDB API integration
│   ├── omdb_enricher.py      # OMDB API integration
│   └── scrappers/            # Web scraping modules
│       ├── __init__.py
│       ├── base_scraper.py   # Base scraper interface
│       ├── main.py           # Scraper orchestration
│       ├── metacritic_scraper.py
│       └── tomatos_scraper.py
├── infrastructure/            # Infrastructure configuration
│   ├── docker-compose.yml    # Docker services orchestration
│   └── trino/               # Trino SQL engine configuration
│       ├── config.properties # Trino coordinator config
│       ├── jvm.config       # JVM settings
│       ├── node.properties  # Node environment config
│       └── catalog/         # Catalog configurations
│           └── iceberg.properties
├── scripts/                  # Management and utility scripts
│   ├── main.py              # Main entry point
│   ├── setup_infrastructure.py # Infrastructure setup
│   ├── setup_polaris.py     # Polaris catalog setup
│   ├── create_tables.py     # Iceberg table creation
│   ├── check_status.py      # Infrastructure status check
│   ├── verify_connection.py # Connectivity verification
│   └── start_infrastructure.sh # Infrastructure startup
├── docs/                    # Documentation
│   └── PROJECT_STRUCTURE.md # This file
├── logs/                    # Log files (created at runtime)
├── data/                    # Local data storage (created at runtime)
├── tmp/                     # Temporary files (created at runtime)
├── __init__.py              # Python package marker
├── requirements.txt          # Python dependencies
└── README.md                # Project overview and usage
```

## Key Improvements

### 1. Logical Organization

- **config/**: All configuration files in one place
- **infrastructure/**: Docker and service configurations
- **scripts/**: All management and utility scripts
- **data_pipeline/**: Data processing and ingestion logic
- **docs/**: Project documentation

### 2. Clean Scripts

- Removed emojis and excessive formatting
- Consistent error handling and logging
- Proper Python class-based architecture
- Type hints for better code quality
- Comprehensive error messages

### 3. Unified Management

- **main.py**: Single entry point for all operations
- Consistent command-line interface
- Proper exit codes for automation
- Verbose mode for debugging

### 4. Configuration Management

- Centralized configuration in YAML
- Environment-specific settings
- Clear separation of concerns
- Easy to modify and extend

## Script Descriptions

### Main Entry Point

- **scripts/main.py**: Central management script with commands:
  - `start`: Start infrastructure services
  - `setup`: Setup Polaris and create tables
  - `status`: Check infrastructure health
  - `verify`: Test connectivity
  - `full-setup`: Complete setup from scratch

### Infrastructure Management

- **scripts/setup_infrastructure.py**: Manages Docker services and health checks
- **scripts/setup_polaris.py**: Configures Polaris catalog and permissions
- **scripts/create_tables.py**: Creates and verifies Iceberg tables
- **scripts/check_status.py**: Comprehensive infrastructure health check
- **scripts/verify_connection.py**: Tests Trino connectivity and operations

### Startup Scripts

- **scripts/start_infrastructure.sh**: Bash script for service startup with health checks

## Configuration Files

### Pipeline Configuration

- **config/pipeline_config.yaml**: Main configuration including:
  - Infrastructure endpoints
  - Iceberg table settings
  - Data source configurations
  - Processing parameters
  - Monitoring settings

### Environment Variables

- **config/env.example**: Template for environment variables
- **.env**: Local environment configuration (not in version control)

### Infrastructure Configuration

- **infrastructure/docker-compose.yml**: Service orchestration
- **infrastructure/trino/**: Trino SQL engine configuration

## Usage Patterns

### Development Workflow

1. **Setup**: `python scripts/main.py full-setup`
2. **Status Check**: `python scripts/main.py status`
3. **Verification**: `python scripts/main.py verify`
4. **Individual Operations**: Use specific commands as needed

### Production Deployment

1. **Environment Setup**: Configure `.env` file
2. **Infrastructure Start**: `python scripts/main.py start`
3. **Catalog Setup**: `python scripts/main.py setup`
4. **Monitoring**: Regular status checks

### Troubleshooting

1. **Check Status**: `python scripts/main.py status`
2. **View Logs**: `docker-compose logs -f [service]`
3. **Verify Connectivity**: `python scripts/main.py verify`
4. **Restart Services**: `docker-compose restart [service]`

## Code Quality Features

### Python Best Practices

- Type hints throughout
- Proper exception handling
- Consistent error messages
- Clean class-based architecture
- Comprehensive documentation

### Error Handling

- Graceful failure handling
- Detailed error messages
- Proper exit codes
- Logging for debugging

### Testing and Validation

- Health checks for all services
- Connection verification
- Table creation validation
- Comprehensive status reporting

## Maintenance

### Adding New Features

1. **New Scripts**: Add to appropriate directory
2. **New Configuration**: Update relevant config files
3. **New Dependencies**: Add to requirements.txt
4. **Documentation**: Update relevant docs

### Updating Configuration

1. **Pipeline Config**: Modify `config/pipeline_config.yaml`
2. **Environment**: Update `.env` file
3. **Infrastructure**: Modify `infrastructure/` files
4. **Dependencies**: Update `requirements.txt`

### Monitoring and Debugging

1. **Status Checks**: Regular use of status command
2. **Log Analysis**: Review service logs
3. **Health Monitoring**: Automated health checks
4. **Performance Metrics**: Monitor Trino query performance

## Future Enhancements

### Planned Improvements

- **Automated Testing**: Unit and integration tests
- **CI/CD Pipeline**: Automated deployment
- **Monitoring Dashboard**: Web-based monitoring
- **Configuration Validation**: Schema validation for configs
- **Backup and Recovery**: Automated backup procedures

### Scalability Considerations

- **Horizontal Scaling**: Multiple Trino workers
- **Load Balancing**: Multiple MinIO instances
- **High Availability**: Service redundancy
- **Performance Optimization**: Query optimization and caching
