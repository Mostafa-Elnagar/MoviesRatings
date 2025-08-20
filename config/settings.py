"""
Configuration settings for Movie Ratings Data Lakehouse
"""

import os

# Infrastructure
DOCKER_COMPOSE_FILE = "infrastructure/docker-compose.yml"

# Service URLs
TRINO_HOST = "localhost"
TRINO_PORT = 8080
TRINO_SERVER = f"http://{TRINO_HOST}:{TRINO_PORT}"
POLARIS_HOST = "localhost"
POLARIS_PORT = 8181
POLARIS_URL = f"http://{POLARIS_HOST}:{POLARIS_PORT}"
MINIO_HOST = "localhost"
MINIO_PORT = 9000
MINIO_URL = f"http://{MINIO_HOST}:{MINIO_PORT}"

# Polaris Configuration
POLARIS_CLIENT_ID = "root"
POLARIS_CLIENT_SECRET = "secret"
POLARIS_SCOPE = "PRINCIPAL_ROLE:ALL"
CATALOG_NAME = "polariscatalog"

# MinIO Configuration
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
MINIO_REGION = "dummy-region"
WAREHOUSE_BUCKET = "warehouse"

# Database Configuration
ICEBERG_SCHEMA = "movies"
TABLE_FORMAT = "PARQUET"

# Timeouts (seconds)
SERVICE_TIMEOUT = 180
REQUEST_TIMEOUT = 10

# Container Names
TRINO_CONTAINER = "trino"
MINIO_CLIENT_CONTAINER = "minio-client"

# Table Names
RAW_MOVIES_TABLE = "raw_movies"
ENRICHED_MOVIES_TABLE = "enriched_movies"
MOVIE_RATINGS_TABLE = "movie_ratings"

# API Endpoints
POLARIS_TOKEN_ENDPOINT = f"{POLARIS_URL}/api/catalog/v1/oauth/tokens"
POLARIS_CATALOG_ENDPOINT = f"{POLARIS_URL}/api/catalog/"
POLARIS_MANAGEMENT_ENDPOINT = f"{POLARIS_URL}/api/management/v1"
MINIO_HEALTH_ENDPOINT = f"{MINIO_URL}/minio/health/live"
TRINO_INFO_ENDPOINT = f"{TRINO_SERVER}/v1/info"

# S3 Configuration for Polaris
S3_ENDPOINT = "http://minio:9000"
S3_BASE_LOCATION = f"s3://{WAREHOUSE_BUCKET}"
ROLE_ARN = "arn:aws:iam::000000000000:role/minio-polaris-role"
