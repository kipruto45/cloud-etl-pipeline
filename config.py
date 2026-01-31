# ETL Pipeline Configuration
# Customize settings for the pipeline here

# Extract settings
EXTRACT_ENCODING = "utf-8"
EXTRACT_ON_BAD_LINES = "warn"  # warn, skip, error
EXTRACT_MAX_FILE_SIZE_MB = 500

# Transform settings
TRANSFORM_NORMALIZE_COLUMNS = True
TRANSFORM_REMOVE_DUPLICATES = False
TRANSFORM_DROP_EMPTY_ROWS = True
TRANSFORM_DROP_HOW = "all"  # 'all' or 'any'

# Load settings
LOAD_CHUNK_SIZE = 5000
LOAD_IF_EXISTS = "append"  # fail, replace, append

# Pipeline settings
PIPELINE_MAX_RETRIES = 3
PIPELINE_RAW_DIR = "data/raw"
PIPELINE_PROCESSED_DIR = "data/processed"
PIPELINE_LOG_DIR = "logs"

# Database settings (override via environment variables)
POSTGRES_HOST = None  # Set via env: POSTGRES_HOST=localhost
POSTGRES_PORT = 5432
POSTGRES_USER = "postgres"
POSTGRES_PASSWORD = "postgres"
POSTGRES_DB = "etl_db"
