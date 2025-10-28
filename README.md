# Patterns

Patterns analyzes your Data Analytics query patterns and provides AI-powered recommendations to optimize table performance through better partitioning, clustering, indexing strategies and more.

![Patterns UI Demo](patterns_demo.gif)

**Important**: Recommendations are AI-generated and should be carefully reviewed before implementation. Always validate suggestions against your specific use case and test in non-production environments first.

## What It Does

Patterns connects to your data warehouse (BigQuery or Snowflake, and more to come), extracts query history, and analyzes how your tables are being accessed. It then uses AI to generate actionable optimization recommendations tailored to your specific usage patterns.

### Key Features

- 🔍 **Query Pattern Analysis** - Automatically extracts and analyzes query history from BigQuery and Snowflake
- 🤖 **AI-Powered Recommendations** - Uses Google Gemini to suggest table optimizations based on actual usage patterns
- 🎯 **Smart Detection** - Identifies optimal partition and cluster column candidates from filter patterns
- 🔒 **Privacy-First** - Anonymizes all sensitive data (table names, columns, schemas) before sending to AI
- 📊 **Visual Dashboard** - Clean web UI to explore tables, view statistics, and get recommendations
- 💾 **Local Storage** - Uses DuckDB for efficient local data storage and analysis
- 🔄 **Incremental Updates** - Refresh query history and metadata on-demand with date range filtering

### How It Works

1. **Extract** - Pulls query history and table metadata from your data warehouse
2. **Analyze** - Identifies which tables are queried, how often, and what filters are commonly used
3. **Optimize** - AI analyzes patterns and suggests specific DDL changes (partitioning, clustering, indexing)
4. **Implement** - Review recommendations and apply optimizations to your warehouse

### Supported Platforms

- **Source Warehouses**: BigQuery, Snowflake
- **Target Warehouses**: Amazon S3 + Athena, Apache Hudi, Apache Iceberg, Azure Synapse Analytics, BigQuery, ClickHouse, Databricks, Delta Lake, IBM Db2 Warehouse, Oracle Autonomous Data Warehouse, Redshift, SAP Data Warehouse Cloud, Snowflake, Teradata Vantage, Vertica

## Quick Start

### Prerequisites

- Python 3.9+
- Poetry (for dependency management)
- Access to BigQuery or Snowflake with appropriate permissions (see below)
- Google Gemini API key (see below)

### Required Permissions

#### BigQuery

Your service account or user needs the following permissions:

- **BigQuery Data Viewer** (`roles/bigquery.dataViewer`) - To read table metadata
- **BigQuery Job User** (`roles/bigquery.jobUser`) - To execute queries
- **BigQuery Resource Viewer** (`roles/bigquery.resourceViewer`) - To list datasets and tables
- Access to **INFORMATION_SCHEMA.JOBS** - To read query history

#### Snowflake

Your user/role needs the following privileges:

**For Query History Analysis:**
- **USAGE** on `SNOWFLAKE` database
- **IMPORTED PRIVILEGES** on `SNOWFLAKE.ACCOUNT_USAGE` schema
- Access to `SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY` view

**For Table Metadata:**
- **USAGE** on target databases and schemas
- **SELECT** on `INFORMATION_SCHEMA.TABLES`
- **SELECT** on `INFORMATION_SCHEMA.COLUMNS`

### Getting a Gemini API Key

Patterns uses Google's Gemini AI for generating optimization recommendations.

**Steps to get your API key:**

1. Go to [Google AI Studio](https://aistudio.google.com/app/apikey)
2. Sign in with your Google account
3. Click **"Get API Key"** or **"Create API Key"**
4. Select a project or create a new one
5. Copy the generated API key

**Free Tier:**
- Gemini offers a generous free tier
- 60 requests per minute
- Perfect for getting started

### Installation

```bash
# Clone the repository
git clone git@github.com:gundageren/patterns.git
cd patterns

# Install dependencies
poetry install
```

### Configuration

Create a `config.json` file based on your source platform. 
See `config.bigquery.example.json` and `config.snowflake.example.json` for authentication examples.

### Running the Application

```bash
# Start the web application, initial extraction may delay the availability of the UI — this is expected
poetry run python patterns_app.py --config config.json

# Skip initial data extraction (use UI to refresh), initial extraction may delay the availability of the UI — this is expected
poetry run python patterns_app.py --skip-initial-extraction

# Disable UI configuration controls
poetry run python patterns_app.py --disable-ui-config
```

Then open your browser to `http://localhost:5000`

## Usage

### Web Interface

1. **Filter Tables** - Click the filter icon to narrow down your table list:
   - Filter by source platform (BigQuery, Snowflake)
   - Filter by source project
   - Filter by database
   - Filter by schema
   - Filters cascade - selecting a platform shows only relevant projects, etc.

2. **Search & Analyze Tables** - Use the search bar to find a specific table:
   - Enter a table name to search across all tables
   - Click on a table to view its statistics
   - Click **"Find Patterns"** to get AI-powered optimization recommendations
   - **Important**: Recommendations are AI-generated and should be carefully reviewed before implementation. Always validate suggestions against your specific use case and test in non-production environments first.

3. **View Statistics** - Once you select a table, review:
   - Query frequency over time (weekly/monthly trends)
   - SELECT * usage patterns
   - Column filter patterns (potential partition candidates)
   - Table metadata (size, row count, columns)

4. **Configuration Panel** - Click the gear icon to manage data:
   - **Refresh Query History** - Pull latest queries from your warehouse
   - **Refresh Tables Metadata** - Update table information
   - **Run Analysis** - Analyze stored queries to identify patterns (requires platform and project selection)
   - Set date ranges for historical data extraction
   - Enable debug mode to see the full AI prompt

## Architecture

```
Patterns
├── patterns_app.py                 # Main application entry point
├── pyproject.toml                  # Poetry dependencies and project config
├── config.json                     # Your configuration file (not in git)
├── api/                            # Flask web application
│   ├── routes/                     # API endpoints (data, stats, info)
│   ├── services/                   # Business logic (AI, privacy, data)
│   ├── templates/                  # Web UI (HTML)
│   ├── static/                     # CSS, JS, images
│   └── utils/                      # Helpers, validators, config
├── patterns/                       # Core analysis engine
│   ├── extract/                    # Data warehouse connectors
│   ├── analyzer/                   # Pattern detection algorithms
│   ├── store/                      # Local storage (DuckDB)
│   └── interface/                  # CLI interface (legacy)
├── data/                           # Local DuckDB storage directory
│   └── patterns.duckdb             # Query history and analysis results
└── tests/                          # Comprehensive test suite (100+ tests)
```

## API Endpoints

### Information
- `GET /` - Web interface
- `GET /list-tables` - List tables (optional filters: `source_platform`, `source_project`, `database`, `schema`)
- `GET /list-warehouses` - List supported data warehouses
- `GET /ui-config` - Get UI configuration settings

### Data Management
- `POST /refresh-query-history-and-tables` - Refresh both queries and tables (optional: `start_date`, `end_date`)
- `POST /refresh-query-history` - Refresh queries from warehouse (optional: `start_date`, `end_date`, `run_analysis`)
- `POST /refresh-tables` - Refresh table metadata
- `POST /run-analysis` - Analyze query patterns (optional: `source_platform`, `source_project`, `start_date`, `end_date`)

### Analysis
- `GET /table-weekly-stats` - Get weekly statistics (required: `source_platform`, `source_project`, `database`, `schema`, `table`)
- `POST /find-patterns` - Get AI recommendations (required: `source_platform`, `source_project`, `database`, `schema`, `table`, `target_warehouse`)

**Common Query Parameters:**
- `start_date`, `end_date` - Date range (format: `YYYY-MM-DD`) - Default range: Last 30 days
- `debug=true` - Show AI prompt (for `/find-patterns` only)

**Response Format:**
```json
{
  "success": true,
  "data": {}
}
```

**Example:**
```bash
curl "http://localhost:5000/list-tables?source_platform=bigquery"
```

## Analysis Details

### Pattern Detection

Patterns analyzes three key patterns:

1. **Read Table Queries** - Which tables are accessed and how frequently
2. **SELECT * Queries** - Tables where full scans are common (optimization opportunity)
3. **Partition Candidates** - Columns used in WHERE clauses that make good partition keys

### Privacy & Security

All data sent to AI services is anonymized:
- Table names → `__TBL_HASH__`
- Column names → `__COL_HASH__`
- Database/schema names → `__DB_HASH__`, `__SCHEMA_HASH__`
- Project identifiers → `__PROJECT_HASH__`

Original names are restored in the final recommendations.

## Running Tests

```bash
poetry run pytest
```

## Credits & Acknowledgments

- **CSS Components** - [@takaneichinose](https://codepen.io/takaneichinose) for the search box animations
- **Magnifying Glass Icon** - [Royyan Wijaya](https://www.flaticon.com/free-icons/magnifying-glass) via Flaticon
- **Filter Icon** - [joalfa](https://www.flaticon.com/free-icons/filter) via Flaticon
- **Settings Icon** - [Freepik](https://www.flaticon.com/free-icons/settings) via Flaticon  
