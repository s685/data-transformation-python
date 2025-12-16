# Data Transformation Framework

A Python framework inspired by **dbt** and **SQLMesh** for executing SQL transformations on Snowflake with advanced features including:

- ✅ **Variable Substitution** - Snowflake-style `$variable` syntax
- ✅ **Dependency Management** - Automatic dependency resolution and topological sorting
- ✅ **Plan-based Execution** - See what will change before running (SQLMesh-style)
- ✅ **Materialization Strategies** - View, table, temporary table, incremental table, and CDC
- ✅ **State Management** - Track model versions and execution history
- ✅ **Hot Reload** - Watch for file changes without downtime
- ✅ **Column-level Lineage** - AST-based lineage tracking
- ✅ **Testing Framework** - Data quality tests (dbt-style)
- ✅ **Error Recovery** - Graceful degradation, never fails to run

## Installation

```bash
# Clone the repository
git clone <repository-url>
cd data-tranformation-python

# Install uv if you haven't already
# On macOS/Linux:
curl -LsSf https://astral.sh/uv/install.sh | sh
# On Windows:
powershell -c "irm https://astral.sh/uv/install.ps1 | iex"

# Install dependencies and the framework using uv
uv sync

# Or install in development mode with dev dependencies
uv sync --dev
```

## Quick Start

### 1. Project Setup

Create the following directory structure:

```
your-project/
├── config/
│   ├── profiles.yml          # Snowflake connection config
│   └── environments.yml      # Environment configs
├── sql/                      # SQL files directory
│   ├── models/              # SQL model files
│   │   ├── bronze/          # Raw/bronze layer models
│   │   ├── silver/          # Cleaned/silver layer models
│   │   └── gold/            # Aggregated/gold layer models
│   └── tests/               # SQL test files
├── config/
│   ├── profiles.yml         # Snowflake connection config
│   ├── environments.yml     # Environment configs
│   └── sources.yml          # Source table definitions
└── .env                      # Environment variables
```

### 2. Configuration

**config/profiles.yml:**
```yaml
default:
  outputs:
    dev:
      type: snowflake
      account: your_account
      user: your_user
      password: ${SNOWFLAKE_PASSWORD}
      warehouse: dev_warehouse
      database: dev_database
      schema: dev_schema
      role: dev_role
    prod:
      type: snowflake
      account: your_account
      user: your_user
      password: ${SNOWFLAKE_PASSWORD}
      warehouse: prod_warehouse
      database: prod_database
      schema: prod_schema
      role: prod_role
  target: dev
```

**config/sources.yml:**
```yaml
sources:
  - name: raw_data
    description: "Raw data source from external system"
    database: RAW_DB
    schema: RAW_SCHEMA
    tables:
      - name: orders
        description: "Raw orders table"
        identifier: ORDERS_TABLE
      - name: customers
        description: "Raw customers table"
        identifier: CUSTOMERS_TABLE
```

**.env:**
```bash
SNOWFLAKE_PASSWORD=your_password_here
```

### 3. Create Your First Model

**sql/models/bronze/raw_orders.sql:**
```sql
-- config: materialized=incremental, incremental_strategy=append, time_column=load_timestamp

-- Use source() for external source tables
SELECT
    order_id,
    customer_id,
    order_date,
    amount,
    CURRENT_TIMESTAMP() as load_timestamp
FROM {{ source('raw_data', 'orders') }}
WHERE order_date BETWEEN $start_date AND $end_date
```

**sql/models/silver/cleaned_orders.sql:**
```sql
-- config: materialized=view
-- depends_on: bronze.raw_orders

-- Use ref() for model dependencies
SELECT
    order_id,
    customer_id,
    CAST(order_date AS DATE) as order_date,
    CAST(amount AS DECIMAL(10,2)) as amount
FROM {{ ref('bronze.raw_orders') }}
WHERE amount > 0
```

**sql/models/gold/sales_report.sql:**
```sql
-- config: materialized=table
-- depends_on: silver.cleaned_orders

SELECT
    DATE_TRUNC('month', order_date) as month,
    COUNT(DISTINCT customer_id) as customers,
    SUM(amount) as total_sales
FROM {{ ref('silver.cleaned_orders') }}
GROUP BY DATE_TRUNC('month', order_date)
```

**sql/models/schema.yml:**
```yaml
models:
  - name: sales_report
    description: "Monthly sales summary"
    config:
      materialized: table
    columns:
      - name: month
        description: "Sales month"
        tests:
          - not_null
      - name: total_sales
        description: "Total sales amount"
        tests:
          - not_null
    vars:
      - start_date
      - end_date
    depends_on:
      - stg_orders
```

## Usage

### Run Models

```bash
# Run a specific model
uv run framework run sales_report --vars start_date=2024-01-01,end_date=2024-12-31

# Run multiple models
uv run framework run stg_orders sales_report --vars start_date=2024-01-01,end_date=2024-12-31

# Run all models
uv run framework run-all --vars start_date=2024-01-01,end_date=2024-12-31

# Dry run (validate without executing)
uv run framework run sales_report --vars start_date=2024-01-01,end_date=2024-12-31 --dry-run

# Run against specific environment
uv run framework run sales_report --target prod --vars start_date=2024-01-01,end_date=2024-12-31
```

### Generate Execution Plan

```bash
# Generate plan for all models
uv run framework plan

# Generate plan for specific models
uv run framework plan --models sales_report,stg_orders

# Plan shows:
# - Models to create
# - Models to update (with reasons)
# - Models to delete
# - Execution order
# - Dependencies affected
```

### Validate Models

```bash
# Validate SQL syntax for all models
uv run framework validate

# Lists any syntax errors or issues
```

### List Models

```bash
# List all models with dependencies
uv run framework list
```

### Show Dependencies

```bash
# Show dependency statistics
uv run framework deps

# Export as Graphviz DOT format
uv run framework deps --format graphviz > deps.dot
```

### Hot Reload (Watch Mode)

```bash
# Start watching for file changes
uv run framework serve --watch

# Framework will automatically reload when files change
# No downtime - SQL can always run!
```

## Advanced Features

### Materialization Types

The framework supports multiple Snowflake object types:

#### 1. **View** (Default)
```sql
-- config: materialized=view
SELECT * FROM source_table
```

#### 2. **Table**
```sql
-- config: materialized=table
SELECT * FROM source_table
```

#### 3. **Temporary Table**
```sql
-- config: materialized=temp_table
SELECT * FROM source_table
```

#### 4. **Incremental Table**

**Time-based Incremental:**
```sql
-- config: materialized=incremental, incremental_strategy=time, time_column=created_at

SELECT
    order_id,
    customer_id,
    created_at,
    amount
FROM raw_orders
{% if is_incremental() %}
    WHERE created_at > (SELECT MAX(created_at) FROM {{ this }})
{% endif %}
```

**Unique Key Incremental:**
```sql
-- config: materialized=incremental, incremental_strategy=unique_key, unique_key=order_id

SELECT
    order_id,
    customer_id,
    order_date,
    amount
FROM raw_orders
```

**Append-only Incremental:**
```sql
-- config: materialized=incremental, incremental_strategy=append

SELECT
    event_id,
    event_time,
    event_data
FROM raw_events
WHERE event_time = CURRENT_DATE()
```

#### 5. **CDC (Change Data Capture)**
```sql
-- config: materialized=cdc, unique_key=order_id
-- meta:
--   cdc:
--     change_type_column: __CDC_OPERATION

SELECT
    order_id,
    customer_id,
    order_date,
    amount,
    __CDC_OPERATION,  -- 'I', 'U', or 'D'
    __CDC_TIMESTAMP
FROM source_cdc_stream
```

### CDC Macros

The framework provides CDC macros for change tracking:

```sql
-- CDC merge macro
{{ cdc_merge('target_table', 'source_table', 'unique_key') }}

-- CDC filter macro
WHERE {{ cdc_filter('__CDC_OPERATION', ['I', 'U']) }}

-- CDC columns macro
SELECT 
    *,
    {{ cdc_columns() }}
FROM source_table
```

### Layer-Specific Macros

**Bronze Layer:**
```sql
{{ bronze_load('source_table', filter_condition='date >= CURRENT_DATE()') }}
```

**Silver Layer:**
```sql
{{ silver_clean('bronze.raw_orders', dedupe_key='order_id', filter_condition='amount > 0') }}
```

**Gold Layer:**
```sql
{{ gold_aggregate(
    'silver.cleaned_orders',
    group_by_columns=['customer_id', 'month'],
    aggregate_columns={'total_revenue': 'SUM(amount)', 'order_count': 'COUNT(*)'}
) }}
```

### Variable Substitution

The framework supports Snowflake-style `$variable` syntax:

```sql
SELECT *
FROM orders
WHERE 
    order_date BETWEEN $start_date AND $end_date
    AND region = $region
    AND amount > $min_amount
```

Pass variables via CLI:
```bash
uv run framework run orders --vars start_date=2024-01-01,end_date=2024-12-31,region=US,min_amount=100
```

### Dependency Management

The framework distinguishes between **model dependencies** and **source tables**:

#### **ref() - Model Dependencies**
Use `ref()` to reference other models in your transformation pipeline:

```sql
-- Silver layer model referencing bronze layer model
SELECT *
FROM {{ ref('bronze.raw_orders') }} o
JOIN {{ ref('bronze.raw_customers') }} c
  ON o.customer_id = c.customer_id
```

**Characteristics:**
- Creates dependency relationship in dependency graph
- Models execute in correct order based on dependencies
- Tracked for lineage and impact analysis

#### **source() - Source Tables**
Use `source()` to reference external source tables (not models):

```sql
-- Bronze layer model referencing external source table
SELECT *
FROM {{ source('raw_data', 'orders') }}
WHERE order_date >= $start_date
```

**Format:** `source('source_name', 'table_name')`

**Configuration:** Define sources in `config/sources.yml`:
```yaml
sources:
  - name: raw_data
    database: RAW_DB
    schema: RAW_SCHEMA
    tables:
      - name: orders
        identifier: ORDERS_TABLE
```

**Characteristics:**
- Does NOT create model dependencies
- Resolved from `sources.yml` configuration
- Used for external/raw data sources

#### **Other Dependency Methods:**

**Explicit Dependencies (comments):**
```sql
-- depends_on: model1, model2

SELECT ...
```

**AST-based (automatic):**
The framework automatically detects direct table references:
```sql
SELECT *
FROM stg_orders  -- Automatically detected as dependency
```

### Data Quality Tests

Define tests in `schema.yml`:

```yaml
models:
  - name: orders
    columns:
      - name: order_id
        tests:
          - unique
          - not_null
      - name: status
        tests:
          - accepted_values:
              values: ['pending', 'completed', 'cancelled']
      - name: amount
        tests:
          - not_null
```

Run tests:
```bash
uv run framework test orders
```

### State Management

The framework tracks:
- Model versions (file hashes)
- Execution history
- Success/failure counts
- Incremental state (last processed timestamps)

State is stored per environment in `.state/{environment}/`

### Column-level Lineage

The framework uses AST parsing to track column-level lineage:

```bash
uv run framework lineage sales_report
```

Shows which source columns affect each output column.

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                        CLI Layer                            │
│  (run, plan, validate, test, list, deps, serve)            │
└─────────────────────────────────────────────────────────────┘
                              │
┌─────────────────────────────────────────────────────────────┐
│                     Core Framework                          │
├─────────────────────────────────────────────────────────────┤
│ • Config Manager    - YAML configs + env vars               │
│ • SQL Parser        - AST parsing with sqlglot              │
│ • Model Registry    - YAML model configs (dbt-style)        │
│ • Dependency Graph  - Topological sort + lineage            │
│ • State Manager     - Version tracking (SQLMesh-style)      │
│ • Plan Generator    - Plan-based execution                  │
│ • Executor          - SQL execution with retry logic        │
│ • Materializer      - View/table/incremental strategies     │
│ • Test Runner       - Data quality tests                    │
│ • File Watcher      - Hot reload with thread safety         │
└─────────────────────────────────────────────────────────────┘
                              │
┌─────────────────────────────────────────────────────────────┐
│                  Connection Layer                           │
│  • Connection Pool  - Snowflake connections                 │
│  • Retry Logic      - Exponential backoff                   │
│  • Health Checks    - Connection monitoring                  │
└─────────────────────────────────────────────────────────────┘
```

## Key Design Principles

### 1. Zero Downtime
- Hot reload with file system watcher
- Thread-safe cache updates
- Atomic state transitions
- Framework never stops running

### 2. Graceful Degradation
- Never fail to run SQL
- Continue execution on errors
- Detailed error context
- Automatic retry for transient errors

### 3. Best of dbt + SQLMesh
- **From dbt**: YAML configs, testing, Jinja2, materialization
- **From SQLMesh**: Plan-based execution, state management, AST lineage, virtual environments

### 4. Advanced Techniques
- AST-based SQL parsing (sqlglot)
- Column-level lineage tracking
- Async/parallel execution
- Connection pooling
- LRU caching
- Circuit breaker pattern

## Error Handling

The framework implements comprehensive error handling:

1. **Retryable Errors**: Automatic retry with exponential backoff
2. **Connection Errors**: Health checks and connection recreation
3. **Execution Errors**: Continue with other models (graceful degradation)
4. **Validation Errors**: Detailed context and suggestions

Example:
```
✗ sales_report: ExecutionError
  Model: sales_report
  Error: Missing required variable: start_date
  Context: provided_variables=['end_date']
  Suggestion: Add --vars start_date=YYYY-MM-DD
```

## Package Management with uv

This project uses [uv](https://github.com/astral-sh/uv) for fast and reliable package management.

### Common uv Commands

```bash
# Install all dependencies
uv sync

# Install with dev dependencies
uv sync --dev

# Add a new dependency
uv add package-name

# Add a dev dependency
uv add --dev package-name

# Remove a dependency
uv remove package-name

# Run a command in the project environment
uv run framework run sales_report

# Run tests
uv run pytest

# Update dependencies
uv sync --upgrade
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Run tests: `uv run pytest tests/`
5. Submit a pull request

## License

MIT License - see LICENSE file

## Support

For issues, questions, or feature requests, please open a GitHub issue.

## Roadmap

- [ ] REST API interface
- [ ] Web UI for monitoring
- [ ] More database adapters (BigQuery, Redshift, PostgreSQL)
- [ ] Custom test macros
- [ ] Documentation generation
- [ ] Metrics and monitoring integration
- [ ] Automated backfill scheduling
- [ ] Multi-environment promotion workflows

---

**Built with ❤️ combining the best of dbt and SQLMesh**
