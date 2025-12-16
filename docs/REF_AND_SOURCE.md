# ref() and source() Functions

## Overview

The framework distinguishes between **model dependencies** and **source tables**:

- **`ref()`**: Used for referencing other **models** in the transformation pipeline
- **`source()`**: Used for referencing **raw source tables** (external data sources)

This separation allows the framework to:
- Build proper dependency graphs for models only
- Track source tables separately
- Optimize execution plans
- Provide better lineage tracking

## Usage

### ref() - Model Dependencies

Use `ref()` to reference other models in your transformation pipeline:

```sql
-- config: materialized=table

SELECT 
    order_id,
    customer_id,
    amount
FROM {{ ref('bronze_orders') }}  -- References another model
WHERE status = 'active'
```

**Behavior**:
- `ref('model_name')` resolves to the actual table name of the model
- Creates a dependency in the dependency graph
- Ensures models are executed in the correct order
- Models referenced by `ref()` must exist in the models directory

### source() - Source Tables

Use `source()` to reference raw source tables (external data sources):

```sql
-- config: materialized=table

SELECT 
    order_id,
    customer_id,
    amount,
    order_date
FROM {{ source('raw_data', 'orders') }}  -- References source table
WHERE order_date >= CURRENT_DATE() - 7
```

**Behavior**:
- `source('source_name', 'table_name')` resolves to the actual source table name
- Does NOT create a model dependency (source tables are external)
- Source tables are tracked separately for lineage purposes
- Source tables should be defined in your source configuration

## Examples

### Example 1: Bronze Layer (Uses source())

```sql
-- sql/models/bronze/orders.sql
-- config: materialized=table

SELECT 
    order_id,
    customer_id,
    amount,
    order_date,
    CURRENT_TIMESTAMP() as load_timestamp
FROM {{ source('raw_database', 'orders') }}  -- Source table
WHERE order_date >= CURRENT_DATE() - 1
```

### Example 2: Silver Layer (Uses ref())

```sql
-- sql/models/silver/clean_orders.sql
-- config: materialized=table

SELECT 
    order_id,
    customer_id,
    amount,
    order_date,
    load_timestamp
FROM {{ ref('bronze_orders') }}  -- References bronze model
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY order_id 
    ORDER BY load_timestamp DESC
) = 1
```

### Example 3: Gold Layer (Uses ref())

```sql
-- sql/models/gold/daily_summary.sql
-- config: materialized=table

SELECT 
    DATE_TRUNC('day', order_date) as order_day,
    COUNT(*) as total_orders,
    SUM(amount) as total_revenue
FROM {{ ref('silver_clean_orders') }}  -- References silver model
GROUP BY DATE_TRUNC('day', order_date)
```

## Dependency Graph

The framework builds a dependency graph based on `ref()` calls:

```
source('raw', 'orders')  [Source - not in graph]
    ↓
bronze_orders  [Model - uses source()]
    ↓
silver_clean_orders  [Model - uses ref('bronze_orders')]
    ↓
gold_daily_summary  [Model - uses ref('silver_clean_orders')]
```

**Key Points**:
- Only `ref()` calls create edges in the dependency graph
- `source()` calls are tracked separately for lineage
- Execution order is determined by `ref()` dependencies only

## Configuration

### Source Configuration

Define source tables in your configuration (e.g., `config/sources.yml`):

```yaml
sources:
  - name: raw_database
    description: Raw data from source system
    tables:
      - name: orders
        description: Orders table
        identifier: RAW_DB.ORDERS  # Actual Snowflake table name
      - name: customers
        description: Customers table
        identifier: RAW_DB.CUSTOMERS
```

### Model Configuration

Models are automatically discovered from SQL files. No separate configuration needed.

## Best Practices

1. **Always use `ref()` for model dependencies**
   - ✅ `FROM {{ ref('bronze_orders') }}`
   - ❌ `FROM bronze_orders` (direct table reference)

2. **Always use `source()` for source tables**
   - ✅ `FROM {{ source('raw_data', 'orders') }}`
   - ❌ `FROM raw_data.orders` (direct table reference)

3. **Never mix ref() and source()**
   - Models should reference other models with `ref()`
   - Models should reference source tables with `source()`

4. **Keep source tables separate**
   - Source tables are external and not managed by the framework
   - Models are managed by the framework and can be materialized

## Implementation Details

### Parser Behavior

The parser extracts:
- **Dependencies**: From `ref()` calls → stored in `parsed_sql.dependencies`
- **Sources**: From `source()` calls → stored in `parsed_sql.sources`

### Placeholder Resolution

During execution:
- `ref('model_name')` → `__REF_model_name__` → replaced with actual table name
- `source('source_name', 'table_name')` → `__SOURCE_source_name_table_name__` → replaced with actual source table name

### Dependency Graph

Only `ref()` dependencies are added to the dependency graph:
- Ensures correct execution order
- Prevents circular dependencies
- Enables parallel execution of independent models

## Migration Guide

If you have existing models using direct table references:

**Before**:
```sql
SELECT * FROM bronze_orders
```

**After**:
```sql
SELECT * FROM {{ ref('bronze_orders') }}
```

**Before**:
```sql
SELECT * FROM raw_db.orders
```

**After**:
```sql
SELECT * FROM {{ source('raw_db', 'orders') }}
```

