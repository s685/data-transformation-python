# Bronze Layer Models

The bronze layer contains raw data models that ingest data directly from source systems.

## Characteristics:
- Raw, unprocessed data
- Minimal transformations
- Preserves source data structure
- Typically incremental loads
- Fast ingestion

## Supported Object Types:

### 1. **View** (Default)
Best for: Real-time access to source data
```sql
-- config: materialized=view
SELECT * FROM source_system.orders
```

### 2. **Table**
Best for: Staging raw data for processing
```sql
-- config: materialized=table
SELECT 
    order_id,
    customer_id,
    order_date,
    amount,
    source_system,
    load_timestamp
FROM source_system.orders
WHERE load_timestamp >= $start_date
```

### 3. **Temporary Table**
Best for: Intermediate processing, session-scoped data
```sql
-- config: materialized=temp_table
SELECT * FROM source_system.orders
WHERE order_date = CURRENT_DATE()
```

### 4. **Incremental Table**
Best for: Large datasets with append-only patterns
```sql
-- config: materialized=incremental, incremental_strategy=append, time_column=load_timestamp
SELECT 
    order_id,
    customer_id,
    order_date,
    amount,
    load_timestamp
FROM source_system.orders
WHERE load_timestamp >= $start_date
```

### 5. **CDC (Change Data Capture)**
Best for: Tracking changes (INSERT, UPDATE, DELETE) from source
```sql
-- config: materialized=cdc, unique_key=order_id
-- meta:
--   cdc:
--     change_type_column: __CDC_OPERATION
--     columns: [order_id, customer_id]

SELECT 
    order_id,
    customer_id,
    order_date,
    amount,
    __CDC_OPERATION,  -- 'I', 'U', or 'D'
    __CDC_TIMESTAMP
FROM source_system.orders_cdc
```

## CDC Macro Usage:

```sql
-- Using CDC merge macro
{{ cdc_merge('bronze.raw_orders', 'source.orders_staging', 'order_id') }}

-- Filter CDC operations
WHERE {{ cdc_filter('__CDC_OPERATION', ['I', 'U']) }}

-- Add CDC columns
SELECT 
    *,
    {{ cdc_columns() }}
FROM source_table
```

## Using source() for External Tables

**Important:** Use `source()` macro for external source tables, NOT `ref()`:

```sql
-- ✅ CORRECT: Use source() for external tables
FROM {{ source('raw_data', 'orders') }}

-- ❌ WRONG: Don't use ref() for external tables
FROM {{ ref('raw_data.orders') }}
```

## Example:
```sql
-- sql/models/bronze/raw_orders.sql
-- config: materialized=incremental, incremental_strategy=append, time_column=load_timestamp

SELECT
    order_id,
    customer_id,
    order_date,
    amount,
    source_system,
    CURRENT_TIMESTAMP() as load_timestamp
FROM {{ source('raw_data', 'orders') }}
WHERE order_date >= $start_date
```

**Note:** `source('raw_data', 'orders')` resolves to the actual table name defined in `config/sources.yml`
