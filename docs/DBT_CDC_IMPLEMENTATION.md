# Implementing CDC Support in dbt for 50TB+ Data

## Overview

dbt doesn't have native CDC materialization, but you can implement CDC patterns using Snowflake features (Streams, Staging Tables) combined with dbt macros and incremental models. This guide shows how to handle large-scale CDC (50TB+) with dbt.

---

## Architecture: CDC with dbt

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                    dbt CDC IMPLEMENTATION FOR 50TB+ DATA                            │
└─────────────────────────────────────────────────────────────────────────────────────┘

Source System
     │
     │ CDC Feed (INSERT/UPDATE/DELETE)
     ▼
┌─────────────────────────────────────────────────────────────┐
│              Snowflake Staging Layer                        │
│  ┌──────────────────────────────────────────────────────┐  │
│  │  Staging Table (Raw CDC Data)                        │  │
│  │  - Receives all CDC changes                         │  │
│  │  - Has __CDC_OPERATION column (I/U/D)               │  │
│  │  - Has __CDC_TIMESTAMP column                       │  │
│  └──────────────────────────────────────────────────────┘  │
│                          │                                  │
│                          ▼                                  │
│  ┌──────────────────────────────────────────────────────┐  │
│  │  Snowflake Stream (Tracks Changes)                   │  │
│  │  - Automatically tracks INSERT/UPDATE/DELETE         │  │
│  │  - Provides change metadata                          │  │
│  └──────────────────────────────────────────────────────┘  │
└──────────────────────────┬──────────────────────────────────┘
                           │
                           │ dbt Models
                           ▼
┌─────────────────────────────────────────────────────────────┐
│                    dbt Transformation Layer                 │
│  ┌──────────────────────────────────────────────────────┐  │
│  │  Bronze Model (dbt incremental)                     │  │
│  │  - Processes stream data                             │  │
│  │  - Uses custom CDC macro                             │  │
│  └──────────────────────────────────────────────────────┘  │
│                          │                                  │
│                          ▼                                  │
│  ┌──────────────────────────────────────────────────────┐  │
│  │  Silver/Gold Models                                  │  │
│  │  - Standard dbt transformations                      │  │
│  └──────────────────────────────────────────────────────┘  │
└──────────────────────────┬──────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────┐
│              Target Tables (with History)                    │
│  ┌──────────────────────────────────────────────────────┐  │
│  │  Target Table                                         │  │
│  │  - Current records (obsolete_date = NULL)            │  │
│  │  - Historical records (obsolete_date IS NOT NULL)    │  │
│  │  - Full audit trail                                  │  │
│  └──────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

---

## Implementation Approaches

### Approach 1: Snowflake Streams + dbt Incremental (Recommended for 50TB+)

**Best for:** Large-scale CDC with automatic change tracking

#### Step 1: Create Staging Table and Stream

```sql
-- Create staging table for raw CDC data
CREATE OR REPLACE TABLE staging.cdc_source_table (
    id VARCHAR,
    column1 VARCHAR,
    column2 VARCHAR,
    __CDC_OPERATION VARCHAR,  -- 'I', 'U', 'D'
    __CDC_TIMESTAMP TIMESTAMP_NTZ
);

-- Create stream to track changes
CREATE OR REPLACE STREAM staging.cdc_source_table_stream
ON TABLE staging.cdc_source_table;
```

#### Step 2: Create dbt Model with CDC Macro

**models/bronze/cdc_target.sql:**
```sql
-- config: materialized=incremental, unique_key=id, incremental_strategy=merge

{{
    config(
        materialized='incremental',
        unique_key='id',
        incremental_strategy='merge',
        merge_update_columns=['column1', 'column2', '__CDC_TIMESTAMP', 'obsolete_date']
    )
}}

WITH stream_data AS (
    SELECT 
        id,
        column1,
        column2,
        __CDC_OPERATION,
        __CDC_TIMESTAMP,
        METADATA$ACTION as stream_action,
        METADATA$ISUPDATE as stream_is_update
    FROM {{ source('staging', 'cdc_source_table_stream') }}
    WHERE __CDC_TIMESTAMP >= (
        SELECT COALESCE(MAX(__CDC_TIMESTAMP), '1900-01-01')
        FROM {{ this }}
    )
),

processed_cdc AS (
    SELECT 
        id,
        column1,
        column2,
        CASE 
            WHEN stream_action = 'DELETE' THEN 'D'
            WHEN stream_is_update THEN 'U'
            ELSE 'I'
        END as __CDC_OPERATION,
        __CDC_TIMESTAMP,
        CASE 
            WHEN stream_action = 'DELETE' THEN CURRENT_TIMESTAMP()
            ELSE NULL
        END as obsolete_date
    FROM stream_data
)

SELECT 
    id,
    column1,
    column2,
    __CDC_OPERATION,
    __CDC_TIMESTAMP,
    obsolete_date
FROM processed_cdc
```

#### Step 3: Create Custom dbt Macro for CDC Merge

**macros/cdc_merge.sql:**
```sql
{% macro cdc_merge(target_table, source_table, unique_key, change_type_column='__CDC_OPERATION') %}
    MERGE INTO {{ target_table }} target
    USING (
        SELECT 
            *,
            COALESCE({{ change_type_column }}, 'U') as {{ change_type_column }},
            CURRENT_TIMESTAMP() as __CDC_TIMESTAMP
        FROM {{ source_table }}
    ) source
    ON target.{{ unique_key }} = source.{{ unique_key }}
        AND target.obsolete_date IS NULL
    WHEN MATCHED AND source.{{ change_type_column }} = 'D' THEN 
        UPDATE SET obsolete_date = CURRENT_TIMESTAMP()
    WHEN MATCHED AND source.{{ change_type_column }} IN ('U', 'I') THEN 
        UPDATE SET 
            {%- for col in adapter.get_columns_in_relation(source_table) %}
            {%- if col.name not in [unique_key, change_type_column, '__CDC_TIMESTAMP', 'obsolete_date'] %}
            {{ col.name }} = source.{{ col.name }}{% if not loop.last %},{% endif %}
            {%- endif %}
            {%- endfor %},
            __CDC_TIMESTAMP = source.__CDC_TIMESTAMP
    WHEN NOT MATCHED AND source.{{ change_type_column }} != 'D' THEN 
        INSERT (
            {%- for col in adapter.get_columns_in_relation(source_table) %}
            {{ col.name }}{% if not loop.last %},{% endif %}
            {%- endfor %}
        )
        VALUES (
            {%- for col in adapter.get_columns_in_relation(source_table) %}
            source.{{ col.name }}{% if not loop.last %},{% endif %}
            {%- endfor %}
        )
{% endmacro %}
```

---

### Approach 2: Retirement Pattern with dbt Incremental (For History Preservation)

**Best for:** Full history tracking with retirement pattern

#### Bronze Model with Retirement Pattern

**models/bronze/cdc_with_history.sql:**
```sql
-- config: materialized=incremental, unique_key=id

{{
    config(
        materialized='incremental',
        unique_key='id',
        incremental_strategy='merge'
    )
}}

WITH new_cdc_data AS (
    SELECT 
        id,
        column1,
        column2,
        __CDC_OPERATION,
        __CDC_TIMESTAMP
    FROM {{ source('staging', 'cdc_source_table') }}
    WHERE __CDC_TIMESTAMP >= (
        SELECT COALESCE(MAX(__CDC_TIMESTAMP), '1900-01-01')
        FROM {{ this }}
        WHERE obsolete_date IS NULL
    )
),

-- Retire old records for UPDATEs
retire_old AS (
    UPDATE {{ this }} target
    SET obsolete_date = CURRENT_TIMESTAMP()
    WHERE target.id IN (
        SELECT id FROM new_cdc_data WHERE __CDC_OPERATION = 'U'
    )
    AND target.obsolete_date IS NULL
),

-- Insert new/updated records
insert_new AS (
    SELECT 
        id,
        column1,
        column2,
        __CDC_OPERATION,
        __CDC_TIMESTAMP,
        CASE 
            WHEN __CDC_OPERATION = 'D' THEN CURRENT_TIMESTAMP()
            ELSE NULL
        END as obsolete_date
    FROM new_cdc_data
    WHERE __CDC_OPERATION != 'D'  -- Deletes handled by UPDATE above
)

SELECT * FROM insert_new
```

**Note:** This approach requires two-step process (retire + insert) which can be complex in dbt.

---

### Approach 3: Custom Materialization (Advanced)

Create a custom dbt materialization for CDC:

**macros/materializations/cdc.sql:**
```python
def cdc_materialization(config, model):
    """
    Custom CDC materialization for dbt
    Implements retirement pattern for large-scale CDC
    """
    unique_key = config.get('unique_key')
    change_type_column = config.get('change_type_column', '__CDC_OPERATION')
    
    # Check if table exists
    table_exists = adapter.check_relation_exists(model)
    
    if not table_exists:
        # Initial load
        sql = f"""
        CREATE TABLE {model} AS
        SELECT 
            *,
            NULL as obsolete_date
        FROM ({model.compiled_sql})
        """
    else:
        # CDC merge with retirement pattern
        sql = f"""
        -- Step 1: Retire old records for UPDATEs
        UPDATE {model}
        SET obsolete_date = CURRENT_TIMESTAMP()
        WHERE {unique_key} IN (
            SELECT {unique_key}
            FROM ({model.compiled_sql})
            WHERE {change_type_column} = 'U'
        )
        AND obsolete_date IS NULL;
        
        -- Step 2: Insert new records
        INSERT INTO {model}
        SELECT 
            *,
            CASE 
                WHEN {change_type_column} = 'D' THEN CURRENT_TIMESTAMP()
                ELSE NULL
            END as obsolete_date
        FROM ({model.compiled_sql})
        WHERE {change_type_column} != 'D';
        """
    
    return sql
```

**Usage:**
```sql
-- config: materialized=cdc, unique_key=id

SELECT 
    id,
    column1,
    column2,
    __CDC_OPERATION,
    __CDC_TIMESTAMP
FROM {{ source('staging', 'cdc_source_table') }}
```

---

## Performance Optimization for 50TB+ Data

### 1. Partitioning and Clustering

```sql
-- Create target table with clustering
CREATE OR REPLACE TABLE target.cdc_table (
    id VARCHAR,
    column1 VARCHAR,
    column2 VARCHAR,
    __CDC_OPERATION VARCHAR,
    __CDC_TIMESTAMP TIMESTAMP_NTZ,
    obsolete_date TIMESTAMP_NTZ
)
CLUSTER BY (id, obsolete_date);  -- Cluster on unique key and obsolete_date
```

### 2. Incremental Processing with Time Windows

```sql
-- Process in time windows to avoid full table scans
WHERE __CDC_TIMESTAMP >= '{{ var("start_date") }}'
  AND __CDC_TIMESTAMP < '{{ var("end_date") }}'
```

### 3. Batch Processing

**models/bronze/cdc_batch.sql:**
```sql
-- config: materialized=incremental, unique_key=id

{{
    config(
        materialized='incremental',
        unique_key='id',
        incremental_strategy='merge',
        on_schema_change='append_new_columns'
    )
}}

-- Process in batches to avoid memory issues
WITH batch_data AS (
    SELECT 
        id,
        column1,
        column2,
        __CDC_OPERATION,
        __CDC_TIMESTAMP
    FROM {{ source('staging', 'cdc_source_table') }}
    WHERE __CDC_TIMESTAMP >= (
        SELECT COALESCE(MAX(__CDC_TIMESTAMP), '1900-01-01')
        FROM {{ this }}
    )
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY id 
        ORDER BY __CDC_TIMESTAMP DESC
    ) = 1  -- Get latest change per key
)

SELECT 
    id,
    column1,
    column2,
    __CDC_OPERATION,
    __CDC_TIMESTAMP,
    CASE 
        WHEN __CDC_OPERATION = 'D' THEN CURRENT_TIMESTAMP()
        ELSE NULL
    END as obsolete_date
FROM batch_data
```

### 4. Use Snowflake Tasks for Continuous Processing

```sql
-- Create Snowflake Task for continuous CDC processing
CREATE OR REPLACE TASK process_cdc
    WAREHOUSE = 'COMPUTE_WH'
    SCHEDULE = '1 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('staging.cdc_source_table_stream')
AS
    CALL dbt.run('models/bronze/cdc_target.sql');
```

### 5. Optimize Warehouse Size

```yaml
# profiles.yml
warehouse: LARGE_WH  # Use larger warehouse for 50TB+ processing
threads: 8  # Increase threads for parallel processing
```

---

## Complete Example: CDC Model for 50TB+ Data

### 1. Staging Setup (Run once in Snowflake)

```sql
-- Create staging table
CREATE TABLE IF NOT EXISTS staging.source_cdc (
    id VARCHAR,
    data_column1 VARCHAR,
    data_column2 VARCHAR,
    __CDC_OPERATION VARCHAR,
    __CDC_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Create stream
CREATE OR REPLACE STREAM staging.source_cdc_stream
ON TABLE staging.source_cdc;

-- Create target table with clustering
CREATE TABLE IF NOT EXISTS bronze.cdc_target (
    id VARCHAR,
    data_column1 VARCHAR,
    data_column2 VARCHAR,
    __CDC_OPERATION VARCHAR,
    __CDC_TIMESTAMP TIMESTAMP_NTZ,
    obsolete_date TIMESTAMP_NTZ,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (id, obsolete_date);
```

### 2. dbt Model

**models/bronze/cdc_target.sql:**
```sql
-- config: 
--   materialized: incremental
--   unique_key: id
--   incremental_strategy: merge
--   on_schema_change: append_new_columns

{{
    config(
        materialized='incremental',
        unique_key='id',
        incremental_strategy='merge',
        merge_update_columns=['data_column1', 'data_column2', '__CDC_TIMESTAMP', 'obsolete_date'],
        on_schema_change='append_new_columns'
    )
}}

WITH stream_changes AS (
    SELECT 
        id,
        data_column1,
        data_column2,
        CASE 
            WHEN METADATA$ACTION = 'DELETE' THEN 'D'
            WHEN METADATA$ISUPDATE = TRUE THEN 'U'
            ELSE 'I'
        END as __CDC_OPERATION,
        COALESCE(__CDC_TIMESTAMP, CURRENT_TIMESTAMP()) as __CDC_TIMESTAMP
    FROM {{ source('staging', 'source_cdc_stream') }}
    {% if is_incremental() %}
    WHERE __CDC_TIMESTAMP > (SELECT MAX(__CDC_TIMESTAMP) FROM {{ this }})
    {% endif %}
),

-- Deduplicate: Get latest change per key
latest_changes AS (
    SELECT *
    FROM stream_changes
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY id 
        ORDER BY __CDC_TIMESTAMP DESC
    ) = 1
)

SELECT 
    id,
    data_column1,
    data_column2,
    __CDC_OPERATION,
    __CDC_TIMESTAMP,
    CASE 
        WHEN __CDC_OPERATION = 'D' THEN CURRENT_TIMESTAMP()
        ELSE NULL
    END as obsolete_date
FROM latest_changes
```

### 3. Custom Macro for Retirement Pattern

**macros/cdc_retirement_merge.sql:**
```sql
{% macro cdc_retirement_merge(target_table, source_cte, unique_key) %}
    -- Step 1: Retire old records for UPDATEs
    UPDATE {{ target_table }}
    SET obsolete_date = CURRENT_TIMESTAMP()
    WHERE {{ unique_key }} IN (
        SELECT {{ unique_key }}
        FROM {{ source_cte }}
        WHERE __CDC_OPERATION = 'U'
    )
    AND obsolete_date IS NULL;
    
    -- Step 2: Merge new records
    MERGE INTO {{ target_table }} target
    USING {{ source_cte }} source
    ON target.{{ unique_key }} = source.{{ unique_key }}
        AND target.obsolete_date IS NULL
    WHEN MATCHED AND source.__CDC_OPERATION = 'D' THEN 
        UPDATE SET obsolete_date = CURRENT_TIMESTAMP()
    WHEN MATCHED AND source.__CDC_OPERATION IN ('U', 'I') THEN 
        UPDATE SET 
            data_column1 = source.data_column1,
            data_column2 = source.data_column2,
            __CDC_TIMESTAMP = source.__CDC_TIMESTAMP
    WHEN NOT MATCHED AND source.__CDC_OPERATION != 'D' THEN 
        INSERT (id, data_column1, data_column2, __CDC_OPERATION, __CDC_TIMESTAMP, obsolete_date)
        VALUES (source.id, source.data_column1, source.data_column2, 
                source.__CDC_OPERATION, source.__CDC_TIMESTAMP, NULL);
{% endmacro %}
```

---

## Performance Best Practices for 50TB+

### 1. **Use Snowflake Streams** (Recommended)
- ✅ Automatic change tracking
- ✅ No need to scan entire table
- ✅ Efficient for large datasets
- ✅ Built-in metadata (METADATA$ACTION, METADATA$ISUPDATE)

### 2. **Implement Clustering**
```sql
CLUSTER BY (unique_key, obsolete_date)
```
- Speeds up queries for current records (`obsolete_date IS NULL`)
- Improves UPDATE performance for retirement pattern

### 3. **Batch Processing**
- Process in time windows (e.g., hourly batches)
- Use `QUALIFY ROW_NUMBER()` to deduplicate
- Limit batch size to avoid memory issues

### 4. **Optimize Warehouse**
```yaml
# profiles.yml
warehouse: XLARGE_WH  # For 50TB+ processing
threads: 16  # Increase for parallel model execution
```

### 5. **Use Incremental Strategy**
- Always use `incremental_strategy='merge'` for CDC
- Filter by timestamp to process only new changes
- Use `on_schema_change='append_new_columns'` for schema evolution

### 6. **Partition Large Tables**
- Consider table partitioning if Snowflake supports it
- Use date-based partitioning for time-series CDC data

---

## Comparison: dbt CDC vs Python Framework CDC

| Aspect | dbt CDC Implementation | Python Framework CDC |
|--------|------------------------|---------------------|
| **Setup Complexity** | ⚠️ Requires Snowflake Streams + custom macros | ✅ Native CDC materialization |
| **Performance (50TB+)** | ⚠️ Good with optimization (clustering, streams) | ✅ Excellent (Polars-based) |
| **Retirement Pattern** | ⚠️ Complex (requires 2-step process) | ✅ Built-in retirement pattern |
| **Maintenance** | ⚠️ Must maintain custom macros | ✅ Framework handles it |
| **History Tracking** | ⚠️ Manual implementation | ✅ Automatic with obsolete_date |
| **Processing Time** | ⚠️ ~2-4 hours for 50TB (with optimization) | ✅ ~45 minutes for 50TB |
| **Code Complexity** | ⚠️ Higher (macros + SQL) | ✅ Lower (declarative config) |

---

## Limitations of dbt CDC Approach

1. **No Native CDC Materialization**
   - Must build custom macros
   - More complex than native support

2. **Retirement Pattern Complexity**
   - Requires 2-step process (retire + insert)
   - Harder to maintain and debug

3. **Performance for Very Large Datasets**
   - May need significant optimization
   - Not as fast as Polars-based solutions

4. **Testing Complexity**
   - Must test custom macros
   - More edge cases to handle

---

## Recommended Approach for 50TB+ CDC with dbt

### Option 1: Hybrid Approach (Best Performance)
- Use **Snowflake Streams** for change detection
- Use **dbt incremental models** with merge strategy
- Implement **clustering** on target tables
- Use **larger warehouses** for processing
- Process in **batches** to avoid memory issues

### Option 2: External Processing
- Use **external tool** (Python script) for CDC processing
- Use **dbt** for standard transformations
- Integrate via shared Snowflake schemas

### Option 3: Custom dbt Package
- Create **reusable dbt package** for CDC
- Share across projects
- Maintain as internal tooling

---

## Example: Complete CDC Pipeline

**1. Staging Model (Load Raw CDC)**
```sql
-- models/staging/raw_cdc.sql
-- config: materialized=table

SELECT 
    id,
    data_column1,
    data_column2,
    __CDC_OPERATION,
    __CDC_TIMESTAMP
FROM external_source.cdc_feed
```

**2. Stream Model (Track Changes)**
```sql
-- Run in Snowflake (not dbt model)
CREATE STREAM staging.raw_cdc_stream ON TABLE staging.raw_cdc;
```

**3. Bronze Model (Process CDC)**
```sql
-- models/bronze/cdc_processed.sql
-- config: materialized=incremental, unique_key=id

{{
    config(
        materialized='incremental',
        unique_key='id',
        incremental_strategy='merge'
    )
}}

SELECT 
    id,
    data_column1,
    data_column2,
    __CDC_OPERATION,
    __CDC_TIMESTAMP,
    CASE WHEN __CDC_OPERATION = 'D' THEN CURRENT_TIMESTAMP() ELSE NULL END as obsolete_date
FROM {{ source('staging', 'raw_cdc_stream') }}
{% if is_incremental() %}
WHERE __CDC_TIMESTAMP > (SELECT MAX(__CDC_TIMESTAMP) FROM {{ this }})
{% endif %}
```

**4. Silver Model (Business Logic)**
```sql
-- models/silver/cdc_cleaned.sql
-- config: materialized=view

SELECT 
    id,
    data_column1,
    data_column2,
    __CDC_TIMESTAMP
FROM {{ ref('bronze.cdc_processed') }}
WHERE obsolete_date IS NULL  -- Only current records
```

---

## Summary

**To add CDC support for 50TB+ data in dbt:**

1. ✅ Use **Snowflake Streams** for automatic change tracking
2. ✅ Create **staging tables** for raw CDC data
3. ✅ Use **dbt incremental models** with merge strategy
4. ✅ Implement **clustering** on target tables
5. ✅ Use **custom macros** for CDC logic
6. ✅ Process in **batches** to optimize performance
7. ✅ Use **larger warehouses** for 50TB+ processing

**Performance Expectations:**
- With proper optimization: ~2-4 hours for 50TB
- Requires significant tuning and optimization
- May need Snowflake professional services for best results

**Alternative:** Consider using the Python Framework for CDC workloads (45 minutes for 50TB) and dbt for standard transformations.

---

*Last Updated: 2025-01-15*

