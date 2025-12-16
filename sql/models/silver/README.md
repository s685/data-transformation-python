# Silver Layer Models

The silver layer contains cleaned and validated data models.

## Characteristics:
- Data quality checks applied
- Standardized formats
- Deduplication
- Business rules applied
- Ready for analytics
- Normalized structures

## Supported Object Types:

### 1. **View** (Recommended)
Best for: Cleaned data that needs to be always up-to-date
```sql
-- config: materialized=view
-- depends_on: bronze.raw_orders

SELECT
    order_id,
    customer_id,
    CAST(order_date AS DATE) as order_date,
    CAST(amount AS DECIMAL(10,2)) as amount,
    UPPER(TRIM(status)) as status,
    source_system,
    load_timestamp
FROM {{ ref('bronze.raw_orders') }}
WHERE amount > 0
  AND order_date IS NOT NULL
  AND status IS NOT NULL
```

### 2. **Table**
Best for: Performance-critical cleaned data
```sql
-- config: materialized=table
-- depends_on: bronze.raw_orders

SELECT
    order_id,
    customer_id,
    order_date,
    amount,
    status
FROM {{ ref('bronze.raw_orders') }}
WHERE amount > 0
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY order_id 
    ORDER BY load_timestamp DESC
) = 1
```

### 3. **Temporary Table**
Best for: One-time data cleaning operations
```sql
-- config: materialized=temp_table
-- depends_on: bronze.raw_orders

SELECT DISTINCT
    customer_id,
    customer_name,
    email
FROM {{ ref('bronze.raw_orders') }}
```

### 4. **Incremental Table**
Best for: Large cleaned datasets with time-based updates
```sql
-- config: materialized=incremental, incremental_strategy=time, time_column=order_date
-- depends_on: bronze.raw_orders

SELECT
    order_id,
    customer_id,
    order_date,
    amount,
    status
FROM {{ ref('bronze.raw_orders') }}
WHERE order_date >= $start_date
  AND amount > 0
{% if is_incremental() %}
    AND order_date > (SELECT MAX(order_date) FROM {{ this }})
{% endif %}
```

### 5. **CDC (Change Data Capture)**
Best for: Tracking changes in cleaned data
```sql
-- config: materialized=cdc, unique_key=order_id
-- depends_on: bronze.raw_orders
-- meta:
--   cdc:
--     change_type_column: __CDC_OPERATION

SELECT
    order_id,
    customer_id,
    order_date,
    amount,
    status,
    __CDC_OPERATION,
    __CDC_TIMESTAMP
FROM {{ ref('bronze.raw_orders') }}
WHERE amount > 0
```

## Silver Layer Macros:

```sql
-- Deduplication macro
{{ silver_clean('bronze.raw_orders', dedupe_key='order_id', filter_condition='amount > 0') }}

-- Standard cleaning pattern
SELECT 
    *,
    CURRENT_TIMESTAMP() as cleaned_timestamp
FROM {{ ref('bronze.raw_orders') }}
WHERE {{ silver_clean('bronze.raw_orders', dedupe_key='order_id') }}
```

## Using ref() for Model Dependencies

**Important:** Use `ref()` macro for model dependencies (other models in the framework):

```sql
-- ✅ CORRECT: Use ref() for model dependencies
FROM {{ ref('bronze.raw_orders') }}

-- ❌ WRONG: Don't use source() for models
FROM {{ source('bronze', 'raw_orders') }}
```

## Example:
```sql
-- sql/models/silver/cleaned_orders.sql
-- config: materialized=view
-- depends_on: bronze.raw_orders

SELECT
    order_id,
    customer_id,
    CAST(order_date AS DATE) as order_date,
    CAST(amount AS DECIMAL(10,2)) as amount,
    UPPER(TRIM(status)) as status,
    source_system,
    load_timestamp
FROM {{ ref('bronze.raw_orders') }}
WHERE amount > 0
  AND order_date IS NOT NULL
  AND status IS NOT NULL
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY order_id 
    ORDER BY load_timestamp DESC
) = 1
```
