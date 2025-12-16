# Gold Layer Models

The gold layer contains aggregated and business-ready analytical models.

## Characteristics:
- Aggregated metrics
- Business logic applied
- Optimized for reporting
- Final analytical layer
- Denormalized for performance
- Typically materialized as tables

## Supported Object Types:

### 1. **Table** (Recommended)
Best for: Final analytical models, reporting tables
```sql
-- config: materialized=table
-- depends_on: silver.cleaned_orders

SELECT
    DATE_TRUNC('month', order_date) as month,
    COUNT(DISTINCT customer_id) as unique_customers,
    COUNT(*) as total_orders,
    SUM(amount) as total_revenue,
    AVG(amount) as avg_order_value
FROM {{ ref('silver.cleaned_orders') }}
GROUP BY DATE_TRUNC('month', order_date)
```

### 2. **View**
Best for: Lightweight aggregations, always up-to-date metrics
```sql
-- config: materialized=view
-- depends_on: silver.cleaned_orders

SELECT
    customer_id,
    COUNT(*) as order_count,
    SUM(amount) as lifetime_value,
    MAX(order_date) as last_order_date
FROM {{ ref('silver.cleaned_orders') }}
GROUP BY customer_id
```

### 3. **Incremental Table** (Highly Recommended)
Best for: Large fact tables, time-series aggregations
```sql
-- config: materialized=incremental, incremental_strategy=time, time_column=order_date
-- depends_on: silver.cleaned_orders

SELECT
    DATE_TRUNC('day', order_date) as order_day,
    customer_id,
    COUNT(*) as daily_orders,
    SUM(amount) as daily_revenue
FROM {{ ref('silver.cleaned_orders') }}
WHERE order_date >= $start_date
{% if is_incremental() %}
    AND order_date > (SELECT MAX(order_date) FROM {{ this }})
{% endif %}
GROUP BY DATE_TRUNC('day', order_date), customer_id
```

### 4. **Temporary Table**
Best for: Ad-hoc analysis, one-time aggregations
```sql
-- config: materialized=temp_table
-- depends_on: silver.cleaned_orders

SELECT
    customer_id,
    SUM(amount) as total_spent
FROM {{ ref('silver.cleaned_orders') }}
WHERE order_date >= CURRENT_DATE() - 30
GROUP BY customer_id
HAVING SUM(amount) > 1000
```

### 5. **CDC (Change Data Capture)**
Best for: Slowly changing dimensions (SCD), historical tracking
```sql
-- config: materialized=cdc, unique_key=customer_id
-- depends_on: silver.cleaned_customers
-- meta:
--   cdc:
--     change_type_column: __CDC_OPERATION

SELECT
    customer_id,
    customer_name,
    email,
    address,
    __CDC_OPERATION,
    __CDC_TIMESTAMP
FROM {{ ref('silver.cleaned_customers') }}
```

## Gold Layer Macros:

```sql
-- Aggregation macro
{{ gold_aggregate(
    'silver.cleaned_orders',
    group_by_columns=['customer_id', 'DATE_TRUNC("month", order_date)'],
    aggregate_columns={
        'total_orders': 'COUNT(*)',
        'total_revenue': 'SUM(amount)',
        'avg_order_value': 'AVG(amount)'
    }
) }}
```

## Best Practices:

1. **Use Tables** for final reporting models (better performance)
2. **Use Incremental** for large fact tables (efficiency)
3. **Use Views** for lightweight metrics (always fresh)
4. **Use CDC** for slowly changing dimensions (SCD Type 2)
5. **Use Temp Tables** for ad-hoc analysis only

## Example:
```sql
-- sql/models/gold/sales_summary.sql
-- config: materialized=incremental, incremental_strategy=time, time_column=order_date
-- depends_on: silver.cleaned_orders

SELECT
    DATE_TRUNC('month', order_date) as month,
    COUNT(DISTINCT customer_id) as unique_customers,
    COUNT(*) as total_orders,
    SUM(amount) as total_revenue,
    AVG(amount) as avg_order_value,
    MIN(amount) as min_order_value,
    MAX(amount) as max_order_value
FROM {{ ref('silver.cleaned_orders') }}
WHERE order_date >= $start_date
{% if is_incremental() %}
    AND order_date > (SELECT MAX(order_date) FROM {{ this }})
{% endif %}
GROUP BY DATE_TRUNC('month', order_date)
```
