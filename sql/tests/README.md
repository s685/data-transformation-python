# SQL Tests

This directory contains SQL test files for data quality validation.

## Test Types:
- Data quality tests
- Referential integrity tests
- Business rule tests
- Custom validation queries

## Example:
```sql
-- sql/tests/test_orders_quality.sql
-- Test that all orders have valid amounts

SELECT COUNT(*) as failed_rows
FROM gold.sales_summary
WHERE total_revenue < 0
```

