# CDC Retirement Pattern

## Overview

The framework uses a **retirement pattern** for CDC processing, maintaining full history while keeping current records easily accessible.

## CDC Logic

### INSERT Operation ('I')
- **Action**: Insert new record
- **obsolete_date**: Set to `NULL`
- **Result**: New active record in table

```sql
-- Source has: __CDC_OPERATION = 'I'
-- Result: New record inserted with obsolete_date = NULL
INSERT INTO table_name (..., obsolete_date) 
VALUES (..., NULL)
```

### UPDATE Operation ('U')
- **Action**: 
  1. Retire old record (set `obsolete_date = CURRENT_TIMESTAMP()`)
  2. Insert new record with `obsolete_date = NULL`
- **Result**: Old record retired, new active record inserted

```sql
-- Step 1: Retire old record
UPDATE table_name
SET obsolete_date = CURRENT_TIMESTAMP()
WHERE unique_key = 'key123' AND obsolete_date IS NULL

-- Step 2: Insert new record
INSERT INTO table_name (..., obsolete_date)
VALUES (..., NULL)
```

### DELETE Operation ('D')
- **Action**: Set `obsolete_date` on existing record
- **Result**: Record retired (not deleted, history preserved)

```sql
-- Source has: __CDC_OPERATION = 'D'
-- Result: Existing record gets obsolete_date set
UPDATE table_name
SET obsolete_date = CURRENT_TIMESTAMP()
WHERE unique_key = 'key123' AND obsolete_date IS NULL
```

### EXPIRED Operation ('E')
- **Action**: Set `obsolete_date` on existing record
- **Result**: Record retired (same as DELETE)

```sql
-- Source has: __CDC_OPERATION = 'E'
-- Result: Existing record gets obsolete_date set
UPDATE table_name
SET obsolete_date = CURRENT_TIMESTAMP()
WHERE unique_key = 'key123' AND obsolete_date IS NULL
```

## Table Structure

CDC tables include these columns:
- All business columns
- `__CDC_OPERATION`: Operation type ('I', 'U', 'D', 'E')
- `__CDC_TIMESTAMP`: When the CDC operation occurred
- `obsolete_date`: When the record was retired (NULL for active records)

## Querying Patterns

### Get Current Records
```sql
SELECT *
FROM cdc_table
WHERE obsolete_date IS NULL
```

### Get All History
```sql
SELECT *
FROM cdc_table
ORDER BY unique_key, __CDC_TIMESTAMP
```

### Get Record at Specific Time
```sql
SELECT *
FROM cdc_table
WHERE unique_key = 'key123'
  AND (obsolete_date IS NULL 
       OR obsolete_date > '2024-01-01')
  AND __CDC_TIMESTAMP <= '2024-01-01'
ORDER BY __CDC_TIMESTAMP DESC
LIMIT 1
```

### Get Latest Version of Each Record
```sql
SELECT *
FROM cdc_table t1
WHERE obsolete_date IS NULL
  OR __CDC_TIMESTAMP = (
    SELECT MAX(__CDC_TIMESTAMP)
    FROM cdc_table t2
    WHERE t2.unique_key = t1.unique_key
  )
```

## Benefits

1. **Full History**: All versions of records are preserved
2. **Easy Current Query**: Simple `WHERE obsolete_date IS NULL` for current data
3. **Time Travel**: Query historical states using `obsolete_date`
4. **No Data Loss**: Updates don't overwrite, they create new records
5. **Audit Trail**: Complete history of all changes

## Performance

- **INSERT**: Fast (single insert)
- **UPDATE**: Two operations (retire + insert) but maintains history
- **DELETE**: Fast (single update)
- **Query Current**: Fast (indexed on `obsolete_date IS NULL`)

## Example

```sql
-- Initial state
INSERT INTO orders (order_id, amount, obsolete_date) 
VALUES ('ORD001', 100, NULL)

-- Update order
-- Step 1: Retire old
UPDATE orders SET obsolete_date = CURRENT_TIMESTAMP() 
WHERE order_id = 'ORD001' AND obsolete_date IS NULL

-- Step 2: Insert new
INSERT INTO orders (order_id, amount, obsolete_date) 
VALUES ('ORD001', 150, NULL)

-- Final state:
-- order_id | amount | obsolete_date
-- ORD001   | 100    | 2024-01-15 10:00:00  (retired)
-- ORD001   | 150    | NULL                  (current)
```

