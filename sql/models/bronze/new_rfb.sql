-- CDC model for new_rfb table with retirement pattern
-- Uses Polars CDC which retires old records and inserts new ones for updates
-- config: materialized=cdc, unique_key=carriername,policy
-- meta:
--   cdc:
--     change_type_column: __CDC_OPERATION
--     obsolete_date_column: obsolete_date  # Column for retirement tracking (auto-added)
--
-- CDC Behavior:
-- - INSERT ('I'): New record with obsolete_date = NULL
-- - UPDATE ('U'): Old record gets obsolete_date set, new record inserted with obsolete_date = NULL
-- - DELETE ('D'): Existing record gets obsolete_date set (history preserved)

SELECT
    carriername,
    policy,
    -- TODO: Replace these placeholder column names with your actual 3 remaining columns
    column1,
    column2,
    column3,
    __CDC_OPERATION,  -- 'I' (Insert), 'U' (Update), 'D' (Delete)
    __CDC_TIMESTAMP
FROM new_rfb
WHERE __CDC_TIMESTAMP >= $start_date

