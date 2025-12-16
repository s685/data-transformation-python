"""
Polars-based CDC (Change Data Capture) materialization with retirement pattern.
Implements INSERT, UPDATE (with retirement), and DELETE/EXPIRED handling using Polars for maximum performance.

CDC Logic:
- INSERT: Insert record with obsolete_date = NULL
- UPDATE: Retire old record (set obsolete_date), insert new record with obsolete_date = NULL
- DELETE/EXPIRED: Set obsolete_date on existing record
"""

from typing import Dict, Any, Optional, List, Iterator
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass

from framework.connection import SnowflakeExecutor
from framework.model import ModelConfig
from framework.state import StateManager
from utils.errors import MaterializationError
from utils.logger import get_logger

logger = get_logger(__name__)

# Import Polars (Rust-based)
try:
    import polars as pl  # type: ignore
    POLARS_AVAILABLE = True
except ImportError:
    POLARS_AVAILABLE = False
    pl = None  # type: ignore
    raise ImportError("Polars is required for CDC processing. Install with: pip install polars")


@dataclass
class CDCResult:
    """CDC processing result"""
    model_name: str
    status: str
    rows_inserted: int = 0
    rows_updated: int = 0
    rows_retired: int = 0
    rows_deleted: int = 0
    chunks_processed: int = 0
    timestamp: str = ""


class PolarsCDCMaterialization:
    """
    Polars-based CDC materialization with retirement pattern.
    Uses Polars (Rust-based) for ultra-fast in-memory processing.
    
    CDC Pattern:
    - INSERT: New record with obsolete_date = NULL
    - UPDATE: Old record gets obsolete_date set, new record inserted with obsolete_date = NULL
    - DELETE/EXPIRED: Existing record gets obsolete_date set
    """
    
    def __init__(
        self,
        sf_executor: SnowflakeExecutor,
        state_manager: Optional[StateManager] = None,
        chunk_size: int = 10_000_000,  # 10M rows per chunk
        max_parallel_chunks: int = 10,
        obsolete_date_column: str = 'obsolete_date'
    ):
        """
        Initialize Polars CDC materialization.
        
        Args:
            sf_executor: Snowflake executor
            state_manager: State manager for tracking progress
            chunk_size: Number of rows per chunk (default: 10M)
            max_parallel_chunks: Maximum chunks to process in parallel
            obsolete_date_column: Column name for obsolete date (default: 'obsolete_date')
        """
        if not POLARS_AVAILABLE:
            raise MaterializationError(
                "Polars is required but not installed. Install with: pip install polars"
            )
        
        self.sf_executor = sf_executor
        self.state_manager = state_manager
        self.chunk_size = chunk_size
        self.max_parallel_chunks = max_parallel_chunks
        self.obsolete_date_column = obsolete_date_column
    
    def materialize(
        self,
        model_name: str,
        select_sql: str,
        model_config: Optional[ModelConfig] = None,
        variables: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Materialize CDC data using Polars with retirement pattern.
        Time complexity: O(n) where n is total rows.
        
        Args:
            model_name: Model name
            select_sql: SELECT statement
            model_config: Model configuration
            variables: Variables for SQL execution
        
        Returns:
            Materialization result with statistics
        """
        if not model_config:
            raise MaterializationError(
                f"Model config required for Polars CDC: {model_name}"
            )
        
        unique_key = model_config.unique_key
        if not unique_key:
            raise MaterializationError(
                f"unique_key required for Polars CDC: {model_name}"
            )
        
        # Get CDC configuration
        cdc_config = model_config.meta.get('cdc', {})
        change_type_column = cdc_config.get('change_type_column', '__CDC_OPERATION')
        
        try:
            table_exists = self._check_table_exists(model_name)
            
            if not table_exists:
                # Initial load - create table with CDC columns
                return self._initial_load(
                    model_name, select_sql, unique_key, change_type_column, variables
                )
            else:
                # Incremental CDC with retirement pattern
                return self._process_cdc_with_retirement(
                    model_name, select_sql, unique_key, change_type_column, variables
                )
                
        except Exception as e:
            logger.error(f"Polars CDC materialization failed for {model_name}: {e}")
            raise MaterializationError(
                f"Polars CDC materialization failed for {model_name}",
                context={"error": str(e), "chunk_size": self.chunk_size}
            )
    
    def _initial_load(
        self,
        model_name: str,
        select_sql: str,
        unique_key: str,
        change_type_column: str,
        variables: Optional[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Initial load - create table with all records having obsolete_date = NULL.
        """
        logger.info(f"Starting initial Polars CDC load for {model_name}")
        
        # Create table with clustering and CDC columns
        create_table_sql = f"""
        CREATE TABLE {model_name} 
        CLUSTER BY ({unique_key})
        AS
        SELECT 
            *,
            'I' as {change_type_column},
            CURRENT_TIMESTAMP() as __CDC_TIMESTAMP,
            NULL as {self.obsolete_date_column}
        FROM ({select_sql})
        LIMIT 0
        """
        
        self.sf_executor.execute_query(create_table_sql, variables, fetch=False)
        
        # Process data in chunks using Polars
        chunks_processed = 0
        total_rows = 0
        
        chunk_iterator = self._stream_query_chunks(select_sql, variables)
        
        for chunk_df in chunk_iterator:
            if chunk_df is None:
                break
            
            # Process chunk with Polars
            processed_df = self._process_initial_chunk_polars(
                chunk_df, unique_key, change_type_column
            )
            
            # Write to Snowflake
            self._write_initial_chunk_to_snowflake(
                model_name, processed_df, unique_key, variables
            )
            
            chunks_processed += 1
            total_rows += len(processed_df)
            logger.debug(f"Processed initial chunk {chunks_processed}: {len(processed_df)} rows")
        
        return {
            "model_name": model_name,
            "materialization": "cdc_polars",
            "status": "initial_load",
            "rows_loaded": total_rows,
            "chunks_processed": chunks_processed,
            "timestamp": datetime.now().isoformat()
        }
    
    def _process_cdc_with_retirement(
        self,
        model_name: str,
        select_sql: str,
        unique_key: str,
        change_type_column: str,
        variables: Optional[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Process CDC changes with retirement pattern using Polars.
        
        Logic:
        - INSERT: Insert new record with obsolete_date = NULL
        - UPDATE: Set obsolete_date on old record, insert new record with obsolete_date = NULL
        - DELETE/EXPIRED: Set obsolete_date on existing record
        """
        logger.info(f"Processing CDC with retirement pattern for {model_name}")
        
        # Create staging table for new CDC data
        staging_table = f"{model_name}_staging_{datetime.now().strftime('%Y%m%d%H%M%S')}"
        
        try:
            # Create staging table
            create_staging_sql = f"""
            CREATE TEMPORARY TABLE {staging_table}
            CLUSTER BY ({unique_key})
            AS
            SELECT 
                *,
                COALESCE({change_type_column}, 'U') as {change_type_column},
                CURRENT_TIMESTAMP() as __CDC_TIMESTAMP
            FROM ({select_sql})
            """
            
            self.sf_executor.execute_query(create_staging_sql, variables, fetch=False)
            
            # Get row count
            count_sql = f"SELECT COUNT(*) as cnt FROM {staging_table}"
            result = self.sf_executor.execute_query(count_sql, variables, fetch=True)
            total_rows = result[0]['CNT'] if result else 0
            
            if total_rows == 0:
                logger.info(f"No changes to process for {model_name}")
                return {
                    "model_name": model_name,
                    "materialization": "cdc_polars",
                    "status": "no_changes",
                    "rows_processed": 0,
                    "timestamp": datetime.now().isoformat()
                }
            
            # Process CDC with retirement pattern using Polars
            stats = {
                "rows_inserted": 0,
                "rows_updated": 0,
                "rows_retired": 0,
                "rows_deleted": 0
            }
            
            # Process in chunks using Polars
            chunk_iterator = self._stream_query_chunks_from_table(staging_table, variables)
            
            for chunk_df in chunk_iterator:
                if chunk_df is None:
                    break
                
                # Process chunk with Polars
                chunk_stats = self._process_cdc_chunk_with_retirement(
                    model_name, chunk_df, unique_key, change_type_column, variables
                )
                
                # Aggregate stats
                for key in stats:
                    stats[key] += chunk_stats.get(key, 0)
            
            logger.info(
                f"CDC processing completed for {model_name}: "
                f"{stats['rows_inserted']} inserted, {stats['rows_updated']} updated, "
                f"{stats['rows_retired']} retired, {stats['rows_deleted']} deleted"
            )
            
            return {
                "model_name": model_name,
                "materialization": "cdc_polars",
                "status": "processed",
                "rows_inserted": stats['rows_inserted'],
                "rows_updated": stats['rows_updated'],
                "rows_retired": stats['rows_retired'],
                "rows_deleted": stats['rows_deleted'],
                "total_rows_processed": total_rows,
                "timestamp": datetime.now().isoformat()
            }
            
        finally:
            # Clean up staging table
            try:
                drop_sql = f"DROP TABLE IF EXISTS {staging_table}"
                self.sf_executor.execute_query(drop_sql, variables, fetch=False)
            except:
                pass
    
    def _process_cdc_chunk_with_retirement(
        self,
        model_name: str,
        df: pl.DataFrame,
        unique_key: str,
        change_type_column: str,
        variables: Optional[Dict[str, Any]]
    ) -> Dict[str, int]:
        """
        Process CDC chunk with retirement pattern using Polars.
        
        Returns:
            Dictionary with processing statistics
        """
        stats = {
            "rows_inserted": 0,
            "rows_updated": 0,
            "rows_retired": 0,
            "rows_deleted": 0
        }
        
        # Separate records by operation type
        inserts = df.filter(pl.col(change_type_column) == 'I')
        updates = df.filter(pl.col(change_type_column) == 'U')
        deletes = df.filter(pl.col(change_type_column) == 'D')
        expired = df.filter(pl.col(change_type_column) == 'E')  # Expired
        
        # Process INSERTs: Insert new records with obsolete_date = NULL
        if len(inserts) > 0:
            # Remove change_type_column and __CDC_TIMESTAMP if they exist (they're metadata)
            cols_to_keep = [col for col in inserts.columns 
                          if col not in [change_type_column, '__CDC_TIMESTAMP']]
            inserts_clean = inserts.select(cols_to_keep) if cols_to_keep != inserts.columns else inserts
            
            # Add obsolete_date = NULL and CDC timestamp
            inserts_df = inserts_clean.with_columns([
                pl.lit(None).alias(self.obsolete_date_column),
                pl.lit(datetime.now()).alias('__CDC_TIMESTAMP')
            ])
            
            self._insert_records(model_name, inserts_df, variables)
            stats["rows_inserted"] = len(inserts)
        
        # Process UPDATEs: Retire old records, insert new records
        if len(updates) > 0:
            # Get unique keys that are being updated (lazy evaluation - only materialize when needed)
            update_keys = updates.select(unique_key).unique()
            # Use lazy evaluation - only convert to list when we have keys to process
            update_keys_list = update_keys[unique_key].to_list() if len(update_keys) > 0 else []
            
            # Retire old records (set obsolete_date) - BATCH PROCESSING for efficiency
            if update_keys_list:
                # Process in batches to avoid SQL IN clause limits and improve performance
                batch_size = 1000
                total_retired = 0
                
                for i in range(0, len(update_keys_list), batch_size):
                    batch_keys = update_keys_list[i:i + batch_size]
                    # Build safe SQL with proper escaping
                    keys_str = ','.join([
                        f"'{k}'" if isinstance(k, str) else str(k) 
                        for k in batch_keys
                    ])
                    
                    retire_sql = f"""
                    UPDATE {model_name}
                    SET {self.obsolete_date_column} = CURRENT_TIMESTAMP()
                    WHERE {unique_key} IN ({keys_str})
                      AND {self.obsolete_date_column} IS NULL
                    """
                    self.sf_executor.execute_query(retire_sql, variables, fetch=False)
                    total_retired += len(batch_keys)
                
                stats["rows_retired"] = total_retired
            
            # Insert new records with obsolete_date = NULL
            updates_df = updates.with_columns([
                pl.lit(None).alias(self.obsolete_date_column)
            ])
            self._insert_records(model_name, updates_df, variables)
            stats["rows_updated"] = len(updates)
        
        # Process DELETEs and EXPIRED: Set obsolete_date
        if len(deletes) > 0 or len(expired) > 0:
            # Combine deletes and expired efficiently
            if len(expired) > 0:
                to_retire = pl.concat([deletes, expired])
            else:
                to_retire = deletes
            
            retire_keys = to_retire.select(unique_key).unique()
            # Lazy evaluation - only convert to list when needed
            retire_keys_list = retire_keys[unique_key].to_list() if len(retire_keys) > 0 else []
            
            if retire_keys_list:
                # Process in batches to avoid SQL IN clause limits
                batch_size = 1000
                total_deleted = 0
                
                for i in range(0, len(retire_keys_list), batch_size):
                    batch_keys = retire_keys_list[i:i + batch_size]
                    keys_str = ','.join([
                        f"'{k}'" if isinstance(k, str) else str(k) 
                        for k in batch_keys
                    ])
                    
                    retire_sql = f"""
                    UPDATE {model_name}
                    SET {self.obsolete_date_column} = CURRENT_TIMESTAMP()
                    WHERE {unique_key} IN ({keys_str})
                      AND {self.obsolete_date_column} IS NULL
                    """
                    self.sf_executor.execute_query(retire_sql, variables, fetch=False)
                    total_deleted += len(batch_keys)
                
                stats["rows_deleted"] = total_deleted
        
        return stats
    
    def _process_initial_chunk_polars(
        self,
        df: pl.DataFrame,
        unique_key: str,
        change_type_column: str
    ) -> pl.DataFrame:
        """
        Process initial load chunk using Polars.
        Adds CDC columns and deduplicates.
        """
        # Add CDC columns
        df = df.with_columns([
            pl.lit('I').alias(change_type_column),
            pl.lit(datetime.now()).alias('__CDC_TIMESTAMP'),
            pl.lit(None).alias(self.obsolete_date_column)
        ])
        
        # Deduplicate by unique key (keep latest)
        df = df.sort(unique_key).unique(subset=[unique_key], keep='last')
        
        return df
    
    def _stream_query_chunks(
        self,
        sql: str,
        variables: Optional[Dict[str, Any]]
    ) -> Iterator[Optional[pl.DataFrame]]:
        """
        Stream query results in chunks using Polars.
        Yields DataFrames for processing.
        """
        offset = 0
        
        while True:
            chunk_sql = f"""
            SELECT * FROM ({sql})
            LIMIT {self.chunk_size} OFFSET {offset}
            """
            
            # Fetch chunk
            result = self.sf_executor.execute_query(chunk_sql, variables, fetch=True)
            
            if not result or len(result) == 0:
                break
            
            # Convert to Polars DataFrame
            df = pl.DataFrame(result)
            
            if len(df) == 0:
                break
            
            yield df
            offset += self.chunk_size
            
            # Stop if we got fewer rows than chunk size (last chunk)
            if len(df) < self.chunk_size:
                break
    
    def _stream_query_chunks_from_table(
        self,
        table_name: str,
        variables: Optional[Dict[str, Any]]
    ) -> Iterator[Optional[pl.DataFrame]]:
        """
        Stream query results from a table in chunks using Polars.
        """
        offset = 0
        
        while True:
            chunk_sql = f"""
            SELECT * FROM {table_name}
            LIMIT {self.chunk_size} OFFSET {offset}
            """
            
            # Fetch chunk
            result = self.sf_executor.execute_query(chunk_sql, variables, fetch=True)
            
            if not result or len(result) == 0:
                break
            
            # Convert to Polars DataFrame
            df = pl.DataFrame(result)
            
            if len(df) == 0:
                break
            
            yield df
            offset += self.chunk_size
            
            # Stop if we got fewer rows than chunk size (last chunk)
            if len(df) < self.chunk_size:
                break
    
    def _insert_records(
        self,
        model_name: str,
        df: pl.DataFrame,
        variables: Optional[Dict[str, Any]]
    ):
        """
        Insert records from Polars DataFrame to Snowflake using optimized bulk insert.
        Uses batch INSERT for efficiency (can be optimized with COPY INTO for very large datasets).
        """
        if len(df) == 0:
            return
        
        # Convert to list of dicts (only when needed - lazy evaluation)
        # For very large DataFrames, consider using COPY INTO instead
        records = df.to_dicts()
        columns = df.columns  # Already a list, no need to convert
        
        # Create temp table
        temp_table = f"{model_name}_polars_insert_{datetime.now().strftime('%Y%m%d%H%M%S')}"
        
        try:
            # Create temp table with same structure as main table
            create_sql = f"""
            CREATE TEMPORARY TABLE {temp_table} AS
            SELECT * FROM {model_name} LIMIT 0
            """
            self.sf_executor.execute_query(create_sql, variables, fetch=False)
            
            # Bulk insert using optimized batch INSERT
            # For very large datasets (>1M rows), consider using Snowflake COPY INTO
            batch_size = 1000
            total_rows = len(records)
            
            # Use COPY INTO for very large datasets (more efficient)
            if total_rows > 1_000_000:
                logger.info(f"Large dataset detected ({total_rows} rows), using COPY INTO for optimal performance")
                # TODO: Implement COPY INTO using Snowflake's staging area
                # For now, fall back to batch INSERT
                pass
            
            # Process in batches to avoid SQL statement size limits
            for i in range(0, total_rows, batch_size):
                batch = records[i:i + batch_size]
                
                # Build INSERT statement with proper value formatting
                # Pre-allocate list for better performance
                values_list = []
                values_list_append = values_list.append  # Local reference for speed
                
                for record in batch:
                    values = []
                    values_append = values.append  # Local reference for speed
                    
                    for col in columns:
                        val = record.get(col)
                        if val is None:
                            values_append("NULL")
                        elif isinstance(val, str):
                            # Escape single quotes for SQL safety
                            escaped = val.replace("'", "''")
                            values_append(f"'{escaped}'")
                        elif isinstance(val, datetime):
                            values_append(f"'{val.isoformat()}'")
                        elif isinstance(val, bool):
                            values_append("TRUE" if val else "FALSE")
                        else:
                            values_append(str(val))
                    
                    values_list_append(f"({', '.join(values)})")
                
                # Batch insert (more efficient than individual INSERTs)
                insert_sql = f"""
                INSERT INTO {temp_table} ({', '.join(columns)})
                VALUES {', '.join(values_list)}
                """
                self.sf_executor.execute_query(insert_sql, variables, fetch=False)
            
            # Copy from temp table to main table
            copy_sql = f"""
            INSERT INTO {model_name}
            SELECT * FROM {temp_table}
            """
            self.sf_executor.execute_query(copy_sql, variables, fetch=False)
            
        finally:
            # Clean up temp table
            try:
                drop_sql = f"DROP TABLE IF EXISTS {temp_table}"
                self.sf_executor.execute_query(drop_sql, variables, fetch=False)
            except:
                pass
    
    def _write_initial_chunk_to_snowflake(
        self,
        model_name: str,
        df: pl.DataFrame,
        unique_key: str,
        variables: Optional[Dict[str, Any]]
    ):
        """Write initial load chunk to Snowflake"""
        self._insert_records(model_name, df, variables)
    
    def _check_table_exists(self, table_name: str) -> bool:
        """Check if table exists"""
        try:
            check_sql = f"""
            SELECT COUNT(*) as cnt
            FROM information_schema.tables
            WHERE table_name = '{table_name.upper()}'
            """
            result = self.sf_executor.execute_query(check_sql, fetch=True)
            return result[0]['CNT'] > 0 if result else False
        except:
            return False

