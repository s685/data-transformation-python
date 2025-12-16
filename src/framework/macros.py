"""
SQL Macros for CDC and common transformations
"""

from typing import Dict, Any, Optional
from jinja2 import Environment
from utils.logger import get_logger

logger = get_logger(__name__)


class CDCMacros:
    """
    CDC (Change Data Capture) macros for SQL generation
    """
    
    @staticmethod
    def cdc_merge(
        target_table: str,
        source_table: str,
        unique_key: str,
        change_type_column: str = '__CDC_OPERATION',
        timestamp_column: str = '__CDC_TIMESTAMP'
    ) -> str:
        """
        Generate CDC MERGE statement
        
        Args:
            target_table: Target table name
            source_table: Source table name
            unique_key: Unique key column for matching
            change_type_column: Column containing CDC operation (I/U/D)
            timestamp_column: Column for CDC timestamp
        
        Returns:
            MERGE SQL statement
        """
        return f"""
        MERGE INTO {target_table} target
        USING (
            SELECT 
                *,
                COALESCE({change_type_column}, 'U') as {change_type_column},
                CURRENT_TIMESTAMP() as {timestamp_column}
            FROM {source_table}
        ) source
        ON target.{unique_key} = source.{unique_key}
        WHEN MATCHED AND source.{change_type_column} = 'D' THEN DELETE
        WHEN MATCHED AND source.{change_type_column} IN ('U', 'I') THEN 
            UPDATE SET *
        WHEN NOT MATCHED AND source.{change_type_column} != 'D' THEN 
            INSERT *
        """
    
    @staticmethod
    def cdc_columns(
        change_type_column: str = '__CDC_OPERATION',
        timestamp_column: str = '__CDC_TIMESTAMP'
    ) -> str:
        """
        Generate CDC column definitions
        
        Args:
            change_type_column: CDC operation column name
            timestamp_column: CDC timestamp column name
        
        Returns:
            Column definitions SQL
        """
        return f"""
        {change_type_column} VARCHAR(1) DEFAULT 'I',
        {timestamp_column} TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        """
    
    @staticmethod
    def cdc_filter(
        change_type_column: str = '__CDC_OPERATION',
        operations: list = None
    ) -> str:
        """
        Generate CDC filter condition
        
        Args:
            change_type_column: CDC operation column name
            operations: List of operations to include (I, U, D)
        
        Returns:
            WHERE clause for filtering CDC operations
        """
        if operations is None:
            operations = ['I', 'U', 'D']
        
        ops_str = "', '".join(operations)
        return f"{change_type_column} IN ('{ops_str}')"


class LayerMacros:
    """
    Layer-specific macros for bronze, silver, gold
    """
    
    @staticmethod
    def bronze_load(
        source_table: str,
        filter_condition: Optional[str] = None,
        columns: Optional[list] = None
    ) -> str:
        """
        Generate bronze layer load statement
        
        Args:
            source_table: Source table name
            filter_condition: Optional WHERE clause
            columns: Optional column list (if None, selects all)
        
        Returns:
            SELECT statement for bronze load
        """
        cols = "*" if not columns else ", ".join(columns)
        where = f"WHERE {filter_condition}" if filter_condition else ""
        
        return f"""
        SELECT 
            {cols},
            CURRENT_TIMESTAMP() as load_timestamp,
            'bronze' as layer
        FROM {source_table}
        {where}
        """
    
    @staticmethod
    def silver_clean(
        source_table: str,
        dedupe_key: Optional[str] = None,
        filter_condition: Optional[str] = None
    ) -> str:
        """
        Generate silver layer cleaning statement
        
        Args:
            source_table: Source table name
            dedupe_key: Optional unique key for deduplication
            filter_condition: Optional WHERE clause for data quality
        
        Returns:
            SELECT statement for silver cleaning
        """
        if dedupe_key:
            dedupe = f"""
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY {dedupe_key} 
                ORDER BY load_timestamp DESC
            ) = 1
            """
        else:
            dedupe = ""
        
        where = f"WHERE {filter_condition}" if filter_condition else ""
        
        return f"""
        SELECT *
        FROM {source_table}
        {where}
        {dedupe}
        """
    
    @staticmethod
    def gold_aggregate(
        source_table: str,
        group_by_columns: list,
        aggregate_columns: Dict[str, str]  # {alias: expression}
    ) -> str:
        """
        Generate gold layer aggregation statement
        
        Args:
            source_table: Source table name
            group_by_columns: Columns to group by
            aggregate_columns: Dictionary of {alias: aggregate_expression}
        
        Returns:
            SELECT statement for gold aggregation
        """
        group_by = ", ".join(group_by_columns)
        aggregates = ", ".join([
            f"{expr} as {alias}" 
            for alias, expr in aggregate_columns.items()
        ])
        
        return f"""
        SELECT 
            {group_by},
            {aggregates}
        FROM {source_table}
        GROUP BY {group_by}
        """


def register_macros(jinja_env: Environment):
    """
    Register all macros with Jinja2 environment
    
    Args:
        jinja_env: Jinja2 environment
    """
    # CDC macros
    jinja_env.globals['cdc_merge'] = CDCMacros.cdc_merge
    jinja_env.globals['cdc_columns'] = CDCMacros.cdc_columns
    jinja_env.globals['cdc_filter'] = CDCMacros.cdc_filter
    
    # Layer macros
    jinja_env.globals['bronze_load'] = LayerMacros.bronze_load
    jinja_env.globals['silver_clean'] = LayerMacros.silver_clean
    jinja_env.globals['gold_aggregate'] = LayerMacros.gold_aggregate
    
    logger.debug("Registered SQL macros with Jinja2 environment")

