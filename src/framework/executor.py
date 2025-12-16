"""
SQL execution engine with error handling, session variable injection, and result formatting
"""

from pathlib import Path
from typing import Dict, List, Optional, Any
from datetime import datetime
import json

from framework.connection import SnowflakeExecutor
from framework.parser import ParsedSQL, SQLParser
from framework.config import Config
from framework.model import ModelConfig
from framework.materialization import Materializer
from framework.state import StateManager
from utils.errors import ExecutionError, ModelExecutionError
from utils.logger import get_logger, ExecutionMetrics

logger = get_logger(__name__)


class ModelExecutor:
    """
    Execute SQL models with error handling and variable substitution
    """
    
    def __init__(
        self,
        snowflake_executor: SnowflakeExecutor,
        sql_parser: SQLParser,
        config: Optional[Config] = None,
        fail_fast: bool = False,
        state_manager: Optional[StateManager] = None
    ):
        """
        Initialize model executor
        
        Args:
            snowflake_executor: SnowflakeExecutor instance
            sql_parser: SQLParser instance
            config: Config instance for resolving sources
            fail_fast: If True, stop on first error. If False, continue execution.
            state_manager: Optional state manager for tracking model state
        """
        self.sf_executor = snowflake_executor
        self.parser = sql_parser
        self.config = config
        self.fail_fast = fail_fast
        self.metrics = ExecutionMetrics()
        self.materializer = Materializer(snowflake_executor, state_manager)
    
    def execute_model(
        self,
        model_name: str,
        variables: Optional[Dict[str, Any]] = None,
        dry_run: bool = False
    ) -> Dict[str, Any]:
        """
        Execute a single model
        
        Args:
            model_name: Model name to execute
            variables: Variables to substitute in SQL
            dry_run: If True, only parse and validate without executing
        
        Returns:
            Dictionary with execution results
        """
        self.metrics.start_execution(model_name)
        logger.execution_start(model_name, variables or {})
        
        start_time = datetime.now()
        
        try:
            # Find and parse model file
            model_file = self._find_model_file(model_name)
            parsed_sql = self.parser.parse_file(model_file)
            
            # Validate required variables
            self._validate_variables(parsed_sql, variables or {})
            
            # Substitute variables in SQL
            sql = self._substitute_variables(parsed_sql.parsed_sql, variables or {})
            
            # Resolve ref() and source() placeholders
            sql = self._resolve_refs_and_sources(sql, parsed_sql)
            
            if dry_run:
                logger.info(f"Dry run mode - SQL validated for {model_name}")
                result = {
                    "model_name": model_name,
                    "status": "validated",
                    "sql": sql,
                    "variables": variables
                }
            else:
                # Convert parsed config to ModelConfig
                model_config = self._create_model_config(model_name, parsed_sql.config)
                
                # Get full table name (database.schema.table) if config available
                full_table_name = self._get_full_table_name(model_name)
                
                # Use Materializer to handle materialization (table, view, CDC, etc.)
                materialization_result = self.materializer.materialize(
                    model_name=full_table_name,
                    select_sql=sql,
                    model_config=model_config,
                    variables=variables
                )
                
                duration_ms = (datetime.now() - start_time).total_seconds() * 1000
                
                logger.execution_end(model_name, duration_ms, True)
                self.metrics.end_execution(model_name, True)
                
                result = {
                    "model_name": model_name,
                    "status": "success",
                    "duration_ms": round(duration_ms, 2),
                    "variables": variables,
                    "dependencies": list(parsed_sql.dependencies),
                    "materialization": materialization_result.get("materialization", "view"),
                    "materialization_status": materialization_result.get("status", "success")
                }
            
            return result
            
        except Exception as e:
            duration_ms = (datetime.now() - start_time).total_seconds() * 1000
            
            logger.execution_error(model_name, e)
            self.metrics.end_execution(model_name, False, e)
            
            error_result = {
                "model_name": model_name,
                "status": "failed",
                "error": str(e),
                "error_type": type(e).__name__,
                "duration_ms": round(duration_ms, 2)
            }
            
            if self.fail_fast:
                raise ModelExecutionError(
                    model_name=model_name,
                    message=f"Model execution failed: {str(e)}",
                    original_error=e
                )
            else:
                # Graceful degradation - log error and continue
                logger.warning(f"Model {model_name} failed but continuing execution (fail_fast=False)")
                return error_result
    
    def execute_models(
        self,
        model_names: List[str],
        variables: Optional[Dict[str, Any]] = None,
        dry_run: bool = False
    ) -> List[Dict[str, Any]]:
        """
        Execute multiple models sequentially
        
        Args:
            model_names: List of model names to execute
            variables: Variables to substitute in SQL
            dry_run: If True, only parse and validate without executing
        
        Returns:
            List of execution results
        """
        results = []
        
        logger.info(f"Executing {len(model_names)} models")
        
        for model_name in model_names:
            result = self.execute_model(model_name, variables, dry_run)
            results.append(result)
            
            # Stop if execution failed and fail_fast is True
            if self.fail_fast and result['status'] == 'failed':
                logger.error(f"Stopping execution due to failure in {model_name}")
                break
        
        # Summary
        successful = sum(1 for r in results if r['status'] in ['success', 'validated'])
        failed = len(results) - successful
        
        logger.info(f"Execution complete: {successful} succeeded, {failed} failed")
        
        return results
    
    def _get_full_table_name(self, model_name: str) -> str:
        """
        Get full qualified table name (database.schema.table)
        
        Args:
            model_name: Model name (e.g., 'bronze.new_rfb' or 'new_rfb')
        
        Returns:
            Full qualified table name
        """
        if not self.config:
            return model_name
        
        # Get connection config for database and schema
        try:
            conn_config = self.config.get_connection_config()
            database = conn_config.get('database', '')
            schema = conn_config.get('schema', '')
            
            # If model_name already has layer prefix (e.g., 'bronze.new_rfb'), use it
            if '.' in model_name:
                parts = model_name.split('.')
                if len(parts) == 2:
                    layer, table = parts
                    if database and schema:
                        return f"{database}.{schema}.{table.upper()}"
                    elif schema:
                        return f"{schema}.{table.upper()}"
                    else:
                        return table.upper()
            
            # Otherwise, use database.schema.model_name
            if database and schema:
                return f"{database}.{schema}.{model_name.upper()}"
            elif schema:
                return f"{schema}.{model_name.upper()}"
            else:
                return model_name.upper()
        except Exception:
            # Fallback to model_name if config unavailable
            return model_name
    
    def _create_model_config(self, model_name: str, config_dict: Dict[str, Any]) -> ModelConfig:
        """
        Create ModelConfig from parsed config dictionary
        
        Args:
            model_name: Model name
            config_dict: Configuration dictionary from parsed SQL comments
        
        Returns:
            ModelConfig instance
        """
        # Parse meta section if present (for CDC config)
        meta = {}
        if 'meta' in config_dict:
            meta = config_dict['meta']
        
        # Extract unique_key (can be comma-separated for composite keys)
        unique_key = config_dict.get('unique_key')
        
        return ModelConfig(
            name=model_name,
            materialized=config_dict.get('materialized', 'view'),
            incremental_strategy=config_dict.get('incremental_strategy'),
            time_column=config_dict.get('time_column'),
            unique_key=unique_key,
            meta=meta,
            depends_on=list(config_dict.get('depends_on', []))
        )
    
    def _find_model_file(self, model_name: str) -> Path:
        """
        Find SQL file for a model with caching for performance.
        Optimized to avoid repeated filesystem scans.
        
        Args:
            model_name: Model name to find
        
        Returns:
            Path to model SQL file
        
        Raises:
            ExecutionError: If model file not found
        
        Time Complexity: O(1) with cache, O(n) without cache where n is number of files
        """
        # Use parser's cache if available
        if hasattr(self.parser, '_model_file_cache'):
            cache = self.parser._model_file_cache
            if model_name in cache:
                cached_path = cache[model_name]
                if cached_path.exists():
                    return cached_path
                # Cache entry is stale, remove it
                del cache[model_name]
        else:
            # Initialize cache if it doesn't exist
            self.parser._model_file_cache = {}
            cache = self.parser._model_file_cache
        
        models_dir = self.parser.models_dir
        
        # Try direct path first (most common case) - O(1)
        direct_path = models_dir / f"{model_name}.sql"
        if direct_path.exists():
            cache[model_name] = direct_path
            return direct_path
        
        # Search in common subdirectories first (bronze, silver, gold)
        common_dirs = ['bronze', 'silver', 'gold', 'models']
        for subdir in common_dirs:
            subdir_path = models_dir / subdir / f"{model_name}.sql"
            if subdir_path.exists():
                cache[model_name] = subdir_path
                return subdir_path
        
        # Fallback to recursive search (slower) - O(n)
        sql_files = list(models_dir.rglob(f"{model_name}.sql"))
        
        if not sql_files:
            raise ExecutionError(
                f"Model file not found: {model_name}",
                context={"models_dir": str(models_dir), "searched_pattern": f"{model_name}.sql"}
            )
        
        if len(sql_files) > 1:
            logger.warning(
                f"Multiple files found for {model_name}, using first one",
                files=[str(f) for f in sql_files]
            )
        
        # Cache the result
        result_path = sql_files[0]
        cache[model_name] = result_path
        return result_path
    
    def _validate_variables(self, parsed_sql: ParsedSQL, variables: Dict[str, Any]):
        """Validate that all required variables are provided"""
        missing_vars = parsed_sql.variables - set(variables.keys())
        
        if missing_vars:
            raise ExecutionError(
                f"Missing required variables for model {parsed_sql.model_name}",
                context={
                    "missing_variables": list(missing_vars),
                    "provided_variables": list(variables.keys())
                }
            )
    
    def _substitute_variables(self, sql: str, variables: Dict[str, Any]) -> str:
        """
        Substitute $variable patterns with values using regex for O(n) complexity.
        Optimized for performance with large SQL strings and many variables.
        
        Args:
            sql: SQL string with $variable patterns
            variables: Dictionary of variable values
        
        Returns:
            SQL with variables substituted
        
        Time Complexity: O(n) where n is SQL length (single pass)
        Previous: O(n*m) where m is number of variables (multiple passes)
        """
        import re
        
        if not variables:
            return sql
        
        # Build regex pattern for all variables at once
        # Escape variable names to prevent regex injection
        escaped_vars = {re.escape(f"${name}"): value for name, value in variables.items()}
        
        # Create pattern that matches any variable
        pattern = '|'.join(re.escape(f"${name}") for name in variables.keys())
        
        def replace_var(match: re.Match) -> str:
            """Replace matched variable with formatted value"""
            var_match = match.group(0)
            var_name = var_match[1:]  # Remove $ prefix
            
            if var_name not in variables:
                return var_match  # Return unchanged if not found
            
            var_value = variables[var_name]
            
            # Format value based on type
            if isinstance(var_value, str):
                # Escape single quotes in strings
                escaped_value = var_value.replace("'", "''")
                formatted_value = f"'{escaped_value}'"
            elif isinstance(var_value, (int, float)):
                formatted_value = str(var_value)
            elif var_value is None:
                formatted_value = "NULL"
            elif isinstance(var_value, bool):
                formatted_value = "TRUE" if var_value else "FALSE"
            else:
                # Default: convert to string and escape
                escaped_value = str(var_value).replace("'", "''")
                formatted_value = f"'{escaped_value}'"
            
            return formatted_value
        
        # Single pass substitution - O(n) complexity
        return re.sub(pattern, replace_var, sql)
    
    def _resolve_refs_and_sources(self, sql: str, parsed_sql: ParsedSQL) -> str:
        """
        Resolve ref() and source() placeholders to actual table names.
        
        - ref('model_name') -> resolves to the actual model table name
        - source('source_name', 'table_name') -> resolves to the actual source table name
        
        Args:
            sql: SQL string with __REF_*__ and __SOURCE_*__ placeholders
            parsed_sql: ParsedSQL object containing dependencies and sources
        
        Returns:
            SQL with placeholders replaced with actual table names
        """
        import re
        
        # Resolve ref() placeholders - these are model dependencies
        for model_name in parsed_sql.dependencies:
            placeholder = f"__REF_{model_name}__"
            # Replace with actual model table name (same as model name by default)
            # In production, this could resolve to schema.table_name based on config
            sql = sql.replace(placeholder, model_name)
        
        # Resolve source() placeholders - these are source table dependencies
        if parsed_sql.sources:
            for source_ref in parsed_sql.sources:
                # source_ref format: "source_name.table_name"
                if '.' in source_ref:
                    source_name, table_name = source_ref.split('.', 1)
                    placeholder = f"__SOURCE_{source_name}_{table_name}__"
                    
                    # Resolve to actual table name from sources.yml if config available
                    if self.config:
                        try:
                            actual_table = self.config.get_source_table(source_name, table_name)
                            sql = sql.replace(placeholder, actual_table)
                            logger.debug(f"Resolved source {source_ref} to {actual_table}")
                        except Exception as e:
                            logger.warning(f"Failed to resolve source {source_ref}: {e}")
                            # Fallback: use table_name directly (may work if fully qualified)
                            sql = sql.replace(placeholder, table_name)
                    else:
                        # No config available, use table_name directly
                        sql = sql.replace(placeholder, table_name)
        
        return sql
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get execution metrics"""
        return self.metrics.get_all_metrics()
    
    def export_metrics_json(self) -> str:
        """Export metrics as JSON"""
        return self.metrics.to_json()


class ResultFormatter:
    """
    Format query results in different formats
    """
    
    @staticmethod
    def to_json(results: List[Dict[str, Any]]) -> str:
        """Format results as JSON"""
        return json.dumps(results, indent=2, default=str)
    
    @staticmethod
    def to_csv(results: List[Dict[str, Any]]) -> str:
        """Format results as CSV"""
        if not results:
            return ""
        
        # Get headers
        headers = list(results[0].keys())
        
        # Build CSV
        lines = [",".join(headers)]
        for row in results:
            values = [str(row.get(h, "")) for h in headers]
            lines.append(",".join(values))
        
        return "\n".join(lines)
    
    @staticmethod
    def to_table(results: List[Dict[str, Any]]) -> str:
        """Format results as ASCII table"""
        if not results:
            return "No results"
        
        try:
            from tabulate import tabulate
            return tabulate(results, headers="keys", tablefmt="grid")
        except ImportError:
            # Fallback to simple formatting
            return ResultFormatter.to_csv(results)
    
    @staticmethod
    def format(results: List[Dict[str, Any]], format_type: str = "table") -> str:
        """
        Format results in specified format
        
        Args:
            results: Query results
            format_type: Output format (json, csv, table)
        
        Returns:
            Formatted string
        """
        if format_type == "json":
            return ResultFormatter.to_json(results)
        elif format_type == "csv":
            return ResultFormatter.to_csv(results)
        elif format_type == "table":
            return ResultFormatter.to_table(results)
        else:
            raise ValueError(f"Unsupported format: {format_type}")


class QueryExecutor:
    """
    Execute arbitrary SQL queries with variable substitution
    """
    
    def __init__(self, snowflake_executor: SnowflakeExecutor):
        """
        Initialize query executor
        
        Args:
            snowflake_executor: SnowflakeExecutor instance
        """
        self.sf_executor = snowflake_executor
    
    def execute_raw_sql(
        self,
        sql: str,
        variables: Optional[Dict[str, Any]] = None,
        fetch: bool = True,
        format_type: str = "table"
    ) -> Optional[str]:
        """
        Execute raw SQL with variable substitution
        
        Args:
            sql: SQL query
            variables: Variables to substitute
            fetch: Whether to fetch results
            format_type: Output format (json, csv, table)
        
        Returns:
            Formatted results if fetch=True, None otherwise
        """
        try:
            # Substitute variables
            if variables:
                for var_name, var_value in variables.items():
                    if isinstance(var_value, str):
                        formatted_value = f"'{var_value}'"
                    else:
                        formatted_value = str(var_value)
                    sql = sql.replace(f"${var_name}", formatted_value)
            
            # Execute
            results = self.sf_executor.execute_query(sql, variables, fetch)
            
            if fetch and results:
                return ResultFormatter.format(results, format_type)
            
            return None
            
        except Exception as e:
            logger.error(f"Failed to execute query: {e}")
            raise ExecutionError(
                f"Query execution failed: {str(e)}",
                context={"sql": sql[:200]}
            )

