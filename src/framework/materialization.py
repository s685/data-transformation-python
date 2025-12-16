"""
Materialization strategies for models (view, table, incremental)
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
from datetime import datetime, timedelta

from framework.connection import SnowflakeExecutor
from framework.model import ModelConfig
from framework.state import StateManager
from utils.errors import MaterializationError, ExecutionError
from utils.logger import get_logger

logger = get_logger(__name__)


class MaterializationStrategy(ABC):
    """
    Abstract base class for materialization strategies.
    Implements polymorphism pattern with method overriding.
    """
    
    def __init__(
        self,
        sf_executor: SnowflakeExecutor,
        state_manager: Optional[StateManager] = None
    ):
        """
        Initialize materialization strategy
        
        Args:
            sf_executor: Snowflake executor
            state_manager: State manager for tracking incremental state
        """
        self.sf_executor = sf_executor
        self.state_manager = state_manager
    
    @abstractmethod
    def materialize(
        self,
        model_name: str,
        select_sql: str,
        model_config: Optional[ModelConfig] = None,
        variables: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Materialize a model (abstract method - must be overridden)
        
        Args:
            model_name: Model name
            select_sql: SELECT statement
            model_config: Model configuration
            variables: Variables for SQL execution
        
        Returns:
            Materialization result dictionary
        
        Raises:
            MaterializationError: If materialization fails
        """
        pass
    
    def _check_table_exists(self, table_name: str) -> bool:
        """
        Check if table exists in Snowflake.
        Shared utility method for all strategies.
        
        Args:
            table_name: Table name to check
        
        Returns:
            True if table exists, False otherwise
        """
        try:
            check_sql = f"""
            SELECT COUNT(*) as cnt
            FROM information_schema.tables
            WHERE table_name = '{table_name.upper()}'
            """
            result = self.sf_executor.execute_query(check_sql, fetch=True)
            return result[0]['CNT'] > 0 if result else False
        except (ExecutionError, Exception) as e:
            logger.warning(f"Failed to check table existence for {table_name}: {e}")
            return False


class ViewMaterialization(MaterializationStrategy):
    """
    View materialization strategy
    Creates or replaces a view
    """
    
    def materialize(
        self,
        model_name: str,
        select_sql: str,
        model_config: Optional[ModelConfig] = None,
        variables: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Create or replace view"""
        try:
            create_view_sql = f"""
            CREATE OR REPLACE VIEW {model_name} AS
            {select_sql}
            """
            
            self.sf_executor.execute_query(create_view_sql, variables, fetch=False)
            
            logger.info(f"Created view: {model_name}")
            
            return {
                "model_name": model_name,
                "materialization": "view",
                "status": "success",
                "timestamp": datetime.now().isoformat()
            }
            
        except (ExecutionError, MaterializationError) as e:
            logger.error(f"Failed to create view {model_name}: {e}")
            raise MaterializationError(
                f"View materialization failed for {model_name}",
                context={"error": str(e), "error_type": type(e).__name__}
            ) from e
        except Exception as e:
            logger.error(f"Unexpected error creating view {model_name}: {e}")
            raise MaterializationError(
                f"View materialization failed for {model_name}",
                context={"error": str(e), "error_type": type(e).__name__}
            ) from e


class TableMaterialization(MaterializationStrategy):
    """
    Table materialization strategy
    Creates or replaces a table
    """
    
    def materialize(
        self,
        model_name: str,
        select_sql: str,
        model_config: Optional[ModelConfig] = None,
        variables: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Create or replace table"""
        try:
            create_table_sql = f"""
            CREATE OR REPLACE TABLE {model_name} AS
            {select_sql}
            """
            
            self.sf_executor.execute_query(create_table_sql, variables, fetch=False)
            
            logger.info(f"Created table: {model_name}")
            
            return {
                "model_name": model_name,
                "materialization": "table",
                "status": "success",
                "timestamp": datetime.now().isoformat()
            }
            
        except (ExecutionError, MaterializationError) as e:
            logger.error(f"Failed to create table {model_name}: {e}")
            raise MaterializationError(
                f"Table materialization failed for {model_name}",
                context={"error": str(e), "error_type": type(e).__name__}
            ) from e
        except Exception as e:
            logger.error(f"Unexpected error creating table {model_name}: {e}")
            raise MaterializationError(
                f"Table materialization failed for {model_name}",
                context={"error": str(e), "error_type": type(e).__name__}
            ) from e


class IncrementalMaterialization(MaterializationStrategy):
    """
    Incremental materialization strategy
    Supports time-based, unique-key, and append strategies
    """
    
    def materialize(
        self,
        model_name: str,
        select_sql: str,
        model_config: Optional[ModelConfig] = None,
        variables: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Incremental materialization"""
        if not model_config:
            raise MaterializationError(
                f"Model config required for incremental materialization: {model_name}"
            )
        
        strategy = model_config.incremental_strategy
        
        if strategy == 'time':
            return self._time_based_incremental(model_name, select_sql, model_config, variables)
        elif strategy == 'unique_key':
            return self._unique_key_incremental(model_name, select_sql, model_config, variables)
        elif strategy == 'append':
            return self._append_incremental(model_name, select_sql, model_config, variables)
        else:
            raise MaterializationError(
                f"Unknown incremental strategy: {strategy}",
                context={"model_name": model_name}
            )
    
    def _time_based_incremental(
        self,
        model_name: str,
        select_sql: str,
        model_config: ModelConfig,
        variables: Optional[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Time-based incremental strategy
        Only process data after last processed timestamp
        """
        try:
            time_column = model_config.time_column
            if not time_column:
                raise MaterializationError(
                    f"time_column required for time-based incremental: {model_name}"
                )
            
            # Check if table exists
            table_exists = self._check_table_exists(model_name)
            
            if not table_exists:
                # Initial load - create table
                create_sql = f"CREATE TABLE {model_name} AS {select_sql}"
                self.sf_executor.execute_query(create_sql, variables, fetch=False)
                
                # Save state
                if self.state_manager:
                    self.state_manager.update_incremental_state(
                        model_name,
                        'last_processed_time',
                        datetime.now().isoformat()
                    )
                
                logger.info(f"Initial load for incremental model: {model_name}")
                
                return {
                    "model_name": model_name,
                    "materialization": "incremental",
                    "strategy": "time",
                    "status": "initial_load",
                    "timestamp": datetime.now().isoformat()
                }
            else:
                # Incremental update - get last processed time
                last_time = None
                if self.state_manager:
                    last_time = self.state_manager.get_incremental_state(
                        model_name,
                        'last_processed_time'
                    )
                
                if not last_time:
                    # Get max time from existing table
                    max_time_sql = f"SELECT MAX({time_column}) as max_time FROM {model_name}"
                    result = self.sf_executor.execute_query(max_time_sql, variables)
                    if result and result[0].get('MAX_TIME'):
                        last_time = result[0]['MAX_TIME']
                
                # Insert new data
                if last_time:
                    insert_sql = f"""
                    INSERT INTO {model_name}
                    {select_sql}
                    WHERE {time_column} > '{last_time}'
                    """
                else:
                    insert_sql = f"INSERT INTO {model_name} {select_sql}"
                
                self.sf_executor.execute_query(insert_sql, variables, fetch=False)
                
                # Update state
                if self.state_manager:
                    self.state_manager.update_incremental_state(
                        model_name,
                        'last_processed_time',
                        datetime.now().isoformat()
                    )
                
                logger.info(f"Incremental update for model: {model_name}")
                
                return {
                    "model_name": model_name,
                    "materialization": "incremental",
                    "strategy": "time",
                    "status": "updated",
                    "last_processed_time": last_time,
                    "timestamp": datetime.now().isoformat()
                }
                
        except (ExecutionError, MaterializationError) as e:
            logger.error(f"Time-based incremental failed for {model_name}: {e}")
            raise MaterializationError(
                f"Time-based incremental failed for {model_name}",
                context={"error": str(e), "error_type": type(e).__name__}
            ) from e
        except Exception as e:
            logger.error(f"Unexpected error in time-based incremental for {model_name}: {e}")
            raise MaterializationError(
                f"Time-based incremental failed for {model_name}",
                context={"error": str(e), "error_type": type(e).__name__}
            ) from e
    
    def _unique_key_incremental(
        self,
        model_name: str,
        select_sql: str,
        model_config: ModelConfig,
        variables: Optional[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Unique key incremental strategy
        Merge based on unique key
        """
        try:
            unique_key = model_config.unique_key
            if not unique_key:
                raise MaterializationError(
                    f"unique_key required for unique_key incremental: {model_name}"
                )
            
            # Check if table exists
            table_exists = self._check_table_exists(model_name)
            
            if not table_exists:
                # Initial load
                create_sql = f"CREATE TABLE {model_name} AS {select_sql}"
                self.sf_executor.execute_query(create_sql, variables, fetch=False)
                
                logger.info(f"Initial load for incremental model: {model_name}")
                
                return {
                    "model_name": model_name,
                    "materialization": "incremental",
                    "strategy": "unique_key",
                    "status": "initial_load",
                    "timestamp": datetime.now().isoformat()
                }
            else:
                # Merge using temp table
                temp_table = f"{model_name}_temp"
                
                # Create temp table with new data
                create_temp_sql = f"CREATE OR REPLACE TEMP TABLE {temp_table} AS {select_sql}"
                self.sf_executor.execute_query(create_temp_sql, variables, fetch=False)
                
                # Merge into main table
                merge_sql = f"""
                MERGE INTO {model_name} target
                USING {temp_table} source
                ON target.{unique_key} = source.{unique_key}
                WHEN MATCHED THEN UPDATE SET *
                WHEN NOT MATCHED THEN INSERT *
                """
                
                self.sf_executor.execute_query(merge_sql, variables, fetch=False)
                
                logger.info(f"Merged incremental update for model: {model_name}")
                
                return {
                    "model_name": model_name,
                    "materialization": "incremental",
                    "strategy": "unique_key",
                    "status": "merged",
                    "timestamp": datetime.now().isoformat()
                }
                
        except (ExecutionError, MaterializationError) as e:
            logger.error(f"Unique key incremental failed for {model_name}: {e}")
            raise MaterializationError(
                f"Unique key incremental failed for {model_name}",
                context={"error": str(e), "error_type": type(e).__name__}
            ) from e
        except Exception as e:
            logger.error(f"Unexpected error in unique key incremental for {model_name}: {e}")
            raise MaterializationError(
                f"Unique key incremental failed for {model_name}",
                context={"error": str(e), "error_type": type(e).__name__}
            ) from e
    
    def _append_incremental(
        self,
        model_name: str,
        select_sql: str,
        model_config: ModelConfig,
        variables: Optional[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Append-only incremental strategy
        Always append new data
        """
        try:
            # Check if table exists
            table_exists = self._check_table_exists(model_name)
            
            if not table_exists:
                # Initial load
                create_sql = f"CREATE TABLE {model_name} AS {select_sql}"
                self.sf_executor.execute_query(create_sql, variables, fetch=False)
                
                logger.info(f"Initial load for incremental model: {model_name}")
                
                return {
                    "model_name": model_name,
                    "materialization": "incremental",
                    "strategy": "append",
                    "status": "initial_load",
                    "timestamp": datetime.now().isoformat()
                }
            else:
                # Append new data
                insert_sql = f"INSERT INTO {model_name} {select_sql}"
                self.sf_executor.execute_query(insert_sql, variables, fetch=False)
                
                logger.info(f"Appended data to model: {model_name}")
                
                return {
                    "model_name": model_name,
                    "materialization": "incremental",
                    "strategy": "append",
                    "status": "appended",
                    "timestamp": datetime.now().isoformat()
                }
                
        except (ExecutionError, MaterializationError) as e:
            logger.error(f"Append incremental failed for {model_name}: {e}")
            raise MaterializationError(
                f"Append incremental failed for {model_name}",
                context={"error": str(e), "error_type": type(e).__name__}
            ) from e
        except Exception as e:
            logger.error(f"Unexpected error in append incremental for {model_name}: {e}")
            raise MaterializationError(
                f"Append incremental failed for {model_name}",
                context={"error": str(e), "error_type": type(e).__name__}
            ) from e
    
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


class TempTableMaterialization(MaterializationStrategy):
    """
    Temporary table materialization strategy
    Creates a temporary table that exists only for the session
    """
    
    def materialize(
        self,
        model_name: str,
        select_sql: str,
        model_config: Optional[ModelConfig] = None,
        variables: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Create temporary table"""
        try:
            create_temp_sql = f"""
            CREATE OR REPLACE TEMPORARY TABLE {model_name} AS
            {select_sql}
            """
            
            self.sf_executor.execute_query(create_temp_sql, variables, fetch=False)
            
            logger.info(f"Created temporary table: {model_name}")
            
            return {
                "model_name": model_name,
                "materialization": "temp_table",
                "status": "success",
                "timestamp": datetime.now().isoformat()
            }
            
        except (ExecutionError, MaterializationError) as e:
            logger.error(f"Failed to create temp table {model_name}: {e}")
            raise MaterializationError(
                f"Temp table materialization failed for {model_name}",
                context={"error": str(e), "error_type": type(e).__name__}
            ) from e
        except Exception as e:
            logger.error(f"Unexpected error creating temp table {model_name}: {e}")
            raise MaterializationError(
                f"Temp table materialization failed for {model_name}",
                context={"error": str(e), "error_type": type(e).__name__}
            ) from e


class CDCMaterialization(MaterializationStrategy):
    """
    Change Data Capture (CDC) materialization strategy
    Tracks changes using CDC patterns (INSERT, UPDATE, DELETE)
    """
    
    def materialize(
        self,
        model_name: str,
        select_sql: str,
        model_config: Optional[ModelConfig] = None,
        variables: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        CDC materialization with change tracking
        Requires: unique_key, cdc_columns (optional)
        """
        if not model_config:
            raise MaterializationError(
                f"Model config required for CDC materialization: {model_name}"
            )
        
        unique_key = model_config.unique_key
        if not unique_key:
            raise MaterializationError(
                f"unique_key required for CDC materialization: {model_name}"
            )
        
        # Get CDC configuration
        cdc_config = model_config.meta.get('cdc', {})
        cdc_columns = cdc_config.get('columns', [])
        change_type_column = cdc_config.get('change_type_column', '__CDC_OPERATION')
        
        try:
            # Check if CDC table exists
            table_exists = self._check_table_exists(model_name)
            
            if not table_exists:
                # Initial load - create table with CDC columns
                create_sql = f"""
                CREATE TABLE {model_name} AS
                SELECT 
                    *,
                    'I' as {change_type_column},
                    CURRENT_TIMESTAMP() as __CDC_TIMESTAMP
                FROM ({select_sql})
                """
                self.sf_executor.execute_query(create_sql, variables, fetch=False)
                
                logger.info(f"Initial CDC load for model: {model_name}")
                
                return {
                    "model_name": model_name,
                    "materialization": "cdc",
                    "status": "initial_load",
                    "timestamp": datetime.now().isoformat()
                }
            else:
                # CDC merge - handle INSERT, UPDATE, DELETE
                temp_table = f"{model_name}_temp"
                
                # Create temp table with CDC data
                create_temp_sql = f"""
                CREATE OR REPLACE TEMP TABLE {temp_table} AS
                {select_sql}
                """
                self.sf_executor.execute_query(create_temp_sql, variables, fetch=False)
                
                # Merge with CDC logic
                merge_sql = f"""
                MERGE INTO {model_name} target
                USING (
                    SELECT 
                        *,
                        COALESCE({change_type_column}, 'U') as {change_type_column},
                        CURRENT_TIMESTAMP() as __CDC_TIMESTAMP
                    FROM {temp_table}
                ) source
                ON target.{unique_key} = source.{unique_key}
                WHEN MATCHED AND source.{change_type_column} = 'D' THEN DELETE
                WHEN MATCHED AND source.{change_type_column} IN ('U', 'I') THEN 
                    UPDATE SET *
                WHEN NOT MATCHED AND source.{change_type_column} != 'D' THEN 
                    INSERT *
                """
                
                self.sf_executor.execute_query(merge_sql, variables, fetch=False)
                
                logger.info(f"CDC merge completed for model: {model_name}")
                
                return {
                    "model_name": model_name,
                    "materialization": "cdc",
                    "status": "merged",
                    "timestamp": datetime.now().isoformat()
                }
                
        except (ExecutionError, MaterializationError) as e:
            logger.error(f"CDC materialization failed for {model_name}: {e}")
            raise MaterializationError(
                f"CDC materialization failed for {model_name}",
                context={"error": str(e), "error_type": type(e).__name__}
            ) from e
        except Exception as e:
            logger.error(f"Unexpected error in CDC materialization for {model_name}: {e}")
            raise MaterializationError(
                f"CDC materialization failed for {model_name}",
                context={"error": str(e), "error_type": type(e).__name__}
            ) from e


class MaterializationStrategyFactory:
    """
    Factory pattern for creating materialization strategies.
    Implements singleton-like caching for strategy instances.
    """
    
    _strategy_cache: Dict[str, MaterializationStrategy] = {}
    _lock = None
    
    @classmethod
    def _get_lock(cls):
        """Lazy initialization of lock for thread safety"""
        if cls._lock is None:
            from threading import Lock
            cls._lock = Lock()
        return cls._lock
    
    @classmethod
    def create_strategy(
        cls,
        materialization_type: str,
        sf_executor: SnowflakeExecutor,
        state_manager: Optional[StateManager] = None
    ) -> MaterializationStrategy:
        """
        Create or retrieve a materialization strategy instance.
        Uses caching to avoid creating duplicate instances.
        
        Args:
            materialization_type: Type of materialization (view, table, etc.)
            sf_executor: Snowflake executor
            state_manager: State manager
        
        Returns:
            MaterializationStrategy instance
        
        Raises:
            MaterializationError: If materialization type is unknown
        """
        # Normalize materialization type
        materialization_type = materialization_type.lower()
        
        # Handle aliases
        if materialization_type == 'incremental_table':
            materialization_type = 'incremental'
        
        # Check cache first
        cache_key = f"{materialization_type}_{id(sf_executor)}_{id(state_manager)}"
        
        with cls._get_lock():
            if cache_key in cls._strategy_cache:
                return cls._strategy_cache[cache_key]
            
            # Create new strategy based on type
            if materialization_type == 'view':
                strategy = ViewMaterialization(sf_executor, state_manager)
            elif materialization_type == 'table':
                strategy = TableMaterialization(sf_executor, state_manager)
            elif materialization_type == 'temp_table':
                strategy = TempTableMaterialization(sf_executor, state_manager)
            elif materialization_type == 'incremental':
                strategy = IncrementalMaterialization(sf_executor, state_manager)
            elif materialization_type == 'cdc':
                strategy = CDCMaterialization(sf_executor, state_manager)
            else:
                raise MaterializationError(
                    f"Unknown materialization type: {materialization_type}",
                    context={"supported_types": ["view", "table", "temp_table", "incremental", "cdc"]}
                )
            
            # Cache the strategy
            cls._strategy_cache[cache_key] = strategy
            return strategy
    
    @classmethod
    def clear_cache(cls):
        """Clear the strategy cache"""
        with cls._get_lock():
            cls._strategy_cache.clear()


class Materializer:
    """
    Main materializer that selects appropriate strategy using factory pattern.
    Demonstrates polymorphism by using base class interface.
    """
    
    def __init__(
        self,
        sf_executor: SnowflakeExecutor,
        state_manager: Optional[StateManager] = None
    ):
        """
        Initialize materializer
        
        Args:
            sf_executor: Snowflake executor
            state_manager: State manager
        """
        self.sf_executor = sf_executor
        self.state_manager = state_manager
        self.factory = MaterializationStrategyFactory
    
    def materialize(
        self,
        model_name: str,
        select_sql: str,
        model_config: Optional[ModelConfig] = None,
        variables: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Materialize a model using appropriate strategy.
        Uses factory pattern to get strategy and polymorphism to execute it.
        
        Args:
            model_name: Model name
            select_sql: SELECT statement
            model_config: Model configuration
            variables: Variables for SQL execution
        
        Returns:
            Materialization result
        
        Raises:
            MaterializationError: If materialization fails
        """
        # Determine materialization strategy type
        materialization_type = 'view'  # default
        if model_config:
            materialization_type = model_config.materialized
        
        # Get strategy using factory (polymorphism - all strategies implement same interface)
        strategy = self.factory.create_strategy(
            materialization_type,
            self.sf_executor,
            self.state_manager
        )
        
        logger.info(f"Materializing {model_name} using {materialization_type} strategy")
        
        # Execute using polymorphism - method overriding in subclasses
        return strategy.materialize(model_name, select_sql, model_config, variables)

