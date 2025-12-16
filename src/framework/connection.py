"""
Snowflake connection pool with retry logic, health checks, and session variable support
Supports multiple authentication methods: password, SSO, and private key pair
"""

import time
import os
from typing import Dict, Any, Optional, List
from contextlib import contextmanager
from threading import Lock
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend
import snowflake.connector
from snowflake.connector import DictCursor
from snowflake.connector.errors import Error as SnowflakeError

from utils.errors import (
    ConnectionError as FrameworkConnectionError,
    TransientConnectionError,
    QueryTimeoutError,
    ConfigurationError
)
from utils.logger import get_logger

logger = get_logger(__name__)


class ConnectionPool:
    """
    Connection pool for Snowflake with health checks and retry logic
    """
    
    def __init__(
        self,
        connection_config: Dict[str, Any],
        pool_size: int = 1,
        max_retries: int = 3,
        retry_delay: float = 1.0,
        query_timeout: int = 300,
        lazy_init: bool = True
    ):
        """
        Initialize connection pool
        
        Args:
            connection_config: Snowflake connection configuration
            pool_size: Maximum number of connections in pool (default: 1 for single operations)
            max_retries: Maximum retry attempts for failed operations
            retry_delay: Initial delay between retries (exponential backoff)
            query_timeout: Query timeout in seconds
            lazy_init: If True, create connections on-demand. If False, pre-create all connections.
        """
        self.connection_config = connection_config
        self.pool_size = pool_size
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.query_timeout = query_timeout
        self.lazy_init = lazy_init
        
        self._pool: List[snowflake.connector.SnowflakeConnection] = []
        self._lock = Lock()
        self._initialized = False
    
    def initialize(self):
        """
        Initialize the connection pool
        If lazy_init is True, only marks as initialized (connections created on-demand)
        If lazy_init is False, pre-creates all connections
        """
        if self._initialized:
            return
        
        if self.lazy_init:
            # Lazy initialization - connections created on-demand
            logger.info(f"Connection pool configured with lazy initialization (max size: {self.pool_size}, lazy_init: {self.lazy_init})")
            self._initialized = True
        else:
            # Pre-create all connections
            logger.info(f"Initializing connection pool with size {self.pool_size}")
            
            with self._lock:
                for i in range(self.pool_size):
                    try:
                        conn = self._create_connection()
                        self._pool.append(conn)
                        logger.debug(f"Created connection {i+1}/{self.pool_size}")
                    except Exception as e:
                        logger.error(f"Failed to create connection {i+1}: {e}")
                        # Continue with smaller pool size
                
                if not self._pool:
                    raise FrameworkConnectionError(
                        "Failed to create any connections in pool",
                        context={"pool_size": self.pool_size}
                    )
                
                self._initialized = True
                logger.info(f"Connection pool initialized with {len(self._pool)} connections")
    
    def _create_connection(self) -> snowflake.connector.SnowflakeConnection:
        """
        Create a new Snowflake connection
        Supports multiple authentication methods:
        - Password authentication (default)
        - SSO (Single Sign-On) via external browser or OAuth
        - Private key pair authentication
        """
        try:
            # Base connection parameters
            conn_params = {
                'account': self.connection_config.get('account'),
                'user': self.connection_config.get('user'),
                'warehouse': self.connection_config.get('warehouse'),
                'database': self.connection_config.get('database'),
                'schema': self.connection_config.get('schema'),
                'role': self.connection_config.get('role'),
                'session_parameters': {
                    'QUERY_TAG': 'data-transformation-framework'
                }
            }
            
            # Determine authentication method based on what's actually configured
            # Priority: explicit authenticator > password (if no authenticator specified)
            authenticator = self.connection_config.get('authenticator')
            
            if authenticator in ['externalbrowser', 'oauth']:
                # SSO Authentication (explicit)
                conn_params['authenticator'] = authenticator
                if authenticator == 'oauth' and 'token' in self.connection_config:
                    conn_params['token'] = self.connection_config.get('token')
                logger.debug(f"Using SSO authentication: {authenticator}")
                
            elif authenticator == 'snowflake' and 'private_key' in self.connection_config:
                # Private key pair authentication (explicit)
                private_key = self._load_private_key(
                    self.connection_config.get('private_key'),
                    self.connection_config.get('private_key_passphrase')
                )
                conn_params['private_key'] = private_key
                logger.debug("Using private key pair authentication")
                
            elif 'password' in self.connection_config:
                # Password authentication (default when no authenticator specified)
                conn_params['password'] = self.connection_config.get('password')
                logger.debug("Using password authentication")
                
            else:
                raise ConfigurationError(
                    "No valid authentication method found. Provide one of: "
                    "password, authenticator='externalbrowser', authenticator='oauth' with token, "
                    "or authenticator='snowflake' with private_key"
                )
            
            conn = snowflake.connector.connect(**conn_params)
            return conn
            
        except SnowflakeError as e:
            raise FrameworkConnectionError(
                f"Failed to create Snowflake connection: {str(e)}",
                context={"account": self.connection_config.get('account')}
            )
        except Exception as e:
            raise FrameworkConnectionError(
                f"Failed to create Snowflake connection: {str(e)}",
                context={"account": self.connection_config.get('account')}
            )
    
    def _load_private_key(
        self, 
        private_key_path_or_pem: str, 
        passphrase: Optional[str] = None
    ) -> bytes:
        """
        Load private key from file path or PEM string
        
        Args:
            private_key_path_or_pem: Path to private key file or PEM string
            passphrase: Optional passphrase for encrypted private key
        
        Returns:
            Private key as bytes
        """
        try:
            # Check if it's a file path or PEM string
            if os.path.exists(private_key_path_or_pem):
                # Load from file
                with open(private_key_path_or_pem, 'rb') as key_file:
                    private_key_pem = key_file.read()
            else:
                # Assume it's a PEM string
                private_key_pem = private_key_path_or_pem.encode('utf-8')
            
            # Load the private key
            private_key = serialization.load_pem_private_key(
                private_key_pem,
                password=passphrase.encode('utf-8') if passphrase else None,
                backend=default_backend()
            )
            
            # Serialize to PKCS8 format (Snowflake requires this format)
            pkb = private_key.private_bytes(
                encoding=serialization.Encoding.DER,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption()
            )
            
            return pkb
            
        except FileNotFoundError:
            raise ConfigurationError(
                f"Private key file not found: {private_key_path_or_pem}"
            )
        except Exception as e:
            raise ConfigurationError(
                f"Failed to load private key: {str(e)}"
            )
    
    @contextmanager
    def get_connection(self):
        """
        Get a connection from the pool (context manager)
        Creates connections on-demand if lazy_init is True
        
        Yields:
            Snowflake connection
        """
        if not self._initialized:
            self.initialize()
        
        conn = None
        try:
            with self._lock:
                if self._pool:
                    # Reuse existing connection from pool
                    conn = self._pool.pop(0)
                elif len(self._pool) < self.pool_size:
                    # Create new connection if under pool size limit
                    conn = self._create_connection()
                    logger.debug(f"Created new connection on-demand (pool: {len(self._pool) + 1}/{self.pool_size}, lazy_init: {self.lazy_init})")
                else:
                    # Pool exhausted, create new connection (will be closed after use)
                    logger.debug("Pool size limit reached, creating temporary connection")
                    conn = self._create_connection()
            
            # Check connection health
            if not self._is_connection_healthy(conn):
                logger.warning("Unhealthy connection detected, creating new one")
                try:
                    conn.close()
                except:
                    pass
                conn = self._create_connection()
            
            yield conn
            
        finally:
            # Return connection to pool if there's room
            if conn:
                with self._lock:
                    if len(self._pool) < self.pool_size:
                        self._pool.append(conn)
                        logger.debug(f"Returned connection to pool ({len(self._pool)}/{self.pool_size})")
                    else:
                        # Pool is full, close the connection
                        try:
                            conn.close()
                            logger.debug("Pool full, closed connection")
                        except:
                            pass
    
    def _is_connection_healthy(self, conn: snowflake.connector.SnowflakeConnection) -> bool:
        """Check if connection is healthy"""
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            cursor.close()
            return True
        except:
            return False
    
    def health_check(self) -> bool:
        """
        Perform health check on the connection pool
        
        Returns:
            True if pool is healthy
        """
        if not self._initialized:
            return False
        
        healthy_connections = 0
        
        with self._lock:
            for conn in self._pool:
                if self._is_connection_healthy(conn):
                    healthy_connections += 1
        
        logger.debug(
            f"Health check: {healthy_connections}/{len(self._pool)} connections healthy"
        )
        
        return healthy_connections > 0
    
    def close_all(self):
        """Close all connections in the pool"""
        logger.info("Closing all connections in pool")
        
        with self._lock:
            for conn in self._pool:
                try:
                    conn.close()
                except:
                    pass
            self._pool = []
            self._initialized = False
        
        logger.info("All connections closed")


class SnowflakeExecutor:
    """
    Snowflake query executor with retry logic and session variable support
    """
    
    def __init__(self, connection_pool: ConnectionPool):
        """
        Initialize executor
        
        Args:
            connection_pool: ConnectionPool instance
        """
        self.pool = connection_pool
        self.current_variables: Dict[str, Any] = {}
    
    def set_session_variables(self, variables: Dict[str, Any]):
        """
        Set session variables for subsequent queries
        
        Args:
            variables: Dictionary of variable name -> value
        """
        self.current_variables = variables.copy()
        logger.debug(f"Set {len(variables)} session variables")
    
    def execute_query(
        self,
        sql: str,
        variables: Optional[Dict[str, Any]] = None,
        fetch: bool = True
    ) -> Optional[List[Dict[str, Any]]]:
        """
        Execute a SQL query with retry logic
        
        Args:
            sql: SQL query to execute
            variables: Session variables to set (merged with current_variables)
            fetch: Whether to fetch results
        
        Returns:
            Query results as list of dictionaries, or None if not fetched
        """
        # Merge variables
        all_variables = {**self.current_variables}
        if variables:
            all_variables.update(variables)
        
        # Retry logic with exponential backoff
        last_error = None
        
        for attempt in range(self.pool.max_retries):
            try:
                with self.pool.get_connection() as conn:
                    cursor = conn.cursor(DictCursor)
                    
                    # Set session variables
                    if all_variables:
                        self._set_snowflake_session_vars(cursor, all_variables)
                    
                    # Execute query
                    logger.debug(f"Executing query (attempt {attempt + 1}/{self.pool.max_retries})")
                    cursor.execute(sql)
                    
                    # Fetch results if requested
                    if fetch:
                        results = cursor.fetchall()
                        cursor.close()
                        logger.debug(f"Query returned {len(results)} rows")
                        return results
                    else:
                        cursor.close()
                        return None
                        
            except SnowflakeError as e:
                last_error = e
                error_code = getattr(e, 'errno', None)
                
                # Check if error is retryable
                if self._is_retryable_error(error_code):
                    delay = self.pool.retry_delay * (2 ** attempt)
                    logger.warning(
                        f"Retryable error on attempt {attempt + 1}: {e}. "
                        f"Retrying in {delay}s..."
                    )
                    time.sleep(delay)
                    continue
                else:
                    # Non-retryable error, raise immediately
                    raise FrameworkConnectionError(
                        f"Query execution failed: {str(e)}",
                        context={"sql": sql[:200], "error_code": error_code}
                    )
            
            except Exception as e:
                last_error = e
                logger.error(f"Unexpected error on attempt {attempt + 1}: {e}")
                # Don't retry unexpected errors
                break
        
        # All retries exhausted
        raise TransientConnectionError(
            f"Query execution failed after {self.pool.max_retries} attempts: {str(last_error)}",
            retry_count=self.pool.max_retries,
            max_retries=self.pool.max_retries
        )
    
    def _set_snowflake_session_vars(self, cursor, variables: Dict[str, Any]):
        """Set session variables in Snowflake"""
        for var_name, var_value in variables.items():
            # Format value based on type
            if isinstance(var_value, str):
                formatted_value = f"'{var_value}'"
            else:
                formatted_value = str(var_value)
            
            try:
                cursor.execute(f"SET {var_name} = {formatted_value}")
            except Exception as e:
                logger.warning(f"Failed to set session variable {var_name}: {e}")
    
    def _is_retryable_error(self, error_code: Optional[int]) -> bool:
        """
        Check if error is retryable
        
        Common retryable Snowflake error codes:
        - 253001: Internal error
        - 253003: Connection was lost
        - 253008: Session token expired
        - 390114: Query timeout
        """
        retryable_codes = {253001, 253003, 253008, 390114}
        return error_code in retryable_codes if error_code else False
    
    def execute_transaction(self, queries: List[str], variables: Optional[Dict[str, Any]] = None):
        """
        Execute multiple queries in a transaction
        
        Args:
            queries: List of SQL queries
            variables: Session variables
        """
        all_variables = {**self.current_variables}
        if variables:
            all_variables.update(variables)
        
        try:
            with self.pool.get_connection() as conn:
                cursor = conn.cursor()
                
                try:
                    # Set session variables
                    if all_variables:
                        self._set_snowflake_session_vars(cursor, all_variables)
                    
                    # Begin transaction
                    cursor.execute("BEGIN TRANSACTION")
                    
                    # Execute all queries
                    for i, sql in enumerate(queries):
                        logger.debug(f"Executing transaction query {i+1}/{len(queries)}")
                        cursor.execute(sql)
                    
                    # Commit transaction
                    cursor.execute("COMMIT")
                    logger.info(f"Transaction completed successfully ({len(queries)} queries)")
                    
                except Exception as e:
                    # Rollback on error
                    logger.error(f"Transaction failed, rolling back: {e}")
                    cursor.execute("ROLLBACK")
                    raise FrameworkConnectionError(
                        f"Transaction failed: {str(e)}",
                        context={"queries_count": len(queries)}
                    )
                finally:
                    cursor.close()
                    
        except Exception as e:
            raise FrameworkConnectionError(
                f"Failed to execute transaction: {str(e)}",
                context={"queries_count": len(queries)}
            )
    
    def test_connection(self) -> bool:
        """
        Test Snowflake connection
        
        Returns:
            True if connection is successful
        """
        try:
            result = self.execute_query("SELECT CURRENT_VERSION()")
            if result:
                version = result[0].get('CURRENT_VERSION()', 'unknown')
                logger.info(f"Connected to Snowflake version: {version}")
                return True
            return False
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False


def create_connection_pool(connection_config: Dict[str, Any], **kwargs) -> ConnectionPool:
    """
    Create and initialize a connection pool
    
    Args:
        connection_config: Snowflake connection configuration
        **kwargs: Additional pool configuration
    
    Returns:
        Initialized ConnectionPool
    """
    pool = ConnectionPool(connection_config, **kwargs)
    pool.initialize()
    return pool


def create_executor(connection_config: Dict[str, Any], **kwargs) -> SnowflakeExecutor:
    """
    Create a Snowflake executor with connection pool
    
    Args:
        connection_config: Snowflake connection configuration
        **kwargs: Additional pool configuration
    
    Returns:
        SnowflakeExecutor instance
    """
    pool = create_connection_pool(connection_config, **kwargs)
    return SnowflakeExecutor(pool)

