"""
Custom exceptions for the data transformation framework
"""

from typing import Optional, Dict, Any


class FrameworkError(Exception):
    """Base exception for all framework errors"""
    
    def __init__(self, message: str, context: Optional[Dict[str, Any]] = None):
        super().__init__(message)
        self.message = message
        self.context = context or {}
    
    def __str__(self):
        if self.context:
            context_str = ", ".join([f"{k}={v}" for k, v in self.context.items()])
            return f"{self.message} [Context: {context_str}]"
        return self.message


class ConfigurationError(FrameworkError):
    """Raised when there's a configuration error"""
    pass


class ConnectionError(FrameworkError):
    """Raised when there's a Snowflake connection error"""
    pass


class SQLParseError(FrameworkError):
    """Raised when SQL parsing fails"""
    pass


class DependencyError(FrameworkError):
    """Raised when there's a dependency-related error"""
    pass


class CircularDependencyError(DependencyError):
    """Raised when a circular dependency is detected"""
    pass


class ModelNotFoundError(FrameworkError):
    """Raised when a model cannot be found"""
    pass


class ExecutionError(FrameworkError):
    """Raised when SQL execution fails"""
    pass


class MaterializationError(FrameworkError):
    """Raised when model materialization fails"""
    pass


class StateError(FrameworkError):
    """Raised when there's a state management error"""
    pass


class PlanError(FrameworkError):
    """Raised when plan generation or execution fails"""
    pass


class TestError(FrameworkError):
    """Raised when a test fails"""
    pass


class ValidationError(FrameworkError):
    """Raised when validation fails"""
    pass


class BackfillError(FrameworkError):
    """Raised when backfill operation fails"""
    pass


class WatcherError(FrameworkError):
    """Raised when file watcher encounters an error"""
    pass


# Error severity levels
class ErrorSeverity:
    """Error severity levels for graceful degradation"""
    CRITICAL = "CRITICAL"  # Framework cannot continue
    ERROR = "ERROR"        # Model failed but framework can continue
    WARNING = "WARNING"    # Issue detected but execution can proceed
    INFO = "INFO"          # Informational message


class RecoverableError(FrameworkError):
    """
    Base class for errors that the framework can recover from
    These errors won't stop execution
    """
    severity = ErrorSeverity.ERROR


class NonRecoverableError(FrameworkError):
    """
    Base class for errors that require framework to stop
    """
    severity = ErrorSeverity.CRITICAL


class ModelExecutionError(RecoverableError):
    """Raised when a single model execution fails (recoverable)"""
    
    def __init__(self, model_name: str, message: str, original_error: Optional[Exception] = None):
        context = {
            "model_name": model_name,
            "original_error": str(original_error) if original_error else None
        }
        super().__init__(message, context)
        self.model_name = model_name
        self.original_error = original_error


class RetryableError(RecoverableError):
    """
    Base class for errors that should trigger a retry
    """
    
    def __init__(self, message: str, retry_count: int = 0, max_retries: int = 3, **kwargs):
        context = {
            "retry_count": retry_count,
            "max_retries": max_retries,
            **kwargs
        }
        super().__init__(message, context)
        self.retry_count = retry_count
        self.max_retries = max_retries
    
    def should_retry(self) -> bool:
        """Check if error should trigger a retry"""
        return self.retry_count < self.max_retries


class TransientConnectionError(RetryableError):
    """Raised when there's a transient connection error that can be retried"""
    pass


class QueryTimeoutError(RetryableError):
    """Raised when a query times out"""
    pass

