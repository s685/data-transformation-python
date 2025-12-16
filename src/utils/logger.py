"""
Structured logging utilities for the data transformation framework
"""

import logging
import sys
from datetime import datetime
from typing import Any, Dict, Optional
from pathlib import Path
import json


class StructuredLogger:
    """
    Structured logger with context support for tracking model execution,
    variables, execution time, and lineage.
    """
    
    def __init__(self, name: str, log_level: str = "INFO"):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(getattr(logging, log_level.upper()))
        
        # Remove existing handlers
        self.logger.handlers = []
        
        # Console handler with formatting
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.DEBUG)
        
        # Custom formatter
        formatter = ColoredFormatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)
        
        # Context storage
        self.context: Dict[str, Any] = {}
    
    def add_context(self, **kwargs):
        """Add context to all subsequent log messages"""
        self.context.update(kwargs)
    
    def clear_context(self):
        """Clear all context"""
        self.context = {}
    
    def _format_message(self, message: str, extra: Optional[Dict[str, Any]] = None) -> str:
        """Format message with context"""
        if not self.context and not extra:
            return message
        
        context_data = {**self.context}
        if extra:
            context_data.update(extra)
        
        context_str = " | ".join([f"{k}={v}" for k, v in context_data.items()])
        return f"{message} [{context_str}]"
    
    def debug(self, message: str, **kwargs):
        """Log debug message"""
        self.logger.debug(self._format_message(message, kwargs))
    
    def info(self, message: str, **kwargs):
        """Log info message"""
        self.logger.info(self._format_message(message, kwargs))
    
    def warning(self, message: str, **kwargs):
        """Log warning message"""
        self.logger.warning(self._format_message(message, kwargs))
    
    def error(self, message: str, **kwargs):
        """Log error message"""
        self.logger.error(self._format_message(message, kwargs))
    
    def critical(self, message: str, **kwargs):
        """Log critical message"""
        self.logger.critical(self._format_message(message, kwargs))
    
    def execution_start(self, model_name: str, variables: Dict[str, Any]):
        """Log model execution start"""
        self.info(
            f"Starting execution of model: {model_name}",
            variables=json.dumps(variables),
            timestamp=datetime.now().isoformat()
        )
    
    def execution_end(self, model_name: str, duration_ms: float, success: bool):
        """Log model execution end"""
        status = "SUCCESS" if success else "FAILED"
        self.info(
            f"Completed execution of model: {model_name}",
            status=status,
            duration_ms=round(duration_ms, 2),
            timestamp=datetime.now().isoformat()
        )
    
    def execution_error(self, model_name: str, error: Exception):
        """Log model execution error with context"""
        self.error(
            f"Error executing model: {model_name}",
            error_type=type(error).__name__,
            error_message=str(error),
            timestamp=datetime.now().isoformat()
        )
    
    def dependency_resolved(self, model_name: str, dependencies: list):
        """Log dependency resolution"""
        self.debug(
            f"Resolved dependencies for model: {model_name}",
            dependencies=", ".join(dependencies)
        )
    
    def lineage_tracked(self, model_name: str, columns: Dict[str, list]):
        """Log column-level lineage tracking"""
        self.debug(
            f"Tracked lineage for model: {model_name}",
            columns=len(columns),
            lineage=json.dumps(columns)
        )


class ColoredFormatter(logging.Formatter):
    """
    Colored log formatter for better console output
    """
    
    # Color codes
    COLORS = {
        'DEBUG': '\033[36m',      # Cyan
        'INFO': '\033[32m',       # Green
        'WARNING': '\033[33m',    # Yellow
        'ERROR': '\033[31m',      # Red
        'CRITICAL': '\033[35m',   # Magenta
        'RESET': '\033[0m'        # Reset
    }
    
    def format(self, record):
        # Add color to level name
        levelname = record.levelname
        if levelname in self.COLORS:
            record.levelname = f"{self.COLORS[levelname]}{levelname}{self.COLORS['RESET']}"
        
        return super().format(record)


class ExecutionMetrics:
    """
    Track execution metrics for models
    """
    
    def __init__(self):
        self.metrics: Dict[str, Dict[str, Any]] = {}
    
    def start_execution(self, model_name: str):
        """Start tracking execution for a model"""
        self.metrics[model_name] = {
            'start_time': datetime.now(),
            'end_time': None,
            'duration_ms': None,
            'success': None,
            'error': None
        }
    
    def end_execution(self, model_name: str, success: bool, error: Optional[Exception] = None):
        """End tracking execution for a model"""
        if model_name in self.metrics:
            end_time = datetime.now()
            start_time = self.metrics[model_name]['start_time']
            duration = (end_time - start_time).total_seconds() * 1000
            
            self.metrics[model_name].update({
                'end_time': end_time,
                'duration_ms': duration,
                'success': success,
                'error': str(error) if error else None
            })
    
    def get_metrics(self, model_name: str) -> Optional[Dict[str, Any]]:
        """Get metrics for a specific model"""
        return self.metrics.get(model_name)
    
    def get_all_metrics(self) -> Dict[str, Dict[str, Any]]:
        """Get all metrics"""
        return self.metrics
    
    def clear_metrics(self):
        """Clear all metrics"""
        self.metrics = {}
    
    def to_json(self) -> str:
        """Export metrics as JSON"""
        serializable_metrics = {}
        for model, data in self.metrics.items():
            serializable_metrics[model] = {
                'start_time': data['start_time'].isoformat() if data['start_time'] else None,
                'end_time': data['end_time'].isoformat() if data['end_time'] else None,
                'duration_ms': data['duration_ms'],
                'success': data['success'],
                'error': data['error']
            }
        return json.dumps(serializable_metrics, indent=2)


# Global logger instance
_global_logger: Optional[StructuredLogger] = None


def get_logger(name: str = "framework", log_level: str = "INFO") -> StructuredLogger:
    """
    Get or create a logger instance
    
    Args:
        name: Logger name
        log_level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    
    Returns:
        StructuredLogger instance
    """
    global _global_logger
    if _global_logger is None:
        _global_logger = StructuredLogger(name, log_level)
    return _global_logger


def setup_file_logging(log_dir: Path, log_level: str = "INFO"):
    """
    Setup file logging in addition to console logging
    
    Args:
        log_dir: Directory to store log files
        log_level: Log level for file logging
    """
    log_dir = Path(log_dir)
    log_dir.mkdir(parents=True, exist_ok=True)
    
    log_file = log_dir / f"framework_{datetime.now().strftime('%Y%m%d')}.log"
    
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(getattr(logging, log_level.upper()))
    
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    file_handler.setFormatter(formatter)
    
    # Add to root logger
    logging.getLogger().addHandler(file_handler)

