"""
File system watcher for hot-reload with thread-safe cache updates
"""

from pathlib import Path
from typing import Callable, Optional
from threading import Thread, Lock
import time

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler, FileModifiedEvent, FileCreatedEvent, FileDeletedEvent

from utils.logger import get_logger
from utils.errors import WatcherError

logger = get_logger(__name__)


class ModelFileHandler(FileSystemEventHandler):
    """
    Handler for model file changes
    """
    
    def __init__(
        self,
        models_dir: Path,
        on_change_callback: Optional[Callable[[str, str], None]] = None
    ):
        """
        Initialize file handler
        
        Args:
            models_dir: Directory to watch
            on_change_callback: Callback function (event_type, file_path)
        """
        super().__init__()
        self.models_dir = Path(models_dir)
        self.on_change_callback = on_change_callback
        self._lock = Lock()
    
    def on_modified(self, event):
        """Handle file modification"""
        if not event.is_directory and event.src_path.endswith(('.sql', '.yml', '.yaml')):
            with self._lock:
                logger.debug(f"File modified: {event.src_path}")
                if self.on_change_callback:
                    self.on_change_callback('modified', event.src_path)
    
    def on_created(self, event):
        """Handle file creation"""
        if not event.is_directory and event.src_path.endswith(('.sql', '.yml', '.yaml')):
            with self._lock:
                logger.debug(f"File created: {event.src_path}")
                if self.on_change_callback:
                    self.on_change_callback('created', event.src_path)
    
    def on_deleted(self, event):
        """Handle file deletion"""
        if not event.is_directory and event.src_path.endswith(('.sql', '.yml', '.yaml')):
            with self._lock:
                logger.debug(f"File deleted: {event.src_path}")
                if self.on_change_callback:
                    self.on_change_callback('deleted', event.src_path)


class FileWatcher:
    """
    Watch model files for changes and trigger reload
    """
    
    def __init__(
        self,
        models_dir: Path,
        on_change_callback: Optional[Callable[[str, str], None]] = None
    ):
        """
        Initialize file watcher
        
        Args:
            models_dir: Directory to watch
            on_change_callback: Callback for file changes
        """
        self.models_dir = Path(models_dir)
        self.on_change_callback = on_change_callback
        
        self.observer = Observer()
        self.handler = ModelFileHandler(models_dir, on_change_callback)
        self._running = False
    
    def start(self):
        """Start watching for file changes"""
        if self._running:
            logger.warning("Watcher already running")
            return
        
        try:
            self.observer.schedule(
                self.handler,
                str(self.models_dir),
                recursive=True
            )
            self.observer.start()
            self._running = True
            
            logger.info(f"Started watching directory: {self.models_dir}")
            
        except Exception as e:
            raise WatcherError(
                f"Failed to start file watcher: {str(e)}",
                context={"models_dir": str(self.models_dir)}
            )
    
    def stop(self):
        """Stop watching for file changes"""
        if not self._running:
            return
        
        try:
            self.observer.stop()
            self.observer.join(timeout=5)
            self._running = False
            
            logger.info("Stopped file watcher")
            
        except Exception as e:
            logger.error(f"Error stopping watcher: {e}")
    
    def is_running(self) -> bool:
        """Check if watcher is running"""
        return self._running


def create_file_watcher(
    models_dir: Path,
    on_change_callback: Optional[Callable[[str, str], None]] = None
) -> FileWatcher:
    """
    Create a file watcher instance
    
    Args:
        models_dir: Directory to watch
        on_change_callback: Callback for file changes
    
    Returns:
        FileWatcher instance
    """
    return FileWatcher(models_dir, on_change_callback)

