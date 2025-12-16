"""
State management system for tracking model versions and execution history
"""

import json
import hashlib
from pathlib import Path
from typing import Dict, List, Optional, Any, Set
from dataclasses import dataclass, asdict
from datetime import datetime
from threading import Lock

from utils.errors import StateError
from utils.logger import get_logger

logger = get_logger(__name__)


@dataclass
class ModelState:
    """State information for a single model"""
    model_name: str
    file_hash: str
    last_executed: Optional[datetime] = None
    last_success: Optional[datetime] = None
    last_failure: Optional[datetime] = None
    execution_count: int = 0
    success_count: int = 0
    failure_count: int = 0
    dependencies: List[str] = None
    config_hash: Optional[str] = None
    incremental_state: Optional[Dict[str, Any]] = None  # For incremental models
    
    def __post_init__(self):
        if self.dependencies is None:
            self.dependencies = []
        if self.incremental_state is None:
            self.incremental_state = {}
    
    def mark_execution(self, success: bool):
        """Mark an execution and update counters"""
        self.last_executed = datetime.now()
        self.execution_count += 1
        
        if success:
            self.last_success = datetime.now()
            self.success_count += 1
        else:
            self.last_failure = datetime.now()
            self.failure_count += 1
    
    def update_incremental_state(self, key: str, value: Any):
        """Update incremental state (e.g., last processed timestamp)"""
        self.incremental_state[key] = value
    
    def get_incremental_state(self, key: str, default: Any = None) -> Any:
        """Get incremental state value"""
        return self.incremental_state.get(key, default)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization"""
        return {
            "model_name": self.model_name,
            "file_hash": self.file_hash,
            "last_executed": self.last_executed.isoformat() if self.last_executed else None,
            "last_success": self.last_success.isoformat() if self.last_success else None,
            "last_failure": self.last_failure.isoformat() if self.last_failure else None,
            "execution_count": self.execution_count,
            "success_count": self.success_count,
            "failure_count": self.failure_count,
            "dependencies": self.dependencies,
            "config_hash": self.config_hash,
            "incremental_state": self.incremental_state
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ModelState':
        """Create ModelState from dictionary"""
        return cls(
            model_name=data['model_name'],
            file_hash=data['file_hash'],
            last_executed=datetime.fromisoformat(data['last_executed']) if data.get('last_executed') else None,
            last_success=datetime.fromisoformat(data['last_success']) if data.get('last_success') else None,
            last_failure=datetime.fromisoformat(data['last_failure']) if data.get('last_failure') else None,
            execution_count=data.get('execution_count', 0),
            success_count=data.get('success_count', 0),
            failure_count=data.get('failure_count', 0),
            dependencies=data.get('dependencies', []),
            config_hash=data.get('config_hash'),
            incremental_state=data.get('incremental_state', {})
        )


class StateManager:
    """
    Manages state for models including versions, execution history, and incremental state
    """
    
    def __init__(self, state_dir: Path, environment: str = "dev"):
        """
        Initialize state manager
        
        Args:
            state_dir: Directory to store state files
            environment: Environment name (dev, prod, etc.)
        """
        self.state_dir = Path(state_dir)
        self.environment = environment
        self.state_file = self.state_dir / f"state_{environment}.json"
        
        # Create state directory if it doesn't exist
        self.state_dir.mkdir(parents=True, exist_ok=True)
        
        # Model states
        self.models: Dict[str, ModelState] = {}
        
        # Lock for thread-safe operations
        self._lock = Lock()
        
        # Load existing state
        self._load_state()
    
    def _load_state(self):
        """Load state from disk"""
        if not self.state_file.exists():
            logger.info(f"No existing state file for environment: {self.environment}")
            return
        
        try:
            with open(self.state_file, 'r') as f:
                data = json.load(f)
            
            # Load model states
            if 'models' in data:
                for model_data in data['models']:
                    model_state = ModelState.from_dict(model_data)
                    self.models[model_state.model_name] = model_state
            
            logger.info(f"Loaded state for {len(self.models)} models from {self.state_file}")
            
        except Exception as e:
            logger.error(f"Failed to load state: {e}")
            raise StateError(
                f"Failed to load state from {self.state_file}",
                context={"error": str(e)}
            )
    
    def _save_state(self):
        """Save state to disk (thread-safe)"""
        try:
            with self._lock:
                data = {
                    "environment": self.environment,
                    "last_updated": datetime.now().isoformat(),
                    "models": [model.to_dict() for model in self.models.values()]
                }
                
                # Write to temporary file first
                temp_file = self.state_file.with_suffix('.tmp')
                with open(temp_file, 'w') as f:
                    json.dump(data, f, indent=2)
                
                # Atomic rename
                temp_file.replace(self.state_file)
            
            logger.debug(f"Saved state for {len(self.models)} models")
            
        except Exception as e:
            logger.error(f"Failed to save state: {e}")
            raise StateError(
                f"Failed to save state to {self.state_file}",
                context={"error": str(e)}
            )
    
    def get_model_state(self, model_name: str) -> Optional[ModelState]:
        """
        Get state for a specific model
        
        Args:
            model_name: Model name
        
        Returns:
            ModelState if exists, None otherwise
        """
        with self._lock:
            return self.models.get(model_name)
    
    def update_model_state(
        self,
        model_name: str,
        file_hash: str,
        dependencies: List[str],
        config_hash: Optional[str] = None
    ):
        """
        Update or create model state
        
        Args:
            model_name: Model name
            file_hash: Hash of model file content
            dependencies: List of model dependencies
            config_hash: Hash of model configuration
        """
        with self._lock:
            if model_name in self.models:
                state = self.models[model_name]
                state.file_hash = file_hash
                state.dependencies = dependencies
                state.config_hash = config_hash
            else:
                state = ModelState(
                    model_name=model_name,
                    file_hash=file_hash,
                    dependencies=dependencies,
                    config_hash=config_hash
                )
                self.models[model_name] = state
        
        self._save_state()
        logger.debug(f"Updated state for model: {model_name}")
    
    def mark_execution(self, model_name: str, success: bool):
        """
        Mark a model execution
        
        Args:
            model_name: Model name
            success: Whether execution was successful
        """
        with self._lock:
            if model_name in self.models:
                self.models[model_name].mark_execution(success)
            else:
                # Create new state if doesn't exist
                state = ModelState(model_name=model_name, file_hash="")
                state.mark_execution(success)
                self.models[model_name] = state
        
        self._save_state()
    
    def has_changed(self, model_name: str, file_hash: str, config_hash: Optional[str] = None) -> bool:
        """
        Check if model has changed since last execution
        
        Args:
            model_name: Model name
            file_hash: Current file hash
            config_hash: Current config hash
        
        Returns:
            True if model has changed
        """
        state = self.get_model_state(model_name)
        
        if not state:
            return True  # New model
        
        # Check file hash
        if state.file_hash != file_hash:
            return True
        
        # Check config hash if provided
        if config_hash and state.config_hash != config_hash:
            return True
        
        return False
    
    def get_changed_models(self, current_models: Dict[str, Any]) -> Set[str]:
        """
        Get list of models that have changed
        
        Args:
            current_models: Dictionary of model_name -> {file_hash, config_hash, dependencies}
        
        Returns:
            Set of changed model names
        """
        changed = set()
        
        for model_name, model_info in current_models.items():
            file_hash = model_info.get('file_hash', '')
            config_hash = model_info.get('config_hash')
            
            if self.has_changed(model_name, file_hash, config_hash):
                changed.add(model_name)
        
        # Check for removed models
        existing_models = set(self.models.keys())
        current_model_names = set(current_models.keys())
        removed = existing_models - current_model_names
        
        logger.info(
            f"State comparison: {len(changed)} changed, {len(removed)} removed models"
        )
        
        return changed
    
    def update_incremental_state(self, model_name: str, key: str, value: Any):
        """
        Update incremental state for a model
        
        Args:
            model_name: Model name
            key: State key (e.g., 'last_processed_date')
            value: State value
        """
        with self._lock:
            if model_name in self.models:
                self.models[model_name].update_incremental_state(key, value)
            else:
                state = ModelState(model_name=model_name, file_hash="")
                state.update_incremental_state(key, value)
                self.models[model_name] = state
        
        self._save_state()
        logger.debug(f"Updated incremental state for {model_name}: {key}={value}")
    
    def get_incremental_state(self, model_name: str, key: str, default: Any = None) -> Any:
        """
        Get incremental state for a model
        
        Args:
            model_name: Model name
            key: State key
            default: Default value if not found
        
        Returns:
            State value or default
        """
        state = self.get_model_state(model_name)
        if state:
            return state.get_incremental_state(key, default)
        return default
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get state statistics"""
        total_models = len(self.models)
        total_executions = sum(m.execution_count for m in self.models.values())
        total_successes = sum(m.success_count for m in self.models.values())
        total_failures = sum(m.failure_count for m in self.models.values())
        
        return {
            "environment": self.environment,
            "total_models": total_models,
            "total_executions": total_executions,
            "total_successes": total_successes,
            "total_failures": total_failures,
            "success_rate": round(total_successes / total_executions * 100, 2) if total_executions > 0 else 0
        }
    
    def clear_state(self, model_name: Optional[str] = None):
        """
        Clear state for a model or all models
        
        Args:
            model_name: Specific model to clear, or None to clear all
        """
        with self._lock:
            if model_name:
                if model_name in self.models:
                    del self.models[model_name]
                    logger.info(f"Cleared state for model: {model_name}")
            else:
                self.models = {}
                logger.info("Cleared all state")
        
        self._save_state()
    
    def export_state(self, output_file: Path):
        """Export state to a file"""
        with self._lock:
            data = {
                "environment": self.environment,
                "exported_at": datetime.now().isoformat(),
                "models": [model.to_dict() for model in self.models.values()]
            }
        
        with open(output_file, 'w') as f:
            json.dump(data, f, indent=2)
        
        logger.info(f"Exported state to {output_file}")
    
    def import_state(self, input_file: Path):
        """Import state from a file"""
        with open(input_file, 'r') as f:
            data = json.load(f)
        
        with self._lock:
            self.models = {}
            if 'models' in data:
                for model_data in data['models']:
                    model_state = ModelState.from_dict(model_data)
                    self.models[model_state.model_name] = model_state
        
        self._save_state()
        logger.info(f"Imported state from {input_file} ({len(self.models)} models)")


def create_state_manager(state_dir: Path, environment: str = "dev") -> StateManager:
    """
    Create a state manager instance
    
    Args:
        state_dir: Directory to store state files
        environment: Environment name
    
    Returns:
        StateManager instance
    """
    return StateManager(state_dir, environment)

