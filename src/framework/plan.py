"""
Plan-based execution system (SQLMesh-style)
"""

from typing import Dict, List, Set, Optional, Any
from dataclasses import dataclass
from enum import Enum
import json

from framework.state import StateManager
from framework.dependency import DependencyGraph
from utils.errors import PlanError
from utils.logger import get_logger

logger = get_logger(__name__)


class ChangeType(Enum):
    """Type of change for a model"""
    CREATE = "create"
    UPDATE = "update"
    DELETE = "delete"
    NO_CHANGE = "no_change"


@dataclass
class ModelChange:
    """Represents a change to a model"""
    model_name: str
    change_type: ChangeType
    reason: str
    dependencies_affected: List[str]
    estimated_runtime: Optional[float] = None


class ExecutionPlan:
    """
    Execution plan showing what will change before execution
    """
    
    def __init__(self):
        """Initialize execution plan"""
        self.changes: List[ModelChange] = []
        self.execution_order: List[List[str]] = []
        self.total_models: int = 0
        self.created_at: str = ""
    
    def add_change(self, change: ModelChange):
        """Add a model change to the plan"""
        self.changes.append(change)
    
    def get_changes_by_type(self, change_type: ChangeType) -> List[ModelChange]:
        """Get all changes of a specific type"""
        return [c for c in self.changes if c.change_type == change_type]
    
    def get_summary(self) -> Dict[str, Any]:
        """Get plan summary"""
        creates = len(self.get_changes_by_type(ChangeType.CREATE))
        updates = len(self.get_changes_by_type(ChangeType.UPDATE))
        deletes = len(self.get_changes_by_type(ChangeType.DELETE))
        no_changes = len(self.get_changes_by_type(ChangeType.NO_CHANGE))
        
        return {
            "total_models": self.total_models,
            "creates": creates,
            "updates": updates,
            "deletes": deletes,
            "no_changes": no_changes,
            "execution_levels": len(self.execution_order),
            "created_at": self.created_at
        }
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert plan to dictionary"""
        return {
            "changes": [
                {
                    "model_name": c.model_name,
                    "change_type": c.change_type.value,
                    "reason": c.reason,
                    "dependencies_affected": c.dependencies_affected
                }
                for c in self.changes
            ],
            "execution_order": self.execution_order,
            "summary": self.get_summary()
        }
    
    def to_json(self) -> str:
        """Export plan as JSON"""
        return json.dumps(self.to_dict(), indent=2)


class PlanGenerator:
    """
    Generate execution plans by comparing current and desired state
    """
    
    def __init__(
        self,
        state_manager: StateManager,
        dependency_graph: DependencyGraph
    ):
        """
        Initialize plan generator
        
        Args:
            state_manager: State manager
            dependency_graph: Dependency graph
        """
        self.state_manager = state_manager
        self.dependency_graph = dependency_graph
    
    def generate_plan(
        self,
        current_models: Dict[str, Any],
        models_to_run: Optional[List[str]] = None,
        full_refresh: bool = False
    ) -> ExecutionPlan:
        """
        Generate execution plan
        
        Args:
            current_models: Current models in codebase (parsed)
            models_to_run: Specific models to run, or None for all
            full_refresh: Force execution of all models
        
        Returns:
            ExecutionPlan
        """
        from datetime import datetime
        
        plan = ExecutionPlan()
        plan.created_at = datetime.now().isoformat()
        
        # Determine which models need to run
        if models_to_run:
            # Run specific models + their dependencies
            models_set = set(models_to_run)
            for model in models_to_run:
                models_set.update(self.dependency_graph.get_all_dependencies(model))
        else:
            # Run all models
            models_set = set(current_models.keys())
        
        # Analyze changes for each model
        for model_name in models_set:
            if model_name not in current_models:
                continue
            
            model_info = current_models[model_name]
            
            # Check if model has changed
            if full_refresh:
                change_type = ChangeType.UPDATE
                reason = "Full refresh requested"
            else:
                change_type, reason = self._determine_change_type(
                    model_name,
                    model_info
                )
            
            # Get affected dependencies
            affected = list(self.dependency_graph.get_dependents(model_name))
            
            change = ModelChange(
                model_name=model_name,
                change_type=change_type,
                reason=reason,
                dependencies_affected=affected
            )
            
            plan.add_change(change)
        
        # Calculate execution order
        models_to_execute = [
            c.model_name for c in plan.changes
            if c.change_type in [ChangeType.CREATE, ChangeType.UPDATE]
        ]
        
        if models_to_execute:
            plan.execution_order = self.dependency_graph.get_execution_order(
                models_to_execute
            )
        
        plan.total_models = len(models_set)
        
        logger.info(f"Generated execution plan: {plan.get_summary()}")
        
        return plan
    
    def _determine_change_type(
        self,
        model_name: str,
        model_info: Dict[str, Any]
    ) -> tuple[ChangeType, str]:
        """
        Determine if model has changed
        
        Returns:
            Tuple of (ChangeType, reason)
        """
        state = self.state_manager.get_model_state(model_name)
        
        if not state:
            return ChangeType.CREATE, "New model"
        
        file_hash = model_info.get('file_hash', '')
        config_hash = model_info.get('config_hash')
        
        # Check file change
        if state.file_hash != file_hash:
            return ChangeType.UPDATE, "Model file changed"
        
        # Check config change
        if config_hash and state.config_hash != config_hash:
            return ChangeType.UPDATE, "Model configuration changed"
        
        # Check dependency changes
        current_deps = set(model_info.get('dependencies', []))
        state_deps = set(state.dependencies)
        
        if current_deps != state_deps:
            return ChangeType.UPDATE, "Dependencies changed"
        
        return ChangeType.NO_CHANGE, "No changes detected"


def create_plan_generator(
    state_manager: StateManager,
    dependency_graph: DependencyGraph
) -> PlanGenerator:
    """
    Create a plan generator instance
    
    Args:
        state_manager: State manager
        dependency_graph: Dependency graph
    
    Returns:
        PlanGenerator instance
    """
    return PlanGenerator(state_manager, dependency_graph)

