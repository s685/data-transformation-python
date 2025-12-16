"""
Dependency graph builder with topological sort and column-level lineage
"""

from typing import Dict, List, Set, Optional, Tuple
from collections import defaultdict, deque
from dataclasses import dataclass
import asyncio

from utils.errors import CircularDependencyError, DependencyError, ModelNotFoundError
from utils.logger import get_logger
from utils.lineage import LineageTracker, ModelLineage

logger = get_logger(__name__)


@dataclass
class DependencyNode:
    """Represents a node in the dependency graph"""
    name: str
    dependencies: Set[str]
    dependents: Set[str]
    level: int = 0  # Execution level (topologically sorted)


class DependencyGraph:
    """
    Dependency graph with topological sort and circular dependency detection
    """
    
    def __init__(self):
        """Initialize dependency graph"""
        self.nodes: Dict[str, DependencyNode] = {}
        self.lineage_tracker = LineageTracker()
    
    def add_model(self, model_name: str, dependencies: Set[str], lineage: Optional[ModelLineage] = None):
        """
        Add a model to the dependency graph
        
        Args:
            model_name: Model name
            dependencies: Set of model names this model depends on
            lineage: Optional ModelLineage for column-level lineage tracking
        """
        if model_name not in self.nodes:
            self.nodes[model_name] = DependencyNode(
                name=model_name,
                dependencies=set(),
                dependents=set()
            )
        
        node = self.nodes[model_name]
        node.dependencies = dependencies.copy()
        
        # Update dependents for upstream models
        for dep in dependencies:
            if dep not in self.nodes:
                self.nodes[dep] = DependencyNode(
                    name=dep,
                    dependencies=set(),
                    dependents=set()
                )
            self.nodes[dep].dependents.add(model_name)
        
        # Add lineage if provided
        if lineage:
            self.lineage_tracker.add_model_lineage(lineage)
        
        logger.debug(f"Added model to graph: {model_name} (deps: {len(dependencies)})")
    
    def remove_model(self, model_name: str):
        """
        Remove a model from the dependency graph
        
        Args:
            model_name: Model name to remove
        """
        if model_name not in self.nodes:
            return
        
        node = self.nodes[model_name]
        
        # Remove from dependents of upstream models
        for dep in node.dependencies:
            if dep in self.nodes:
                self.nodes[dep].dependents.discard(model_name)
        
        # Remove from dependencies of downstream models
        for dependent in node.dependents:
            if dependent in self.nodes:
                self.nodes[dependent].dependencies.discard(model_name)
        
        del self.nodes[model_name]
        logger.debug(f"Removed model from graph: {model_name}")
    
    def get_dependencies(self, model_name: str) -> Set[str]:
        """
        Get direct dependencies of a model
        
        Args:
            model_name: Model name
        
        Returns:
            Set of dependency names
        """
        if model_name not in self.nodes:
            return set()
        return self.nodes[model_name].dependencies.copy()
    
    def get_dependents(self, model_name: str) -> Set[str]:
        """
        Get direct dependents of a model
        
        Args:
            model_name: Model name
        
        Returns:
            Set of dependent names
        """
        if model_name not in self.nodes:
            return set()
        return self.nodes[model_name].dependents.copy()
    
    def get_all_dependencies(self, model_name: str) -> Set[str]:
        """
        Get all transitive dependencies of a model
        
        Args:
            model_name: Model name
        
        Returns:
            Set of all dependency names (transitive)
        """
        if model_name not in self.nodes:
            return set()
        
        all_deps = set()
        visited = set()
        
        def traverse(name: str):
            if name in visited:
                return
            visited.add(name)
            
            if name in self.nodes:
                for dep in self.nodes[name].dependencies:
                    all_deps.add(dep)
                    traverse(dep)
        
        traverse(model_name)
        return all_deps
    
    def get_all_dependents(self, model_name: str) -> Set[str]:
        """
        Get all transitive dependents of a model
        
        Args:
            model_name: Model name
        
        Returns:
            Set of all dependent names (transitive)
        """
        if model_name not in self.nodes:
            return set()
        
        all_dependents = set()
        visited = set()
        
        def traverse(name: str):
            if name in visited:
                return
            visited.add(name)
            
            if name in self.nodes:
                for dependent in self.nodes[name].dependents:
                    all_dependents.add(dependent)
                    traverse(dependent)
        
        traverse(model_name)
        return all_dependents
    
    def detect_circular_dependencies(self) -> Optional[List[str]]:
        """
        Detect circular dependencies in the graph
        
        Returns:
            List representing the circular dependency path, or None if no cycles
        """
        visited = set()
        rec_stack = set()
        path = []
        
        def dfs(node_name: str) -> bool:
            visited.add(node_name)
            rec_stack.add(node_name)
            path.append(node_name)
            
            if node_name in self.nodes:
                for dep in self.nodes[node_name].dependencies:
                    if dep not in visited:
                        if dfs(dep):
                            return True
                    elif dep in rec_stack:
                        # Found cycle
                        cycle_start = path.index(dep)
                        return True
            
            path.pop()
            rec_stack.remove(node_name)
            return False
        
        for node_name in self.nodes:
            if node_name not in visited:
                if dfs(node_name):
                    return path
        
        return None
    
    def topological_sort(self) -> List[List[str]]:
        """
        Perform topological sort on the dependency graph
        
        Returns:
            List of lists, where each inner list contains models that can be executed in parallel
        
        Raises:
            CircularDependencyError: If circular dependency is detected
        """
        # Check for circular dependencies first
        cycle = self.detect_circular_dependencies()
        if cycle:
            raise CircularDependencyError(
                "Circular dependency detected",
                context={"cycle": " -> ".join(cycle)}
            )
        
        # Calculate in-degree for each node
        in_degree = {name: len(node.dependencies) for name, node in self.nodes.items()}
        
        # Find all nodes with in-degree 0
        queue = deque([name for name, degree in in_degree.items() if degree == 0])
        
        result = []
        current_level = []
        
        while queue:
            # Process all nodes at current level (can be executed in parallel)
            level_size = len(queue)
            current_level = []
            
            for _ in range(level_size):
                node_name = queue.popleft()
                current_level.append(node_name)
                
                # Update execution level
                if node_name in self.nodes:
                    self.nodes[node_name].level = len(result)
                
                # Reduce in-degree for dependents
                if node_name in self.nodes:
                    for dependent in self.nodes[node_name].dependents:
                        in_degree[dependent] -= 1
                        if in_degree[dependent] == 0:
                            queue.append(dependent)
            
            if current_level:
                result.append(current_level)
        
        # Check if all nodes were processed
        processed_count = sum(len(level) for level in result)
        if processed_count != len(self.nodes):
            raise CircularDependencyError(
                "Circular dependency detected (incomplete topological sort)"
            )
        
        logger.info(f"Topological sort completed: {len(result)} execution levels")
        return result
    
    def get_execution_order(self, models: Optional[List[str]] = None) -> List[List[str]]:
        """
        Get execution order for specified models (or all models)
        
        Args:
            models: List of model names to execute, or None for all models
        
        Returns:
            List of execution levels
        """
        if models is None:
            # Execute all models
            return self.topological_sort()
        
        # Build subgraph for specified models including their dependencies
        required_models = set(models)
        
        for model in models:
            required_models.update(self.get_all_dependencies(model))
        
        # Filter graph to only include required models
        subgraph = DependencyGraph()
        for model in required_models:
            if model in self.nodes:
                node = self.nodes[model]
                # Only include dependencies that are in required_models
                filtered_deps = node.dependencies & required_models
                subgraph.add_model(model, filtered_deps)
        
        return subgraph.topological_sort()
    
    def get_changed_models_impact(self, changed_models: Set[str]) -> Set[str]:
        """
        Get all models affected by changes to specified models
        
        Args:
            changed_models: Set of changed model names
        
        Returns:
            Set of all affected model names (including changed models)
        """
        affected = set(changed_models)
        
        for model in changed_models:
            affected.update(self.get_all_dependents(model))
        
        logger.info(
            f"Impact analysis: {len(changed_models)} changed models affect "
            f"{len(affected)} total models"
        )
        
        return affected
    
    def get_column_lineage(self, model_name: str, column_name: str) -> Set[str]:
        """
        Get column-level lineage for a specific column
        
        Args:
            model_name: Model name
            column_name: Column name
        
        Returns:
            Set of affected downstream columns (format: "model.column")
        """
        return self.lineage_tracker.get_column_impact(model_name, column_name)
    
    def export_graphviz(self) -> str:
        """
        Export dependency graph as Graphviz DOT format
        
        Returns:
            DOT format string
        """
        lines = ["digraph dependencies {", "  rankdir=LR;", "  node [shape=box];"]
        
        # Add nodes with levels
        for node_name, node in self.nodes.items():
            color = "lightblue" if node.level == 0 else "white"
            lines.append(f'  "{node_name}" [style=filled, fillcolor={color}];')
        
        # Add edges
        for node_name, node in self.nodes.items():
            for dep in node.dependencies:
                lines.append(f'  "{dep}" -> "{node_name}";')
        
        lines.append("}")
        return "\n".join(lines)
    
    def get_stats(self) -> Dict[str, any]:
        """Get dependency graph statistics"""
        total_models = len(self.nodes)
        total_edges = sum(len(node.dependencies) for node in self.nodes.values())
        
        # Find leaf nodes (no dependencies)
        leaf_nodes = [name for name, node in self.nodes.items() if not node.dependencies]
        
        # Find root nodes (no dependents)
        root_nodes = [name for name, node in self.nodes.items() if not node.dependents]
        
        # Calculate max depth
        try:
            levels = self.topological_sort()
            max_depth = len(levels)
        except:
            max_depth = 0
        
        return {
            "total_models": total_models,
            "total_edges": total_edges,
            "leaf_nodes": len(leaf_nodes),
            "root_nodes": len(root_nodes),
            "max_depth": max_depth,
            "avg_dependencies": round(total_edges / total_models, 2) if total_models > 0 else 0
        }


class ParallelExecutor:
    """
    Execute models in parallel based on dependency graph
    """
    
    def __init__(self, dependency_graph: DependencyGraph, max_parallelism: int = 5):
        """
        Initialize parallel executor
        
        Args:
            dependency_graph: DependencyGraph instance
            max_parallelism: Maximum number of models to execute in parallel
        """
        self.graph = dependency_graph
        self.max_parallelism = max_parallelism
    
    async def execute_models_async(
        self,
        execute_func,
        models: Optional[List[str]] = None
    ) -> Dict[str, bool]:
        """
        Execute models in parallel respecting dependencies
        
        Args:
            execute_func: Async function to execute a model (signature: async def execute(model_name))
            models: List of models to execute, or None for all
        
        Returns:
            Dictionary mapping model names to success status
        """
        execution_order = self.graph.get_execution_order(models)
        results = {}
        
        logger.info(f"Executing {len(models) if models else 'all'} models in {len(execution_order)} levels")
        
        for level_idx, level_models in enumerate(execution_order):
            logger.info(f"Executing level {level_idx + 1}/{len(execution_order)} ({len(level_models)} models)")
            
            # Execute models in this level in parallel (up to max_parallelism)
            tasks = []
            for i in range(0, len(level_models), self.max_parallelism):
                batch = level_models[i:i + self.max_parallelism]
                for model_name in batch:
                    tasks.append(self._execute_model_with_result(execute_func, model_name))
                
                # Wait for this batch to complete
                batch_results = await asyncio.gather(*tasks, return_exceptions=True)
                tasks = []
                
                # Process results
                for model_name, result in zip(batch, batch_results):
                    if isinstance(result, Exception):
                        logger.error(f"Model {model_name} failed: {result}")
                        results[model_name] = False
                    else:
                        results[model_name] = result
        
        logger.info(
            f"Execution completed: {sum(results.values())} succeeded, "
            f"{len(results) - sum(results.values())} failed"
        )
        
        return results
    
    async def _execute_model_with_result(self, execute_func, model_name: str) -> bool:
        """Execute a model and return success status"""
        try:
            await execute_func(model_name)
            return True
        except Exception as e:
            logger.error(f"Error executing model {model_name}: {e}")
            return False


def build_dependency_graph(parsed_models: Dict[str, any]) -> DependencyGraph:
    """
    Build dependency graph from parsed models
    
    Args:
        parsed_models: Dictionary of model_name -> ParsedSQL
    
    Returns:
        DependencyGraph instance
    """
    graph = DependencyGraph()
    
    for model_name, parsed_sql in parsed_models.items():
        graph.add_model(
            model_name=model_name,
            dependencies=parsed_sql.dependencies,
            lineage=parsed_sql.lineage
        )
    
    logger.info(f"Built dependency graph with {len(parsed_models)} models")
    
    return graph

