"""
Column-level lineage utilities for tracking data flow
"""

from typing import Dict, List, Set, Optional, Tuple
from dataclasses import dataclass, field
import json


@dataclass
class ColumnLineage:
    """
    Represents lineage information for a single column
    """
    column_name: str
    source_table: Optional[str] = None
    source_columns: List[Tuple[str, str]] = field(default_factory=list)  # [(table, column), ...]
    transformations: List[str] = field(default_factory=list)
    
    def add_source(self, table: str, column: str):
        """Add a source column"""
        self.source_columns.append((table, column))
    
    def add_transformation(self, transformation: str):
        """Add a transformation description"""
        self.transformations.append(transformation)
    
    def to_dict(self) -> Dict:
        """Convert to dictionary"""
        return {
            "column_name": self.column_name,
            "source_table": self.source_table,
            "source_columns": [{"table": t, "column": c} for t, c in self.source_columns],
            "transformations": self.transformations
        }


@dataclass
class ModelLineage:
    """
    Represents complete lineage information for a model
    """
    model_name: str
    columns: Dict[str, ColumnLineage] = field(default_factory=dict)
    dependencies: Set[str] = field(default_factory=set)
    
    def add_column_lineage(self, column_lineage: ColumnLineage):
        """Add column lineage"""
        self.columns[column_lineage.column_name] = column_lineage
    
    def add_dependency(self, dependency: str):
        """Add model dependency"""
        self.dependencies.add(dependency)
    
    def get_column_lineage(self, column_name: str) -> Optional[ColumnLineage]:
        """Get lineage for a specific column"""
        return self.columns.get(column_name)
    
    def get_all_source_tables(self) -> Set[str]:
        """Get all source tables used by this model"""
        tables = set()
        for col_lineage in self.columns.values():
            if col_lineage.source_table:
                tables.add(col_lineage.source_table)
            for table, _ in col_lineage.source_columns:
                tables.add(table)
        return tables
    
    def to_dict(self) -> Dict:
        """Convert to dictionary"""
        return {
            "model_name": self.model_name,
            "columns": {name: lineage.to_dict() for name, lineage in self.columns.items()},
            "dependencies": list(self.dependencies)
        }
    
    def to_json(self) -> str:
        """Convert to JSON string"""
        return json.dumps(self.to_dict(), indent=2)


class LineageTracker:
    """
    Tracks lineage information across all models
    """
    
    def __init__(self):
        self.models: Dict[str, ModelLineage] = {}
    
    def add_model_lineage(self, model_lineage: ModelLineage):
        """Add lineage for a model"""
        self.models[model_lineage.model_name] = model_lineage
    
    def get_model_lineage(self, model_name: str) -> Optional[ModelLineage]:
        """Get lineage for a specific model"""
        return self.models.get(model_name)
    
    def get_downstream_models(self, model_name: str) -> Set[str]:
        """
        Get all models that depend on the given model
        """
        downstream = set()
        for name, lineage in self.models.items():
            if model_name in lineage.dependencies:
                downstream.add(name)
        return downstream
    
    def get_upstream_models(self, model_name: str) -> Set[str]:
        """
        Get all models that the given model depends on
        """
        if model_name in self.models:
            return self.models[model_name].dependencies.copy()
        return set()
    
    def get_column_impact(self, model_name: str, column_name: str) -> Set[str]:
        """
        Get all downstream models/columns affected by a column change
        """
        impacted = set()
        
        # Find all downstream models
        downstream_models = self.get_downstream_models(model_name)
        
        for downstream_model in downstream_models:
            model_lineage = self.models.get(downstream_model)
            if model_lineage:
                # Check which columns in downstream model depend on this column
                for col_name, col_lineage in model_lineage.columns.items():
                    for table, col in col_lineage.source_columns:
                        if table == model_name and col == column_name:
                            impacted.add(f"{downstream_model}.{col_name}")
                            # Recursively find further impacts
                            further_impacts = self.get_column_impact(downstream_model, col_name)
                            impacted.update(further_impacts)
        
        return impacted
    
    def get_full_lineage_path(self, model_name: str) -> List[str]:
        """
        Get full lineage path from source to target model
        """
        path = []
        visited = set()
        
        def traverse(current_model: str):
            if current_model in visited:
                return
            visited.add(current_model)
            
            upstream = self.get_upstream_models(current_model)
            for upstream_model in upstream:
                traverse(upstream_model)
            
            path.append(current_model)
        
        traverse(model_name)
        return path
    
    def export_lineage(self, output_format: str = "json") -> str:
        """
        Export all lineage information
        
        Args:
            output_format: Output format (json, graphviz)
        
        Returns:
            Lineage data in requested format
        """
        if output_format == "json":
            return json.dumps(
                {name: lineage.to_dict() for name, lineage in self.models.items()},
                indent=2
            )
        elif output_format == "graphviz":
            return self._export_graphviz()
        else:
            raise ValueError(f"Unsupported format: {output_format}")
    
    def _export_graphviz(self) -> str:
        """Export lineage as Graphviz DOT format"""
        lines = ["digraph lineage {", "  rankdir=LR;"]
        
        # Add nodes
        for model_name in self.models.keys():
            lines.append(f'  "{model_name}" [shape=box];')
        
        # Add edges
        for model_name, lineage in self.models.items():
            for dependency in lineage.dependencies:
                lines.append(f'  "{dependency}" -> "{model_name}";')
        
        lines.append("}")
        return "\n".join(lines)
    
    def clear(self):
        """Clear all lineage data"""
        self.models = {}


# Global lineage tracker instance
_global_tracker: Optional[LineageTracker] = None


def get_lineage_tracker() -> LineageTracker:
    """Get or create global lineage tracker instance"""
    global _global_tracker
    if _global_tracker is None:
        _global_tracker = LineageTracker()
    return _global_tracker

