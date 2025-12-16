"""
YAML-based model configuration system (dbt-style)
"""

from pathlib import Path
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field
import yaml

from utils.errors import ConfigurationError, ModelNotFoundError
from utils.logger import get_logger

logger = get_logger(__name__)


@dataclass
class TestConfig:
    """Configuration for a model test"""
    test_type: str
    params: Dict[str, Any] = field(default_factory=dict)
    
    @classmethod
    def from_dict(cls, data: Any) -> 'TestConfig':
        """Create TestConfig from dictionary or string"""
        if isinstance(data, str):
            return cls(test_type=data, params={})
        elif isinstance(data, dict):
            # Handle format: {test_name: {param: value}}
            test_type = list(data.keys())[0]
            params = data[test_type] if data[test_type] else {}
            return cls(test_type=test_type, params=params)
        else:
            raise ConfigurationError(f"Invalid test configuration: {data}")


@dataclass
class ColumnConfig:
    """Configuration for a model column"""
    name: str
    description: Optional[str] = None
    tests: List[TestConfig] = field(default_factory=list)
    meta: Dict[str, Any] = field(default_factory=dict)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ColumnConfig':
        """Create ColumnConfig from dictionary"""
        tests = []
        if 'tests' in data:
            for test_data in data['tests']:
                tests.append(TestConfig.from_dict(test_data))
        
        return cls(
            name=data.get('name', ''),
            description=data.get('description'),
            tests=tests,
            meta=data.get('meta', {})
        )


@dataclass
class ModelConfig:
    """Configuration for a data model"""
    name: str
    description: Optional[str] = None
    materialized: str = 'view'  # view, table, temp_table, incremental, incremental_table, cdc
    incremental_strategy: Optional[str] = None  # time, unique_key, append
    time_column: Optional[str] = None
    unique_key: Optional[str] = None
    tags: List[str] = field(default_factory=list)
    columns: List[ColumnConfig] = field(default_factory=list)
    tests: List[TestConfig] = field(default_factory=list)
    vars: List[str] = field(default_factory=list)
    depends_on: List[str] = field(default_factory=list)
    meta: Dict[str, Any] = field(default_factory=dict)
    enabled: bool = True
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ModelConfig':
        """Create ModelConfig from dictionary"""
        # Extract config section
        config_data = data.get('config', {})
        
        # Parse columns
        columns = []
        if 'columns' in data:
            for col_data in data['columns']:
                columns.append(ColumnConfig.from_dict(col_data))
        
        # Parse tests
        tests = []
        if 'tests' in data:
            for test_data in data['tests']:
                tests.append(TestConfig.from_dict(test_data))
        
        return cls(
            name=data.get('name', ''),
            description=data.get('description'),
            materialized=config_data.get('materialized', 'view'),
            incremental_strategy=config_data.get('incremental_strategy'),
            time_column=config_data.get('time_column'),
            unique_key=config_data.get('unique_key'),
            tags=data.get('tags', []),
            columns=columns,
            tests=tests,
            vars=data.get('vars', []),
            depends_on=data.get('depends_on', []),
            meta=data.get('meta', {}),
            enabled=config_data.get('enabled', True)
        )
    
    def is_incremental(self) -> bool:
        """Check if model is incremental"""
        return self.materialized == 'incremental'
    
    def get_column_config(self, column_name: str) -> Optional[ColumnConfig]:
        """Get configuration for a specific column"""
        for col in self.columns:
            if col.name == column_name:
                return col
        return None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            'name': self.name,
            'description': self.description,
            'config': {
                'materialized': self.materialized,
                'incremental_strategy': self.incremental_strategy,
                'time_column': self.time_column,
                'unique_key': self.unique_key,
                'enabled': self.enabled
            },
            'tags': self.tags,
            'columns': [
                {
                    'name': col.name,
                    'description': col.description,
                    'tests': [
                        {test.test_type: test.params} if test.params else test.test_type
                        for test in col.tests
                    ],
                    'meta': col.meta
                }
                for col in self.columns
            ],
            'tests': [
                {test.test_type: test.params} if test.params else test.test_type
                for test in self.tests
            ],
            'vars': self.vars,
            'depends_on': self.depends_on,
            'meta': self.meta
        }


class ModelRegistry:
    """
    Registry for managing model configurations
    """
    
    def __init__(self, models_dir: Path):
        """
        Initialize model registry
        
        Args:
            models_dir: Directory containing model files and schema.yml
        """
        self.models_dir = Path(models_dir)
        self.models: Dict[str, ModelConfig] = {}
        self._load_schema_files()
    
    def _load_schema_files(self):
        """Load all schema.yml files from models directory"""
        if not self.models_dir.exists():
            logger.warning(f"Models directory not found: {self.models_dir}")
            return
        
        # Find all schema.yml files recursively
        schema_files = list(self.models_dir.rglob("schema.yml"))
        schema_files.extend(self.models_dir.rglob("*.yml"))
        
        logger.info(f"Found {len(schema_files)} schema files")
        
        for schema_file in schema_files:
            if schema_file.name.startswith('schema'):
                try:
                    self._load_schema_file(schema_file)
                except Exception as e:
                    logger.error(f"Failed to load schema file {schema_file}: {e}")
    
    def _load_schema_file(self, schema_file: Path):
        """Load a single schema.yml file"""
        try:
            with open(schema_file, 'r', encoding='utf-8') as f:
                data = yaml.safe_load(f)
            
            if not data:
                return
            
            # Extract models from schema file
            if 'models' in data:
                for model_data in data['models']:
                    model_config = ModelConfig.from_dict(model_data)
                    self.models[model_config.name] = model_config
                    logger.debug(f"Loaded config for model: {model_config.name}")
            
        except Exception as e:
            raise ConfigurationError(
                f"Failed to load schema file: {schema_file}",
                context={"error": str(e)}
            )
    
    def get_model_config(self, model_name: str) -> Optional[ModelConfig]:
        """
        Get configuration for a specific model
        
        Args:
            model_name: Model name
        
        Returns:
            ModelConfig if found, None otherwise
        """
        return self.models.get(model_name)
    
    def get_all_models(self) -> Dict[str, ModelConfig]:
        """Get all model configurations"""
        return self.models.copy()
    
    def get_models_by_tag(self, tag: str) -> List[ModelConfig]:
        """Get all models with a specific tag"""
        return [model for model in self.models.values() if tag in model.tags]
    
    def get_incremental_models(self) -> List[ModelConfig]:
        """Get all incremental models"""
        return [model for model in self.models.values() if model.is_incremental()]
    
    def add_model_config(self, model_config: ModelConfig):
        """
        Add or update model configuration
        
        Args:
            model_config: ModelConfig to add
        """
        self.models[model_config.name] = model_config
        logger.debug(f"Added/updated config for model: {model_config.name}")
    
    def remove_model_config(self, model_name: str):
        """
        Remove model configuration
        
        Args:
            model_name: Model name to remove
        """
        if model_name in self.models:
            del self.models[model_name]
            logger.debug(f"Removed config for model: {model_name}")
    
    def reload(self):
        """Reload all schema files"""
        self.models = {}
        self._load_schema_files()
        logger.info("Reloaded all model configurations")
    
    def validate_model_config(self, model_name: str) -> bool:
        """
        Validate model configuration
        
        Args:
            model_name: Model name to validate
        
        Returns:
            True if valid
        
        Raises:
            ConfigurationError: If configuration is invalid
        """
        config = self.get_model_config(model_name)
        
        if not config:
            raise ModelNotFoundError(f"Model config not found: {model_name}")
        
        # Validate incremental configuration
        if config.is_incremental():
            if not config.incremental_strategy:
                raise ConfigurationError(
                    f"Incremental model '{model_name}' must specify incremental_strategy"
                )
            
            if config.incremental_strategy == 'time' and not config.time_column:
                raise ConfigurationError(
                    f"Incremental model '{model_name}' with time strategy must specify time_column"
                )
            
            if config.incremental_strategy == 'unique_key' and not config.unique_key:
                raise ConfigurationError(
                    f"Incremental model '{model_name}' with unique_key strategy must specify unique_key"
                )
        
        logger.debug(f"Validation passed for model: {model_name}")
        return True
    
    def export_to_yaml(self, output_file: Path):
        """
        Export all model configurations to a YAML file
        
        Args:
            output_file: Output file path
        """
        data = {
            'models': [model.to_dict() for model in self.models.values()]
        }
        
        with open(output_file, 'w', encoding='utf-8') as f:
            yaml.dump(data, f, default_flow_style=False, sort_keys=False)
        
        logger.info(f"Exported {len(self.models)} model configs to {output_file}")


def load_model_registry(models_dir: Path) -> ModelRegistry:
    """
    Load model registry from models directory
    
    Args:
        models_dir: Directory containing model files
    
    Returns:
        ModelRegistry instance
    """
    return ModelRegistry(models_dir)

