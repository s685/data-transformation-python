"""
Configuration management with YAML support and environment variable substitution
"""

import os
import re
from pathlib import Path
from typing import Any, Dict, Optional
import yaml
from dotenv import load_dotenv

from utils.errors import ConfigurationError
from utils.logger import get_logger

logger = get_logger(__name__)


class Config:
    """
    Configuration manager for the framework
    Handles multi-environment configs with environment variable substitution
    """
    
    def __init__(self, project_dir: Optional[Path] = None):
        """
        Initialize configuration
        
        Args:
            project_dir: Project directory path. Defaults to current directory.
        """
        self.project_dir = Path(project_dir) if project_dir else Path.cwd()
        self.config_dir = self.project_dir / "config"
        
        # Load .env file if it exists
        env_file = self.project_dir / ".env"
        if env_file.exists():
            load_dotenv(env_file)
            logger.debug(f"Loaded environment variables from {env_file}")
        
        # Configuration storage
        self.profiles: Dict[str, Any] = {}
        self.environments: Dict[str, Any] = {}
        self.sources: Dict[str, Any] = {}
        self.current_target: str = "dev"
        
        # Load configurations
        self._load_profiles()
        self._load_environments()
        self._load_sources()
    
    def _load_profiles(self):
        """Load profiles.yml configuration"""
        profiles_file = self.config_dir / "profiles.yml"
        
        if not profiles_file.exists():
            logger.warning(f"profiles.yml not found at {profiles_file}")
            return
        
        try:
            with open(profiles_file, 'r') as f:
                content = f.read()
                # Substitute environment variables
                content = self._substitute_env_vars(content)
                self.profiles = yaml.safe_load(content)
            
            # Set current target from profiles
            if 'default' in self.profiles and 'target' in self.profiles['default']:
                self.current_target = self.profiles['default']['target']
            
            logger.info(f"Loaded profiles configuration from {profiles_file}")
        except Exception as e:
            raise ConfigurationError(
                f"Failed to load profiles.yml: {str(e)}",
                context={"file": str(profiles_file)}
            )
    
    def _load_environments(self):
        """Load environments.yml configuration"""
        env_file = self.config_dir / "environments.yml"
        
        if not env_file.exists():
            logger.warning(f"environments.yml not found at {env_file}")
            return
        
        try:
            with open(env_file, 'r') as f:
                content = f.read()
                # Substitute environment variables
                content = self._substitute_env_vars(content)
                self.environments = yaml.safe_load(content)
            
            logger.info(f"Loaded environments configuration from {env_file}")
        except Exception as e:
            raise ConfigurationError(
                f"Failed to load environments.yml: {str(e)}",
                context={"file": str(env_file)}
            )
    
    def _load_sources(self):
        """Load sources.yml configuration"""
        sources_file = self.config_dir / "sources.yml"
        
        if not sources_file.exists():
            logger.warning(f"sources.yml not found at {sources_file}")
            return
        
        try:
            with open(sources_file, 'r') as f:
                content = f.read()
                # Substitute environment variables
                content = self._substitute_env_vars(content)
                self.sources = yaml.safe_load(content) or {}
            
            logger.info(f"Loaded sources configuration from {sources_file}")
        except Exception as e:
            raise ConfigurationError(
                f"Failed to load sources.yml: {str(e)}",
                context={"file": str(sources_file)}
            )
    
    def get_source_table(self, source_name: str, table_name: str) -> str:
        """
        Get fully qualified table name for a source
        
        Args:
            source_name: Source name
            table_name: Table name within the source
        
        Returns:
            Fully qualified table name (database.schema.table)
        
        Raises:
            ConfigurationError: If source or table not found
        """
        if 'sources' not in self.sources:
            raise ConfigurationError(
                f"Source '{source_name}.{table_name}' not found: sources.yml not loaded or empty"
            )
        
        # Find source
        source_config = None
        for source in self.sources.get('sources', []):
            if source.get('name') == source_name:
                source_config = source
                break
        
        if not source_config:
            raise ConfigurationError(
                f"Source '{source_name}' not found in sources.yml",
                context={"available_sources": [s.get('name') for s in self.sources.get('sources', [])]}
            )
        
        # Find table
        table_config = None
        for table in source_config.get('tables', []):
            if table.get('name') == table_name:
                table_config = table
                break
        
        if not table_config:
            raise ConfigurationError(
                f"Table '{table_name}' not found in source '{source_name}'",
                context={"available_tables": [t.get('name') for t in source_config.get('tables', [])]}
            )
        
        # Build fully qualified name
        database = source_config.get('database', '')
        schema = source_config.get('schema', '')
        identifier = table_config.get('identifier', table_name.upper())
        
        if database and schema:
            return f"{database}.{schema}.{identifier}"
        elif schema:
            return f"{schema}.{identifier}"
        else:
            return identifier
    
    def _substitute_env_vars(self, content: str) -> str:
        """
        Substitute environment variables in YAML content
        Supports ${VAR_NAME} and ${VAR_NAME:-default_value} syntax
        
        Args:
            content: YAML content with environment variable placeholders
        
        Returns:
            Content with environment variables substituted
        """
        # Pattern: ${VAR_NAME} or ${VAR_NAME:-default}
        pattern = r'\$\{([^}:]+)(?::-(.[^}]*))?\}'
        
        def replace_env_var(match):
            var_name = match.group(1)
            default_value = match.group(2) if match.group(2) else ""
            
            value = os.environ.get(var_name, default_value)
            
            if not value and not default_value:
                logger.warning(f"Environment variable {var_name} not set and no default provided")
            
            return value
        
        return re.sub(pattern, replace_env_var, content)
    
    def get_connection_config(self, target: Optional[str] = None) -> Dict[str, Any]:
        """
        Get Snowflake connection configuration for a target environment
        
        Args:
            target: Target environment name. Defaults to current target.
        
        Returns:
            Connection configuration dictionary
        """
        target = target or self.current_target
        
        if 'default' not in self.profiles:
            raise ConfigurationError("No 'default' profile found in profiles.yml")
        
        profile = self.profiles['default']
        
        if 'outputs' not in profile:
            raise ConfigurationError("No 'outputs' section found in default profile")
        
        if target not in profile['outputs']:
            raise ConfigurationError(
                f"Target '{target}' not found in profiles.yml",
                context={"available_targets": list(profile['outputs'].keys())}
            )
        
        config = profile['outputs'][target].copy()
        
        # Validate required fields
        required_fields = ['account', 'user', 'warehouse', 'database', 'schema']
        missing_fields = [field for field in required_fields if field not in config]
        
        if missing_fields:
            raise ConfigurationError(
                f"Missing required fields in connection config: {', '.join(missing_fields)}",
                context={"target": target}
            )
        
        # Validate authentication method
        authenticator = config.get('authenticator', 'password')
        
        if authenticator in ['externalbrowser', 'oauth']:
            # SSO authentication
            if authenticator == 'oauth' and 'token' not in config:
                raise ConfigurationError(
                    "OAuth authentication requires 'token' field in connection config",
                    context={"target": target, "authenticator": authenticator}
                )
        elif authenticator == 'snowflake':
            # Private key pair authentication
            if 'private_key' not in config:
                raise ConfigurationError(
                    "Private key authentication requires 'private_key' field in connection config",
                    context={"target": target, "authenticator": authenticator}
                )
        else:
            # Password authentication (default)
            if 'password' not in config:
                raise ConfigurationError(
                    "Password authentication requires 'password' field in connection config",
                    context={"target": target, "authenticator": authenticator}
                )
        
        logger.debug(f"Loaded connection config for target: {target} (auth: {authenticator})")
        
        # Extract connection pool settings (if provided)
        # These are not part of Snowflake connection params, so remove them
        pool_config = {}
        pool_keys = ['threads', 'pool_size', 'lazy_init', 'max_retries', 'retry_delay', 'query_timeout']
        for key in pool_keys:
            if key in config:
                value = config.pop(key)
                # Skip None or empty values
                if value is None or value == '':
                    continue
                # Convert YAML string booleans to Python booleans
                if key == 'lazy_init':
                    if isinstance(value, str):
                        pool_config[key] = value.lower() in ('true', '1', 'yes')
                    elif isinstance(value, bool):
                        pool_config[key] = value
                # Convert threads/pool_size to int
                elif key in ['threads', 'pool_size', 'max_retries', 'query_timeout']:
                    try:
                        pool_config[key] = int(value)
                    except (ValueError, TypeError):
                        continue  # Skip invalid values
                # Convert retry_delay to float
                elif key == 'retry_delay':
                    try:
                        pool_config[key] = float(value)
                    except (ValueError, TypeError):
                        continue  # Skip invalid values
                else:
                    pool_config[key] = value
        
        # dbt-style: threads parameter controls pool size
        # If threads is specified, use it as pool_size (dbt compatibility)
        if 'threads' in pool_config and 'pool_size' not in pool_config:
            pool_config['pool_size'] = pool_config['threads']
        
        # Store pool config separately if any pool settings were provided
        if pool_config:
            config['_pool_config'] = pool_config
        
        return config
    
    def get_environment_config(self, environment: str) -> Dict[str, Any]:
        """
        Get environment-specific configuration
        
        Args:
            environment: Environment name (dev, prod, etc.)
        
        Returns:
            Environment configuration dictionary
        """
        if 'environments' not in self.environments:
            return {}
        
        if environment not in self.environments['environments']:
            logger.warning(f"Environment '{environment}' not found in environments.yml")
            return {}
        
        return self.environments['environments'][environment].copy()
    
    def set_target(self, target: str):
        """
        Set the current target environment
        
        Args:
            target: Target environment name
        """
        # Validate target exists
        if 'default' in self.profiles and 'outputs' in self.profiles['default']:
            if target not in self.profiles['default']['outputs']:
                raise ConfigurationError(
                    f"Target '{target}' not found in profiles.yml",
                    context={"available_targets": list(self.profiles['default']['outputs'].keys())}
                )
        
        self.current_target = target
        logger.info(f"Set current target to: {target}")
    
    def get_models_dir(self) -> Path:
        """Get models directory path"""
        return self.project_dir / "sql" / "models"
    
    def get_state_dir(self, environment: Optional[str] = None) -> Path:
        """
        Get state directory path for an environment
        
        Args:
            environment: Environment name. Defaults to current target.
        
        Returns:
            State directory path
        """
        environment = environment or self.current_target
        state_dir = self.project_dir / ".state" / environment
        state_dir.mkdir(parents=True, exist_ok=True)
        return state_dir
    
    def get_config_value(self, key: str, default: Any = None) -> Any:
        """
        Get a configuration value by key path (e.g., 'default.target')
        
        Args:
            key: Configuration key path separated by dots
            default: Default value if key not found
        
        Returns:
            Configuration value
        """
        keys = key.split('.')
        value = self.profiles
        
        for k in keys:
            if isinstance(value, dict) and k in value:
                value = value[k]
            else:
                return default
        
        return value
    
    def validate(self) -> bool:
        """
        Validate configuration
        
        Returns:
            True if configuration is valid
        
        Raises:
            ConfigurationError: If configuration is invalid
        """
        # Check if profiles.yml exists
        if not (self.config_dir / "profiles.yml").exists():
            raise ConfigurationError(
                "profiles.yml not found. Please create config/profiles.yml"
            )
        
        # Validate connection config for current target
        try:
            self.get_connection_config()
        except ConfigurationError as e:
            raise ConfigurationError(
                f"Invalid connection configuration: {str(e)}"
            )
        
        # Check if models directory exists
        if not self.get_models_dir().exists():
            logger.warning(f"Models directory not found at {self.get_models_dir()}")
        
        logger.info("Configuration validation passed")
        return True
    
    def to_dict(self) -> Dict[str, Any]:
        """Export configuration as dictionary"""
        return {
            "project_dir": str(self.project_dir),
            "config_dir": str(self.config_dir),
            "current_target": self.current_target,
            "profiles": self.profiles,
            "environments": self.environments
        }


def load_config(project_dir: Optional[Path] = None) -> Config:
    """
    Load configuration from project directory
    
    Args:
        project_dir: Project directory path. Defaults to current directory.
    
    Returns:
        Config instance
    """
    return Config(project_dir)

