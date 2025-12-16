"""
SQL parser with AST support for variable extraction, dependency resolution, and column-level lineage
"""

import re
from pathlib import Path
from typing import Dict, List, Set, Optional, Tuple
from dataclasses import dataclass
import hashlib
from datetime import datetime

import sqlglot
from sqlglot import exp
from jinja2 import Environment, FileSystemLoader, meta

from utils.errors import SQLParseError, ModelNotFoundError
from utils.logger import get_logger
from utils.lineage import ColumnLineage, ModelLineage

logger = get_logger(__name__)


@dataclass
class ParsedSQL:
    """
    Represents a parsed SQL file with extracted metadata
    """
    model_name: str
    file_path: Path
    raw_sql: str
    parsed_sql: str  # After Jinja2 rendering
    variables: Set[str]
    dependencies: Set[str]  # Model dependencies (from ref())
    sources: Set[str]  # Source table dependencies (from source())
    config: Dict[str, any]
    lineage: Optional[ModelLineage] = None
    file_hash: Optional[str] = None
    last_modified: Optional[datetime] = None
    
    def __post_init__(self):
        """Calculate file hash and last modified time"""
        if self.file_hash is None:
            self.file_hash = hashlib.md5(self.raw_sql.encode()).hexdigest()
        if self.last_modified is None and self.file_path.exists():
            self.last_modified = datetime.fromtimestamp(self.file_path.stat().st_mtime)


class SQLParser:
    """
    SQL parser with AST support for extracting variables, dependencies, and lineage
    """
    
    def __init__(self, models_dir: Path, cache_enabled: bool = True):
        """
        Initialize SQL parser
        
        Args:
            models_dir: Directory containing SQL model files
            cache_enabled: Whether to cache parsed SQL
        """
        self.models_dir = Path(models_dir)
        self.cache_enabled = cache_enabled
        self._cache: Dict[str, ParsedSQL] = {}
        
        # Initialize Jinja2 environment
        if self.models_dir.exists():
            self.jinja_env = Environment(
                loader=FileSystemLoader(str(self.models_dir)),
                autoescape=False
            )
        else:
            self.jinja_env = Environment(autoescape=False)
        
        # Register custom Jinja2 functions
        self.jinja_env.globals['ref'] = self._ref_function
        self.jinja_env.globals['source'] = self._source_function
        self.jinja_env.globals['this'] = self._this_function
        self.jinja_env.globals['is_incremental'] = self._is_incremental_function
        
        # Register SQL macros
        from framework.macros import register_macros
        register_macros(self.jinja_env)
    
    def _ref_function(self, model_name: str) -> str:
        """
        Jinja2 ref() function for referencing other models.
        Used for model dependencies (other models in the transformation pipeline).
        
        Args:
            model_name: Name of the model to reference
        
        Returns:
            Placeholder string that will be replaced with actual table name
        """
        return f"__REF_{model_name}__"
    
    def _source_function(self, source_name: str, table_name: str) -> str:
        """
        Jinja2 source() function for referencing source tables.
        Used for raw source tables (not models in the transformation pipeline).
        
        Args:
            source_name: Name of the source (schema/database)
            table_name: Name of the table in the source
        
        Returns:
            Placeholder string that will be replaced with actual table name
        """
        # Combine source_name and table_name to create unique identifier
        return f"__SOURCE_{source_name}_{table_name}__"
    
    def _this_function(self) -> str:
        """
        Jinja2 this() function for referencing current model
        Returns a placeholder
        """
        return "__THIS__"
    
    def _is_incremental_function(self) -> bool:
        """
        Jinja2 is_incremental() function
        Returns False during parsing (actual value determined at runtime)
        """
        return False
    
    def parse_file(self, file_path: Path, render_jinja: bool = True) -> ParsedSQL:
        """
        Parse a SQL file and extract metadata
        
        Args:
            file_path: Path to SQL file
            render_jinja: Whether to render Jinja2 templates
        
        Returns:
            ParsedSQL object with extracted metadata
        """
        if not file_path.exists():
            raise ModelNotFoundError(
                f"Model file not found: {file_path}",
                context={"file_path": str(file_path)}
            )
        
        # Check cache
        file_hash = self._calculate_file_hash(file_path)
        cache_key = str(file_path)
        
        if self.cache_enabled and cache_key in self._cache:
            cached = self._cache[cache_key]
            if cached.file_hash == file_hash:
                logger.debug(f"Using cached parse result for {file_path.name}")
                return cached
        
        try:
            # Read SQL file
            with open(file_path, 'r', encoding='utf-8') as f:
                raw_sql = f.read()
            
            model_name = file_path.stem
            
            # Extract inline config from comments
            config = self._extract_config_from_comments(raw_sql)
            
            # Extract dependencies from comments
            comment_deps = self._extract_dependencies_from_comments(raw_sql)
            
            # Render Jinja2 template if requested
            parsed_sql = raw_sql
            jinja_deps = set()
            jinja_sources = set()
            if render_jinja:
                parsed_sql, jinja_deps, jinja_sources = self._render_jinja(raw_sql, model_name)
            
            # Extract $variables
            variables = self._extract_dollar_variables(parsed_sql)
            
            # Extract dependencies using AST (only for direct table references, not ref/source)
            ast_deps = self._extract_dependencies_from_ast(parsed_sql)
            
            # Combine all dependencies (ref() calls)
            dependencies = comment_deps | jinja_deps | ast_deps
            
            # Sources are tracked separately (source() calls)
            sources = jinja_sources
            
            # Extract column-level lineage
            lineage = self._extract_lineage(parsed_sql, model_name, dependencies)
            
            parsed = ParsedSQL(
                model_name=model_name,
                file_path=file_path,
                raw_sql=raw_sql,
                parsed_sql=parsed_sql,
                variables=variables,
                dependencies=dependencies,
                sources=sources,
                config=config,
                lineage=lineage,
                file_hash=file_hash
            )
            
            # Cache result
            if self.cache_enabled:
                self._cache[cache_key] = parsed
            
            logger.debug(
                f"Parsed model: {model_name}",
                variables=len(variables),
                dependencies=len(dependencies),
                sources=len(sources)
            )
            
            return parsed
            
        except Exception as e:
            raise SQLParseError(
                f"Failed to parse SQL file: {file_path}",
                context={"file_path": str(file_path), "error": str(e)}
            )
    
    def _calculate_file_hash(self, file_path: Path) -> str:
        """Calculate MD5 hash of file content"""
        with open(file_path, 'rb') as f:
            return hashlib.md5(f.read()).hexdigest()
    
    def _extract_config_from_comments(self, sql: str) -> Dict[str, any]:
        """
        Extract configuration from SQL comments
        Format: -- config: key=value, key2=value2
        """
        config = {}
        pattern = r'--\s*config:\s*(.+?)$'
        
        for match in re.finditer(pattern, sql, re.MULTILINE):
            config_str = match.group(1).strip()
            # Parse key=value pairs
            pairs = [pair.strip() for pair in config_str.split(',')]
            for pair in pairs:
                if '=' in pair:
                    key, value = pair.split('=', 1)
                    config[key.strip()] = value.strip()
        
        return config
    
    def _extract_dependencies_from_comments(self, sql: str) -> Set[str]:
        """
        Extract dependencies from SQL comments
        Format: -- depends_on: model1, model2
        """
        dependencies = set()
        pattern = r'--\s*depends_on:\s*(.+?)$'
        
        for match in re.finditer(pattern, sql, re.MULTILINE):
            deps_str = match.group(1).strip()
            # Split by comma and clean up
            deps = [dep.strip() for dep in deps_str.split(',')]
            dependencies.update(deps)
        
        return dependencies
    
    def _render_jinja(self, sql: str, model_name: str) -> Tuple[str, Set[str], Set[str]]:
        """
        Render Jinja2 template and extract ref() and source() dependencies
        
        Returns:
            Tuple of (rendered SQL, set of model dependencies from ref(), set of source dependencies from source())
        """
        dependencies = set()  # Model dependencies from ref()
        sources = set()  # Source table dependencies from source()
        
        try:
            # Parse template to find undeclared variables
            template = self.jinja_env.from_string(sql)
            
            # Extract ref() calls - these are model dependencies
            ref_pattern = r'\{\{\s*ref\([\'"](.+?)[\'"]\)\s*\}\}'
            for match in re.finditer(ref_pattern, sql):
                dep_name = match.group(1)
                dependencies.add(dep_name)
            
            # Extract source() calls - these are source table dependencies
            # Pattern: source('source_name', 'table_name')
            source_pattern = r'\{\{\s*source\([\'"](.+?)[\'"]\s*,\s*[\'"](.+?)[\'"]\)\s*\}\}'
            for match in re.finditer(source_pattern, sql):
                source_name = match.group(1)
                table_name = match.group(2)
                # Store as "source_name.table_name" for easy identification
                sources.add(f"{source_name}.{table_name}")
            
            # Render template with empty context (we'll handle variables later)
            rendered = template.render({})
            
            return rendered, dependencies, sources
            
        except Exception as e:
            logger.warning(f"Failed to render Jinja2 template for {model_name}: {e}")
            return sql, dependencies, sources
    
    def _extract_dollar_variables(self, sql: str) -> Set[str]:
        """
        Extract $variable_name patterns from SQL
        """
        # Pattern: $variable_name (alphanumeric and underscore)
        pattern = r'\$([a-zA-Z_][a-zA-Z0-9_]*)'
        variables = set(re.findall(pattern, sql))
        return variables
    
    def _extract_dependencies_from_ast(self, sql: str) -> Set[str]:
        """
        Extract table dependencies using AST parsing
        """
        dependencies = set()
        
        try:
            # Parse SQL using sqlglot
            parsed = sqlglot.parse_one(sql, read='snowflake')
            
            # Find all table references
            for table in parsed.find_all(exp.Table):
                table_name = table.name
                if table_name and not table_name.startswith('__'):
                    dependencies.add(table_name)
            
        except Exception as e:
            logger.debug(f"AST parsing failed, skipping: {e}")
        
        return dependencies
    
    def _extract_lineage(self, sql: str, model_name: str, dependencies: Set[str]) -> ModelLineage:
        """
        Extract column-level lineage using AST parsing
        """
        lineage = ModelLineage(model_name=model_name)
        
        # Add dependencies
        for dep in dependencies:
            lineage.add_dependency(dep)
        
        try:
            # Parse SQL using sqlglot
            parsed = sqlglot.parse_one(sql, read='snowflake')
            
            # Extract column lineage from SELECT statement
            if isinstance(parsed, exp.Select):
                for select_col in parsed.expressions:
                    col_lineage = self._extract_column_lineage(select_col, dependencies)
                    if col_lineage:
                        lineage.add_column_lineage(col_lineage)
            
        except Exception as e:
            logger.debug(f"Lineage extraction failed for {model_name}: {e}")
        
        return lineage
    
    def _extract_column_lineage(self, expression: exp.Expression, dependencies: Set[str]) -> Optional[ColumnLineage]:
        """
        Extract lineage for a single column expression
        """
        try:
            # Get column alias/name
            if isinstance(expression, exp.Alias):
                column_name = expression.alias
                source_expr = expression.this
            else:
                column_name = expression.name if hasattr(expression, 'name') else str(expression)
                source_expr = expression
            
            col_lineage = ColumnLineage(column_name=column_name)
            
            # Find all column references in the expression
            for col_ref in source_expr.find_all(exp.Column):
                source_table = col_ref.table if hasattr(col_ref, 'table') else None
                source_column = col_ref.name if hasattr(col_ref, 'name') else str(col_ref)
                
                if source_table:
                    col_lineage.add_source(source_table, source_column)
            
            # Detect transformations
            if isinstance(source_expr, exp.Func):
                func_name = source_expr.__class__.__name__
                col_lineage.add_transformation(func_name)
            
            return col_lineage
            
        except Exception as e:
            logger.debug(f"Column lineage extraction failed: {e}")
            return None
    
    def parse_directory(self, directory: Optional[Path] = None) -> Dict[str, ParsedSQL]:
        """
        Parse all SQL files in a directory
        
        Args:
            directory: Directory to parse. Defaults to models_dir.
        
        Returns:
            Dictionary mapping model names to ParsedSQL objects
        """
        directory = Path(directory) if directory else self.models_dir
        
        if not directory.exists():
            logger.warning(f"Directory not found: {directory}")
            return {}
        
        parsed_models = {}
        
        # Find all .sql files recursively
        sql_files = list(directory.rglob("*.sql"))
        
        logger.info(f"Found {len(sql_files)} SQL files in {directory}")
        
        for sql_file in sql_files:
            try:
                parsed = self.parse_file(sql_file)
                parsed_models[parsed.model_name] = parsed
            except Exception as e:
                logger.error(f"Failed to parse {sql_file}: {e}")
                # Continue with other files (graceful degradation)
        
        logger.info(f"Successfully parsed {len(parsed_models)} models")
        
        return parsed_models
    
    def invalidate_cache(self, model_name: Optional[str] = None):
        """
        Invalidate parser cache
        
        Args:
            model_name: Specific model to invalidate, or None to clear all
        """
        if model_name:
            self._cache = {k: v for k, v in self._cache.items() if v.model_name != model_name}
            logger.debug(f"Invalidated cache for model: {model_name}")
        else:
            self._cache = {}
            logger.debug("Cleared all parser cache")
    
    def validate_sql(self, sql: str, dialect: str = 'snowflake') -> bool:
        """
        Validate SQL syntax
        
        Args:
            sql: SQL string to validate
            dialect: SQL dialect (default: snowflake)
        
        Returns:
            True if SQL is valid
        
        Raises:
            SQLParseError: If SQL is invalid
        """
        try:
            sqlglot.parse_one(sql, read=dialect)
            return True
        except Exception as e:
            raise SQLParseError(
                f"Invalid SQL syntax: {str(e)}",
                context={"sql": sql[:200]}
            )

