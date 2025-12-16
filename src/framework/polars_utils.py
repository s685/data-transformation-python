"""
Polars utility functions for high-performance data processing.
Provides optimized alternatives to SQL operations using Polars.
"""

from typing import Dict, Any, Optional, List, Set, Tuple
from functools import lru_cache
import logging

logger = logging.getLogger(__name__)

# Try to import Polars
try:
    import polars as pl  # type: ignore
    POLARS_AVAILABLE = True
except ImportError:
    POLARS_AVAILABLE = False
    pl = None  # type: ignore
    logger.warning("Polars not available. Install with: pip install polars")


class PolarsDataProcessor:
    """
    High-performance data processing using Polars.
    Optimized for large-scale data transformations, aggregations, and validations.
    """
    
    @staticmethod
    def deduplicate(
        df: 'pl.DataFrame',
        unique_key: str,
        sort_column: Optional[str] = None,
        keep: str = 'last'
    ) -> 'pl.DataFrame':
        """
        Deduplicate DataFrame by unique key using Polars (ultra-fast).
        Time complexity: O(n log n) for sorting, O(n) for deduplication.
        
        Args:
            df: Polars DataFrame
            unique_key: Column to use for deduplication
            sort_column: Optional column to sort by before deduplication
            keep: Which duplicate to keep ('first', 'last', 'none')
        
        Returns:
            Deduplicated DataFrame
        """
        if not POLARS_AVAILABLE:
            raise ImportError("Polars is required. Install with: pip install polars")
        
        if sort_column:
            df = df.sort(sort_column)
        
        return df.unique(subset=[unique_key], keep=keep)
    
    @staticmethod
    def aggregate(
        df: 'pl.DataFrame',
        group_by: List[str],
        aggregations: Dict[str, List[str]]
    ) -> 'pl.DataFrame':
        """
        Perform aggregations using Polars (much faster than SQL for complex aggregations).
        
        Args:
            df: Polars DataFrame
            group_by: Columns to group by
            aggregations: Dict mapping column names to aggregation functions
                          e.g., {'amount': ['sum', 'avg'], 'order_id': ['count']}
        
        Returns:
            Aggregated DataFrame
        """
        if not POLARS_AVAILABLE:
            raise ImportError("Polars is required. Install with: pip install polars")
        
        # Build aggregation expressions
        agg_exprs = []
        for col, funcs in aggregations.items():
            for func in funcs:
                func_name = func.lower()
                if func_name == 'sum':
                    agg_exprs.append(pl.col(col).sum().alias(f"{col}_sum"))
                elif func_name == 'avg' or func_name == 'mean':
                    agg_exprs.append(pl.col(col).mean().alias(f"{col}_avg"))
                elif func_name == 'count':
                    agg_exprs.append(pl.col(col).count().alias(f"{col}_count"))
                elif func_name == 'min':
                    agg_exprs.append(pl.col(col).min().alias(f"{col}_min"))
                elif func_name == 'max':
                    agg_exprs.append(pl.col(col).max().alias(f"{col}_max"))
                elif func_name == 'std' or func_name == 'stddev':
                    agg_exprs.append(pl.col(col).std().alias(f"{col}_std"))
                elif func_name == 'distinct' or func_name == 'n_unique':
                    agg_exprs.append(pl.col(col).n_unique().alias(f"{col}_distinct"))
        
        return df.group_by(group_by).agg(agg_exprs)
    
    @staticmethod
    def validate_data_quality(
        df: 'pl.DataFrame',
        checks: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Perform data quality checks using Polars (faster than SQL queries).
        
        Args:
            df: Polars DataFrame
            checks: Dictionary of checks to perform
                   e.g., {
                       'not_null': ['order_id', 'customer_id'],
                       'unique': ['order_id'],
                       'accepted_values': {'status': ['pending', 'completed', 'cancelled']},
                       'range': {'amount': (0, 1000000)}
                   }
        
        Returns:
            Dictionary with check results
        """
        if not POLARS_AVAILABLE:
            raise ImportError("Polars is required. Install with: pip install polars")
        
        results = {}
        
        # Not null checks
        if 'not_null' in checks:
            for col in checks['not_null']:
                null_count = df.filter(pl.col(col).is_null()).height
                results[f"{col}_not_null"] = {
                    "passed": null_count == 0,
                    "null_count": null_count,
                    "total_rows": df.height
                }
        
        # Unique checks
        if 'unique' in checks:
            for col in checks['unique']:
                unique_count = df[col].n_unique()
                total_count = df.height
                duplicates = total_count - unique_count
                results[f"{col}_unique"] = {
                    "passed": duplicates == 0,
                    "duplicate_count": duplicates,
                    "unique_count": unique_count,
                    "total_rows": total_count
                }
        
        # Accepted values checks
        if 'accepted_values' in checks:
            for col, values in checks['accepted_values'].items():
                invalid = df.filter(~pl.col(col).is_in(values)).height
                results[f"{col}_accepted_values"] = {
                    "passed": invalid == 0,
                    "invalid_count": invalid,
                    "total_rows": df.height
                }
        
        # Range checks
        if 'range' in checks:
            for col, (min_val, max_val) in checks['range'].items():
                out_of_range = df.filter(
                    (pl.col(col) < min_val) | (pl.col(col) > max_val)
                ).height
                results[f"{col}_range"] = {
                    "passed": out_of_range == 0,
                    "out_of_range_count": out_of_range,
                    "total_rows": df.height
                }
        
        return results
    
    @staticmethod
    def filter_and_transform(
        df: 'pl.DataFrame',
        filters: Optional[List[str]] = None,
        transformations: Optional[Dict[str, str]] = None
    ) -> 'pl.DataFrame':
        """
        Apply filters and transformations using Polars expressions (vectorized operations).
        
        Args:
            df: Polars DataFrame
            filters: List of filter expressions (Polars expressions as strings)
            transformations: Dict mapping new column names to transformation expressions
        
        Returns:
            Transformed DataFrame
        """
        if not POLARS_AVAILABLE:
            raise ImportError("Polars is required. Install with: pip install polars")
        
        result = df
        
        # Apply filters
        if filters:
            for filter_expr in filters:
                # Note: In production, use proper Polars expression parsing
                # This is a simplified version
                result = result.filter(pl.col(filter_expr.split()[0]) == filter_expr.split()[2])
        
        # Apply transformations
        if transformations:
            transform_exprs = []
            for new_col, expr in transformations.items():
                # Parse and create Polars expression
                # In production, use a proper expression parser
                transform_exprs.append(pl.col(expr).alias(new_col))
            
            if transform_exprs:
                result = result.with_columns(transform_exprs)
        
        return result
    
    @staticmethod
    def join_dataframes(
        left: 'pl.DataFrame',
        right: 'pl.DataFrame',
        on: str,
        how: str = 'inner'
    ) -> 'pl.DataFrame':
        """
        Join two DataFrames using Polars (faster than SQL JOIN for large datasets).
        
        Args:
            left: Left DataFrame
            right: Right DataFrame
            on: Column name to join on
            how: Join type ('inner', 'left', 'right', 'outer')
        
        Returns:
            Joined DataFrame
        """
        if not POLARS_AVAILABLE:
            raise ImportError("Polars is required. Install with: pip install polars")
        
        return left.join(right, on=on, how=how)
    
    @staticmethod
    def convert_to_dicts(df: 'pl.DataFrame') -> List[Dict[str, Any]]:
        """
        Convert Polars DataFrame to list of dictionaries (optimized).
        
        Args:
            df: Polars DataFrame
        
        Returns:
            List of dictionaries
        """
        if not POLARS_AVAILABLE:
            raise ImportError("Polars is required. Install with: pip install polars")
        
        return df.to_dicts()
    
    @staticmethod
    def convert_from_dicts(data: List[Dict[str, Any]]) -> 'pl.DataFrame':
        """
        Convert list of dictionaries to Polars DataFrame (optimized).
        
        Args:
            data: List of dictionaries
        
        Returns:
            Polars DataFrame
        """
        if not POLARS_AVAILABLE:
            raise ImportError("Polars is required. Install with: pip install polars")
        
        return pl.DataFrame(data)


class PolarsTestRunner:
    """
    High-performance test runner using Polars for data quality tests.
    Much faster than SQL-based tests for large datasets.
    """
    
    def __init__(self, sf_executor=None):
        """
        Initialize Polars test runner.
        
        Args:
            sf_executor: Optional Snowflake executor for fetching data
        """
        self.sf_executor = sf_executor
        self.processor = PolarsDataProcessor()
    
    def run_unique_test(
        self,
        df: 'pl.DataFrame',
        column_name: str
    ) -> Dict[str, Any]:
        """
        Run unique test using Polars (faster than SQL COUNT/GROUP BY).
        
        Args:
            df: Polars DataFrame
            column_name: Column to test for uniqueness
        
        Returns:
            Test result dictionary
        """
        if not POLARS_AVAILABLE:
            raise ImportError("Polars is required. Install with: pip install polars")
        
        total_rows = df.height
        unique_count = df[column_name].n_unique()
        duplicate_count = total_rows - unique_count
        
        return {
            "test_name": "unique",
            "column_name": column_name,
            "passed": duplicate_count == 0,
            "duplicate_count": duplicate_count,
            "unique_count": unique_count,
            "total_rows": total_rows
        }
    
    def run_not_null_test(
        self,
        df: 'pl.DataFrame',
        column_name: str
    ) -> Dict[str, Any]:
        """
        Run not null test using Polars (faster than SQL WHERE IS NULL).
        
        Args:
            df: Polars DataFrame
            column_name: Column to test
        
        Returns:
            Test result dictionary
        """
        if not POLARS_AVAILABLE:
            raise ImportError("Polars is required. Install with: pip install polars")
        
        null_count = df.filter(pl.col(column_name).is_null()).height
        total_rows = df.height
        
        return {
            "test_name": "not_null",
            "column_name": column_name,
            "passed": null_count == 0,
            "null_count": null_count,
            "total_rows": total_rows
        }
    
    def run_accepted_values_test(
        self,
        df: 'pl.DataFrame',
        column_name: str,
        values: List[Any]
    ) -> Dict[str, Any]:
        """
        Run accepted values test using Polars (faster than SQL WHERE NOT IN).
        
        Args:
            df: Polars DataFrame
            column_name: Column to test
            values: List of accepted values
        
        Returns:
            Test result dictionary
        """
        if not POLARS_AVAILABLE:
            raise ImportError("Polars is required. Install with: pip install polars")
        
        invalid_count = df.filter(~pl.col(column_name).is_in(values)).height
        total_rows = df.height
        
        return {
            "test_name": "accepted_values",
            "column_name": column_name,
            "passed": invalid_count == 0,
            "invalid_count": invalid_count,
            "total_rows": total_rows,
            "accepted_values": values
        }
    
    def run_all_tests(
        self,
        df: 'pl.DataFrame',
        test_configs: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """
        Run all configured tests on DataFrame using Polars.
        
        Args:
            df: Polars DataFrame
            test_configs: Dictionary of test configurations
        
        Returns:
            List of test results
        """
        results = []
        
        # Run unique tests
        if 'unique' in test_configs:
            for col in test_configs['unique']:
                results.append(self.run_unique_test(df, col))
        
        # Run not null tests
        if 'not_null' in test_configs:
            for col in test_configs['not_null']:
                results.append(self.run_not_null_test(df, col))
        
        # Run accepted values tests
        if 'accepted_values' in test_configs:
            for col, values in test_configs['accepted_values'].items():
                results.append(self.run_accepted_values_test(df, col, values))
        
        return results


class PolarsResultFormatter:
    """
    High-performance result formatting using Polars.
    Optimized for large result sets.
    """
    
    @staticmethod
    def format_results(
        results: List[Dict[str, Any]],
        format_type: str = "json",
        limit: Optional[int] = None
    ) -> str:
        """
        Format results using Polars for fast processing.
        
        Args:
            results: List of result dictionaries
            format_type: Output format ('json', 'csv', 'table')
            limit: Optional limit on number of rows
        
        Returns:
            Formatted string
        """
        if not POLARS_AVAILABLE:
            # Fallback to standard formatting
            from framework.executor import ResultFormatter
            return ResultFormatter.format(results, format_type)
        
        # Convert to Polars DataFrame
        df = pl.DataFrame(results)
        
        # Apply limit if specified
        if limit:
            df = df.head(limit)
        
        if format_type == "json":
            # Polars write_json is already optimized (uses Rust internally)
            return df.write_json()
        elif format_type == "csv":
            return df.write_csv()
        elif format_type == "table":
            # Use Polars' built-in display
            return str(df)
        else:
            raise ValueError(f"Unsupported format: {format_type}")
    
    @staticmethod
    def aggregate_results(
        results: List[Dict[str, Any]],
        group_by: List[str],
        aggregations: Dict[str, List[str]]
    ) -> List[Dict[str, Any]]:
        """
        Aggregate results using Polars (much faster than Python loops).
        
        Args:
            results: List of result dictionaries
            group_by: Columns to group by
            aggregations: Aggregation functions
        
        Returns:
            Aggregated results as list of dictionaries
        """
        if not POLARS_AVAILABLE:
            # Fallback to standard aggregation
            from collections import defaultdict
            grouped = defaultdict(lambda: defaultdict(list))
            for result in results:
                key = tuple(result.get(col) for col in group_by)
                for col, funcs in aggregations.items():
                    grouped[key][col].append(result.get(col))
            
            aggregated = []
            for key, values in grouped.items():
                row = dict(zip(group_by, key))
                for col, funcs in values.items():
                    for func in funcs:
                        if func == 'sum':
                            row[f"{col}_sum"] = sum(values[col])
                        elif func == 'avg':
                            row[f"{col}_avg"] = sum(values[col]) / len(values[col])
                aggregated.append(row)
            return aggregated
        
        # Use Polars for fast aggregation
        df = pl.DataFrame(results)
        processor = PolarsDataProcessor()
        aggregated_df = processor.aggregate(df, group_by, aggregations)
        return aggregated_df.to_dicts()

