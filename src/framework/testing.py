"""
Testing framework with data quality tests (dbt-inspired)
"""

from typing import Dict, List, Any, Optional
from dataclasses import dataclass

from framework.connection import SnowflakeExecutor
from framework.model import TestConfig, ModelConfig
from utils.errors import TestError
from utils.logger import get_logger

logger = get_logger(__name__)


@dataclass
class TestResult:
    """Test execution result"""
    test_name: str
    model_name: str
    column_name: Optional[str]
    status: str  # passed, failed, error
    error_message: Optional[str] = None
    rows_failed: int = 0


class DataQualityTests:
    """Built-in data quality tests"""
    
    @staticmethod
    def unique(table_name: str, column_name: str) -> str:
        """Test for unique values"""
        return f"""
        SELECT COUNT(*) as failures
        FROM (
            SELECT {column_name}, COUNT(*) as cnt
            FROM {table_name}
            GROUP BY {column_name}
            HAVING COUNT(*) > 1
        )
        """
    
    @staticmethod
    def not_null(table_name: str, column_name: str) -> str:
        """Test for non-null values"""
        return f"""
        SELECT COUNT(*) as failures
        FROM {table_name}
        WHERE {column_name} IS NULL
        """
    
    @staticmethod
    def accepted_values(table_name: str, column_name: str, values: List[Any]) -> str:
        """Test for accepted values"""
        value_list = ", ".join([f"'{v}'" if isinstance(v, str) else str(v) for v in values])
        return f"""
        SELECT COUNT(*) as failures
        FROM {table_name}
        WHERE {column_name} NOT IN ({value_list})
        """


class TestRunner:
    """Execute data quality tests"""
    
    def __init__(self, sf_executor: SnowflakeExecutor):
        """Initialize test runner"""
        self.sf_executor = sf_executor
        self.quality_tests = DataQualityTests()
    
    def run_test(
        self,
        test_config: TestConfig,
        model_name: str,
        column_name: Optional[str] = None
    ) -> TestResult:
        """Run a single test"""
        try:
            # Generate test SQL
            test_sql = self._generate_test_sql(test_config, model_name, column_name)
            
            # Execute test
            result = self.sf_executor.execute_query(test_sql, fetch=True)
            
            # Check result
            failures = result[0].get('FAILURES', 0) if result else 0
            
            if failures > 0:
                status = "failed"
                error_msg = f"Test failed with {failures} rows"
            else:
                status = "passed"
                error_msg = None
            
            return TestResult(
                test_name=test_config.test_type,
                model_name=model_name,
                column_name=column_name,
                status=status,
                error_message=error_msg,
                rows_failed=failures
            )
            
        except Exception as e:
            logger.error(f"Test execution error: {e}")
            return TestResult(
                test_name=test_config.test_type,
                model_name=model_name,
                column_name=column_name,
                status="error",
                error_message=str(e)
            )
    
    def _generate_test_sql(
        self,
        test_config: TestConfig,
        model_name: str,
        column_name: Optional[str]
    ) -> str:
        """Generate test SQL based on test type"""
        test_type = test_config.test_type
        params = test_config.params
        
        if test_type == "unique":
            return self.quality_tests.unique(model_name, column_name)
        elif test_type == "not_null":
            return self.quality_tests.not_null(model_name, column_name)
        elif test_type == "accepted_values":
            values = params.get('values', [])
            return self.quality_tests.accepted_values(model_name, column_name, values)
        else:
            raise TestError(f"Unknown test type: {test_type}")
    
    def run_model_tests(self, model_config: ModelConfig) -> List[TestResult]:
        """Run all tests for a model"""
        results = []
        
        # Run model-level tests
        for test in model_config.tests:
            result = self.run_test(test, model_config.name)
            results.append(result)
        
        # Run column-level tests
        for column in model_config.columns:
            for test in column.tests:
                result = self.run_test(test, model_config.name, column.name)
                results.append(result)
        
        return results

