"""
Tests for SQL parser
"""

import pytest
from pathlib import Path
from framework.parser import SQLParser


def test_extract_dollar_variables():
    """Test extraction of $variable patterns"""
    parser = SQLParser(Path('.'))
    sql = "SELECT * FROM table WHERE date = $start_date AND id = $user_id"
    variables = parser._extract_dollar_variables(sql)
    assert variables == {'start_date', 'user_id'}


def test_extract_dependencies_from_comments():
    """Test extraction of dependencies from comments"""
    parser = SQLParser(Path('.'))
    sql = "-- depends_on: model1, model2\nSELECT * FROM table"
    deps = parser._extract_dependencies_from_comments(sql)
    assert deps == {'model1', 'model2'}


def test_extract_config_from_comments():
    """Test extraction of config from comments"""
    parser = SQLParser(Path('.'))
    sql = "-- config: materialized=table, incremental_strategy=time\nSELECT * FROM table"
    config = parser._extract_config_from_comments(sql)
    assert config == {'materialized': 'table', 'incremental_strategy': 'time'}

