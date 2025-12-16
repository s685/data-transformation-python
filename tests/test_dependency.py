"""
Tests for dependency graph
"""

import pytest
from framework.dependency import DependencyGraph
from utils.errors import CircularDependencyError


def test_dependency_graph_creation():
    """Test creating a dependency graph"""
    graph = DependencyGraph()
    graph.add_model('model_a', set())
    graph.add_model('model_b', {'model_a'})
    graph.add_model('model_c', {'model_a', 'model_b'})
    
    assert len(graph.nodes) == 3
    assert graph.get_dependencies('model_c') == {'model_a', 'model_b'}


def test_topological_sort():
    """Test topological sorting"""
    graph = DependencyGraph()
    graph.add_model('model_a', set())
    graph.add_model('model_b', {'model_a'})
    graph.add_model('model_c', {'model_b'})
    
    order = graph.topological_sort()
    
    # model_a should be in first level
    assert 'model_a' in order[0]
    # model_b should be after model_a
    assert 'model_b' in order[1]
    # model_c should be last
    assert 'model_c' in order[2]


def test_circular_dependency_detection():
    """Test detection of circular dependencies"""
    graph = DependencyGraph()
    graph.add_model('model_a', {'model_b'})
    graph.add_model('model_b', {'model_a'})
    
    with pytest.raises(CircularDependencyError):
        graph.topological_sort()


def test_get_all_dependencies():
    """Test getting all transitive dependencies"""
    graph = DependencyGraph()
    graph.add_model('model_a', set())
    graph.add_model('model_b', {'model_a'})
    graph.add_model('model_c', {'model_b'})
    
    deps = graph.get_all_dependencies('model_c')
    assert deps == {'model_a', 'model_b'}

