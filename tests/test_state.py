"""
Tests for state management
"""

import pytest
from pathlib import Path
import tempfile
import shutil
from framework.state import StateManager, ModelState


def test_model_state_creation():
    """Test creating model state"""
    state = ModelState(model_name='test_model', file_hash='abc123')
    assert state.model_name == 'test_model'
    assert state.file_hash == 'abc123'
    assert state.execution_count == 0


def test_mark_execution():
    """Test marking executions"""
    state = ModelState(model_name='test_model', file_hash='abc123')
    
    state.mark_execution(success=True)
    assert state.execution_count == 1
    assert state.success_count == 1
    assert state.failure_count == 0
    
    state.mark_execution(success=False)
    assert state.execution_count == 2
    assert state.success_count == 1
    assert state.failure_count == 1


def test_state_manager():
    """Test state manager"""
    # Create temporary directory
    temp_dir = Path(tempfile.mkdtemp())
    
    try:
        manager = StateManager(temp_dir, 'test')
        
        # Update model state
        manager.update_model_state('model_a', 'hash1', ['dep1'])
        
        # Get model state
        state = manager.get_model_state('model_a')
        assert state.model_name == 'model_a'
        assert state.file_hash == 'hash1'
        assert state.dependencies == ['dep1']
        
        # Mark execution
        manager.mark_execution('model_a', True)
        
        state = manager.get_model_state('model_a')
        assert state.execution_count == 1
        
    finally:
        # Cleanup
        shutil.rmtree(temp_dir)


def test_has_changed():
    """Test change detection"""
    temp_dir = Path(tempfile.mkdtemp())
    
    try:
        manager = StateManager(temp_dir, 'test')
        manager.update_model_state('model_a', 'hash1', [])
        
        # No change
        assert not manager.has_changed('model_a', 'hash1')
        
        # File hash changed
        assert manager.has_changed('model_a', 'hash2')
        
        # New model
        assert manager.has_changed('model_b', 'hash1')
        
    finally:
        shutil.rmtree(temp_dir)

