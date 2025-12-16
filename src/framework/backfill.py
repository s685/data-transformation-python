"""
Backfill capabilities for incremental models
"""

from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
import asyncio

from framework.executor import ModelExecutor
from utils.logger import get_logger

logger = get_logger(__name__)


class BackfillExecutor:
    """Execute backfill operations for incremental models"""
    
    def __init__(self, model_executor: ModelExecutor):
        """Initialize backfill executor"""
        self.model_executor = model_executor
    
    def backfill_date_range(
        self,
        model_name: str,
        start_date: datetime,
        end_date: datetime,
        interval_days: int = 1,
        variables: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """
        Backfill a model for a date range
        
        Args:
            model_name: Model name
            start_date: Start date
            end_date: End date
            interval_days: Days per interval
            variables: Additional variables
        
        Returns:
            List of execution results
        """
        results = []
        current_date = start_date
        
        logger.info(
            f"Starting backfill for {model_name} "
            f"from {start_date} to {end_date} "
            f"(interval: {interval_days} days)"
        )
        
        while current_date <= end_date:
            interval_end = min(
                current_date + timedelta(days=interval_days),
                end_date
            )
            
            # Prepare variables for this interval
            interval_vars = variables.copy() if variables else {}
            interval_vars.update({
                'start_date': current_date.strftime('%Y-%m-%d'),
                'end_date': interval_end.strftime('%Y-%m-%d')
            })
            
            logger.info(f"Backfilling interval: {interval_vars['start_date']} to {interval_vars['end_date']}")
            
            # Execute model for this interval
            result = self.model_executor.execute_model(
                model_name,
                interval_vars,
                dry_run=False
            )
            
            results.append({
                **result,
                'interval_start': current_date.isoformat(),
                'interval_end': interval_end.isoformat()
            })
            
            current_date = interval_end + timedelta(days=1)
        
        logger.info(f"Backfill completed: {len(results)} intervals processed")
        
        return results

