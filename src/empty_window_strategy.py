from abc import ABC, abstractmethod
from typing import Optional, Dict, Any


class EmptyWindowStrategy(ABC):
    """Base strategy for handling empty windows"""
    
    @abstractmethod
    def handle(self, cell_id: str, window_start: int, window_end: int, 
               context: Optional[Dict[str, Any]] = None) -> Optional[Dict]:
        """
        Handle an empty window.
        
        Args:
            cell_id: Identifier for the cell
            window_start: Start timestamp of the window
            window_end: End timestamp of the window
            context: Optional context data (e.g., profile-specific fields, last known values)
            
        Returns:
            Dictionary with processed data for the empty window, or None to skip
        """
        raise NotImplementedError


class SkipStrategy(EmptyWindowStrategy):
    """Skip empty windows - don't generate any output"""
    
    def handle(self, cell_id: str, window_start: int, window_end: int, 
               context: Optional[Dict[str, Any]] = None) -> Optional[Dict]:
        """Return None to skip processing this window"""
        return None


class ZeroFillStrategy(EmptyWindowStrategy):
    """Fill empty windows with zero/null values"""
    
    def handle(self, cell_id: str, window_start: int, window_end: int, 
               context: Optional[Dict[str, Any]] = None) -> Optional[Dict]:
        """
        Generate a record with zero/null values for all metrics.
        
        Context should contain:
            - fields: List of field names to zero-fill
            - metadata: Dict of non-metric metadata to include (network, bandwidth, etc.)
        """
        if not context or 'fields' not in context:
            return None
            
        result = {
            'cell_index': cell_id,
            'sample_count': 0,
            'start_time': window_start,
            'end_time': window_end,
            'is_empty_window': True
        }
        
        # Add metadata if provided
        if 'metadata' in context:
            result.update(context['metadata'])
        
        # Zero-fill all metric fields
        for field in context['fields']:
            result[field] = {
                'min': None,
                'max': None,
                'mean': None,
                'std': None,
                'samples': 0
            }
            
        return result


class ForwardFillStrategy(EmptyWindowStrategy):
    """Forward-fill empty windows with the last known values"""
    
    def handle(self, cell_id: str, window_start: int, window_end: int, 
               context: Optional[Dict[str, Any]] = None) -> Optional[Dict]:
        """
        Generate a record using the last known values.
        
        Context should contain:
            - last_values: Dict with the last known processed values
        """
        if not context or 'last_values' not in context:
            # If no previous values, fall back to skip
            return None
            
        last_values = context['last_values'].copy()
        
        # Update timestamps to reflect the current window
        last_values['start_time'] = window_start
        last_values['end_time'] = window_end
        last_values['sample_count'] = 0
        last_values['is_empty_window'] = True
        last_values['forward_filled'] = True
        
        return last_values
