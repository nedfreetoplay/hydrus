import collections
import collections.abc
import threading
import typing

from hydrus.core import HydrusData
from hydrus.core import HydrusGlobals as HG
from hydrus.core import HydrusTime

from hydrus.client import ClientGlobals as CG

class CacheableObject( object ):
    
    def get_estimated_memory_footprint(self) -> int:
        
        raise NotImplementedError()
        
    
    def is_finished_loading(self):
        
        raise NotImplementedError()
        
    

class DataCache( object ):
    
    def __init__( self, controller: "CG.ClientController.Controller", name, cache_size, timeout = 1200 ):
        
        self._controller = controller
        self._name = name
        self._cache_size = cache_size
        self._timeout = timeout
        
        self._keys_to_data: dict[ typing.Any, tuple[ CacheableObject, int ] ] = {}
        self._keys_fifo = collections.OrderedDict()
        
        self._total_estimated_memory_footprint = 0
        
        self._lock = threading.Lock()
        
        self._controller.sub( self, 'MaintainCache', 'memory_maintenance_pulse' )
        
    
    def _delete(self, key):
        
        if key in self._keys_fifo:
            
            del self._keys_fifo[ key ]
            
        
        if key not in self._keys_to_data:
            
            return
            
        
        ( data, size_estimate ) = self._keys_to_data[ key ]
        
        del self._keys_to_data[ key ]
        
        self._total_estimated_memory_footprint -= size_estimate
        
        if HG.cache_report_mode:
            
            HydrusData.show_text( 'Cache "{}" removing "{}", size "{}". Current size {}.'.format( self._name, key, HydrusData.to_human_bytes( size_estimate ), HydrusData.convert_value_range_to_bytes( self._total_estimated_memory_footprint, self._cache_size ) ) )
            
        
    
    def _delete_item(self):
        
        ( deletee_key, last_access_time ) = self._keys_fifo.popitem( last = False )
        
        self._delete(deletee_key)
        
    
    def _get_data(self, key) -> CacheableObject:
        
        if key not in self._keys_to_data:
            
            raise Exception( 'Cache error! Looking for {}, but it was missing.'.format( key ) )
            
        
        self._touch_key(key)

        ( data, size_estimate ) = self._keys_to_data[ key ]
        
        new_estimate = data.get_estimated_memory_footprint()
        
        if new_estimate != size_estimate:
            
            self._total_estimated_memory_footprint += new_estimate - size_estimate
            
            self._keys_to_data[ key ] = ( data, new_estimate )
            
        
        return data
        
    
    def _touch_key(self, key):
        
        # have to delete first, rather than overwriting, so the ordereddict updates its internal order
        if key in self._keys_fifo:
            
            del self._keys_fifo[ key ]
            
        
        self._keys_fifo[ key ] = HydrusTime.get_now()
        
    
    def clear(self):
        
        with self._lock:
            
            self._keys_to_data = {}
            self._keys_fifo = collections.OrderedDict()
            
            self._total_estimated_memory_footprint = 0
            
        
    
    def add_data(self, key, data: CacheableObject):
        
        with self._lock:
            
            if key not in self._keys_to_data:
                
                while self._total_estimated_memory_footprint > self._cache_size:
                    
                    self._delete_item()
                    
                
                size_estimate = data.get_estimated_memory_footprint()
                
                self._keys_to_data[ key ] = ( data, size_estimate )
                
                self._total_estimated_memory_footprint += size_estimate
                
                self._touch_key(key)
                
                if HG.cache_report_mode:
                    
                    HydrusData.show_text(
                        'Cache "{}" adding "{}" ({}). Current size {}.'.format(
                            self._name,
                            key,
                            HydrusData.to_human_bytes( size_estimate ),
                            HydrusData.convert_value_range_to_bytes( self._total_estimated_memory_footprint, self._cache_size )
                        )
                    )
                    
                
            
        
    
    def delete_data(self, key):
        
        with self._lock:
            
            self._delete(key)
            
        
    
    def get_all_keys(self) -> list[ object]:
        
        with self._lock:
            
            return list( self._keys_to_data.keys() )
            
        
    
    def get_data(self, key) -> CacheableObject:
        
        with self._lock:
            
            return self._get_data(key)
            
        
    
    def get_if_has_data(self, key) -> CacheableObject | None:
        
        with self._lock:
            
            if key in self._keys_to_data:
                
                return self._get_data(key)
                
            else:
                
                return None
                
            
        
    
    def get_size_limit(self) -> int:
        
        with self._lock:
            
            return self._cache_size
            
        
    
    def has_data(self, key) -> bool:
        
        with self._lock:
            
            return key in self._keys_to_data
            
        
    
    def maintain_cache(self) -> None:
        
        with self._lock:
            
            while self._total_estimated_memory_footprint > self._cache_size:
                
                self._delete_item()
                
            
            while True:
                
                if len( self._keys_fifo ) == 0:
                    
                    break
                    
                else:
                    
                    ( key, last_access_time ) = next( iter( self._keys_fifo.items() ) )
                    
                    if HydrusTime.time_has_passed( last_access_time + self._timeout ):
                        
                        self._delete_item()
                        
                    else:
                        
                        break
                        
                    
                
            
        
    
    def set_cache_size_and_timeout(self, cache_size, timeout) -> None:
        
        with self._lock:
            
            self._cache_size = cache_size
            self._timeout = timeout
            
        
        self.maintain_cache()
        
    
    def touch_key(self, key):
        
        with self._lock:
            
            self._touch_key(key)
            
        
    
    def _delete_loaded_items_until_free_space(self, free_space_desired: int) -> bool:
        
        current_free_space = self._cache_size - self._total_estimated_memory_footprint
        
        if current_free_space > free_space_desired:
            
            return True
            
        
        for key in list( self._keys_fifo.keys() ):
            
            ( data, size_estimate ) = self._keys_to_data[ key ]
            
            if not data.is_finished_loading():
                
                # this guy is still rendering, let's not push him out for a different prefetch
                continue
                
            
            self._delete(key)
            
            current_free_space = self._cache_size - self._total_estimated_memory_footprint
            
            if current_free_space > free_space_desired:
                
                return True
                
            
        
        # we went through the whole cache and couldn't find enough easy freed-up space to fit this new guy in. not a good time to prefetch it!
        return False
        
    
    def try_to_flush_easy_space_for_prefetch(self, free_space_desired: int):
        
        # ok, caller wants to do a prefetch. question is, is there enough soft space in the fifo queue to jam another guy in?
        # or, are we actually pretty busy now with image rendering and stuff and we don't really want to cut too hard?
        # let's see if we can free up some space and let the caller know
        
        with self._lock:
            
            successful = self._delete_loaded_items_until_free_space(free_space_desired)
            
            return successful
            
        
    
