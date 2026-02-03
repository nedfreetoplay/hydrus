import os
import tempfile
import threading

from hydrus.core import HydrusConstants as HC
from hydrus.core import HydrusData
from hydrus.core import HydrusGlobals as HG
from hydrus.core import HydrusPaths
from hydrus.core import HydrusTime

TEMP_PATH_LOCK = threading.Lock()
IN_USE_TEMP_PATHS = set()

def clean_ip_temp_path( os_file_handle, temp_path ):
    
    try:
        
        os.close( os_file_handle )
        
    except OSError:
        
        try:
            
            os.close( os_file_handle )
            
        except OSError:
            
            HydrusData.print_text( 'Could not close the temporary file ' + temp_path )
            
            return
            
        
    
    try:
        
        if HC.PLATFORM_WINDOWS:
            
            path_stat = os.stat( temp_path )
            
            # this can be needed on a Windows device
            HydrusPaths.try_to_make_file_writeable( temp_path, path_stat )
            
        
        os.remove( temp_path )
        
    except OSError:
        
        with TEMP_PATH_LOCK:
            
            IN_USE_TEMP_PATHS.add( ( HydrusTime.get_now(), temp_path ) )
            
        
    

def clean_up_old_temp_paths():
    
    with TEMP_PATH_LOCK:
        
        data = list( IN_USE_TEMP_PATHS )
        
        for row in data:
            
            ( time_failed, temp_path ) = row
            
            if HydrusTime.time_has_passed( time_failed + 60 ):
                
                try:
                    
                    os.remove( temp_path )
                    
                    IN_USE_TEMP_PATHS.discard( row )
                    
                except OSError:
                    
                    if HydrusTime.time_has_passed( time_failed + 1200 ):
                        
                        IN_USE_TEMP_PATHS.discard( row )
                        
                    
                
            
        
    

def get_current_sqlite_temp_dir():
    
    if 'SQLITE_TMPDIR' in os.environ:
        
        return os.environ[ 'SQLITE_TMPDIR' ]
        
    
    return get_current_temp_dir()
    

def get_current_temp_dir():
    
    return tempfile.gettempdir()
    

def initialise_hydrus_temp_dir():
    
    return tempfile.mkdtemp( prefix = 'hydrus' )
    

def set_env_temp_dir( path ):
    
    try:
        
        HydrusPaths.make_sure_directory_exists( path )
        
    except Exception as e:
        
        raise Exception( f'Could not create the temp dir "{path}"!' )
        
    
    if not HydrusPaths.directory_is_writeable( path ):
        
        raise Exception( f'The given temp directory, "{path}", does not seem to be writeable-to!' )
        
    
    for tmp_name in ( 'TMPDIR', 'TEMP', 'TMP' ):
        
        if tmp_name in os.environ:
            
            os.environ[ tmp_name ] = path
            
        
    
    tempfile.tempdir = path
    

def get_sub_temp_dir( prefix = '' ):
    
    hydrus_temp_dir = HG.controller.get_hydrus_temp_dir()
    
    return tempfile.mkdtemp( prefix = prefix, dir = hydrus_temp_dir )
    

def get_temp_path( suffix = '', dir = None ):
    
    if dir is None:
        
        dir = HG.controller.get_hydrus_temp_dir()
        
    
    return tempfile.mkstemp( suffix = suffix, prefix = 'hydrus', dir = dir )
    
