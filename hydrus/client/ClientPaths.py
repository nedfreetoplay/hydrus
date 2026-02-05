import collections.abc
import webbrowser

from hydrus.core import HydrusConstants as HC
from hydrus.core import HydrusData
from hydrus.core import HydrusPaths

from hydrus.client import ClientGlobals as CG

try:
    
    from showinfm import show_in_file_manager
    
    SHOW_IN_FILE_MANAGER_OK = True
    
except:
    
    SHOW_IN_FILE_MANAGER_OK = False
    

if HC.PLATFORM_WINDOWS:
    
    try:
        
        from hydrus.client import ClientWindowsIntegration
        
    except Exception as e:
        
        HydrusData.print_text( 'Could not import ClientWindowsIntegration--maybe you need PyWin32 in your venv?' )
        HydrusData.print_exception( e, do_wait = False )
        
    

CAN_OPEN_FILE_LOCATION = HC.PLATFORM_WINDOWS or HC.PLATFORM_MACOS or ( HC.PLATFORM_LINUX and SHOW_IN_FILE_MANAGER_OK )

def delete_path(path, always_delete_fully = False):
    
    delete_to_recycle_bin = HC.options[ 'delete_to_recycle_bin' ]
    
    if delete_to_recycle_bin and not always_delete_fully:
        
        HydrusPaths.recycle_path( path )
        
    else:
        
        HydrusPaths.delete_path( path )
        
    

def launch_path_in_web_browser(path):
    
    launch_url_in_web_browser('file:///' + path)
    

def launch_url_in_web_browser(url):
    
    web_browser_path = CG.client_controller.new_options.get_noneable_string('web_browser_path')
    
    if web_browser_path is None:
        
        webbrowser.open( url )
        
    else:
        
        HydrusPaths.launch_file( url, launch_path = web_browser_path )
        
    

def open_file_location(path: str):
    
    if SHOW_IN_FILE_MANAGER_OK:
        
        show_in_file_manager( path )
                
    else:
        
        HydrusPaths.open_file_location( path )
        
    

def open_file_locations(paths: collections.abc.Sequence[str]):
    
    if SHOW_IN_FILE_MANAGER_OK:
        
        show_in_file_manager( paths )
        
    else:
        
        for path in paths:
        
            HydrusPaths.open_file_location( path )
            
    

def open_native_file_properties(path: str):
    
    if HC.PLATFORM_WINDOWS:
        
        ClientWindowsIntegration.open_file_properties(path)
        
    

def open_file_with_dialog(path: str):
    
    if HC.PLATFORM_WINDOWS:
        
        ClientWindowsIntegration.open_file_with(path)
        
    
