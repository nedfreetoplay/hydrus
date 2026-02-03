import os
import sys
import threading
import time

from hydrus.core import HydrusConstants as HC

# this guy catches crashes and dumps all thread stacks to original stderr or the stable file handle you pass to it
# I am informed it has zero overhead but it will pre-empt or otherwise mess around with other dump creators
# Update: MPV playback causes crashes with this on because of pre-emption of internal dll exception gubbins, hooray
import faulthandler

class HydrusLogger( object ):
    
    def __init__( self, db_dir, prefix ):
        
        self._db_dir = db_dir
        self._prefix = prefix
        
        self._currently_crash_reporting = False
        
        self._lock = threading.Lock()
        
        self._log_closed = False
        
        self._problem_with_previous_stdout = False
        
        self._previous_sys_stdout = None
        self._previous_sys_stderr = None
        
    
    def __enter__( self ):
        
        self._previous_sys_stdout = sys.stdout
        self._previous_sys_stderr = sys.stderr
        
        self._problem_with_previous_stdout = False
        
        self._open_log()
        
        sys.stdout = self
        sys.stderr = self
        
        return self
        
    
    def __exit__( self, exc_type, exc_val, exc_tb ):
        
        self._close_log()
        
        sys.stdout = self._previous_sys_stdout
        sys.stderr = self._previous_sys_stderr
        
        self._previous_sys_stdout = None
        self._previous_sys_stderr = None
        
        self._log_closed = True
        
        return False
        
    
    def _close_log( self ) -> None:
        
        if self._currently_crash_reporting:
            
            faulthandler.disable()
            
        
        self._log_file.close()
        
    
    def _get_log_path( self ) -> str:
        
        current_time_struct = time.localtime()
        
        ( current_year, current_month ) = ( current_time_struct.tm_year, current_time_struct.tm_mon )
        
        log_filename = '{} - {}-{:02}.log'.format( self._prefix, current_year, current_month )
        
        log_path = os.path.join( self._db_dir, log_filename )
        
        return log_path
        
    
    def _open_log( self ) -> None:
        
        self._log_path = self._get_log_path()
        
        is_new_file = not os.path.exists( self._log_path )
        
        self._log_file = open( self._log_path, 'a', encoding = 'utf-8' )
        
        if self._currently_crash_reporting:
            
            faulthandler.enable( file = self._log_file, all_threads = True )
            
        
        if is_new_file:
            
            self._log_file.write( HC.UNICODE_BYTE_ORDER_MARK ) # Byte Order Mark, BOM, to help reader software interpret this as utf-8
            
        
    
    def _switch_to_a_new_log_file_if_due( self ) -> None:
        
        correct_log_path = self._get_log_path()
        
        if correct_log_path != self._log_path:
            
            self._close_log()
            
            self._open_log()
            
        
    
    def flip_crash_reporting( self ):
        
        if self._currently_crash_reporting:
            
            faulthandler.disable()
            
        else:
            
            faulthandler.enable( self._log_file, all_threads = True )
            
        
        self._currently_crash_reporting = not self._currently_crash_reporting
        
    
    def flush( self ) -> None:
        
        if self._log_closed:
            
            return
            
        
        with self._lock:
            
            if not self._problem_with_previous_stdout:
                
                try:
                    
                    self._previous_sys_stdout.flush()
                    
                except IOError:
                    
                    self._problem_with_previous_stdout = True
                    
                
            
            self._log_file.flush()
            
            self._switch_to_a_new_log_file_if_due()
            
        
    
    def isatty( self ) -> bool:
        
        return False
        
    
    def currently_crash_reporting( self ):
        
        return self._currently_crash_reporting
        
    
    def write( self, value ) -> None:
        
        if self._log_closed:
            
            return
            
        
        with self._lock:
            
            if value in ( '\n', '\n' ):
                
                prefix = ''
                
            else:
                
                prefix = 'v{}, {}: '.format( HC.SOFTWARE_VERSION, time.strftime( '%Y-%m-%d %H:%M:%S' ) )
                
            
            message = prefix + value
            
            if not self._problem_with_previous_stdout:
                
                try:
                    
                    self._previous_sys_stdout.write( message )
                    
                except:
                    
                    self._problem_with_previous_stdout = True
                    
                
            
            self._log_file.write( message )
            
        
    
