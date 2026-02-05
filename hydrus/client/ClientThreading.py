import threading
import time

from hydrus.core import HydrusConstants as HC
from hydrus.core import HydrusData
from hydrus.core import HydrusLists
from hydrus.core import HydrusTime
from hydrus.core.processes import HydrusThreading

from hydrus.client import ClientGlobals as CG
from hydrus.client.gui import QtPorting as QP

class JobStatus( object ):
    
    def __init__( self, pausable = False, cancellable = False, maintenance_mode = HC.MAINTENANCE_FORCED, only_start_if_unbusy = False, stop_time = None, cancel_on_shutdown = True ):
        
        self._key = HydrusData.generate_key()
        
        self._creation_time = HydrusTime.get_now_float()
        
        self._pausable = pausable
        self._cancellable = cancellable
        self._maintenance_mode = maintenance_mode
        self._only_start_if_unbusy = only_start_if_unbusy
        self._stop_time = stop_time
        self._cancel_on_shutdown = cancel_on_shutdown and maintenance_mode != HC.MAINTENANCE_SHUTDOWN
        
        self._cancelled = False
        self._paused = False
        self._dismissed = False
        self._finish_and_dismiss_time = None
        
        self._i_am_an_ongoing_job = self._pausable or self._cancellable
        
        self._done_event = threading.Event()
        
        if self._i_am_an_ongoing_job:
            
            self._job_finish_time = None
            
        else:
            
            self._done_event.set()
            self._job_finish_time = HydrusTime.get_now_float()
            
        
        self._ui_update_pauser = HydrusThreading.BigJobPauser( 0.1, 0.00001 )
        
        self._yield_pauser = HydrusThreading.BigJobPauser()
        
        self._cancel_tests_regular_checker = HydrusThreading.RegularJobChecker( 1.0 )
        
        self._exception = None
        
        self._urls = []
        self._variable_lock = threading.Lock()
        self._variables = dict()
        
    
    def __eq__( self, other ):
        
        if isinstance( other, JobStatus ):
            
            return self.__hash__() == other.__hash__()
            
        
        return NotImplemented
        
    
    def __hash__( self ):
        
        return self._key.__hash__()
        
    
    def _check_cancel_tests(self):
        
        if self._cancel_tests_regular_checker.due():
            
            if not self._done_event.is_set():
                
                if self._cancel_on_shutdown and HydrusThreading.is_thread_shutting_down():
                    
                    self.cancel()
                    
                    return
                    
                
                if CG.client_controller.should_stop_this_work( self._maintenance_mode, self._stop_time ):
                    
                    self.cancel()
                    
                    return
                    
                
            
            if not self._dismissed:
                
                if self._finish_and_dismiss_time is not None:
                    
                    if HydrusTime.time_has_passed( self._finish_and_dismiss_time ):
                        
                        self.finish_and_dismiss()
                        
                    
                
            
        
    
    def add_url(self, url):
        
        with self._variable_lock:
            
            if url not in self._urls:
                
                self._urls.append( url )
                
            
        
    
    def cancel(self):
        
        self._cancelled = True
        
        self.finish()
        
    
    def delete_files(self):
        
        self.delete_variable('attached_files')
        
    
    def delete_gauge(self, level = 1):
        
        self.delete_variable(f'popup_gauge_{level}')
        
    
    def delete_network_job(self):
        
        self.delete_variable('network_job')
        
    
    def delete_status_text(self, level = 1):
        
        self.delete_variable(f'status_text_{level}')
        
    
    def delete_status_title(self):
        
        self.delete_variable('status_title')
        

    def delete_variable(self, name):
        
        with self._variable_lock:
            
            if name in self._variables:
                
                del self._variables[ name ]
                
            
        
        self._ui_update_pauser.pause()
        
    
    def finish(self):
        
        self._job_finish_time = HydrusTime.get_now_float()
        
        self._paused = False
        
        self._pausable = False
        self._cancellable = False
        
        self._done_event.set()
        
    
    def finish_and_dismiss(self, seconds = None):
        
        self.finish()
        
        if seconds is None:
            
            self._dismissed = True
            
        else:
            
            self._finish_and_dismiss_time = HydrusTime.get_now() + seconds
            
        
    
    def get_creation_time(self):
        
        return self._creation_time
        
    
    def get_done_event(self) -> threading.Event:
        
        return self._done_event
        
    
    def get_error_exception(self) -> Exception:
        
        if self._exception is None:
            
            raise Exception( 'No exception to return!' )
            
        else:
            
            return self._exception
            
        
    
    def get_files(self):
        
        return self.get_if_has_variable('attached_files')
        
    
    def get_if_has_variable(self, name):
        
        with self._variable_lock:
            
            if name in self._variables:
                
                return self._variables[ name ]
                
            else:
                
                return None
                
            
        
    
    def get_gauge(self, level = 1) -> tuple[int | None, int | None] | None:
        
        return self.get_if_has_variable(f'popup_gauge_{level}')
        
    
    def get_key(self):
        
        return self._key
        
    
    def get_network_job(self):
        
        return self.get_if_has_variable('network_job')
        
    
    def get_status_text(self, level = 1) -> str | None:
        
        return self.get_if_has_variable('status_text_{}'.format(level))
        
    
    def get_status_title(self) -> str | None:
        
        return self.get_if_has_variable('status_title')
        
    
    def get_traceback(self):
        
        return self.get_if_has_variable('traceback')
        
    
    def get_urls(self):
        
        with self._variable_lock:
            
            return list( self._urls )
            
        
    
    def get_user_callable(self) -> HydrusData.Call | None:
        
        return self.get_if_has_variable('user_callable')
        
    
    def had_error(self):
        
        return self._exception is not None
        
    
    def has_variable(self, name):
        
        with self._variable_lock: return name in self._variables
        
    
    def is_cancellable(self):
        
        return self._cancellable
        
    
    def is_cancelled(self):
        
        self._check_cancel_tests()
        
        return self._cancelled
        
    
    def is_dismissed(self):
        
        self._check_cancel_tests()
        
        return self._dismissed
        
    
    def is_done(self):
        
        self._check_cancel_tests()
        
        return self._done_event.is_set()
        
    
    def is_pausable(self):
        
        return self._pausable
        
    
    def is_paused(self):
        
        return self._paused
        
    
    def pause_play(self):
        
        self._paused = not self._paused
        
    
    def set_error_exception(self, e: Exception):
        
        self._exception = e
        
        self.cancel()
        
    
    def set_files(self, hashes: list[ bytes], label: str):
        
        if len( hashes ) == 0:
            
            self.delete_files()
            
        else:
            
            hashes = HydrusLists.dedupe_list( list( hashes ) )
            
            self.set_variable('attached_files', (hashes, label))
            
        
    
    def set_gauge(self, num_done: int | None, num_to_do: int | None, level = 1):
        
        self.set_variable(f'popup_gauge_{level}', (num_done, num_to_do))
        
    
    def set_network_job(self, network_job):
        
        self.set_variable('network_job', network_job)
        
    
    def set_status_text(self, text: str, level = 1):
        
        self.set_variable('status_text_{}'.format(level), text)
        
    
    def set_status_title(self, title: str):
        
        self.set_variable('status_title', title)
        
    
    def set_traceback(self, trace: str):
        
        self.set_variable('traceback', trace)
        
    
    def set_user_callable(self, call: HydrusData.Call):
        
        self.set_variable('user_callable', call)
        
    
    def set_variable(self, name, value):
        
        with self._variable_lock: self._variables[ name ] = value
        
        self._ui_update_pauser.pause()
        
    
    def time_running(self):
        
        if self._job_finish_time is None:
            
            return HydrusTime.get_now_float() - self._creation_time
            
        else:
            
            return self._job_finish_time - self._creation_time
            
        
    
    def to_string(self):
        
        stuff_to_print = []
        
        status_title = self.get_status_title()
        
        if status_title is not None:
            
            stuff_to_print.append( status_title )
            
        
        status_text_1 = self.get_status_text()
        
        if status_text_1 is not None:
            
            stuff_to_print.append( status_text_1 )
            
        
        status_text_2 = self.get_status_text(2)
        
        if status_text_2 is not None:
            
            stuff_to_print.append( status_text_2 )
            
        
        trace = self.get_traceback()
        
        if trace is not None:
            
            stuff_to_print.append( trace )
            
        
        stuff_to_print = [ str( s ) for s in stuff_to_print ]
        
        try:
            
            return '\n'.join( stuff_to_print )
            
        except Exception as e:
            
            return repr( stuff_to_print )
            
        
    
    def wait_if_needed(self):
        
        self._yield_pauser.pause()
        
        i_paused = False
        should_quit = False
        
        while self.is_paused():
            
            i_paused = True
            
            time.sleep( 0.1 )
            
            if self.is_done():
                
                break
                
            
        
        if self.is_cancelled():
            
            should_quit = True
            
        
        return ( i_paused, should_quit )
        
    

class FileRWLock( object ):
    
    class RLock( object ):
        
        def __init__( self, parent ):
            
            self.parent = parent
            
        
        def __enter__( self ):
            
            while not HydrusThreading.is_thread_shutting_down():
                
                with self.parent.lock:
                    
                    # if there are no writers, we can start reading
                    
                    if not self.parent.there_is_an_active_writer and self.parent.num_waiting_writers == 0:
                        
                        self.parent.num_readers += 1
                        
                        return
                        
                    
                
                # otherwise wait a bit
                
                self.parent.read_available_event.wait( 1 )
                
                self.parent.read_available_event.clear()
                
            
        
        def __exit__( self, exc_type, exc_val, exc_tb ):
            
            with self.parent.lock:
                
                self.parent.num_readers -= 1
                
                do_write_notify = self.parent.num_readers == 0 and self.parent.num_waiting_writers > 0
                
            
            if do_write_notify:
                
                self.parent.write_available_event.set()
                
            
        
    
    class WLock( object ):
        
        def __init__( self, parent ):
            
            self.parent = parent
            
        
        def __enter__( self ):
            
            # let all the readers know that we are bumping up to the front of the queue
            
            with self.parent.lock:
                
                self.parent.num_waiting_writers += 1
                
            
            while not HydrusThreading.is_thread_shutting_down():
                
                with self.parent.lock:
                    
                    # if nothing reading or writing atm, sieze the opportunity
                    
                    if not self.parent.there_is_an_active_writer and self.parent.num_readers == 0:
                        
                        self.parent.num_waiting_writers -= 1
                        
                        self.parent.there_is_an_active_writer = True
                        
                        return
                        
                    
                
                # otherwise wait a bit
                
                self.parent.write_available_event.wait( 1 )
                
                self.parent.write_available_event.clear()
                
            
        
        def __exit__( self, exc_type, exc_val, exc_tb ):
            
            with self.parent.lock:
                
                self.parent.there_is_an_active_writer = False
                
                do_read_notify = self.parent.num_waiting_writers == 0 # reading is now available
                do_write_notify = self.parent.num_waiting_writers > 0 # another writer is waiting
                
            
            if do_read_notify:
                
                self.parent.read_available_event.set()
                
            
            if do_write_notify:
                
                self.parent.write_available_event.set()
                
            
        
    
    def __init__( self ):
        
        self.read = self.RLock( self )
        self.write = self.WLock( self )
        
        self.lock = threading.Lock()
        
        self.read_available_event = threading.Event()
        self.write_available_event = threading.Event()
        
        self.num_readers = 0
        self.num_waiting_writers = 0
        self.there_is_an_active_writer = False
        
    
    def is_locked(self):
        
        with self.lock:
            
            return self.num_waiting_writers > 0 or self.there_is_an_active_writer or self.num_readers > 0
            
        
    
    def readers_are_working(self):
        
        with self.lock:
            
            return self.num_readers > 0
            
        
    
    def writers_are_waiting_or_working(self):
        
        with self.lock:
            
            return self.num_waiting_writers > 0 or self.there_is_an_active_writer
            
        
    

# TODO: a FileSystemRWLock, which will offer locks on a prefix basis. I had a think and some playing around, and I think best answer is to copy the FileRWLock here and just make it more complicated
# The RLock and WLock will be asked for either global or prefix based lock
# if grabbing global lock, work as normal
# if grabbing a prefix lock, they first get the global read lock, to block global writes
# only track the 'largest' (i.e. shortest) prefix we have in the file system. access to 'f333' will need '33' lock, if we are in transition and 'f33' (or, say, 'f27') still exists
# think and plan more, write some good unit tests, make sure we aren't deadlocking by being stupid somehow

class QtAwareJob( HydrusThreading.SingleJob ):
    
    PRETTY_CLASS_NAME = 'single UI job'
    
    def __init__( self, controller, scheduler, window, initial_delay, work_callable ):
        
        super().__init__( controller, scheduler, initial_delay, work_callable )
        
        self._window = window
        
    
    def _boot_worker( self ):
        
        def qt_code():
            
            if self._window is None or not QP.isValid( self._window ):
                
                return
                
            
            self.work()
            
        
        # yo if you change this, alter how profile_mode (ui) works
        CG.client_controller.call_after_qt_safe(self._window, qt_code)
        
    
    def _my_window_dead(self):
        
        return self._window is None or not QP.isValid( self._window )
        
    
    def is_cancelled( self ):
        
        my_window_dead = self._my_window_dead()
        
        if my_window_dead:
            
            self._is_cancelled.set()
            
        
        return HydrusThreading.SingleJob.is_cancelled( self )
        
    
    def is_dead( self ):
        
        return self._my_window_dead()
        
    

class QtAwareRepeatingJob( HydrusThreading.RepeatingJob ):
    
    PRETTY_CLASS_NAME = 'repeating UI job'
    
    def __init__( self, controller, scheduler, window, initial_delay, period, work_callable ):
        
        super().__init__( controller, scheduler, initial_delay, period, work_callable )
        
        self._window = window
        
    
    def _qt_work(self):
        
        if self._window is None or not QP.isValid( self._window ):
            
            self._window = None
            
            return
            
        
        self.work()
        
    
    def _boot_worker( self ):
        
        # yo if you change this, alter how profile_mode (ui) works
        CG.client_controller.call_after_qt_safe(self._window, self._qt_work)
        
    
    def _my_window_dead(self):
        
        return self._window is None or not QP.isValid( self._window )
        
    
    def is_cancelled( self ):
        
        my_window_dead = self._my_window_dead()
        
        if my_window_dead:
            
            self._is_cancelled.set()
            
        
        return HydrusThreading.SingleJob.is_cancelled( self )
        
    
    def is_dead( self ):
        
        return self._my_window_dead()
        
    
