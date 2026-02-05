import bisect
import queue
import random
import threading
import time
import traceback

from hydrus.core import HydrusData
from hydrus.core import HydrusExceptions
from hydrus.core import HydrusGlobals as HG
from hydrus.core import HydrusNumbers
from hydrus.core import HydrusProfiling
from hydrus.core import HydrusTime

NEXT_THREAD_CLEAROUT = 0

THREADS_TO_THREAD_INFO = {}
THREAD_INFO_LOCK = threading.Lock()

def check_if_thread_shutting_down()-> None:
    
    if is_thread_shutting_down():
        
        raise HydrusExceptions.ShutdownException( 'Thread is shutting down!' )
        
    

def clear_out_dead_threads() -> None:
    
    with THREAD_INFO_LOCK:
        
        all_threads = list( THREADS_TO_THREAD_INFO.keys() )
        
        for thread in all_threads:
            
            if not thread.is_alive():
                
                del THREADS_TO_THREAD_INFO[ thread ]
                
            
        
    
def get_thread_info( thread = None ):
    
    global NEXT_THREAD_CLEAROUT
    
    if HydrusTime.time_has_passed( NEXT_THREAD_CLEAROUT ):
        
        clear_out_dead_threads()
        
        NEXT_THREAD_CLEAROUT = HydrusTime.get_now() + 600
        
    
    if thread is None:
        
        thread = threading.current_thread()
        
    
    with THREAD_INFO_LOCK:
        
        if thread not in THREADS_TO_THREAD_INFO:
            
            thread_info = {}
            
            thread_info[ 'shutting_down' ] = False
            
            THREADS_TO_THREAD_INFO[ thread ] = thread_info
            
        
        return THREADS_TO_THREAD_INFO[ thread ]
        
    
def is_thread_shutting_down() -> bool:
    
    if HG.controller.doing_fast_exit():
        
        return True
        
    
    me = threading.current_thread()
    
    if isinstance( me, DAEMON ):
        
        if HG.started_shutdown:
            
            return True
            
        
    else:
        
        if HG.model_shutdown:
            
            return True
            
        
    
    thread_info = get_thread_info()
    
    return thread_info[ 'shutting_down' ]
    

def shutdown_thread( thread ) -> None:
    
    thread_info = get_thread_info( thread )
    
    thread_info[ 'shutting_down' ] = True
    

class RegularJobChecker( object ):
    
    def __init__( self, period = 10 ):
        
        self._period = period
        
        self._next_check = HydrusTime.get_now_float()
        
    
    def due( self ) -> bool:
        
        if HydrusTime.time_has_passed_float( self._next_check ):
            
            self._next_check = HydrusTime.get_now_float() + self._period
            
            return True
            
        else:
            
            return False
            
        
    

class BigJobPauser( object ):
    
    def __init__( self, period = 10, wait_time = 0.1 ):
        
        self._period = period
        self._wait_time = wait_time
        
        self._next_pause = HydrusTime.get_now_float() + self._period
        
    
    def pause( self ):
        
        if HydrusTime.time_has_passed_float( self._next_pause ):
            
            time.sleep( self._wait_time )
            
            self._next_pause = HydrusTime.get_now_float() + self._period
            
        
    

class DAEMON( threading.Thread ):
    
    def __init__( self, controller: "HG.HydrusController.HydrusController", name: str ):
        
        super().__init__( name = name )
        
        self._controller = controller
        self._name = name
        
        self._event = threading.Event()
        
        self._controller.sub( self, 'wake', 'wake_daemons' )
        self._controller.sub( self, 'shutdown', 'shutdown' )
        
    
    def _do_pre_call( self ):
        
        if HG.daemon_report_mode:
            
            HydrusData.show_text( self._name + ' doing a job.' )
            
        
    
    def get_current_job_summary( self ) -> str:
        
        return 'unknown job'
        
    
    def get_name( self ):
        
        return self._name
        
    
    def shutdown( self ):
        
        shutdown_thread( self )
        
        self.wake()
        
    
    def wake( self ) -> None:
        
        self._event.set()
        
    
class DAEMONWorker( DAEMON ):
    
    def __init__( self, controller, name, callable, topics = None, period = 3600, init_wait = 3, pre_call_wait = 0 ):
        
        if topics is None:
            
            topics = []
            
        
        super().__init__( controller, name )
        
        self._callable = callable
        self._topics = topics
        self._period = period
        self._init_wait = init_wait
        self._pre_call_wait = pre_call_wait
        
        for topic in topics:
            
            self._controller.sub( self, 'set', topic )
            
        
        self.start()
        
    
    def _can_start( self ) -> bool:
        
        return self._controller_is_ok_with_it()
        
    
    def _controller_is_ok_with_it( self ) -> bool:
        
        return True
        
    
    def _do_await( self, wait_time, event_can_wake = True )-> None:
        
        time_to_start = HydrusTime.get_now() + wait_time
        
        while not HydrusTime.time_has_passed( time_to_start ):
            
            if event_can_wake:
                
                event_was_set = self._event.wait( 1.0 )
                
                if event_was_set:
                    
                    self._event.clear()
                    
                    return
                    
                
            else:
                
                time.sleep( 1.0 )
                
            
            check_if_thread_shutting_down()
            
        
    
    def _wait_until_can_start( self ):
        
        while not self._can_start():
            
            time.sleep( 1.0 )
            
            check_if_thread_shutting_down()
            
        
    
    def get_current_job_summary( self ):
        
        return self._callable
        
    
    def run( self ) -> None:
        
        try:
            
            self._do_await( self._init_wait )
            
            while True:
                
                check_if_thread_shutting_down()
                
                self._do_await( self._pre_call_wait, event_can_wake = False )
                
                check_if_thread_shutting_down()
                
                self._wait_until_can_start()
                
                check_if_thread_shutting_down()
                
                self._do_pre_call()
                
                try:
                    
                    self._callable( self._controller )
                    
                except HydrusExceptions.ShutdownException:
                    
                    return
                    
                except Exception as e:
                    
                    HydrusData.show_text( 'Daemon ' + self._name + ' encountered an exception:' )
                    
                    HydrusData.ShowException( e )
                    
                
                self._do_await( self._period )
                
            
        except HydrusExceptions.ShutdownException:
            
            return
            
        
    
    def set( self, *args, **kwargs ):
        
        self._event.set()
        
    
# Big stuff like DB maintenance that we don't want to run while other important stuff is going on, like user interaction or vidya on another process
class DAEMONBackgroundWorker( DAEMONWorker ):
    
    def _controller_is_ok_with_it( self ) -> bool:
        
        return self._controller.good_time_to_start_background_work()
        
    
# Big stuff that we want to run when the user sees, but not at the expense of something else, like laggy session load
class DAEMONForegroundWorker( DAEMONWorker ):
    
    def _controller_is_ok_with_it( self ) -> bool:
        
        return self._controller.good_time_to_start_foreground_work()
        
    
class THREADCallToThread( DAEMON ):
    
    def __init__( self, controller, name ):
        
        super().__init__( controller, name )
        
        self._callable = None
        
        self._queue = queue.Queue()
        
        self._currently_working = True # start off true so new threads aren't used twice by two quick successive calls
        
    
    def currently_working( self ) -> bool:
        
        return self._currently_working
        
    
    def get_current_job_summary( self ):
        
        return self._callable
        
    
    def put( self, callable, *args, **kwargs ) -> None:
        
        self._currently_working = True
        
        self._queue.put( ( callable, args, kwargs ) )
        
        self._event.set()
        
    
    def run( self ) -> None:
        
        try:
            
            while True:
                
                while self._queue.empty():
                    
                    check_if_thread_shutting_down()
                    
                    self._event.wait( 10.0 )
                    
                    self._event.clear()
                    
                
                check_if_thread_shutting_down()
                
                try:
                    
                    try:
                        
                        ( callable, args, kwargs ) = self._queue.get( timeout = 1.0 )
                        
                    except queue.Empty:
                        
                        # https://github.com/hydrusnetwork/hydrus/issues/750
                        # this shouldn't happen, but...
                        # even if we assume we'll never get this, we don't want to make a business of hanging forever on things
                        
                        continue
                        
                    
                    self._do_pre_call()
                    
                    self._callable = ( callable, args, kwargs )
                    
                    if HydrusProfiling.is_profile_mode( 'threads' ):
                        
                        summary = 'Profiling CallTo Job: {}'.format( callable )
                        
                        HydrusProfiling.profile( summary, HydrusData.Call( callable, *args, **kwargs ), min_duration_ms = HG.callto_profile_min_job_time_ms )
                        
                    else:
                        
                        callable( *args, **kwargs )
                        
                    
                    self._callable = None
                    
                    del callable
                    
                except HydrusExceptions.ShutdownException:
                    
                    return
                    
                except Exception as e:
                    
                    HydrusData.print_text( traceback.format_exc() )
                    
                    HydrusData.ShowException( e )
                    
                finally:
                    
                    self._currently_working = False
                    
                
                time.sleep( 0.00001 )
                
            
        except HydrusExceptions.ShutdownException:
            
            return
            
        
    

class JobScheduler( threading.Thread ):
    
    def __init__( self, controller: "HG.HydrusController.HydrusController" ):
        
        super().__init__( name = 'Job Scheduler' )
        
        self._controller = controller
        
        self._waiting = []
        
        self._waiting_lock = threading.Lock()
        
        self._new_job_arrived = threading.Event()
        
        self._current_job = None
        
        self._cancel_filter_needed = threading.Event()
        self._sort_needed = threading.Event()
        
        self._controller.sub( self, 'shutdown', 'shutdown' )
        
    
    def _filter_cancelled( self ):
        
        with self._waiting_lock:
            
            self._waiting = [job for job in self._waiting if not job.is_cancelled()]
            
        
    
    def _get_loop_wait_time( self ):
        
        with self._waiting_lock:
            
            if len( self._waiting ) == 0:
                
                return 0.2
                
            
            next_job = self._waiting[0]
            
        
        time_delta_until_due = next_job.GetTimeDeltaUntilDue()
        
        return min( 1.0, time_delta_until_due )
        
    
    def _no_work_to_start( self ) -> bool:
        
        with self._waiting_lock:
            
            if len( self._waiting ) == 0:
                
                return True
                
            
            next_job = self._waiting[0]
            
        
        if next_job.IsDue():
            
            return False
            
        else:
            
            return True
            
        
    
    def _sort_waiting( self ):
        
        # sort the waiting jobs in ascending order of expected work time
        
        with self._waiting_lock: # this uses __lt__ to sort
            
            self._waiting.sort()
            
        
    
    def _start_work( self ) -> None:
        
        jobs_started = 0
        
        while True:
            
            with self._waiting_lock:
                
                if len( self._waiting ) == 0:
                    
                    break
                    
                
                if jobs_started >= 10: # try to avoid spikes
                    
                    break
                    
                
                next_job = self._waiting[0]
                
                if not next_job.IsDue():
                    
                    # front is not due, so nor is the rest of the list
                    break
                    
                
                next_job = self._waiting.pop( 0 )
                
            
            if next_job.is_cancelled():
                
                continue
                
            
            if next_job.SlotOK():
                
                # important this happens outside of the waiting lock lmao!
                next_job.StartWork()
                
                jobs_started += 1
                
            else:
                
                # delay is automatically set by SlotOK
                
                with self._waiting_lock:
                    
                    bisect.insort( self._waiting, next_job )
                    
                
            
        
    
    def add_job( self, job ) -> None:
        
        with self._waiting_lock:
            
            bisect.insort( self._waiting, job )
            
        
        self._new_job_arrived.set()
        
    
    def clear_out_dead( self ) -> None:
        
        with self._waiting_lock:
            
            self._waiting = [ job for job in self._waiting if not job.IsDead() ]
            
        
    
    def get_name( self ) -> str:
        
        return 'Job Scheduler'
        
    
    def get_current_job_summary( self ) -> str:
        
        with self._waiting_lock:
            
            return HydrusNumbers.to_human_int( len( self._waiting ) ) + ' jobs'
            
        
    
    def get_jobs( self ):
        
        with self._waiting_lock:
            
            return list( self._waiting )
            
        
    
    def get_pretty_job_summary( self ) -> str:
        
        with self._waiting_lock:
            
            num_jobs = len( self._waiting )
            
            job_lines = [ repr( job ) for job in self._waiting ]
            
            lines = [ HydrusNumbers.to_human_int( num_jobs ) + ' jobs:' ] + job_lines
            
            text = '\n'.join( lines )
            
            return text
            
        
    
    def job_cancelled( self ) -> None:
        
        self._cancel_filter_needed.set()
        
    
    def shutdown( self ) -> None:
        
        shutdown_thread( self )
        
        self._new_job_arrived.set()
        
    
    def work_times_have_changed( self ) -> None:
        
        self._sort_needed.set()
        
    
    def run( self ) -> None:
        
        while True:
            
            try:
                
                while self._no_work_to_start():
                    
                    if is_thread_shutting_down():
                        
                        return
                        
                    
                    #
                    
                    if self._cancel_filter_needed.is_set():
                        
                        self._filter_cancelled()
                        
                        self._cancel_filter_needed.clear()
                        
                    
                    if self._sort_needed.is_set():
                        
                        self._sort_waiting()
                        
                        self._sort_needed.clear()
                        
                        continue # if some work is now due, let's do it!
                        
                    
                    #
                    
                    wait_time = self._get_loop_wait_time()
                    
                    self._new_job_arrived.wait( wait_time )
                    
                    self._new_job_arrived.clear()
                    
                
                self._start_work()
                
            except HydrusExceptions.ShutdownException:
                
                return
                
            except Exception as e:
                
                HydrusData.print_text( traceback.format_exc() )
                
                HydrusData.ShowException( e )
                
            
            time.sleep( 0.00001 )
            
        
    

class SchedulableJob( object ):
    
    PRETTY_CLASS_NAME = 'job base'
    
    def __init__( self, controller: "HG.HydrusController.HydrusController", scheduler: JobScheduler, initial_delay, work_callable ):
        
        super().__init__()
        
        self._controller = controller
        self._scheduler = scheduler
        self._work_callable = work_callable
        
        self._should_delay_on_wakeup = False
        
        self._next_work_time = HydrusTime.get_now_float() + initial_delay
        
        self._thread_slot_type = None
        
        self._work_lock = threading.Lock()
        
        self._currently_working = threading.Event()
        self._actual_work_started = threading.Event()
        self._is_cancelled = threading.Event()
        
    
    def __lt__( self, other ): # for the scheduler to do bisect.insort noice
        
        return self._next_work_time < other._next_work_time
        
    
    def __repr__( self ):
        
        return '{}: {} {}'.format( self.PRETTY_CLASS_NAME, self.get_pretty_job(), self.get_due_string() )
        
    
    def _boot_worker( self ):
        
        self._controller.call_to_thread( self.work )
        
    
    def cancel( self ) -> None:
        
        self._is_cancelled.set()
        
        self._scheduler.job_cancelled()
        
    
    def currently_working( self ) -> bool:
        
        if self._is_cancelled.is_set() and not self._actual_work_started.is_set():
            
            return False
            
        
        return self._currently_working.is_set()
        
    
    def delay( self, delay ) -> None:
        
        self._next_work_time = HydrusTime.get_now_float() + delay
        
        self._scheduler.work_times_have_changed()
        
    
    def get_due_string( self ) -> str:
        
        due_delta = self._next_work_time - HydrusTime.get_now_float()
        
        due_string = HydrusTime.timedelta_to_pretty_timedelta( due_delta )
        
        if due_delta < 0:
            
            due_string = 'was due {} ago'.format( due_string )
            
        else:
            
            due_string = 'due in {}'.format( due_string )
            
        
        return due_string
        
    
    def get_next_work_time( self ):
        
        return self._next_work_time
        
    
    def get_pretty_job( self ):
        
        return repr( self._work_callable )
        
    
    def get_time_delta_until_due( self ):
        
        return HydrusTime.get_time_delta_until_time_float( self._next_work_time )
        
    
    def is_cancelled( self ) -> bool:
        
        return self._is_cancelled.is_set()
        
    
    def is_dead( self ) -> bool:
        
        return False
        
    
    def is_due( self ) -> bool:
        
        return HydrusTime.time_has_passed_float( self._next_work_time )
        
    
    def pub_sub_wake( self, *args, **kwargs ) -> None:
        
        self.wake()
        
    
    def set_thread_slot_type( self, thread_type ) -> None:
        
        self._thread_slot_type = thread_type
        
    
    def should_delay_on_wakeup( self, value ) -> None:
        
        self._should_delay_on_wakeup = value
        
    
    def slot_ok( self ) -> bool:
        
        if self._thread_slot_type is not None:
            
            if HG.controller.acquire_thread_slot( self._thread_slot_type ):
                
                return True
                
            else:
                
                self._next_work_time = HydrusTime.get_now_float() + 10 + random.random()
                
                return False
                
            
        
        return True
        
    
    def start_work( self ) -> None:
        
        if self._is_cancelled.is_set():
            
            return
            
        
        self._currently_working.set()
        
        self._boot_worker()
        
    
    def wake( self, next_work_time = None ) -> None:
        
        if next_work_time is None:
            
            next_work_time = HydrusTime.get_now_float()
            
        
        self._next_work_time = next_work_time
        
        self._scheduler.work_times_have_changed()
        
    
    def wake_on_pub_sub( self, topic ) -> None:
        
        HG.controller.sub( self, 'PubSubWake', topic )
        
    
    def waiting_on_work_slot( self ):
        
        if self._thread_slot_type is not None:
            
            if not self._currently_working.set() and self.is_due() and not HG.controller.thread_slots_are_available( self._thread_slot_type ):
                
                return True
                
            
        
        return False
        
    
    def work( self ) -> None:
        
        try:
            
            if self._should_delay_on_wakeup:
                
                while HG.controller.just_woke_from_sleep():
                    
                    if is_thread_shutting_down():
                        
                        return
                        
                    
                    time.sleep( 1 )
                    
                
            
            with self._work_lock:
                
                self._actual_work_started.set()
                
                self._work_callable()
                
            
        finally:
            
            if self._thread_slot_type is not None:
                
                HG.controller.release_thread_slot( self._thread_slot_type )
                
            
            self._actual_work_started.clear()
            self._currently_working.clear()
            
        
    

class SingleJob( SchedulableJob ):
    
    PRETTY_CLASS_NAME = 'single job'
    
    def __init__( self, controller, scheduler: JobScheduler, initial_delay, work_callable ):
        
        super().__init__( controller, scheduler, initial_delay, work_callable )
        
        self._work_complete = threading.Event()
        
    
    def is_work_complete( self ) -> bool:
        
        return self._work_complete.is_set()
        
    
    def work( self ) -> None:
        
        SchedulableJob.work( self )
        
        self._work_complete.set()
        
    

class RepeatingJob( SchedulableJob ):
    
    PRETTY_CLASS_NAME = 'repeating job'
    
    def __init__( self, controller, scheduler: JobScheduler, initial_delay, period, work_callable ):
        
        super().__init__( controller, scheduler, initial_delay, work_callable )
        
        self._period = period
        
        self._stop_repeating = threading.Event()
        
    
    def cancel( self ) -> None:
        
        SchedulableJob.cancel( self )
        
        self._stop_repeating.set()
        
    
    def start_work( self ) -> None:
        
        if self._stop_repeating.is_set():
            
            return
            
        
        SchedulableJob.start_work( self )
        
    
    def work( self ) -> None:
        
        SchedulableJob.work( self )
        
        if not self._stop_repeating.is_set():
            
            self._next_work_time = HydrusTime.get_now_float() + self._period
            
            self._scheduler.add_job( self )
            
        
    
