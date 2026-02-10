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

def CheckIfThreadShuttingDown()-> None:
    
    """Executes `CheckIfThreadShuttingDown`."""
    if IsThreadShuttingDown():
        
        raise HydrusExceptions.ShutdownException( 'Thread is shutting down!' )
        
    

def ClearOutDeadThreads() -> None:
    
    """Executes `ClearOutDeadThreads`."""
    with THREAD_INFO_LOCK:
        
        all_threads = list( THREADS_TO_THREAD_INFO.keys() )
        
        for thread in all_threads:
            
            if not thread.is_alive():
                
                del THREADS_TO_THREAD_INFO[ thread ]
                
            
        
    
def GetThreadInfo( thread = None ):
    
    """Executes `GetThreadInfo`."""
    global NEXT_THREAD_CLEAROUT
    
    if HydrusTime.TimeHasPassed( NEXT_THREAD_CLEAROUT ):
        
        ClearOutDeadThreads()
        
        NEXT_THREAD_CLEAROUT = HydrusTime.GetNow() + 600
        
    
    if thread is None:
        
        thread = threading.current_thread()
        
    
    with THREAD_INFO_LOCK:
        
        if thread not in THREADS_TO_THREAD_INFO:
            
            thread_info = {}
            
            thread_info[ 'shutting_down' ] = False
            
            THREADS_TO_THREAD_INFO[ thread ] = thread_info
            
        
        return THREADS_TO_THREAD_INFO[ thread ]
        
    
def IsThreadShuttingDown() -> bool:
    
    """Executes `IsThreadShuttingDown`."""
    if HG.controller.DoingFastExit():
        
        return True
        
    
    me = threading.current_thread()
    
    if isinstance( me, DAEMON ):
        
        if HG.started_shutdown:
            
            return True
            
        
    else:
        
        if HG.model_shutdown:
            
            return True
            
        
    
    thread_info = GetThreadInfo()
    
    return thread_info[ 'shutting_down' ]
    

def ShutdownThread( thread ) -> None:
    
    """Executes `ShutdownThread`."""
    thread_info = GetThreadInfo( thread )
    
    thread_info[ 'shutting_down' ] = True
    

class RegularJobChecker( object ):
    
    def __init__( self, period = 10 ):
        
        """Initializes the instance."""
        self._period = period
        
        self._next_check = HydrusTime.GetNowFloat()
        
    
    def Due( self ) -> bool:
        
        """Executes `Due`."""
        if HydrusTime.TimeHasPassedFloat( self._next_check ):
            
            self._next_check = HydrusTime.GetNowFloat() + self._period
            
            return True
            
        else:
            
            return False
            
        
    

class BigJobPauser( object ):
    
    def __init__( self, period = 10, wait_time = 0.1 ):
        
        """Initializes the instance."""
        self._period = period
        self._wait_time = wait_time
        
        self._next_pause = HydrusTime.GetNowFloat() + self._period
        
    
    def Pause( self ):
        
        """Executes `Pause`."""
        if HydrusTime.TimeHasPassedFloat( self._next_pause ):
            
            time.sleep( self._wait_time )
            
            self._next_pause = HydrusTime.GetNowFloat() + self._period
            
        
    

class DAEMON( threading.Thread ):
    
    def __init__( self, controller: "HG.HydrusController.HydrusController", name: str ):
        
        """Initializes the instance."""
        super().__init__( name = name )
        
        self._controller = controller
        self._name = name
        
        self._event = threading.Event()
        
        self._controller.sub( self, 'wake', 'wake_daemons' )
        self._controller.sub( self, 'shutdown', 'shutdown' )
        
    
    def _DoPreCall( self ):
        
        """Executes `_DoPreCall`."""
        if HG.daemon_report_mode:
            
            HydrusData.ShowText( self._name + ' doing a job.' )
            
        
    
    def GetCurrentJobSummary( self ) -> str:
        
        """Executes `GetCurrentJobSummary`."""
        return 'unknown job'
        
    
    def GetName( self ):
        
        """Executes `GetName`."""
        return self._name
        
    
    def shutdown( self ):
        
        """Executes `shutdown`."""
        ShutdownThread( self )
        
        self.wake()
        
    
    def wake( self ) -> None:
        
        """Executes `wake`."""
        self._event.set()
        
    
class DAEMONWorker( DAEMON ):
    
    def __init__( self, controller, name, callable, topics = None, period = 3600, init_wait = 3, pre_call_wait = 0 ):
        
        """Initializes the instance."""
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
        
    
    def _CanStart( self ) -> bool:
        
        """Executes `_CanStart`."""
        return self._ControllerIsOKWithIt()
        
    
    def _ControllerIsOKWithIt( self ) -> bool:
        
        """Executes `_ControllerIsOKWithIt`."""
        return True
        
    
    def _DoAWait( self, wait_time, event_can_wake = True )-> None:
        
        """Executes `_DoAWait`."""
        time_to_start = HydrusTime.GetNow() + wait_time
        
        while not HydrusTime.TimeHasPassed( time_to_start ):
            
            if event_can_wake:
                
                event_was_set = self._event.wait( 1.0 )
                
                if event_was_set:
                    
                    self._event.clear()
                    
                    return
                    
                
            else:
                
                time.sleep( 1.0 )
                
            
            CheckIfThreadShuttingDown()
            
        
    
    def _WaitUntilCanStart( self ):
        
        """Executes `_WaitUntilCanStart`."""
        while not self._CanStart():
            
            time.sleep( 1.0 )
            
            CheckIfThreadShuttingDown()
            
        
    
    def GetCurrentJobSummary( self ):
        
        """Executes `GetCurrentJobSummary`."""
        return self._callable
        
    
    def run( self ) -> None:
        
        """Executes `run`."""
        try:
            
            self._DoAWait( self._init_wait )
            
            while True:
                
                CheckIfThreadShuttingDown()
                
                self._DoAWait( self._pre_call_wait, event_can_wake = False )
                
                CheckIfThreadShuttingDown()
                
                self._WaitUntilCanStart()
                
                CheckIfThreadShuttingDown()
                
                self._DoPreCall()
                
                try:
                    
                    self._callable( self._controller )
                    
                except HydrusExceptions.ShutdownException:
                    
                    return
                    
                except Exception as e:
                    
                    HydrusData.ShowText( 'Daemon ' + self._name + ' encountered an exception:' )
                    
                    HydrusData.ShowException( e )
                    
                
                self._DoAWait( self._period )
                
            
        except HydrusExceptions.ShutdownException:
            
            return
            
        
    
    def set( self, *args, **kwargs ):
        
        """Executes `set`."""
        self._event.set()
        
    
# Big stuff like DB maintenance that we don't want to run while other important stuff is going on, like user interaction or vidya on another process
class DAEMONBackgroundWorker( DAEMONWorker ):
    
    def _ControllerIsOKWithIt( self ) -> bool:
        
        """Executes `_ControllerIsOKWithIt`."""
        return self._controller.GoodTimeToStartBackgroundWork()
        
    
# Big stuff that we want to run when the user sees, but not at the expense of something else, like laggy session load
class DAEMONForegroundWorker( DAEMONWorker ):
    
    def _ControllerIsOKWithIt( self ) -> bool:
        
        """Executes `_ControllerIsOKWithIt`."""
        return self._controller.GoodTimeToStartForegroundWork()
        
    
class THREADCallToThread( DAEMON ):
    
    def __init__( self, controller, name ):
        
        """Initializes the instance."""
        super().__init__( controller, name )
        
        self._callable = None
        
        self._queue = queue.Queue()
        
        self._currently_working = True # start off true so new threads aren't used twice by two quick successive calls
        
    
    def CurrentlyWorking( self ) -> bool:
        
        """Executes `CurrentlyWorking`."""
        return self._currently_working
        
    
    def GetCurrentJobSummary( self ):
        
        """Executes `GetCurrentJobSummary`."""
        return self._callable
        
    
    def put( self, callable, *args, **kwargs ) -> None:
        
        """Executes `put`."""
        self._currently_working = True
        
        self._queue.put( ( callable, args, kwargs ) )
        
        self._event.set()
        
    
    def run( self ) -> None:
        
        """Executes `run`."""
        try:
            
            while True:
                
                while self._queue.empty():
                    
                    CheckIfThreadShuttingDown()
                    
                    self._event.wait( 10.0 )
                    
                    self._event.clear()
                    
                
                CheckIfThreadShuttingDown()
                
                try:
                    
                    try:
                        
                        ( callable, args, kwargs ) = self._queue.get( timeout = 1.0 )
                        
                    except queue.Empty:
                        
                        # https://github.com/hydrusnetwork/hydrus/issues/750
                        # this shouldn't happen, but...
                        # even if we assume we'll never get this, we don't want to make a business of hanging forever on things
                        
                        continue
                        
                    
                    self._DoPreCall()
                    
                    self._callable = ( callable, args, kwargs )
                    
                    if HydrusProfiling.IsProfileMode( 'threads' ):
                        
                        summary = 'Profiling CallTo Job: {}'.format( callable )
                        
                        HydrusProfiling.Profile( summary, HydrusData.Call( callable, *args, **kwargs ), min_duration_ms = HG.callto_profile_min_job_time_ms )
                        
                    else:
                        
                        callable( *args, **kwargs )
                        
                    
                    self._callable = None
                    
                    del callable
                    
                except HydrusExceptions.ShutdownException:
                    
                    return
                    
                except Exception as e:
                    
                    HydrusData.Print( traceback.format_exc() )
                    
                    HydrusData.ShowException( e )
                    
                finally:
                    
                    self._currently_working = False
                    
                
                time.sleep( 0.00001 )
                
            
        except HydrusExceptions.ShutdownException:
            
            return
            
        
    

class JobScheduler( threading.Thread ):
    
    def __init__( self, controller: "HG.HydrusController.HydrusController" ):
        
        """Initializes the instance."""
        super().__init__( name = 'Job Scheduler' )
        
        self._controller = controller
        
        self._waiting = []
        
        self._waiting_lock = threading.Lock()
        
        self._new_job_arrived = threading.Event()
        
        self._current_job = None
        
        self._cancel_filter_needed = threading.Event()
        self._sort_needed = threading.Event()
        
        self._controller.sub( self, 'shutdown', 'shutdown' )
        
    
    def _FilterCancelled( self ):
        
        """Executes `_FilterCancelled`."""
        with self._waiting_lock:
            
            self._waiting = [ job for job in self._waiting if not job.IsCancelled() ]
            
        
    
    def _GetLoopWaitTime( self ):
        
        """Executes `_GetLoopWaitTime`."""
        with self._waiting_lock:
            
            if len( self._waiting ) == 0:
                
                return 0.2
                
            
            next_job = self._waiting[0]
            
        
        time_delta_until_due = next_job.GetTimeDeltaUntilDue()
        
        return min( 1.0, time_delta_until_due )
        
    
    def _NoWorkToStart( self ) -> bool:
        
        """Executes `_NoWorkToStart`."""
        with self._waiting_lock:
            
            if len( self._waiting ) == 0:
                
                return True
                
            
            next_job = self._waiting[0]
            
        
        if next_job.IsDue():
            
            return False
            
        else:
            
            return True
            
        
    
    def _SortWaiting( self ):
        
        # sort the waiting jobs in ascending order of expected work time
        
        """Executes `_SortWaiting`."""
        with self._waiting_lock: # this uses __lt__ to sort
            
            self._waiting.sort()
            
        
    
    def _StartWork( self ) -> None:
        
        """Executes `_StartWork`."""
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
                
            
            if next_job.IsCancelled():
                
                continue
                
            
            if next_job.SlotOK():
                
                # important this happens outside of the waiting lock lmao!
                next_job.StartWork()
                
                jobs_started += 1
                
            else:
                
                # delay is automatically set by SlotOK
                
                with self._waiting_lock:
                    
                    bisect.insort( self._waiting, next_job )
                    
                
            
        
    
    def AddJob( self, job ) -> None:
        
        """Executes `AddJob`."""
        with self._waiting_lock:
            
            bisect.insort( self._waiting, job )
            
        
        self._new_job_arrived.set()
        
    
    def ClearOutDead( self ) -> None:
        
        """Executes `ClearOutDead`."""
        with self._waiting_lock:
            
            self._waiting = [ job for job in self._waiting if not job.IsDead() ]
            
        
    
    def GetName( self ) -> str:
        
        """Executes `GetName`."""
        return 'Job Scheduler'
        
    
    def GetCurrentJobSummary( self ) -> str:
        
        """Executes `GetCurrentJobSummary`."""
        with self._waiting_lock:
            
            return HydrusNumbers.ToHumanInt( len( self._waiting ) ) + ' jobs'
            
        
    
    def GetJobs( self ):
        
        """Executes `GetJobs`."""
        with self._waiting_lock:
            
            return list( self._waiting )
            
        
    
    def GetPrettyJobSummary( self ) -> str:
        
        """Executes `GetPrettyJobSummary`."""
        with self._waiting_lock:
            
            num_jobs = len( self._waiting )
            
            job_lines = [ repr( job ) for job in self._waiting ]
            
            lines = [ HydrusNumbers.ToHumanInt( num_jobs ) + ' jobs:' ] + job_lines
            
            text = '\n'.join( lines )
            
            return text
            
        
    
    def JobCancelled( self ) -> None:
        
        """Executes `JobCancelled`."""
        self._cancel_filter_needed.set()
        
    
    def shutdown( self ) -> None:
        
        """Executes `shutdown`."""
        ShutdownThread( self )
        
        self._new_job_arrived.set()
        
    
    def WorkTimesHaveChanged( self ) -> None:
        
        """Executes `WorkTimesHaveChanged`."""
        self._sort_needed.set()
        
    
    def run( self ) -> None:
        
        """Executes `run`."""
        while True:
            
            try:
                
                while self._NoWorkToStart():
                    
                    if IsThreadShuttingDown():
                        
                        return
                        
                    
                    #
                    
                    if self._cancel_filter_needed.is_set():
                        
                        self._FilterCancelled()
                        
                        self._cancel_filter_needed.clear()
                        
                    
                    if self._sort_needed.is_set():
                        
                        self._SortWaiting()
                        
                        self._sort_needed.clear()
                        
                        continue # if some work is now due, let's do it!
                        
                    
                    #
                    
                    wait_time = self._GetLoopWaitTime()
                    
                    self._new_job_arrived.wait( wait_time )
                    
                    self._new_job_arrived.clear()
                    
                
                self._StartWork()
                
            except HydrusExceptions.ShutdownException:
                
                return
                
            except Exception as e:
                
                HydrusData.Print( traceback.format_exc() )
                
                HydrusData.ShowException( e )
                
            
            time.sleep( 0.00001 )
            
        
    

class SchedulableJob( object ):
    
    PRETTY_CLASS_NAME = 'job base'
    
    def __init__( self, controller: "HG.HydrusController.HydrusController", scheduler: JobScheduler, initial_delay, work_callable ):
        
        """Initializes the instance."""
        super().__init__()
        
        self._controller = controller
        self._scheduler = scheduler
        self._work_callable = work_callable
        
        self._should_delay_on_wakeup = False
        
        self._next_work_time = HydrusTime.GetNowFloat() + initial_delay
        
        self._thread_slot_type = None
        
        self._work_lock = threading.Lock()
        
        self._currently_working = threading.Event()
        self._actual_work_started = threading.Event()
        self._is_cancelled = threading.Event()
        
    
    def __lt__( self, other ): # for the scheduler to do bisect.insort noice
        
        """Executes `__lt__`."""
        return self._next_work_time < other._next_work_time
        
    
    def __repr__( self ):
        
        """Returns the developer-oriented string representation of the instance."""
        return '{}: {} {}'.format( self.PRETTY_CLASS_NAME, self.GetPrettyJob(), self.GetDueString() )
        
    
    def _BootWorker( self ):
        
        """Executes `_BootWorker`."""
        self._controller.CallToThread( self.Work )
        
    
    def Cancel( self ) -> None:
        
        """Executes `Cancel`."""
        self._is_cancelled.set()
        
        self._scheduler.JobCancelled()
        
    
    def CurrentlyWorking( self ) -> bool:
        
        """Executes `CurrentlyWorking`."""
        if self._is_cancelled.is_set() and not self._actual_work_started.is_set():
            
            return False
            
        
        return self._currently_working.is_set()
        
    
    def Delay( self, delay ) -> None:
        
        """Executes `Delay`."""
        self._next_work_time = HydrusTime.GetNowFloat() + delay
        
        self._scheduler.WorkTimesHaveChanged()
        
    
    def GetDueString( self ) -> str:
        
        """Executes `GetDueString`."""
        due_delta = self._next_work_time - HydrusTime.GetNowFloat()
        
        due_string = HydrusTime.TimeDeltaToPrettyTimeDelta( due_delta )
        
        if due_delta < 0:
            
            due_string = 'was due {} ago'.format( due_string )
            
        else:
            
            due_string = 'due in {}'.format( due_string )
            
        
        return due_string
        
    
    def GetNextWorkTime( self ):
        
        """Executes `GetNextWorkTime`."""
        return self._next_work_time
        
    
    def GetPrettyJob( self ):
        
        """Executes `GetPrettyJob`."""
        return repr( self._work_callable )
        
    
    def GetTimeDeltaUntilDue( self ):
        
        """Executes `GetTimeDeltaUntilDue`."""
        return HydrusTime.GetTimeDeltaUntilTimeFloat( self._next_work_time )
        
    
    def IsCancelled( self ) -> bool:
        
        """Executes `IsCancelled`."""
        return self._is_cancelled.is_set()
        
    
    def IsDead( self ) -> bool:
        
        """Executes `IsDead`."""
        return False
        
    
    def IsDue( self ) -> bool:
        
        """Executes `IsDue`."""
        return HydrusTime.TimeHasPassedFloat( self._next_work_time )
        
    
    def PubSubWake( self, *args, **kwargs ) -> None:
        
        """Executes `PubSubWake`."""
        self.Wake()
        
    
    def SetThreadSlotType( self, thread_type ) -> None:
        
        """Executes `SetThreadSlotType`."""
        self._thread_slot_type = thread_type
        
    
    def ShouldDelayOnWakeup( self, value ) -> None:
        
        """Executes `ShouldDelayOnWakeup`."""
        self._should_delay_on_wakeup = value
        
    
    def SlotOK( self ) -> bool:
        
        """Executes `SlotOK`."""
        if self._thread_slot_type is not None:
            
            if HG.controller.AcquireThreadSlot( self._thread_slot_type ):
                
                return True
                
            else:
                
                self._next_work_time = HydrusTime.GetNowFloat() + 10 + random.random()
                
                return False
                
            
        
        return True
        
    
    def StartWork( self ) -> None:
        
        """Executes `StartWork`."""
        if self._is_cancelled.is_set():
            
            return
            
        
        self._currently_working.set()
        
        self._BootWorker()
        
    
    def Wake( self, next_work_time = None ) -> None:
        
        """Executes `Wake`."""
        if next_work_time is None:
            
            next_work_time = HydrusTime.GetNowFloat()
            
        
        self._next_work_time = next_work_time
        
        self._scheduler.WorkTimesHaveChanged()
        
    
    def WakeOnPubSub( self, topic ) -> None:
        
        """Executes `WakeOnPubSub`."""
        HG.controller.sub( self, 'PubSubWake', topic )
        
    
    def WaitingOnWorkSlot( self ):
        
        """Executes `WaitingOnWorkSlot`."""
        if self._thread_slot_type is not None:
            
            if not self._currently_working.set() and self.IsDue() and not HG.controller.ThreadSlotsAreAvailable( self._thread_slot_type ):
                
                return True
                
            
        
        return False
        
    
    def Work( self ) -> None:
        
        """Executes `Work`."""
        try:
            
            if self._should_delay_on_wakeup:
                
                while HG.controller.JustWokeFromSleep():
                    
                    if IsThreadShuttingDown():
                        
                        return
                        
                    
                    time.sleep( 1 )
                    
                
            
            with self._work_lock:
                
                self._actual_work_started.set()
                
                self._work_callable()
                
            
        finally:
            
            if self._thread_slot_type is not None:
                
                HG.controller.ReleaseThreadSlot( self._thread_slot_type )
                
            
            self._actual_work_started.clear()
            self._currently_working.clear()
            
        
    

class SingleJob( SchedulableJob ):
    
    PRETTY_CLASS_NAME = 'single job'
    
    def __init__( self, controller, scheduler: JobScheduler, initial_delay, work_callable ):
        
        """Initializes the instance."""
        super().__init__( controller, scheduler, initial_delay, work_callable )
        
        self._work_complete = threading.Event()
        
    
    def IsWorkComplete( self ) -> bool:
        
        """Executes `IsWorkComplete`."""
        return self._work_complete.is_set()
        
    
    def Work( self ) -> None:
        
        """Executes `Work`."""
        SchedulableJob.Work( self )
        
        self._work_complete.set()
        
    

class RepeatingJob( SchedulableJob ):
    
    PRETTY_CLASS_NAME = 'repeating job'
    
    def __init__( self, controller, scheduler: JobScheduler, initial_delay, period, work_callable ):
        
        """Initializes the instance."""
        super().__init__( controller, scheduler, initial_delay, work_callable )
        
        self._period = period
        
        self._stop_repeating = threading.Event()
        
    
    def Cancel( self ) -> None:
        
        """Executes `Cancel`."""
        SchedulableJob.Cancel( self )
        
        self._stop_repeating.set()
        
    
    def StartWork( self ) -> None:
        
        """Executes `StartWork`."""
        if self._stop_repeating.is_set():
            
            return
            
        
        SchedulableJob.StartWork( self )
        
    
    def Work( self ) -> None:
        
        """Executes `Work`."""
        SchedulableJob.Work( self )
        
        if not self._stop_repeating.is_set():
            
            self._next_work_time = HydrusTime.GetNowFloat() + self._period
            
            self._scheduler.AddJob( self )
            
        
    
