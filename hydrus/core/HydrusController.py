import collections
import collections.abc
import os
import random
import sys
import threading
import time

from hydrus.core import HydrusConstants as HC
from hydrus.core import HydrusData
from hydrus.core import HydrusGlobals as HG
from hydrus.core import HydrusLogger
from hydrus.core import HydrusPaths
from hydrus.core import HydrusPubSub
from hydrus.core import HydrusTemp
from hydrus.core import HydrusTime
from hydrus.core.networking import HydrusNATPunch
from hydrus.core.processes import HydrusProcess
from hydrus.core.processes import HydrusSubprocess
from hydrus.core.processes import HydrusThreading

class HydrusController( object ):
    
    def __init__( self, db_dir: str, logger: HydrusLogger.HydrusLogger ):
        
        super().__init__()
        
        HG.controller = self
        
        self._name = 'hydrus'
        
        self._last_shutdown_was_bad = False
        self._i_own_running_file = False
        
        self.db_dir = db_dir
        self.logger = logger
        
        self.db = None
        
        pubsub_valid_callable = self._get_pubsub_valid_callable()
        
        self._pubsub = HydrusPubSub.HydrusPubSub( pubsub_valid_callable )
        self._daemon_jobs = {}
        self._managers = {}
        
        self._fast_job_scheduler = None
        self._slow_job_scheduler = None
        
        self._thread_slots = {}
        
        self._thread_slots[ 'misc' ] = ( 0, 10 )
        
        self._thread_slot_lock = threading.Lock()
        
        self._call_to_threads = []
        self._long_running_call_to_threads = []
        
        self._thread_pool_busy_status_text = ''
        self._thread_pool_busy_status_text_new_check_time = 0
        
        self._call_to_thread_lock = threading.Lock()
        
        self._timestamps_lock = threading.Lock()
        
        self._timestamps_ms = collections.defaultdict( lambda: 0 )
        
        self._sleep_lock = threading.Lock()
        
        self._just_woke_from_sleep = False
        
        self._system_busy = False
        
        self._doing_fast_exit = False
        
        self.touch_time('boot')
        self.touch_time('last_sleep_check')
        
    
    def _get_call_to_thread(self):
        
        with self._call_to_thread_lock:
            
            for call_to_thread in self._call_to_threads:
                
                if not call_to_thread.CurrentlyWorking():
                    
                    return call_to_thread
                    
                
            
            # all the threads in the pool are currently busy
            
            ok_to_make_one = len( self._call_to_threads ) < 200
            
            if not ok_to_make_one:
                
                my_thread = threading.current_thread()
                
                calling_from_the_thread_pool = my_thread in self._call_to_threads or my_thread in self._long_running_call_to_threads
                
                ok_to_make_one = calling_from_the_thread_pool
                
            
            if ok_to_make_one:
                
                call_to_thread = HydrusThreading.THREADCallToThread( self, 'CallToThread' )
                
                self._call_to_threads.append( call_to_thread )
                
                call_to_thread.start()
                
            else:
                
                call_to_thread = random.choice( self._call_to_threads )
                
            
            return call_to_thread
            
        
    
    def _get_call_to_thread_long_running(self):
        
        with self._call_to_thread_lock:
            
            for call_to_thread in self._long_running_call_to_threads:
                
                if not call_to_thread.CurrentlyWorking():
                    
                    return call_to_thread
                    
                
            
            call_to_thread = HydrusThreading.THREADCallToThread( self, 'CallToThreadLongRunning' )
            
            self._long_running_call_to_threads.append( call_to_thread )
            
            call_to_thread.start()
            
            return call_to_thread
            
        
    
    def _get_pubsub_valid_callable(self):
        
        return lambda o: True
        
    
    def _get_appropriate_job_scheduler(self, time_delta):
        
        if time_delta <= 1.0:
            
            return self._fast_job_scheduler
            
        else:
            
            return self._slow_job_scheduler
            
        
    
    def _get_upnp_services(self):
        
        return []
        
    
    def _get_wake_delay_period_ms(self):
        
        return 15 * 1000
        
    
    def _init_db(self):
        
        raise NotImplementedError()
        
    
    def _init_hydrus_temp_dir(self):
        
        self._hydrus_temp_dir = HydrusTemp.initialise_hydrus_temp_dir()
        
    
    def _maintain_call_to_threads(self):
        
        # we don't really want to hang on to threads that are done as event.wait() has a bit of idle cpu
        # so, any that are in the pools that aren't doing anything can be killed and sent to garbage
        
        with self._call_to_thread_lock:
            
            def filter_call_to_threads( t ):
                
                if t.CurrentlyWorking():
                    
                    return True
                    
                else:
                    
                    t.shutdown()
                    
                    return False
                    
                
            
            self._call_to_threads = list( filter( filter_call_to_threads, self._call_to_threads ) )
            
            self._long_running_call_to_threads = list( filter( filter_call_to_threads, self._long_running_call_to_threads ) )
            
        
    
    def _publish_shutdown_subtext(self, text):
        
        pass
        
    
    def _read(self, action, *args, **kwargs):
        
        result = self.db.read(action, *args, **kwargs)
        
        return result
        
    
    def _report_shutdown_daemons_status(self):
        
        pass
        
    
    def _show_just_woke_to_user(self) -> None:
        
        HydrusData.print_text('Just woke from sleep.')
        
    
    def _shutdown_daemons(self):
        
        for job in self._daemon_jobs.values():
            
            job.Cancel()
            
        
        if not self._doing_fast_exit:
            
            started = HydrusTime.GetNow()
            
            while True in ( daemon_job.CurrentlyWorking() for daemon_job in self._daemon_jobs.values() ):
                
                self._report_shutdown_daemons_status()
                
                time.sleep( 0.1 )
                
                if HydrusTime.TimeHasPassed( started + 30 ):
                    
                    break
                    
                
            
        
        self._daemon_jobs = {}
        
    
    def _write(self, action, synchronous, *args, **kwargs):
        
        result = self.db.write(action, synchronous, *args, **kwargs)
        
        return result
        
    
    def pub( self, topic, *args, **kwargs ) -> None:
        
        if HG.model_shutdown:
            
            self._pubsub.pub_immediate(topic, *args, **kwargs)
            
        else:
            
            self._pubsub.pub( topic, *args, **kwargs )
            
        
    
    def pub_immediate(self, topic, *args, **kwargs) -> None:
        
        self._pubsub.pub_immediate(topic, *args, **kwargs)
        
    
    def sub( self, object, method_name, topic ) -> None:
        
        self._pubsub.sub( object, method_name, topic )
        
    
    def acquire_thread_slot(self, thread_type) -> bool:
        
        with self._thread_slot_lock:
            
            if thread_type not in self._thread_slots:
                
                return True # assume no max if no max set
                
            
            ( current_threads, max_threads ) = self._thread_slots[ thread_type ]
            
            if current_threads < max_threads:
                
                self._thread_slots[ thread_type ] = ( current_threads + 1, max_threads )
                
                return True
                
            else:
                
                return False
                
            
        
    
    def blocking_safe_show_critical_message(self, title: str, message: str):
        
        HydrusData.debug_print(title)
        HydrusData.debug_print(message)
        
        input( 'Press Enter to continue.' )
        
    
    def blocking_safe_show_message(self, message: str):
        
        HydrusData.debug_print(message)
        
        input( 'Press Enter to continue.' )
        
    
    def thread_slots_are_available(self, thread_type) -> bool:
        
        with self._thread_slot_lock:
            
            if thread_type not in self._thread_slots:
                
                return True # assume no max if no max set
                
            
            ( current_threads, max_threads ) = self._thread_slots[ thread_type ]
            
            return current_threads < max_threads
            
        
    
    def call_later(self, initial_delay, func, *args, **kwargs) -> HydrusThreading.SingleJob:
        
        job_scheduler = self._get_appropriate_job_scheduler(initial_delay)
        
        call = HydrusData.Call( func, *args, **kwargs )
        
        job = HydrusThreading.SingleJob( self, job_scheduler, initial_delay, call )
        
        job_scheduler.AddJob( job )
        
        return job
        
    
    def call_repeating(self, initial_delay, period, func, *args, **kwargs) -> HydrusThreading.RepeatingJob:
        
        job_scheduler = self._get_appropriate_job_scheduler(period)
        
        call = HydrusData.Call( func, *args, **kwargs )
        
        job = HydrusThreading.RepeatingJob( self, job_scheduler, initial_delay, period, call )
        
        job_scheduler.AddJob( job )
        
        return job
        
    
    def call_to_thread(self, callable, *args, **kwargs) -> None:
        
        if HG.callto_report_mode:
            
            what_to_report = [ callable ]
            
            if len( args ) > 0:
                
                what_to_report.append( args )
                
            
            if len( kwargs ) > 0:
                
                what_to_report.append( kwargs )
                
            
            HydrusData.show_text(tuple(what_to_report))
            
        
        call_to_thread = self._get_call_to_thread()
        
        call_to_thread.put( callable, *args, **kwargs )
        
    
    def call_to_thread_long_running(self, callable, *args, **kwargs) -> None:
        
        if HG.callto_report_mode:
            
            what_to_report = [ callable ]
            
            if len( args ) > 0:
                
                what_to_report.append( args )
                
            
            if len( kwargs ) > 0:
                
                what_to_report.append( kwargs )
                
            
            HydrusData.show_text(tuple(what_to_report))
            
        
        call_to_thread = self._get_call_to_thread_long_running()
        
        call_to_thread.put( callable, *args, **kwargs )
        
    
    def clean_running_file(self) -> None:
        
        if self._i_own_running_file:
            
            HydrusData.clean_running_file(self.db_dir, self._name)
            
        
    
    def clear_caches(self) -> None:
        
        pass
        
    
    def currently_idle(self) -> bool:
        
        return True
        
    
    def currently_pub_subbing(self) -> bool:
        
        return self._pubsub.work_to_do() or self._pubsub.doing_work()
        
    
    def db_currently_doing_job(self) -> bool:
        
        if self.db is None:
            
            return False
            
        else:
            
            return self.db.currently_doing_job()
            
        
    
    def debug_show_scheduled_jobs(self):
        
        summary = self._fast_job_scheduler.GetPrettyJobSummary()
        
        HydrusData.show_text('fast scheduler:')
        HydrusData.show_text(summary)
        
        summary = self._slow_job_scheduler.GetPrettyJobSummary()
        
        HydrusData.show_text('slow scheduler:')
        HydrusData.show_text(summary)
        
    
    def doing_fast_exit(self) -> bool:
        
        return self._doing_fast_exit
        
    
    def force_database_commit(self):
        
        if self.db is None:
            
            raise Exception( 'Sorry, database does not seem to be alive at the moment!' )
            
        
        self.db.force_a_commit()
        
    
    def get_boot_timestamp_ms(self):
        
        return self.get_timestamp_ms('boot')
        
    
    def get_db_dir(self):
        
        return self.db_dir
        
    
    def get_db_status(self):
        
        return self.db.get_status()
        
    
    def get_hydrus_temp_dir(self):
        
        if not os.path.exists( self._hydrus_temp_dir ):
            
            self._init_hydrus_temp_dir()
            
        
        return self._hydrus_temp_dir
        
    
    def get_job_scheduler_snapshot(self, scheduler_name):
        
        if scheduler_name == 'fast':
            
            scheduler = self._fast_job_scheduler
            
        else:
            
            scheduler = self._slow_job_scheduler
            
        
        return scheduler.GetJobs()
        
    
    def get_manager(self, name):
        
        return self._managers[ name ]
        
    
    def get_name(self):
        
        return self._name
        
    
    def get_thread_pool_busy_status(self):
        
        if HydrusTime.TimeHasPassed( self._thread_pool_busy_status_text_new_check_time ):
            
            with self._call_to_thread_lock:
                
                num_threads = sum( ( 1 for t in self._call_to_threads if t.CurrentlyWorking() ) )
                
            
            if num_threads < 4:
                
                self._thread_pool_busy_status_text = ''
                
            elif num_threads < 10:
                
                self._thread_pool_busy_status_text = 'working'
                
            elif num_threads < 20:
                
                self._thread_pool_busy_status_text = 'busy'
                
            else:
                
                self._thread_pool_busy_status_text = 'very busy!'
                
            
            self._thread_pool_busy_status_text_new_check_time = HydrusTime.GetNow() + 10
            
        
        return self._thread_pool_busy_status_text
        
    
    def get_threads_snapshot(self):
        
        threads = []
        
        threads.extend( self._call_to_threads )
        threads.extend( self._long_running_call_to_threads )
        
        threads.append( self._slow_job_scheduler )
        threads.append( self._fast_job_scheduler )
        
        return threads
        
    
    def get_timestamp_ms(self, name: str) -> int:
        
        with self._timestamps_lock:
            
            return self._timestamps_ms[ name ]
            
        
    
    def good_time_to_start_background_work(self) -> bool:
        
        return self.currently_idle() and not (self.just_woke_from_sleep() or self.system_busy())
        
    
    def good_time_to_start_foreground_work(self) -> bool:
        
        return not self.just_woke_from_sleep()
        
    
    def just_woke_from_sleep(self):
        
        self.sleep_check()
        
        return self._just_woke_from_sleep
        
    
    def init_model(self) -> None:
        
        try:
            
            self._init_hydrus_temp_dir()
            
        except Exception as e:
            
            HydrusData.print_text('Failed to initialise temp folder.')
            
        
        from hydrus.core.files import HydrusFileHandling
        
        HydrusFileHandling.InitialiseMimesToDefaultThumbnailPaths()
        
        self._fast_job_scheduler = HydrusThreading.JobScheduler( self )
        self._slow_job_scheduler = HydrusThreading.JobScheduler( self )
        
        self._fast_job_scheduler.start()
        self._slow_job_scheduler.start()
        
        self._init_db()
        
        # reset after a long db update
        self.touch_time('last_sleep_check')
        
    
    def init_view(self):
        
        job = self.call_repeating(60.0, 300.0, self.maintain_db, maintenance_mode = HC.MAINTENANCE_IDLE)
        
        job.WakeOnPubSub( 'wake_idle_workers' )
        job.ShouldDelayOnWakeup( True )
        
        self._daemon_jobs[ 'maintain_db' ] = job
        
        job = self.call_repeating(0.0, 15.0, self.sleep_check)
        
        self._daemon_jobs[ 'sleep_check' ] = job
        
        job = self.call_repeating(10.0, 60.0, self.maintain_memory_fast)
        
        self._daemon_jobs[ 'maintain_memory_fast' ] = job
        
        job = self.call_repeating(10.0, 300.0, self.maintain_memory_slow)
        
        self._daemon_jobs[ 'maintain_memory_slow' ] = job
        
        upnp_services = self._get_upnp_services()
        
        self.services_upnp_manager = HydrusNATPunch.ServicesUPnPManager( upnp_services )
        
        job = self.call_repeating(10.0, 43200.0, self.services_upnp_manager.RefreshUPnP)
        
        self._daemon_jobs[ 'services_upnp' ] = job
        
    
    def is_first_start(self):
        
        if self.db is None:
            
            return False
            
        else:
            
            return self.db.is_first_start()
            
        
    
    def last_shutdown_was_bad(self):
        
        return self._last_shutdown_was_bad
        
    
    def maintain_db(self, maintenance_mode = HC.MAINTENANCE_IDLE, stop_time = None):
        
        pass
        
    
    def maintain_memory_fast(self):
        
        sys.stdout.flush()
        sys.stderr.flush()
        
        self.pub( 'memory_maintenance_pulse' )
        
        self._fast_job_scheduler.ClearOutDead()
        self._slow_job_scheduler.ClearOutDead()
        
        HydrusSubprocess.ReapDeadLongLivedExternalProcesses()
        
    
    def maintain_memory_slow(self):
        
        HydrusTemp.clean_up_old_temp_paths()
        
        self._maintain_call_to_threads()
        
    
    def read(self, action, *args, **kwargs):
        
        return self._read(action, *args, **kwargs)
        
    
    def record_running_start(self):
        
        self._last_shutdown_was_bad = HydrusData.last_shutdown_was_bad(self.db_dir, self._name)
        
        self._i_own_running_file = True
        
        HydrusProcess.RecordRunningStart( self.db_dir, self._name )
        
    
    def release_thread_slot(self, thread_type):
        
        with self._thread_slot_lock:
            
            if thread_type not in self._thread_slots:
                
                return
                
            
            ( current_threads, max_threads ) = self._thread_slots[ thread_type ]
            
            self._thread_slots[ thread_type ] = ( current_threads - 1, max_threads )
            
        
    
    def report_data_used(self, num_bytes):
        
        pass
        
    
    def report_request_used(self):
        
        pass
        
    
    def reset_idle_timer(self) -> None:
        
        self.touch_time('last_user_action')
        
    
    def set_timestamp_ms(self, name: str, timestamp_ms: int) -> None:
        
        with self._timestamps_lock:
            
            self._timestamps_ms[ name ] = timestamp_ms
            
        
    
    def should_stop_this_work(self, maintenance_mode, stop_time = None) -> bool:
        
        if maintenance_mode == HC.MAINTENANCE_IDLE:
            
            if not self.currently_idle():
                
                return True
                
            
        elif maintenance_mode == HC.MAINTENANCE_SHUTDOWN:
            
            if not HG.do_idle_shutdown_work:
                
                return True
                
            
        
        if stop_time is not None:
            
            if HydrusTime.TimeHasPassed( stop_time ):
                
                return True
                
            
        
        return False
        
    
    def shutdown_model(self) -> None:
        
        if self.db is not None:
            
            self.db.shutdown()
            
            if not self._doing_fast_exit:
                
                while not self.db.loop_is_finished():
                    
                    self._publish_shutdown_subtext('waiting for db to finish up' + HC.UNICODE_ELLIPSIS)
                    
                    time.sleep( 0.1 )
                    
                
            
        
        if self._fast_job_scheduler is not None:
            
            self._fast_job_scheduler.shutdown()
            
            self._fast_job_scheduler = None
            
        
        if self._slow_job_scheduler is not None:
            
            self._slow_job_scheduler.shutdown()
            
            self._slow_job_scheduler = None
            
        
        HydrusTemp.clean_up_old_temp_paths()
        
        if hasattr( self, '_hydrus_temp_dir' ):
            
            HydrusPaths.delete_path(self._hydrus_temp_dir)
            
        
        with self._call_to_thread_lock:
            
            for call_to_thread in self._call_to_threads:
                
                call_to_thread.shutdown()
                
            
            for long_running_call_to_thread in self._long_running_call_to_threads:
                
                long_running_call_to_thread.shutdown()
                
            
        
        HG.model_shutdown = True
        
        self._pubsub.wake()
        
    
    def shutdown_view(self) -> None:
        
        HG.view_shutdown = True
        
        self._shutdown_daemons()
        
    
    def shutdown_from_server(self):
        
        raise Exception( 'This hydrus application cannot be shut down from the server!' )
        
    
    def sleep_check(self) -> None:
        
        with self._sleep_lock:
            
            if HydrusTime.TimeHasPassedMS(self.get_timestamp_ms('last_sleep_check') + 60000): # it has been way too long since this method last fired, so we've prob been asleep
                
                self._just_woke_from_sleep = True
                
                self.reset_idle_timer() # this will stop the background jobs from kicking in as soon as the grace period is over
                
                wake_delay_period_ms = self._get_wake_delay_period_ms()
                
                self.set_timestamp_ms('now_awake', HydrusTime.GetNowMS() + wake_delay_period_ms) # enough time for ethernet to get back online and all that
                
                self._show_just_woke_to_user()
                
            elif self._just_woke_from_sleep and HydrusTime.TimeHasPassedMS(self.get_timestamp_ms('now_awake')):
                
                self._just_woke_from_sleep = False
                
            
            self.touch_time('last_sleep_check')
            
        
    
    def simulate_wake_from_sleep_event(self) -> None:
        
        with self._sleep_lock:
            
            self.set_timestamp_ms('last_sleep_check', HydrusTime.GetNowMS() - (3600 * 1000))
            
        
        self.sleep_check()
        
    
    def system_busy(self):
        
        return self._system_busy
        
    
    def touch_time(self, name: str) -> None:
        
        with self._timestamps_lock:
            
            self._timestamps_ms[ name ] = HydrusTime.GetNowMS()
            
        
    
    def wait_until_db_empty(self) -> None:
        
        self.db.wait_until_free()
        
    
    def wait_until_model_free(self) -> None:
        
        self.wait_until_pub_subs_empty()
        
        self.wait_until_db_empty()
        
    
    def wait_until_pub_subs_empty(self):
        
        self._pubsub.wait_until_free()
        
    
    def wake_daemon(self, name):
        
        if name in self._daemon_jobs:
            
            self._daemon_jobs[ name ].wake()
            
        
    
    def write(self, action, *args, **kwargs):
        
        return self._write(action, False, *args, **kwargs)
        
    
    def write_synchronous(self, action, *args, **kwargs):
        
        return self._write(action, True, *args, **kwargs)
        
    
