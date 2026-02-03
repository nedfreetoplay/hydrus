import cProfile
import io
import os
import pstats
import threading
import time

from hydrus.core import HydrusData
from hydrus.core import HydrusGlobals as HG
from hydrus.core import HydrusNumbers
from hydrus.core import HydrusTime

profile_mode = False
profile_mode_name = ''

profile_start_time = 0
profile_slow_count = 0
profile_fast_count = 0
profile_counter_lock = threading.Lock()

query_planner_mode = False

query_planner_start_time = 0
query_planner_query_count = 0
queries_planned = set()

# OK, as of Py 3.12, you can no longer invoke two profilers at once. for my purposes that generally meant nested
# Apaarrrrrently, up until now, that caused the 'original' profile to be disabled somehow. perhaps this explains some odd numbers we sometimes had
# so now we'll just have a test to catch an existing profile and dump out with an error message
# It seems a profile does only gather data from the current thread, but Py 3.12+ has some funky way of turning it on for all threads, which might be something to consider trying
# I am moving (2025-09) to multiple profile modes (UI, db, etc..) to try and better target single threads and reduce overlap EXCLUSIVE spam
CURRENT_PROFILE_LOCK = threading.Lock()

# TODO: The next step here is probably to have a simple 'profile.enable' or whatever it is profiling mode on the Qt thread and then we just see what Qt is doing right now
# this mode appears to just profile all calls mate and would be useful to catch the various hooks I don't usually have easy access to. we'd just dump one mega-profile to log on completion
# and py 3.12 has the ability to do this for all threads, so we could even have a super mode there too

def flip_profile_mode( name ):
    
    if profile_mode:
        
        start_new_mode = profile_mode_name != name
        
        stop_profile_mode()
        
        if start_new_mode:
            
            start_profile_mode( name )
            
        
    else:
        
        start_profile_mode( name )
        
    

def flip_query_planner_mode():
    
    global query_planner_mode
    global query_planner_start_time
    global query_planner_query_count
    global queries_planned
    
    if not query_planner_mode:
        
        now = HydrusTime.get_now()
        
        query_planner_start_time = now
        query_planner_query_count = 0
        
        query_planner_mode = True
        
        HydrusData.show_text( 'Query Planner mode on!' )
        
    else:
        
        query_planner_mode = False
        
        queries_planned = set()
        
        HydrusData.show_text( 'Query Planning done: {} queries analyzed'.format( HydrusNumbers.to_human_int( query_planner_query_count ) ) )
        
    

def is_profile_mode( name ):
    
    return profile_mode and profile_mode_name == name
    

def print_profile( summary, profile_text = None ):
    
    name = HG.controller.get_name()
    db_dir = HG.controller.get_db_dir()
    
    pretty_timestamp = time.strftime( '%Y-%m-%d %H-%M-%S', time.localtime( profile_start_time ) )
    
    profile_log_filename = f'{name} profile ({profile_mode_name}) - {pretty_timestamp}.log'
    
    profile_log_path = os.path.join( db_dir, profile_log_filename )
    
    with open( profile_log_path, 'a', encoding = 'utf-8' ) as f:
        
        prefix = time.strftime( '%Y-%m-%d %H:%M:%S: ' )
        
        f.write( prefix + summary )
        
        if profile_text is not None:
            
            f.write( '\n\n' )
            f.write( profile_text )
            
        
    

def print_query_plan( query, plan_lines ):
    
    global queries_planned
    global query_planner_query_count
    
    query_planner_query_count += 1
    
    if query in queries_planned:
        
        return
        
    
    name = HG.controller.get_name()
    db_dir = HG.controller.get_db_dir()
    
    queries_planned.add( query )
    
    pretty_timestamp = time.strftime( '%Y-%m-%d %H-%M-%S', time.localtime( query_planner_start_time ) )
    
    query_planner_log_filename = '{} query planner - {}.log'.format( name, pretty_timestamp )
    
    query_planner_log_path = os.path.join( db_dir, query_planner_log_filename )
    
    with open( query_planner_log_path, 'a', encoding = 'utf-8' ) as f:
        
        prefix = time.strftime( '%Y-%m-%d %H:%M:%S: ' )
        
        if ' ' in query:
            
            first_word = query.split( ' ', 1 )[0]
            
        else:
            
            first_word = 'unknown'
            
        
        f.write( prefix + first_word )
        f.write( '\n' )
        f.write( query )
        
        if len( plan_lines ) > 0:
            
            f.write( '\n' )
            f.write( '\n'''.join( ( str( p ) for p in plan_lines ) ) )
            
        
        f.write( '\n\n' )
        
    

def profile( summary, func, min_duration_ms = 20, show_summary = False ):
    
    global CURRENT_PROFILE_LOCK
    global profile_counter_lock
    global profile_slow_count
    global profile_fast_count
    
    got_it = CURRENT_PROFILE_LOCK.acquire( blocking = False )
    
    if got_it:
        
        try:
            
            started = HydrusTime.get_now_precise()
            
            profile = cProfile.Profile()
            
            profile.runcall( func )
            
            finished = HydrusTime.get_now_precise()
            
            time_took = finished - started
            time_took_ms = HydrusTime.millisecondise_s( time_took )
            
            if time_took_ms > min_duration_ms:
                
                output = io.StringIO()
                
                stats = pstats.Stats( profile, stream = output )
                
                stats.strip_dirs()
                
                stats.sort_stats( 'tottime' )
                
                output.write( 'Stats' )
                output.write( '\n' * 2 )
                
                stats.print_stats()
                
                output.write( 'Callers' )
                output.write( '\n' * 2 )
                
                stats.print_callers()
                
                output.seek( 0 )
                
                profile_text = output.read()
                
                with profile_counter_lock:
                    
                    profile_slow_count += 1
                    
                
                if show_summary:
                    
                    HydrusData.show_text( summary )
                    
                
                print_profile( summary, profile_text = profile_text )
                
            else:
                
                with profile_counter_lock:
                    
                    profile_fast_count += 1
                    
                
                if show_summary:
                    
                    print_profile( summary + '\n\n' )
                    
                
            
            del profile
            
        finally:
            
            CURRENT_PROFILE_LOCK.release()
            
        
    else:
        
        started = HydrusTime.get_now_precise()
        
        func()
        
        finished = HydrusTime.get_now_precise()
        
        time_took = finished - started
        time_took_ms = HydrusTime.millisecondise_s( time_took )
        
        if time_took_ms > min_duration_ms:
            
            with profile_counter_lock:
                
                profile_slow_count += 1
                
            
        else:
            
            with profile_counter_lock:
                
                profile_fast_count += 1
                
            
        
        print_profile( f'EXCLUSIVE: {summary} took {HydrusTime.timedelta_to_pretty_timedelta( time_took )}\n\n')
        
    

def start_profile_mode( name ):
    
    global profile_mode
    global profile_mode_name
    
    if profile_mode:
        
        stop_profile_mode()
        
    
    profile_mode = True
    profile_mode_name = name
    
    now = HydrusTime.get_now()
    
    global profile_counter_lock
    
    with profile_counter_lock:
        
        global profile_start_time
        global profile_slow_count
        global profile_fast_count
        
        profile_start_time = now
        profile_slow_count = 0
        profile_fast_count = 0
        
    
    HydrusData.show_text( f'Profile mode "{name}" on!' )
    

def stop_profile_mode():
    
    global profile_mode
    global profile_mode_name
    
    old_name = profile_mode_name
    
    profile_mode = False
    profile_mode_name = ''
    
    global profile_counter_lock
    
    with profile_counter_lock:
        
        ( slow, fast ) = ( profile_slow_count, profile_fast_count )
        
    
    HydrusData.show_text( f'Profiling "{old_name}" done: {HydrusNumbers.to_human_int( slow )} slow jobs, {HydrusNumbers.to_human_int( fast )} fast jobs' )
    
