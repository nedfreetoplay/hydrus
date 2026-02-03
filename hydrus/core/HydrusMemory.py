from hydrus.core import HydrusData

try:
    
    import pympler
    
    from pympler import asizeof
    from pympler import muppy
    from pympler import summary
    from pympler import classtracker
    from pympler import tracker
    
    PYMPLER_OK = True
    
except:
    
    PYMPLER_OK = False
    

CURRENT_TRACKER = None

# good examples here:
# https://pympler.readthedocs.io/en/latest/muppy.html#muppy
# this can do other stuff, class tracking and even charts with matplotlib

# pretty sure the Client should only ever call this stuff on the GUI thread of course, since it'll be touching Qt stuff

def check_pympler_ok():
    
    if not PYMPLER_OK:
        
        raise Exception( 'Pympler is not available!' )
        
    

def print_current_memory_use( classes_to_track = None ):
    
    check_pympler_ok()
    
    HydrusData.print_text( '---printing memory use to log---' )
    
    all_objects = muppy.get_objects()
    
    sm = summary.summarize( all_objects )
    
    summary.print_( sm, limit = 500 )
    
    HydrusData.debug_print( '----memory-use snapshot done----' )
    
    if classes_to_track is None:
        
        return
        
    
    HydrusData.print_text( '----printing class use to log---' )
    
    ct = classtracker.ClassTracker()
    
    for o in all_objects:
        
        if isinstance( o, classes_to_track ):
            
            ct.track_object( o )
            
        
    
    ct.create_snapshot()
    
    ct.stats.print_summary()
    
    HydrusData.debug_print( '-----class-use snapshot done----' )
    

def print_snapshot_diff():
    
    check_pympler_ok()
    
    global CURRENT_TRACKER
    
    if CURRENT_TRACKER is None:
        
        take_memory_use_snapshot()
        
    
    HydrusData.print_text( '---printing memory diff to log--' )
    
    # noinspection PyUnresolvedReferences
    diff = CURRENT_TRACKER.diff()
    
    summary.print_( diff, limit = 500 )
    
    HydrusData.debug_print( '----memory-use snapshot done----' )
    

def take_memory_use_snapshot():
    
    global CURRENT_TRACKER
    
    CURRENT_TRACKER = tracker.SummaryTracker()
    
