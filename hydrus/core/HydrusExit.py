import os

def critical_initiate_immediate_program_halt():
    
    # note that sys.exit just raises an exception, so we have to really hit the hammer to make this work
    
    # noinspection PyUnresolvedReferences
    os._exit( 1 )
    
