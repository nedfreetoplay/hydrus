import datetime
import sys

def now_utc():
    
    if sys.version_info < ( 3, 11 ):
        
        # noinspection PyDeprecation
        return datetime.datetime.utcnow()
        
    else:
        
        return datetime.datetime.now( datetime.UTC )
        
    

def from_timestamp_utc(timestamp):
    
    if sys.version_info < ( 3, 11 ):
        
        # noinspection PyDeprecation
        return datetime.datetime.utcfromtimestamp( timestamp )
        
    else:
        
        return datetime.datetime.fromtimestamp( timestamp, datetime.UTC )
        
    
