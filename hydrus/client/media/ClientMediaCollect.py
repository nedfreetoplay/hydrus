from hydrus.core import HydrusSerialisable

from hydrus.client import ClientConstants as CC
from hydrus.client import ClientGlobals as CG
from hydrus.client.search import ClientSearchTagContext

class MediaCollect( HydrusSerialisable.SerialisableBase ):
    
    SERIALISABLE_TYPE = HydrusSerialisable.SERIALISABLE_TYPE_MEDIA_COLLECT
    SERIALISABLE_NAME = 'Media Collect'
    SERIALISABLE_VERSION = 2
    
    def __init__( self, namespaces = None, rating_service_keys = None, collect_unmatched = None, tag_context = None ):
        
        if namespaces is None:
            
            namespaces = []
            
        
        if rating_service_keys is None:
            
            rating_service_keys = []
            
        
        if collect_unmatched is None:
            
            collect_unmatched = True
            
        
        if tag_context is None:
            
            tag_context = ClientSearchTagContext.TagContext( service_key = CC.COMBINED_TAG_SERVICE_KEY )
            
        
        self.namespaces = namespaces
        self.rating_service_keys = rating_service_keys
        self.collect_unmatched = collect_unmatched
        self.tag_context = tag_context
        
    
    def _get_serialisable_info( self ):
        
        serialisable_rating_service_keys = [ key.hex() for key in self.rating_service_keys ]
        
        serialisable_tag_context = self.tag_context.get_serialisable_tuple()
        
        return ( self.namespaces, serialisable_rating_service_keys, self.collect_unmatched, serialisable_tag_context )
        
    
    def _initialise_from_serialisable_info( self, serialisable_info ):
        
        ( self.namespaces, serialisable_rating_service_keys, self.collect_unmatched, serialisable_tag_context ) = serialisable_info
        
        self.rating_service_keys = [ bytes.fromhex( serialisable_key ) for serialisable_key in serialisable_rating_service_keys ]
        
        self.tag_context = HydrusSerialisable.create_from_serialisable_tuple( serialisable_tag_context )
        
    
    def _update_serialisable_info( self, version, old_serialisable_info ):
        
        if version == 1:
            
            ( namespaces, serialisable_rating_service_keys, collect_unmatched ) = old_serialisable_info
            
            tag_context = ClientSearchTagContext.TagContext( service_key = CC.COMBINED_TAG_SERVICE_KEY )
            
            serialisable_tag_context = tag_context.get_serialisable_tuple()
            
            new_serialisable_info = ( namespaces, serialisable_rating_service_keys, collect_unmatched, serialisable_tag_context )
            
            return ( 2, new_serialisable_info )
            
        
    
    def DoesACollect( self ):
        
        return len( self.namespaces ) > 0 or len( self.rating_service_keys ) > 0
        
    
    def ToString( self ):
        
        s_list = list( self.namespaces )
        s_list.extend( [ CG.client_controller.services_manager.GetNameSafe( service_key ) for service_key in self.rating_service_keys if CG.client_controller.services_manager.ServiceExists( service_key ) ] )
        
        if len( s_list ) == 0:
            
            return 'no collections'
            
        else:
            
            return ', '.join( s_list )
            
        
    
HydrusSerialisable.SERIALISABLE_TYPES_TO_OBJECT_TYPES[ HydrusSerialisable.SERIALISABLE_TYPE_MEDIA_COLLECT ] = MediaCollect
