from hydrus.core import HydrusSerialisable

from hydrus.client.media import ClientMediaResult
from hydrus.client.search import ClientSearchFileSearchContext

class MetadataConditional( HydrusSerialisable.SerialisableBase ):
    
    SERIALISABLE_TYPE = HydrusSerialisable.SERIALISABLE_TYPE_METADATA_CONDITIONAL
    SERIALISABLE_NAME = 'Metadata Conditional'
    SERIALISABLE_VERSION = 1
    
    def __init__( self ):
        
        super().__init__()
        
        self._file_search_context = ClientSearchFileSearchContext.FileSearchContext( predicates = [] )
        
    
    def _get_serialisable_info(self):
        
        serialisable_file_search_context = self._file_search_context.get_serialisable_tuple()
        
        return serialisable_file_search_context
        
    
    def _initialise_from_serialisable_info(self, serialisable_info):
        
        serialisable_file_search_context = serialisable_info
        
        self._file_search_context = HydrusSerialisable.create_from_serialisable_tuple(serialisable_file_search_context)
        
    
    def GetFileSearchContext( self ) -> ClientSearchFileSearchContext.FileSearchContext:
        
        return self._file_search_context
        
    
    def GetSummary( self ):
        
        return self._file_search_context.GetSummary()
        
    
    def SetFileSearchContext( self, file_search_context: ClientSearchFileSearchContext.FileSearchContext ):
        
        file_search_context = file_search_context.duplicate()
        
        predicates = file_search_context.GetPredicates()
        
        predicates = [ predicate for predicate in predicates if predicate.CanTestMediaResult() ]
        
        file_search_context.SetPredicates( predicates )
        
        self._file_search_context = file_search_context
        
    
    def Test( self, media_result: ClientMediaResult.MediaResult ) -> bool:
        
        return self._file_search_context.TestMediaResult( media_result )
        
    

HydrusSerialisable.SERIALISABLE_TYPES_TO_OBJECT_TYPES[ HydrusSerialisable.SERIALISABLE_TYPE_METADATA_CONDITIONAL ] = MetadataConditional
