import collections.abc

from hydrus.core import HydrusConstants as HC
from hydrus.core import HydrusNumbers
from hydrus.core import HydrusSerialisable

from hydrus.client import ClientConstants as CC
from hydrus.client import ClientGlobals as CG

def filter_out_redundant_meta_services(list_of_service_keys: list[ bytes]):
    
    services_manager = CG.client_controller.services_manager
    
    special_local_file_service_keys = { CC.TRASH_SERVICE_KEY, CC.COMBINED_LOCAL_FILE_DOMAINS_SERVICE_KEY, CC.LOCAL_UPDATE_SERVICE_KEY }
    
    if len( special_local_file_service_keys.intersection( list_of_service_keys ) ) <= 1:
        
        if CC.HYDRUS_LOCAL_FILE_STORAGE_SERVICE_KEY in list_of_service_keys:
            
            list_of_service_keys.remove( CC.HYDRUS_LOCAL_FILE_STORAGE_SERVICE_KEY )
            
        
    
    local_file_service_keys = set(services_manager.get_service_keys((HC.LOCAL_FILE_DOMAIN,)))
    
    if len( local_file_service_keys.intersection( list_of_service_keys ) ) <= 1:
        
        if CC.COMBINED_LOCAL_FILE_DOMAINS_SERVICE_KEY in list_of_service_keys:
            
            list_of_service_keys.remove( CC.COMBINED_LOCAL_FILE_DOMAINS_SERVICE_KEY )
            
        
    
    return list_of_service_keys
    

def get_possible_file_domain_services_in_order(all_known_files_allowed: bool, only_importable_domains_allowed: bool, only_local_file_domains_allowed: bool, only_combined_local_file_domains_allowed: bool):
    
    # TODO: WOW the 'only_x' parameters here are awful!!! rewrite all this!
    # seems like it cascades, so set up an enum instead I think!
    
    services_manager = CG.client_controller.services_manager
    
    service_types_in_order = [ HC.LOCAL_FILE_DOMAIN ]
    
    if not only_importable_domains_allowed:
        
        advanced_mode = CG.client_controller.new_options.get_boolean('advanced_mode')
        
        if len(services_manager.get_services((HC.LOCAL_FILE_DOMAIN,))) > 1 or advanced_mode:
            
            service_types_in_order.append( HC.COMBINED_LOCAL_FILE_DOMAINS )
            
        
        if not only_combined_local_file_domains_allowed:
            
            service_types_in_order.append( HC.LOCAL_FILE_TRASH_DOMAIN )
            
            if advanced_mode:
                
                service_types_in_order.append( HC.LOCAL_FILE_UPDATE_DOMAIN )
                
            
            if advanced_mode:
                
                service_types_in_order.append( HC.HYDRUS_LOCAL_FILE_STORAGE )
                
            
            if not only_local_file_domains_allowed:
                
                if advanced_mode:
                    
                    service_types_in_order.append( HC.COMBINED_DELETED_FILE )
                    
                
                service_types_in_order.append( HC.FILE_REPOSITORY )
                service_types_in_order.append( HC.IPFS )
                
                if all_known_files_allowed:
                    
                    service_types_in_order.append( HC.COMBINED_FILE )
                    
                
            
        
    
    services = services_manager.get_services(service_types_in_order)
    
    return services
    

def sort_file_service_keys_nicely(list_of_service_keys):
    
    services_in_nice_order = get_possible_file_domain_services_in_order(False, False, False, False)
    
    service_keys_in_nice_order = [service.get_service_key() for service in services_in_nice_order]
    
    list_of_service_keys = [ service_key for service_key in service_keys_in_nice_order if service_key in list_of_service_keys ]
    
    return list_of_service_keys
    

def valid_local_domains_filter(service_keys):
    
    return [service_key for service_key in service_keys if CG.client_controller.services_manager.service_exists(service_key) and CG.client_controller.services_manager.get_service_type(service_key) == HC.LOCAL_FILE_DOMAIN]
    
class LocationContext( HydrusSerialisable.SerialisableBase ):
    
    SERIALISABLE_TYPE = HydrusSerialisable.SERIALISABLE_TYPE_LOCATION_CONTEXT
    SERIALISABLE_NAME = 'Location Search Context'
    SERIALISABLE_VERSION = 1
    
    def __init__( self, current_service_keys: collections.abc.Collection[ bytes ] | None = None, deleted_service_keys: collections.abc.Collection[ bytes ] | None = None ):
        
        # note this is pretty much a read-only class
        # sometimes we'll run FixMissingServices, but usually only on load and who cares if that fix is propagated around
        # hence no need to duplicate this for every handler, since it won't be changing
        
        if current_service_keys is None:
            
            current_service_keys = []
            
        
        if deleted_service_keys is None:
            
            deleted_service_keys = []
            
        
        self.current_service_keys = frozenset( current_service_keys )
        self.deleted_service_keys = frozenset( deleted_service_keys )
        
        if self.is_all_known_files():
            
            self.current_service_keys = frozenset( [ CC.COMBINED_FILE_SERVICE_KEY ] )
            self.deleted_service_keys = frozenset()
            
        
    
    def __eq__( self, other ):
        
        if isinstance( other, LocationContext ):
            
            return self.__hash__() == other.__hash__()
            
        
        return NotImplemented
        
    
    def __hash__( self ):
        
        # works because frozenset
        return ( self.current_service_keys, self.deleted_service_keys ).__hash__()
        
    
    def _get_serialisable_info( self ):
        
        serialisable_current_service_keys = [ service_key.hex() for service_key in self.current_service_keys ]
        serialisable_deleted_service_keys = [ service_key.hex() for service_key in self.deleted_service_keys ]
        
        return ( serialisable_current_service_keys, serialisable_deleted_service_keys )
        
    
    def _initialise_from_serialisable_info( self, serialisable_info ):
        
        ( serialisable_current_service_keys, serialisable_deleted_service_keys ) = serialisable_info
        
        self.current_service_keys = frozenset( { bytes.fromhex( service_key ) for service_key in serialisable_current_service_keys } )
        self.deleted_service_keys = frozenset( { bytes.fromhex( service_key ) for service_key in serialisable_deleted_service_keys } )
        
    
    def clear_surplus_local_files_services(self, service_type_func: collections.abc.Callable):
        # if we have combined local files, then we don't need specific local domains
        
        if CC.HYDRUS_LOCAL_FILE_STORAGE_SERVICE_KEY in self.current_service_keys:
            
            self.current_service_keys = frozenset( ( service_key for service_key in self.current_service_keys if service_type_func( service_key ) not in HC.FILE_SERVICES_COVERED_BY_HYDRUS_LOCAL_FILE_STORAGE ) )
            
        
        if CC.HYDRUS_LOCAL_FILE_STORAGE_SERVICE_KEY in self.deleted_service_keys:
            
            self.deleted_service_keys = frozenset( ( service_key for service_key in self.deleted_service_keys if service_type_func( service_key ) not in HC.FILE_SERVICES_COVERED_BY_HYDRUS_LOCAL_FILE_STORAGE ) )
            
        
        if CC.COMBINED_LOCAL_FILE_DOMAINS_SERVICE_KEY in self.current_service_keys:
            
            self.current_service_keys = frozenset( ( service_key for service_key in self.current_service_keys if service_type_func( service_key ) not in HC.FILE_SERVICES_COVERED_BY_COMBINED_LOCAL_FILE_DOMAINS ) )
            
        
        if CC.COMBINED_LOCAL_FILE_DOMAINS_SERVICE_KEY in self.deleted_service_keys:
            
            self.deleted_service_keys = frozenset( ( service_key for service_key in self.deleted_service_keys if service_type_func( service_key ) not in HC.FILE_SERVICES_COVERED_BY_COMBINED_LOCAL_FILE_DOMAINS ) )
            
        
    
    def fix_missing_services(self, services_exist_func: collections.abc.Callable) -> bool:
        
        prev_len = len( self.current_service_keys ) + len( self.deleted_service_keys )
        
        self.current_service_keys = frozenset( services_exist_func( self.current_service_keys ) )
        self.deleted_service_keys = frozenset( services_exist_func( self.deleted_service_keys ) )
        
        post_len = len( self.current_service_keys ) + len( self.deleted_service_keys )
        
        some_removed = prev_len != post_len
        
        return some_removed
        
    
    def get_covering_current_file_service_keys(self):
        
        file_location_is_cross_referenced = not (self.is_all_known_files() or self.includes_deleted())
        
        file_service_keys = list( self.current_service_keys )
        
        if self.includes_deleted():
            
            file_service_keys.append( CC.COMBINED_DELETED_FILE_SERVICE_KEY )
            
        
        return ( file_service_keys, file_location_is_cross_referenced )
        
    
    def get_deleted_inverse(self):
        
        inverse = self.duplicate()
        
        a = inverse.current_service_keys
        inverse.current_service_keys = inverse.deleted_service_keys
        inverse.deleted_service_keys = a
        
        return inverse
        
    
    def get_statuses_and_service_keys_list(self):
        
        statuses_and_service_keys = [ ( HC.CONTENT_STATUS_CURRENT, service_key ) for service_key in self.current_service_keys ]
        statuses_and_service_keys.extend( [ ( HC.CONTENT_STATUS_DELETED, service_key ) for service_key in self.deleted_service_keys ] )
        
        return statuses_and_service_keys
        
    
    def includes_current(self):
        
        return len( self.current_service_keys ) > 0
        
    
    def includes_deleted(self):
        
        return len( self.deleted_service_keys ) > 0
        
    
    def is_all_known_files(self):
        
        return CC.COMBINED_FILE_SERVICE_KEY in self.current_service_keys
        
    
    def is_hydrus_local_file_storage(self):
        
        return self.is_one_domain() and CC.HYDRUS_LOCAL_FILE_STORAGE_SERVICE_KEY in self.current_service_keys
        
    
    def is_combined_local_file_domains(self):
        
        return self.is_one_domain() and CC.COMBINED_LOCAL_FILE_DOMAINS_SERVICE_KEY in self.current_service_keys
        
    
    def is_empty(self):
        
        return len( self.current_service_keys ) + len( self.deleted_service_keys ) == 0
        
    
    def is_one_domain(self):
        
        return len( self.current_service_keys ) + len( self.deleted_service_keys ) == 1
        
    
    def limit_to_service_types(self, service_type_func: collections.abc.Callable, service_types):
        
        self.current_service_keys = frozenset( ( service_key for service_key in self.current_service_keys if service_type_func( service_key ) in service_types ) )
        
        self.deleted_service_keys = frozenset( ( service_key for service_key in self.deleted_service_keys if service_type_func( service_key ) in service_types ) )
        
    
    def to_string(self, name_method):
        
        # this probably needs some params for 'short string' and stuff later on
        
        if self.is_empty():
            
            return 'nothing'
            
        
        if self.includes_current() and self.includes_deleted():
            
            if self.current_service_keys == self.deleted_service_keys:
                
                prefix = 'current and deleted files of '
                
            else:
                
                prefix = 'a mix of current and deleted files of '
                
            
        elif self.includes_deleted():
            
            prefix = 'deleted files of '
            
        else:
            
            prefix = ''
            
        
        if self.current_service_keys == self.deleted_service_keys:
            
            service_keys_to_consider = self.current_service_keys
            
        else:
            
            service_keys_to_consider = self.current_service_keys.union( self.deleted_service_keys )
            
        
        if len( service_keys_to_consider ) <= 2:
            
            service_strings = sorted( ( name_method( service_key ) for service_key in service_keys_to_consider ) )
            
            service_string = ', '.join( service_strings )
            
        else:
            
            service_string = '{} services'.format( HydrusNumbers.to_human_int( len( service_keys_to_consider ) ) )
            
        
        return prefix + service_string
        
    
    def to_dict_for_api(self):
        
        return {
            'current_service_keys' : [ service_key.hex() for service_key in self.current_service_keys ],
            'deleted_service_keys' : [ service_key.hex() for service_key in self.deleted_service_keys ]
        }
        
    
    @staticmethod
    def static_create_all_current(current_service_keys) -> "LocationContext":
        
        return LocationContext( current_service_keys, [] )
        
    
    @staticmethod
    def static_create_simple(file_service_key) -> "LocationContext":
        
        return LocationContext( [ file_service_key ], [] )
        
    
HydrusSerialisable.SERIALISABLE_TYPES_TO_OBJECT_TYPES[ HydrusSerialisable.SERIALISABLE_TYPE_LOCATION_CONTEXT ] = LocationContext

