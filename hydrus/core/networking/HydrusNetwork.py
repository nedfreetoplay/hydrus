import collections
import collections.abc
import threading
import time

from hydrus.core import HydrusConstants as HC
from hydrus.core import HydrusData
from hydrus.core import HydrusExceptions
from hydrus.core import HydrusGlobals as HG
from hydrus.core import HydrusLists
from hydrus.core import HydrusNumbers
from hydrus.core import HydrusSerialisable
from hydrus.core import HydrusTags
from hydrus.core import HydrusTime
from hydrus.core.networking import HydrusNetworking

UPDATE_CHECKING_PERIOD = 240
MIN_UPDATE_PERIOD = 600
MAX_UPDATE_PERIOD = 100000 * 100 # three months or so jej

MIN_NULLIFICATION_PERIOD = 86400
MAX_NULLIFICATION_PERIOD = 86400 * 365 * 5

def generate_default_service_dictionary( service_type ):
    
    # don't store bytes key/value data here until ~version 537
    # the server kicks out a patched version 1 of serialisabledict, so it can't handle byte gubbins, lad
    
    dictionary = HydrusSerialisable.SerialisableDictionary()
    
    dictionary[ 'upnp_port' ] = None
    dictionary[ 'bandwidth_tracker' ] = HydrusNetworking.BandwidthTracker()
    
    if service_type in HC.RESTRICTED_SERVICES:
        
        dictionary[ 'bandwidth_rules' ] = HydrusNetworking.BandwidthRules()
        dictionary[ 'service_options' ] = HydrusSerialisable.SerialisableDictionary()
        
        dictionary[ 'service_options' ][ 'server_message' ] = 'Welcome to the server!'
        
        if service_type in HC.REPOSITORIES:
            
            update_period = 100000
            
            dictionary[ 'service_options' ][ 'update_period' ] = update_period
            dictionary[ 'service_options' ][ 'nullification_period' ] = 90 * 86400
            
            dictionary[ 'next_nullification_update_index' ] = 0
            
            metadata = Metadata()
            
            now = HydrusTime.get_now()
            
            update_hashes = []
            begin = 0
            end = now
            next_update_due = now + update_period
            
            metadata.append_update( update_hashes, begin, end, next_update_due )
            
            dictionary[ 'metadata' ] = metadata
            
            if service_type == HC.FILE_REPOSITORY:
                
                dictionary[ 'log_uploader_ips' ] = False
                dictionary[ 'max_storage' ] = None
                
            
            if service_type == HC.TAG_REPOSITORY:
                
                dictionary[ 'service_options' ][ 'tag_filter' ] = HydrusTags.TagFilter()
                
            
        
        if service_type == HC.SERVER_ADMIN:
            
            dictionary[ 'server_bandwidth_tracker' ] = HydrusNetworking.BandwidthTracker()
            dictionary[ 'server_bandwidth_rules' ] = HydrusNetworking.BandwidthRules()
            
        
    
    return dictionary
    
def generate_service( service_key, service_type, name, port, dictionary = None ):
    
    if dictionary is None:
        
        dictionary = generate_default_service_dictionary( service_type )
        
    
    if service_type == HC.SERVER_ADMIN:
        
        cl = ServerServiceAdmin
        
    elif service_type == HC.TAG_REPOSITORY:
        
        cl = ServerServiceRepositoryTag
        
    elif service_type == HC.FILE_REPOSITORY:
        
        cl = ServerServiceRepositoryFile
        
    
    return cl( service_key, service_type, name, port, dictionary )
    
def generate_service_from_serialisable_tuple( serialisable_info ):
    
    ( service_key_encoded, service_type, name, port, dictionary_string ) = serialisable_info
    
    try:
        
        service_key = bytes.fromhex( service_key_encoded )
        
    except TypeError:
        
        raise HydrusExceptions.BadRequestException( 'Could not decode that service key!' )
        
    
    dictionary = HydrusSerialisable.create_from_string( dictionary_string )
    
    return generate_service( service_key, service_type, name, port, dictionary )
    
def get_possible_permissions( service_type ):
    
    permissions = []
    
    permissions.append( ( HC.CONTENT_TYPE_ACCOUNTS, [ None, HC.PERMISSION_ACTION_CREATE, HC.PERMISSION_ACTION_MODERATE ] ) )
    permissions.append( ( HC.CONTENT_TYPE_ACCOUNT_TYPES, [ None, HC.PERMISSION_ACTION_MODERATE ] ) )
    permissions.append( ( HC.CONTENT_TYPE_OPTIONS, [ None, HC.PERMISSION_ACTION_MODERATE ] ) )
    
    if service_type == HC.FILE_REPOSITORY:
        
        permissions.append( ( HC.CONTENT_TYPE_FILES, [ None, HC.PERMISSION_ACTION_PETITION, HC.PERMISSION_ACTION_CREATE, HC.PERMISSION_ACTION_MODERATE ] ) )
        
    elif service_type == HC.TAG_REPOSITORY:
        
        permissions.append( ( HC.CONTENT_TYPE_MAPPINGS, [ None, HC.PERMISSION_ACTION_PETITION, HC.PERMISSION_ACTION_CREATE, HC.PERMISSION_ACTION_MODERATE ] ) )
        permissions.append( ( HC.CONTENT_TYPE_TAG_PARENTS, [ None, HC.PERMISSION_ACTION_PETITION, HC.PERMISSION_ACTION_MODERATE ] ) )
        permissions.append( ( HC.CONTENT_TYPE_TAG_SIBLINGS, [ None, HC.PERMISSION_ACTION_PETITION, HC.PERMISSION_ACTION_MODERATE ] ) )
        
    elif service_type == HC.SERVER_ADMIN:
        
        permissions.append( ( HC.CONTENT_TYPE_SERVICES, [ None, HC.PERMISSION_ACTION_MODERATE ] ) )
        
    
    return permissions
    
class Account( object ):
    
    def __init__( self, account_key: bytes, account_type: "AccountType", created: int, expires: int | None ):
        
        super().__init__()
        
        self._lock = threading.Lock()
        
        self._account_key = account_key
        self._account_type = account_type
        self._created = created
        self._expires = expires
        
        self._message = ''
        self._message_created = 0
        
        self._banned_info = None
        self._bandwidth_tracker = HydrusNetworking.BandwidthTracker()
        
        self._dirty = False
        
    
    def __repr__( self ):
        
        return 'Account: ' + self._account_type.get_title()
        
    
    def __str__( self ):
        
        return self.__repr__()
        
    
    def _check_banned( self ):
        
        if self._is_banned():
            
            raise HydrusExceptions.InsufficientCredentialsException( 'This account is banned: ' + self._get_banned_string() )
            
        
    
    def _check_expired( self ):
        
        if self._is_expired():
            
            raise HydrusExceptions.InsufficientCredentialsException( 'This account is expired: ' + self._get_expires_string() )
            
        
    
    def _check_functional( self ):
        
        if self._created == 0:
            
            raise HydrusExceptions.ConflictException( 'account is unsynced' )
            
        
        if self._is_admin():
            
            # admins can do anything
            return
            
        
        self._check_banned()
        
        self._check_expired()
        
        if not self._account_type.bandwidth_ok( self._bandwidth_tracker ):
            
            raise HydrusExceptions.BandwidthException( 'account has exceeded bandwidth' )
            
        
    
    def _get_banned_string( self ):
        
        if self._banned_info is None:
            
            return 'not banned'
            
        else:
            
            ( reason, created, expires ) = self._banned_info
            
            return 'banned ' + HydrusTime.timestamp_to_pretty_timedelta( created ) + ', ' + HydrusTime.timestamp_to_pretty_expires( expires ) + ' because: ' + reason
            
        
    
    def _get_expires_string( self ):
        
        return HydrusTime.timestamp_to_pretty_expires( self._expires )
        
    
    def _is_admin( self ):
        
        return self._account_type.has_permission( HC.CONTENT_TYPE_SERVICES, HC.PERMISSION_ACTION_MODERATE )
        
    
    def _is_banned( self ):
        
        if self._banned_info is None:
            
            return False
            
        else:
            
            ( reason, created, expires ) = self._banned_info
            
            if expires is None:
                
                return True
                
            else:
                
                if HydrusTime.time_has_passed( expires ):
                    
                    self._banned_info = None
                    
                    return False
                    
                else:
                    
                    return True
                    
                
            
        
    
    def _is_expired( self ):
        
        if self._expires is None:
            
            return False
            
        else:
            
            return HydrusTime.time_has_passed( self._expires )
            
        
    
    def _set_dirty( self ):
        
        self._dirty = True
        
    
    def ban( self, reason, created, expires ):
        
        with self._lock:
            
            self._banned_info = ( reason, created, expires )
            
            self._set_dirty()
            
        
    
    def check_at_least_one_permission( self, content_types_and_actions ):
        
        with self._lock:
            
            if True not in ( self._account_type.has_permission( content_type, action ) for ( content_type, action ) in content_types_and_actions ):
                
                raise HydrusExceptions.InsufficientCredentialsException( 'You do not have permission to do that.' )
                
            
        
    
    def check_functional( self ):
        
        with self._lock:
        
            self._check_functional()
            
        
    
    def check_permission( self, content_type, action ):
        
        with self._lock:
            
            if self._is_admin():
                
                return
                
            
            self._check_banned()
            
            self._check_expired()
            
            if not self._account_type.has_permission( content_type, action ):
                
                raise HydrusExceptions.InsufficientCredentialsException( 'You do not have permission to do that.' )
                
            
        
    
    def get_account_key( self ):
        
        with self._lock:
            
            return self._account_key
            
        
    
    def get_account_type( self ):
        
        with self._lock:
            
            return self._account_type
            
        
    
    def get_bandwidth_current_month_summary( self ):
        
        with self._lock:
            
            return self._bandwidth_tracker.GetCurrentMonthSummary()
            
        
    
    def get_bandwidth_strings_and_gauge_tuples( self ):
        
        with self._lock:
            
            return self._account_type.get_bandwidth_strings_and_gauge_tuples( self._bandwidth_tracker )
            
        
    
    def get_bandwidth_tracker( self ):
        
        with self._lock:
            
            return self._bandwidth_tracker
            
        
    
    def get_banned_info( self ):
        
        with self._lock:
            
            return self._banned_info
            
        
    
    def get_created( self ):
        
        with self._lock:
            
            return self._created
            
        
    
    def get_expires( self ):
        
        with self._lock:
            
            return self._expires
            
        
    
    def get_expires_string( self ):
        
        with self._lock:
            
            if self._is_banned():
                
                return self._get_banned_string()
                
            else:
                
                return self._get_expires_string()
                
            
        
    
    def get_message_and_timestamp( self ):
        
        with self._lock:
            
            return ( self._message, self._message_created )
            
        
    
    def get_single_line_title( self ):
        
        with self._lock:
            
            text = self._account_key.hex()
            
            text = '{}: {}'.format( self._account_type.get_title(), text )
            
            if self._is_expired():
                
                text = 'Expired: {}'.format( text )
                
            
            if self._is_banned():
                
                text = 'Banned: {}'.format( text )
                
            
            if self._account_type.is_null_account():
                
                text = 'THIS IS NULL ACCOUNT: {}'.format( text )
                
            
            return text
            
        
    
    def get_status_info( self ) -> tuple[ bool, str ]:
        
        with self._lock:
            
            try:
                
                self._check_functional()
                
                return ( True, 'account is functional' )
                
            except Exception as e:
                
                return ( False, str( e ) )
                
            
        
    
    def has_permission( self, content_type, action ):
        
        with self._lock:
            
            if self._is_admin():
                
                return True
                
            
            if self._is_banned() or self._is_expired():
                
                return False
                
            
            return self._account_type.has_permission( content_type, action )
            
        
    
    def is_banned( self ):
        
        with self._lock:
            
            return self._is_banned()
            
        
    
    def is_dirty( self ):
        
        with self._lock:
            
            return self._dirty
            
        
    
    def is_expired( self ):
        
        with self._lock:
            
            return self._is_expired()
            
        
    
    def is_functional( self ):
        
        with self._lock:
            
            try:
                
                self._check_functional()
                
                return True
                
            except:
                
                return False
                
            
        
    
    def is_null_account( self ):
        
        with self._lock:
            
            return self._account_type.is_null_account()
            
        
    
    def is_unknown( self ):
        
        with self._lock:
            
            return self._created == 0
            
        
    
    def report_data_used( self, num_bytes ):
        
        with self._lock:
            
            self._bandwidth_tracker.ReportDataUsed( num_bytes )
            
            self._set_dirty()
            
        
    
    def report_request_used( self ):
        
        with self._lock:
            
            self._bandwidth_tracker.ReportRequestUsed()
            
            self._set_dirty()
            
        
    
    def set_bandwidth_tracker( self, bandwidth_tracker: HydrusNetworking.BandwidthTracker ):
        
        with self._lock:
            
            self._bandwidth_tracker = bandwidth_tracker
            
            self._set_dirty()
            
        
    
    def set_clean( self ):
        
        with self._lock:
            
            self._dirty = False
            
        
    
    def set_expires( self, expires: int | None ):
        
        with self._lock:
            
            self._expires = expires
            
            self._set_dirty()
            
        
    
    def set_message( self, message, created ):
        
        with self._lock:
            
            self._message = message
            self._message_created = created
            
            self._set_dirty()
            
        
    
    def to_string( self ):
        
        if self.is_null_account():
            
            return 'This is the NULL ACCOUNT. It takes possession of old content to anonymise it. It cannot be modified.'
            
        
        with self._lock:
            
            return self._account_type.get_title() + ' -- created ' + HydrusTime.timestamp_to_pretty_timedelta( self._created )
            
        
    
    def unban( self ):
        
        with self._lock:
            
            self._banned_info = None
            
            self._set_dirty()
            
        
    
    @staticmethod
    def generate_account_from_serialisable_tuple( serialisable_info ):
        
        ( account_key_encoded, serialisable_account_type, created, expires, dictionary_string ) = serialisable_info
        
        account_key = bytes.fromhex( account_key_encoded )
        
        if isinstance( serialisable_account_type, list ) and isinstance( serialisable_account_type[0], str ):
            
            # this is a legacy account
            
            ( encoded_account_type_key, title, account_type_dictionary_string ) = serialisable_account_type
            
            account_type_key = bytes.fromhex( encoded_account_type_key )
            
            from hydrus.core.networking import HydrusNetworkLegacy
            
            account_type = HydrusNetworkLegacy.ConvertToNewAccountType( account_type_key, title, account_type_dictionary_string )
            
        else:
            
            account_type = HydrusSerialisable.create_from_serialisable_tuple( serialisable_account_type )
            
        
        dictionary = HydrusSerialisable.create_from_string( dictionary_string )
        
        return Account.generate_account_from_tuple( ( account_key, account_type, created, expires, dictionary ) )
        
    
    @staticmethod
    def generate_account_from_tuple( serialisable_info ):
        
        ( account_key, account_type, created, expires, dictionary ) = serialisable_info
        
        if 'message' not in dictionary:
            
            dictionary[ 'message' ] = ''
            dictionary[ 'message_created' ] = 0
            
        
        banned_info = dictionary[ 'banned_info' ]
        bandwidth_tracker = dictionary[ 'bandwidth_tracker' ]
        
        account = Account( account_key, account_type, created, expires )
        
        account.set_bandwidth_tracker( bandwidth_tracker )
        
        if banned_info is not None:
            
            ( reason, created, expires ) = banned_info
            
            account.ban( reason, created, expires )
            
        
        message = dictionary[ 'message' ]
        message_created = dictionary[ 'message_created' ]
        
        account.set_message( message, message_created )
        
        account.set_clean()
        
        return account
        
    
    @staticmethod
    def generate_serialisable_tuple_from_account( account ):
        
        ( account_key, account_type, created, expires, dictionary ) = Account.generate_tuple_from_account( account )
        
        account_key_encoded = account_key.hex()
        
        serialisable_account_type = account_type.get_serialisable_tuple()
        
        dictionary_string = dictionary.dump_to_string()
        
        return ( account_key_encoded, serialisable_account_type, created, expires, dictionary_string )
        
    
    @staticmethod
    def generate_tuple_from_account( account: "Account" ):
        
        account_key = account.get_account_key()
        account_type = account.get_account_type()
        created = account.get_created()
        expires = account.get_expires()
        
        banned_info = account.get_banned_info()
        bandwidth_tracker = account.get_bandwidth_tracker()
        
        ( message, message_created ) = account.get_message_and_timestamp()
        
        dictionary = HydrusSerialisable.SerialisableDictionary()
        
        dictionary[ 'banned_info' ] = banned_info
        
        dictionary[ 'bandwidth_tracker' ] = bandwidth_tracker
        
        dictionary[ 'message' ] = message
        dictionary[ 'message_created' ] = message_created
        
        dictionary = dictionary.duplicate()
        
        return ( account_key, account_type, created, expires, dictionary )
        
    
    @staticmethod
    def generate_unknown_account( account_key = b'' ):
        
        account_type = AccountType.generate_unknown_account_type()
        created = 0
        expires = None
        
        unknown_account = Account( account_key, account_type, created, expires )
        
        return unknown_account
        
    
class AccountIdentifier( HydrusSerialisable.SerialisableBase ):
    
    SERIALISABLE_TYPE = HydrusSerialisable.SERIALISABLE_TYPE_ACCOUNT_IDENTIFIER
    SERIALISABLE_NAME = 'Account Identifier'
    SERIALISABLE_VERSION = 1
    
    TYPE_ACCOUNT_KEY = 1
    TYPE_CONTENT = 2
    
    def __init__( self, account_key = None, content = None ):
        
        super().__init__()
        
        if account_key is not None:
            
            self._type = self.TYPE_ACCOUNT_KEY
            self._data = account_key
            
        elif content is not None:
            
            self._type = self.TYPE_CONTENT
            self._data = content
            
        
    
    def __eq__( self, other ):
        
        if isinstance( other, AccountIdentifier ):
            
            return self.__hash__() == other.__hash__()
            
        
        return NotImplemented
        
    
    def __hash__( self ): return ( self._type, self._data ).__hash__()
    
    def __repr__( self ): return 'Account Identifier: ' + str( ( self._type, self._data ) )
    
    def _get_serialisable_info( self ):
        
        if self._type == self.TYPE_ACCOUNT_KEY:
            
            serialisable_data = self._data.hex()
            
        elif self._type == self.TYPE_CONTENT:
            
            serialisable_data = self._data.GetSerialisableTuple()
            
        
        return ( self._type, serialisable_data )
        
    
    def _initialise_from_serialisable_info( self, serialisable_info ):
        
        ( self._type, serialisable_data ) = serialisable_info
        
        if self._type == self.TYPE_ACCOUNT_KEY:
            
            self._data = bytes.fromhex( serialisable_data )
            
        elif self._type == self.TYPE_CONTENT:
            
            self._data = HydrusSerialisable.create_from_serialisable_tuple( serialisable_data )
            
        
    
    def get_account_key( self ) -> bytes:
        
        if not self.has_account_key():
            
            raise Exception( 'This Account Identifier does not have an account id!' )
            
        
        return self._data
        
    
    def get_content( self ) -> "Content":
        
        if not self.has_content():
            
            raise Exception( 'This Account Identifier does not have content!' )
            
        
        return self._data
        
    
    def get_data( self ):
        
        return self._data
        
    
    def has_account_key( self ):
        
        return self._type == self.TYPE_ACCOUNT_KEY
        
    
    def has_content( self ):
        
        return self._type == self.TYPE_CONTENT
        
    
HydrusSerialisable.SERIALISABLE_TYPES_TO_OBJECT_TYPES[ HydrusSerialisable.SERIALISABLE_TYPE_ACCOUNT_IDENTIFIER ] = AccountIdentifier

class AccountType( HydrusSerialisable.SerialisableBase ):
    
    SERIALISABLE_TYPE = HydrusSerialisable.SERIALISABLE_TYPE_ACCOUNT_TYPE
    SERIALISABLE_NAME = 'Account Type'
    SERIALISABLE_VERSION = 2
    
    def __init__(
        self,
        account_type_key = None,
        title = None,
        permissions = None,
        bandwidth_rules = None,
        auto_creation_velocity = None,
        auto_creation_history = None
        ):
        
        super().__init__()
        
        if account_type_key is None:
            
            account_type_key = HydrusData.generate_key()
            
        
        if title is None:
            
            title = 'standard user'
            
        
        if permissions is None:
            
            permissions = {}
            
        
        if bandwidth_rules is None:
            
            bandwidth_rules = HydrusNetworking.BandwidthRules()
            
        
        if auto_creation_velocity is None:
            
            auto_creation_velocity = ( 0, 86400 )
            
        
        if auto_creation_history is None:
            
            auto_creation_history = HydrusNetworking.BandwidthTracker()
            
        
        self._account_type_key = account_type_key
        self._title = title
        self._permissions = permissions
        self._bandwidth_rules = bandwidth_rules
        self._auto_creation_velocity = auto_creation_velocity
        self._auto_creation_history = auto_creation_history
        
    
    def __repr__( self ):
        
        return 'AccountType: ' + self._title
        
    
    def __str__( self ):
        
        return self.__repr__()
        
    
    def _get_serialisable_info( self ):
        
        serialisable_account_type_key = self._account_type_key.hex()
        serialisable_permissions = list( self._permissions.items() )
        serialisable_bandwidth_rules = self._bandwidth_rules.get_serialisable_tuple()
        serialisable_auto_creation_history = self._auto_creation_history.get_serialisable_tuple()
        
        return ( serialisable_account_type_key, self._title, serialisable_permissions, serialisable_bandwidth_rules, self._auto_creation_velocity, serialisable_auto_creation_history )
        
    
    def _initialise_from_serialisable_info( self, serialisable_info ):
        
        ( serialisable_account_type_key, self._title, serialisable_permissions, serialisable_bandwidth_rules, self._auto_creation_velocity, serialisable_auto_creation_history ) = serialisable_info
        
        self._account_type_key = bytes.fromhex( serialisable_account_type_key )
        self._permissions = dict( serialisable_permissions )
        self._bandwidth_rules = HydrusSerialisable.create_from_serialisable_tuple( serialisable_bandwidth_rules )
        self._auto_creation_history = HydrusSerialisable.create_from_serialisable_tuple( serialisable_auto_creation_history )
        
    
    def _update_serialisable_info( self, version, old_serialisable_info ):
        
        if version == 1:
            
            ( serialisable_account_type_key, title, serialisable_permissions, serialisable_bandwidth_rules, auto_creation_velocity, serialisable_auto_creation_history ) = old_serialisable_info
            
            permissions = dict( serialisable_permissions )
            
            # admins can do options
            if HC.CONTENT_TYPE_ACCOUNT_TYPES in permissions and permissions[ HC.CONTENT_TYPE_ACCOUNT_TYPES ] == HC.PERMISSION_ACTION_MODERATE:
                
                permissions[ HC.CONTENT_TYPE_OPTIONS ] = HC.PERMISSION_ACTION_MODERATE
                
            
            serialisable_permissions = list( permissions.items() )
            
            new_serialisable_info = ( serialisable_account_type_key, title, serialisable_permissions, serialisable_bandwidth_rules, auto_creation_velocity, serialisable_auto_creation_history )
            
            return ( 2, new_serialisable_info )
            
        
    
    def bandwidth_ok( self, bandwidth_tracker ):
        
        return self._bandwidth_rules.CanStartRequest( bandwidth_tracker )
        
    
    def can_auto_create_account_now( self ):
        
        if not self.supports_auto_create_account():
            
            return False
            
        
        ( num_accounts_per_time_delta, time_delta ) = self._auto_creation_velocity
        
        num_created = self._auto_creation_history.GetUsage( HC.BANDWIDTH_TYPE_DATA, time_delta )
        
        return num_created < num_accounts_per_time_delta
        
    
    def get_auto_create_account_history( self ) -> HydrusNetworking.BandwidthTracker:
        
        return self._auto_creation_history
        
    
    def get_auto_create_account_velocity( self ):
        
        return self._auto_creation_velocity
        
    
    def get_bandwidth_rules( self ):
        
        return self._bandwidth_rules
        
    
    def get_bandwidth_strings_and_gauge_tuples( self, bandwidth_tracker ):
        
        return self._bandwidth_rules.GetBandwidthStringsAndGaugeTuples( bandwidth_tracker )
        
    
    def get_account_type_key( self ):
        
        return self._account_type_key
        
    
    def get_permissions( self ):
        
        return { k : v for ( k, v ) in self._permissions.items() if k != 'null' }
        
    
    def get_permission_strings( self ):
        
        if self.is_null_account():
            
            return [ 'is null account, cannot do anything' ]
            
        
        s = []
        
        for ( content_type, action ) in self.get_permissions().items():
            
            s.append( HC.permission_pair_string_lookup[ ( content_type, action ) ] )
            
        
        return s
        
    
    def get_title( self ):
        
        return self._title
        
    
    def has_permission( self, content_type, permission ):
        
        if self.is_null_account():
            
            return False
            
        
        if content_type not in self._permissions:
            
            return False
            
        
        my_permission = self._permissions[ content_type ]
        
        if permission == HC.PERMISSION_ACTION_MODERATE:
            
            return my_permission == HC.PERMISSION_ACTION_MODERATE
            
        elif permission == HC.PERMISSION_ACTION_CREATE:
            
            return my_permission in ( HC.PERMISSION_ACTION_CREATE, HC.PERMISSION_ACTION_MODERATE )
            
        elif permission == HC.PERMISSION_ACTION_PETITION:
            
            return my_permission in ( HC.PERMISSION_ACTION_PETITION, HC.PERMISSION_ACTION_CREATE, HC.PERMISSION_ACTION_MODERATE )
            
        
        return False
        
    
    def is_null_account( self ):
        
        # I had to tuck this in permissions dict because this was not during a network version update and I couldn't update the serialised object. bleargh
        # ideally though, we move this sometime to a self._is_null_account boolean
        
        return 'null' in self._permissions
        
    
    def report_auto_create_account( self ):
        
        self._auto_creation_history.ReportRequestUsed()
        
    
    def set_to_null_account( self ):
        
        # I had to tuck this in permissions dict because this was not during a network version update and I couldn't update the serialised object. bleargh
        # ideally though, we move this sometime to a self._is_null_account boolean
        
        self._permissions[ 'null' ] = True
        
    
    def supports_auto_create_account( self ):
        
        if self.is_null_account():
            
            return False
            
        
        ( num_accounts_per_time_delta, time_delta ) = self._auto_creation_velocity
        
        return num_accounts_per_time_delta > 0
        
    
    @staticmethod
    def generate_admin_account_type( service_type ):
        
        bandwidth_rules = HydrusNetworking.BandwidthRules()
        
        permissions = {}
        
        permissions[ HC.CONTENT_TYPE_ACCOUNTS ] = HC.PERMISSION_ACTION_MODERATE
        permissions[ HC.CONTENT_TYPE_ACCOUNT_TYPES ] = HC.PERMISSION_ACTION_MODERATE
        permissions[ HC.CONTENT_TYPE_OPTIONS ] = HC.PERMISSION_ACTION_MODERATE
        
        if service_type in HC.REPOSITORIES:
            
            for content_type in HC.SERVICE_TYPES_TO_CONTENT_TYPES[ service_type ]:
                
                permissions[ content_type ] = HC.PERMISSION_ACTION_MODERATE
                
            
        elif service_type == HC.SERVER_ADMIN:
            
            permissions[ HC.CONTENT_TYPE_SERVICES ] = HC.PERMISSION_ACTION_MODERATE
            
        else:
            
            raise NotImplementedError( 'Do not have a default admin account type set up for this service yet!' )
            
        
        account_type = AccountType.generate_new_account_type( 'administrator', permissions, bandwidth_rules )
        
        return account_type
        
    
    @staticmethod
    def generate_new_account_type( title, permissions, bandwidth_rules ):
        
        account_type_key = HydrusData.generate_key()
        
        return AccountType( account_type_key = account_type_key, title = title, permissions = permissions, bandwidth_rules = bandwidth_rules )
        
    
    @staticmethod
    def generate_null_account_type():
        
        account_type_key = HydrusData.generate_key()
        
        title = 'null account'
        permissions = {}
        bandwidth_rules = HydrusNetworking.BandwidthRules()
        
        at = AccountType( account_type_key = account_type_key, title = title, permissions = permissions, bandwidth_rules = bandwidth_rules )
        
        at.set_to_null_account()
        
        return at
        
    
    @staticmethod
    def generate_unknown_account_type():
        
        title = 'unknown account'
        
        permissions = {}
        
        bandwidth_rules = HydrusNetworking.BandwidthRules()
        bandwidth_rules.AddRule( HC.BANDWIDTH_TYPE_REQUESTS, None, 0 )
        
        unknown_account_type = AccountType.generate_new_account_type( title, permissions, bandwidth_rules )
        
        return unknown_account_type
        
    
HydrusSerialisable.SERIALISABLE_TYPES_TO_OBJECT_TYPES[ HydrusSerialisable.SERIALISABLE_TYPE_ACCOUNT_TYPE ] = AccountType

class ClientToServerUpdate( HydrusSerialisable.SerialisableBase ):
    
    SERIALISABLE_TYPE = HydrusSerialisable.SERIALISABLE_TYPE_CLIENT_TO_SERVER_UPDATE
    SERIALISABLE_NAME = 'Client To Server Update'
    SERIALISABLE_VERSION = 1
    
    def __init__( self ):
        
        super().__init__()
        
        self._actions_to_contents_and_reasons = collections.defaultdict( list )
        
    
    def _get_serialisable_info( self ):
        
        serialisable_info = []
        
        for ( action, contents_and_reasons ) in list(self._actions_to_contents_and_reasons.items()):
            
            serialisable_contents_and_reasons = [ ( content.GetSerialisableTuple(), reason ) for ( content, reason ) in contents_and_reasons ]
            
            serialisable_info.append( ( action, serialisable_contents_and_reasons ) )
            
        
        return serialisable_info
        
    
    def _initialise_from_serialisable_info( self, serialisable_info ):
        
        for ( action, serialisable_contents_and_reasons ) in serialisable_info:
            
            contents_and_reasons = [ ( HydrusSerialisable.create_from_serialisable_tuple( serialisable_content ), reason ) for ( serialisable_content, reason ) in serialisable_contents_and_reasons ]
            
            self._actions_to_contents_and_reasons[ action ] = contents_and_reasons
            
        
    
    def add_content( self, action, content, reason = None ):
        
        if reason is None:
            
            reason = ''
            
        
        self._actions_to_contents_and_reasons[ action ].append( ( content, reason ) )
        
    
    def apply_tag_filter_to_pending_mappings( self, tag_filter: HydrusTags.TagFilter ):
        
        if HC.CONTENT_UPDATE_PEND in self._actions_to_contents_and_reasons:
            
            contents_and_reasons = self._actions_to_contents_and_reasons[ HC.CONTENT_UPDATE_PEND ]
            
            new_contents_and_reasons = []
            
            for ( content, reason ) in contents_and_reasons:
                
                if content.GetContentType() == HC.CONTENT_TYPE_MAPPINGS:
                    
                    ( tag, hashes ) = content.GetContentData()
                    
                    if not tag_filter.tag_ok( tag ):
                        
                        continue
                        
                    
                
                new_contents_and_reasons.append( ( content, reason ) )
                
            
            self._actions_to_contents_and_reasons[ HC.CONTENT_UPDATE_PEND ] = new_contents_and_reasons
            
        
    
    def get_content_data_iterator( self, content_type, action ):
        
        contents_and_reasons = self._actions_to_contents_and_reasons[ action ]
        
        for ( content, reason ) in contents_and_reasons:
            
            if content.GetContentType() == content_type:
                
                yield ( content.GetContentData(), reason )
                
            
        
    
    def get_hashes( self ):
        
        hashes = set()
        
        for contents_and_reasons in self._actions_to_contents_and_reasons.values():
            
            for ( content, reason ) in contents_and_reasons:
                
                hashes.update( content.GetHashes() )
                
            
        
        return hashes
        
    
    def has_content( self ):
        
        return len( self._actions_to_contents_and_reasons ) > 0
        
    
    def iterate_all_actions_and_contents_and_reasons( self ):
        
        for ( action, contents_and_reasons ) in self._actions_to_contents_and_reasons.items():
            
            for ( content, reason ) in contents_and_reasons:
                
                yield ( action, content, reason )
                
            
        
    

HydrusSerialisable.SERIALISABLE_TYPES_TO_OBJECT_TYPES[ HydrusSerialisable.SERIALISABLE_TYPE_CLIENT_TO_SERVER_UPDATE ] = ClientToServerUpdate

class Content( HydrusSerialisable.SerialisableBase ):
    
    SERIALISABLE_TYPE = HydrusSerialisable.SERIALISABLE_TYPE_CONTENT
    SERIALISABLE_NAME = 'Content'
    SERIALISABLE_VERSION = 1
    
    def __init__( self, content_type = None, content_data = None ):
        
        super().__init__()
        
        self._content_type = content_type
        self._content_data = content_data
        
    
    def __eq__( self, other ):
        
        if isinstance( other, Content ):
            
            return self.__hash__() == other.__hash__()
            
        
        return NotImplemented
        
    
    def __hash__( self ): return ( self._content_type, self._content_data ).__hash__()
    
    def __repr__( self ): return 'Content: ' + self.to_string()
    
    def _get_serialisable_info( self ):
        
        def EncodeHashes( hs ):
            
            return [ h.hex() for h in hs ]
            
        
        if self._content_type == HC.CONTENT_TYPE_FILES:
            
            hashes = self._content_data
            
            serialisable_content = EncodeHashes( hashes )
            
        elif self._content_type == HC.CONTENT_TYPE_MAPPING:
            
            ( tag, hash ) = self._content_data
            
            serialisable_content = ( tag, hash.hex() )
            
        elif self._content_type == HC.CONTENT_TYPE_MAPPINGS:
            
            ( tag, hashes ) = self._content_data
            
            serialisable_content = ( tag, EncodeHashes( hashes ) )
            
        elif self._content_type in ( HC.CONTENT_TYPE_TAG_PARENTS, HC.CONTENT_TYPE_TAG_SIBLINGS ):
            
            ( old_tag, new_tag ) = self._content_data
            
            serialisable_content = ( old_tag, new_tag )
            
        
        return ( self._content_type, serialisable_content )
        
    
    def _initialise_from_serialisable_info( self, serialisable_info ):
        
        def DecodeHashes( hs ):
            
            return [ bytes.fromhex( h ) for h in hs ]
            
        
        ( self._content_type, serialisable_content ) = serialisable_info
        
        if self._content_type == HC.CONTENT_TYPE_FILES:
            
            serialisable_hashes = serialisable_content
            
            self._content_data = DecodeHashes( serialisable_hashes )
            
        elif self._content_type == HC.CONTENT_TYPE_MAPPING:
            
            ( tag, serialisable_hash ) = serialisable_content
            
            self._content_data = ( tag, bytes.fromhex( serialisable_hash ) )
            
        elif self._content_type == HC.CONTENT_TYPE_MAPPINGS:
            
            ( tag, serialisable_hashes ) = serialisable_content
            
            self._content_data = ( tag, DecodeHashes( serialisable_hashes ) )
            
        elif self._content_type in ( HC.CONTENT_TYPE_TAG_PARENTS, HC.CONTENT_TYPE_TAG_SIBLINGS ):
            
            ( old_tag, new_tag ) = serialisable_content
            
            self._content_data = ( old_tag, new_tag )
            
        
    
    def get_content_data( self ):
        
        return self._content_data
        
    
    def get_content_type( self ):
        
        return self._content_type
        
    
    def get_hashes( self ):
        
        if self._content_type == HC.CONTENT_TYPE_FILES:
            
            hashes = self._content_data
            
        elif self._content_type == HC.CONTENT_TYPE_MAPPING:
            
            ( tag, hash ) = self._content_data
            
            return [ hash ]
            
        elif self._content_type == HC.CONTENT_TYPE_MAPPINGS:
            
            ( tag, hashes ) = self._content_data
            
        else:
            
            hashes = []
            
        
        return hashes
        
    
    def get_actual_weight( self ):
        
        if self._content_type in ( HC.CONTENT_TYPE_FILES, HC.CONTENT_TYPE_MAPPINGS ):
            
            return len( self.get_hashes() )
            
        else:
            
            return 1
            
        
    
    def get_virtual_weight( self ):
        
        if self._content_type in ( HC.CONTENT_TYPE_FILES, HC.CONTENT_TYPE_MAPPINGS ):
            
            return len( self.get_hashes() )
            
        elif self._content_type == HC.CONTENT_TYPE_TAG_PARENTS:
            
            return 5000
            
        elif self._content_type == HC.CONTENT_TYPE_TAG_SIBLINGS:
            
            return 5
            
        elif self._content_type == HC.CONTENT_TYPE_MAPPING:
            
            return 1
            
        
    
    def has_hashes( self ):
        
        return self._content_type in ( HC.CONTENT_TYPE_FILES, HC.CONTENT_TYPE_MAPPING, HC.CONTENT_TYPE_MAPPINGS )
        
    
    def iterate_uploadable_chunks( self ):
        
        if self._content_type == HC.CONTENT_TYPE_FILES:
            
            hashes = self._content_data
            
            for chunk_of_hashes in HydrusLists.split_list_into_chunks( hashes, 100 ):
                
                content = Content( content_type = self._content_type, content_data = chunk_of_hashes )
                
                yield content
                
            
        elif self._content_type == HC.CONTENT_TYPE_MAPPINGS:
            
            ( tag, hashes ) = self._content_data
            
            for chunk_of_hashes in HydrusLists.split_list_into_chunks( hashes, 500 ):
                
                content = Content( content_type = self._content_type, content_data = ( tag, chunk_of_hashes ) )
                
                yield content
                
            
        else:
            
            yield self
            
        
    
    def to_string( self ):
        
        if self._content_type == HC.CONTENT_TYPE_FILES:
            
            hashes = self._content_data
            
            text = 'FILES: ' + HydrusNumbers.to_human_int( len( hashes ) ) + ' files'
            
        elif self._content_type == HC.CONTENT_TYPE_MAPPING:
            
            ( tag, hash ) = self._content_data
            
            text = 'MAPPING: ' + tag + ' for ' + hash.hex()
            
        elif self._content_type == HC.CONTENT_TYPE_MAPPINGS:
            
            ( tag, hashes ) = self._content_data
            
            text = 'MAPPINGS: ' + tag + ' for ' + HydrusNumbers.to_human_int( len( hashes ) ) + ' files'
            
        elif self._content_type == HC.CONTENT_TYPE_TAG_PARENTS:
            
            ( child, parent ) = self._content_data
            
            text = 'PARENT: ' '"' + child + '" -> "' + parent + '"'
            
        elif self._content_type == HC.CONTENT_TYPE_TAG_SIBLINGS:
            
            ( old_tag, new_tag ) = self._content_data
            
            text = 'SIBLING: ' + '"' + old_tag + '" -> "' + new_tag + '"'
            
        
        return text
        
    
HydrusSerialisable.SERIALISABLE_TYPES_TO_OBJECT_TYPES[ HydrusSerialisable.SERIALISABLE_TYPE_CONTENT ] = Content

class ContentUpdate( HydrusSerialisable.SerialisableBase ):
    
    SERIALISABLE_TYPE = HydrusSerialisable.SERIALISABLE_TYPE_CONTENT_UPDATE
    SERIALISABLE_NAME = 'Content Update'
    SERIALISABLE_VERSION = 1
    
    def __init__( self ):
        
        super().__init__()
        
        self._content_data = {}
        
    
    def _get_content( self, content_type, action ):
        
        if content_type in self._content_data:
            
            if action in self._content_data[ content_type ]:
                
                return self._content_data[ content_type ][ action ]
                
            
        
        return []
        
    
    def _get_serialisable_info( self ):
        
        serialisable_info = []
        
        for ( content_type, actions_to_datas ) in list(self._content_data.items()):
            
            serialisable_actions_to_datas = list(actions_to_datas.items())
            
            serialisable_info.append( ( content_type, serialisable_actions_to_datas ) )
            
        
        return serialisable_info
        
    
    def _initialise_from_serialisable_info( self, serialisable_info ):
        
        for ( content_type, serialisable_actions_to_datas ) in serialisable_info:
            
            actions_to_datas = dict( serialisable_actions_to_datas )
            
            self._content_data[ content_type ] = actions_to_datas
            
        
    
    def add_row( self, row ):
        
        ( content_type, action, data ) = row
        
        if content_type not in self._content_data:
            
            self._content_data[ content_type ] = {}
            
        
        if action not in self._content_data[ content_type ]:
            
            self._content_data[ content_type ][ action ] = []
            
        
        self._content_data[ content_type ][ action ].append( data )
        
    
    def get_deleted_files( self ):
        
        return self._get_content( HC.CONTENT_TYPE_FILES, HC.CONTENT_UPDATE_DELETE )
        
    
    def get_deleted_mappings( self ):
        
        return self._get_content( HC.CONTENT_TYPE_MAPPINGS, HC.CONTENT_UPDATE_DELETE )
        
    
    def get_deleted_tag_parents( self ):
        
        return self._get_content( HC.CONTENT_TYPE_TAG_PARENTS, HC.CONTENT_UPDATE_DELETE )
        
    
    def get_deleted_tag_siblings( self ):
        
        return self._get_content( HC.CONTENT_TYPE_TAG_SIBLINGS, HC.CONTENT_UPDATE_DELETE )
        
    
    def get_new_files( self ):
        
        return self._get_content( HC.CONTENT_TYPE_FILES, HC.CONTENT_UPDATE_ADD )
        
    
    def get_new_mappings( self ):
        
        return self._get_content( HC.CONTENT_TYPE_MAPPINGS, HC.CONTENT_UPDATE_ADD )
        
    
    def get_new_tag_parents( self ):
        
        return self._get_content( HC.CONTENT_TYPE_TAG_PARENTS, HC.CONTENT_UPDATE_ADD )
        
    
    def get_new_tag_siblings( self ):
        
        return self._get_content( HC.CONTENT_TYPE_TAG_SIBLINGS, HC.CONTENT_UPDATE_ADD )
        
    
    def get_num_rows( self, content_types_to_count = None ):
        
        num = 0
        
        for content_type in self._content_data:
            
            if content_types_to_count is not None and content_type not in content_types_to_count:
                
                continue
                
            
            for action in self._content_data[ content_type ]:
                
                data = self._content_data[ content_type ][ action ]
                
                if content_type == HC.CONTENT_TYPE_MAPPINGS:
                    
                    num_rows = sum( ( len( hash_ids ) for ( tag_id, hash_ids ) in data ) )
                    
                else:
                    
                    num_rows = len( data )
                    
                
                num += num_rows
                
            
        
        return num
        
    
HydrusSerialisable.SERIALISABLE_TYPES_TO_OBJECT_TYPES[ HydrusSerialisable.SERIALISABLE_TYPE_CONTENT_UPDATE ] = ContentUpdate

class Credentials( HydrusSerialisable.SerialisableBase ):
    
    SERIALISABLE_TYPE = HydrusSerialisable.SERIALISABLE_TYPE_CREDENTIALS
    SERIALISABLE_NAME = 'Credentials'
    SERIALISABLE_VERSION = 1
    
    def __init__( self, host = None, port = None, access_key = None ):
        
        super().__init__()
        
        self._host = host
        self._port = port
        self._access_key = access_key
        
    
    def __eq__( self, other ):
        
        if isinstance( other, Credentials ):
            
            return self.__hash__() == other.__hash__()
            
        
        return NotImplemented
        
    
    def __hash__( self ):
        
        return ( self._host, self._port, self._access_key ).__hash__()
        
    
    def __repr__( self ):
        
        if self._access_key is None:
            
            access_key_str = 'no access key'
            
        else:
            
            access_key_str = self._access_key.hex()
            
        
        return 'Credentials: ' + str( ( self._host, self._port, access_key_str ) )
        
    
    def _get_serialisable_info( self ):
        
        if self._access_key is None:
            
            serialisable_access_key = self._access_key
            
        else:
            
            serialisable_access_key = self._access_key.hex()
            
        
        return ( self._host, self._port, serialisable_access_key )
        
    
    def _initialise_from_serialisable_info( self, serialisable_info ):
        
        ( self._host, self._port, serialisable_access_key ) = serialisable_info
        
        if serialisable_access_key is None:
            
            self._access_key = serialisable_access_key
            
        else:
            
            self._access_key = bytes.fromhex( serialisable_access_key )
            
        
    
    def get_access_key( self ):
        
        return self._access_key
        
    
    def get_address( self ):
        
        return ( self._host, self._port )
        
    
    def get_connection_string( self ):
        
        connection_string = ''
        
        if self.has_access_key():
            
            connection_string += self._access_key.hex() + '@'
            
        
        connection_string += self._host + ':' + str( self._port )
        
        return connection_string
        
    
    def get_ported_address( self ):
        
        if self._host.endswith( '/' ):
            
            host = self._host[:-1]
            
        else:
            
            host = self._host
            
        
        if '/' in host:
            
            ( actual_host, gubbins ) = self._host.split( '/', 1 )
            
            address = '{}:{}/{}'.format( actual_host, self._port, gubbins )
            
        else:
            
            address = '{}:{}'.format( self._host, self._port )
            
        
        return address
        
    
    def has_access_key( self ):
        
        return self._access_key is not None
        
    
    def set_access_key( self, access_key ):
        
        if access_key == '':
            
            access_key = None
            
        
        self._access_key = access_key
        
    
    def set_address( self, host, port ):
        
        self._host = host
        self._port = port
        
    
    @staticmethod
    def generate_credentials_from_connection_string( connection_string ):
        
        ( host, port, access_key ) = Credentials.parse_connection_string( connection_string )
        
        return Credentials( host, port, access_key )
        
    
    @staticmethod
    def parse_connection_string( connection_string ):
        
        if connection_string is None:
            
            return ( 'hostname', 80, None )
            
        
        if '@' in connection_string:
            
            ( access_key_encoded, address ) = connection_string.split( '@', 1 )
            
            try:
                
                access_key = bytes.fromhex( access_key_encoded )
                
            except TypeError:
                
                raise HydrusExceptions.DataMissing( 'Could not parse that access key! It should be a 64 character hexadecimal string!' )
                
            
            if access_key == '':
                
                access_key = None
                
            
        else:
            
            access_key = None
            
        
        if ':' in connection_string:
            
            ( host, port ) = connection_string.split( ':', 1 )
            
            try:
                
                port = int( port )
                
                if port < 0 or port > 65535:
                    
                    raise ValueError()
                    
                
            except ValueError:
                
                raise HydrusExceptions.DataMissing( 'Could not parse that port! It should be an integer between 0 and 65535!' )
                
            
        
        if host == 'localhost':
            
            host = '127.0.0.1'
            
        
        return ( host, port, access_key )
        
    
HydrusSerialisable.SERIALISABLE_TYPES_TO_OBJECT_TYPES[ HydrusSerialisable.SERIALISABLE_TYPE_CREDENTIALS ] = Credentials

class DefinitionsUpdate( HydrusSerialisable.SerialisableBase ):
    
    SERIALISABLE_TYPE = HydrusSerialisable.SERIALISABLE_TYPE_DEFINITIONS_UPDATE
    SERIALISABLE_NAME = 'Definitions Update'
    SERIALISABLE_VERSION = 1
    
    def __init__( self ):
        
        super().__init__()
        
        self._hash_ids_to_hashes = {}
        self._tag_ids_to_tags = {}
        
    
    def _get_serialisable_info( self ):
        
        serialisable_info = []
        
        if len( self._hash_ids_to_hashes ) > 0:
            
            serialisable_info.append( ( HC.DEFINITIONS_TYPE_HASHES, [ ( hash_id, hash.hex() ) for ( hash_id, hash ) in list(self._hash_ids_to_hashes.items()) ] ) )
            
        
        if len( self._tag_ids_to_tags ) > 0:
            
            serialisable_info.append( ( HC.DEFINITIONS_TYPE_TAGS, list(self._tag_ids_to_tags.items()) ) )
            
        
        return serialisable_info
        
    
    def _initialise_from_serialisable_info( self, serialisable_info ):
        
        for ( definition_type, definitions ) in serialisable_info:
            
            if definition_type == HC.DEFINITIONS_TYPE_HASHES:
                
                self._hash_ids_to_hashes = { hash_id : bytes.fromhex( encoded_hash ) for ( hash_id, encoded_hash ) in definitions }
                
            elif definition_type == HC.DEFINITIONS_TYPE_TAGS:
                
                self._tag_ids_to_tags = { tag_id : tag for ( tag_id, tag ) in definitions }
                
            
        
    
    def add_row( self, row ):
        
        ( definitions_type, key, value ) = row
        
        if definitions_type == HC.DEFINITIONS_TYPE_HASHES:
            
            self._hash_ids_to_hashes[ key ] = value
            
        elif definitions_type == HC.DEFINITIONS_TYPE_TAGS:
            
            self._tag_ids_to_tags[ key ] = value
            
        
    
    def get_hash_ids_to_hashes( self ):
        
        return self._hash_ids_to_hashes
        
    
    def get_num_rows( self ):
        
        return len( self._hash_ids_to_hashes ) + len( self._tag_ids_to_tags )
        
    
    def get_tag_ids_to_tags( self ):
        
        return self._tag_ids_to_tags
        
    
HydrusSerialisable.SERIALISABLE_TYPES_TO_OBJECT_TYPES[ HydrusSerialisable.SERIALISABLE_TYPE_DEFINITIONS_UPDATE ] = DefinitionsUpdate

class Metadata( HydrusSerialisable.SerialisableBase ):
    
    SERIALISABLE_TYPE = HydrusSerialisable.SERIALISABLE_TYPE_METADATA
    SERIALISABLE_NAME = 'Metadata'
    SERIALISABLE_VERSION = 1
    
    def __init__( self, metadata = None, next_update_due = None ):
        
        if metadata is None:
            
            metadata = {}
            
        
        if next_update_due is None:
            
            next_update_due = 0
            
        
        super().__init__()
        
        self._lock = threading.Lock()
        
        now = HydrusTime.get_now()
        
        self._metadata = metadata
        self._next_update_due = next_update_due
        
        self._update_hashes = set()
        self._update_hashes_ordered = []
        
        self._biggest_end = self._calculate_biggest_end()
        
    
    def _calculate_biggest_end( self ):
        
        if len( self._metadata ) == 0:
            
            return None
            
        else:
            
            biggest_index = max( self._metadata.keys() )
            
            ( update_hashes, begin, end ) = self._get_update( biggest_index )
            
            return end
            
        
    
    def _get_next_update_due_time( self, from_client = False ):
        
        delay = 10
        
        if from_client:
            
            delay = UPDATE_CHECKING_PERIOD * 2
            
        
        return self._next_update_due + delay
        
    
    def _get_serialisable_info( self ):
        
        serialisable_metadata = [ ( update_index, [ update_hash.hex() for update_hash in update_hashes ], begin, end ) for ( update_index, ( update_hashes, begin, end ) ) in list(self._metadata.items()) ]
        
        return ( serialisable_metadata, self._next_update_due )
        
    
    def _get_update_hashes( self, update_index ):
        
        ( update_hashes, begin, end ) = self._get_update( update_index )
        
        return update_hashes
        
    
    def _get_update( self, update_index ):
        
        if update_index not in self._metadata:
            
            raise HydrusExceptions.DataMissing( 'That update does not exist!' )
            
        
        return self._metadata[ update_index ]
        
    
    def _initialise_from_serialisable_info( self, serialisable_info ):
        
        ( serialisable_metadata, self._next_update_due ) = serialisable_info
        
        self._metadata = {}
        
        for ( update_index, encoded_update_hashes, begin, end ) in serialisable_metadata:
            
            update_hashes = [ bytes.fromhex( encoded_update_hash ) for encoded_update_hash in encoded_update_hashes ]
            
            self._metadata[ update_index ] = ( update_hashes, begin, end )
            
        
        self._recalc_hashes()
        
        self._biggest_end = self._calculate_biggest_end()
        
    
    def _recalc_hashes( self ):
        
        self._update_hashes = set()
        self._update_hashes_ordered = []
        
        for ( update_index, ( update_hashes, begin, end ) ) in sorted( self._metadata.items() ):
            
            self._update_hashes.update( update_hashes )
            self._update_hashes_ordered.extend( update_hashes )
            
        
    
    def append_update( self, update_hashes, begin, end, next_update_due ):
        
        with self._lock:
            
            update_index = len( self._metadata )
            
            self._metadata[ update_index ] = ( update_hashes, begin, end )
            
            self._update_hashes.update( update_hashes )
            self._update_hashes_ordered.extend( update_hashes )
            
            self._next_update_due = next_update_due
            
            self._biggest_end = end
            
        
    
    def calculate_new_next_update_due( self, update_period ):
        
        with self._lock:
            
            if self._biggest_end is None:
                
                self._next_update_due = 0
                
            else:
                
                self._next_update_due = self._biggest_end + update_period
                
            
        
    
    def get_earliest_timestamp_for_these_hashes( self, hashes ):
        
        hashes = set( hashes )
        
        with self._lock:
            
            for ( update_index, ( update_hashes, begin, end ) ) in sorted( self._metadata.items() ):
                
                if HydrusLists.sets_intersect( hashes, update_hashes ):
                    
                    return end
                    
                
            
        
        return 0
        
    
    def get_next_update_index( self ):
        
        with self._lock:
            
            return len( self._metadata )
            
        
    
    def get_next_update_begin( self ):
        
        with self._lock:
            
            if self._biggest_end is None:
                
                return HydrusTime.get_now()
                
            else:
                
                return self._biggest_end + 1
                
            
        
    
    def get_next_update_due_string( self, from_client = False ):
        
        with self._lock:
            
            if self._next_update_due == 0:
                
                return 'have not yet synced metadata'
                
            elif self._biggest_end is None:
                
                return 'the metadata appears to be uninitialised'
                
            else:
                
                update_due = self._get_next_update_due_time( from_client )
                
                if HydrusTime.time_has_passed( update_due ):
                    
                    s = 'checking for updates imminently'
                    
                else:
                    
                    s = 'checking for updates {}'.format( HydrusTime.timestamp_to_pretty_timedelta( update_due ) )
                    
                
                return 'metadata synced up to {}, {}'.format( HydrusTime.timestamp_to_pretty_timedelta( self._biggest_end ), s )
                
            
        
    
    def get_num_update_hashes( self ):
        
        with self._lock:
            
            return len( self._update_hashes )
            
        
    
    def get_slice( self, from_update_index ):
        
        with self._lock:
            
            metadata = { update_index : row for ( update_index, row ) in self._metadata.items() if update_index >= from_update_index }
            
            return Metadata( metadata, self._next_update_due )
            
        
    
    def get_update_hashes( self, update_index = None ):
        
        with self._lock:
            
            if update_index is None:
                
                return set( self._update_hashes )
                
            else:
                
                return set( self._get_update_hashes( update_index ) )
                
            
        
    
    def get_update_index_begin_and_end( self, update_index ):
        
        with self._lock:
            
            if update_index in self._metadata:
                
                ( update_hashes, begin, end ) = self._metadata[ update_index ]
                
                return ( begin, end )
                
            
            raise HydrusExceptions.DataMissing( 'That update index does not seem to exist!' )
            
        
    
    def get_update_indices_and_times( self ):
        
        with self._lock:
            
            result = []
            
            for ( update_index, ( update_hashes, begin, end ) ) in self._metadata.items():
                
                result.append( ( update_index, begin, end ) )
                
            
            return result
            
        
    
    def get_update_indices_and_hashes( self ):
        
        with self._lock:
            
            result = []
            
            for ( update_index, ( update_hashes, begin, end ) ) in self._metadata.items():
                
                result.append( ( update_index, update_hashes ) )
                
            
            return result
            
        
    
    def has_done_initial_sync( self ):
        
        with self._lock:
            
            return self._next_update_due != 0
            
        
    
    def has_update_hash( self, update_hash ):
        
        with self._lock:
            
            return update_hash in self._update_hashes
            
        
    
    def sort_content_hashes_and_content_types( self, content_hashes_and_content_types ):
        
        with self._lock:
            
            content_hashes_to_content_types = dict( content_hashes_and_content_types )
            
            content_hashes_and_content_types = [ ( update_hash, content_hashes_to_content_types[ update_hash ] ) for update_hash in self._update_hashes_ordered if update_hash in content_hashes_to_content_types ]
            
            return content_hashes_and_content_types
            
        
    
    def update_asap( self ):
        
        with self._lock:
            
            # not 0, that's reserved
            self._next_update_due = 1
            
        
    
    def update_due( self, from_client = False ):
        
        with self._lock:
            
            next_update_due_time = self._get_next_update_due_time( from_client )
            
            return HydrusTime.time_has_passed( next_update_due_time )
            
        
    
    def update_from_slice( self, metadata_slice: "Metadata" ):
        
        with self._lock:
            
            self._metadata.update( metadata_slice._metadata )
            
            new_next_update_due = metadata_slice._next_update_due
            
            if HydrusTime.time_has_passed( new_next_update_due ):
                
                new_next_update_due = HydrusTime.get_now() + 100000
                
            
            self._next_update_due = new_next_update_due
            self._biggest_end = self._calculate_biggest_end()
            
            self._recalc_hashes()
            
        
    
    def update_is_empty( self, update_index ):
        
        with self._lock:
            
            return len( self._get_update_hashes( update_index ) ) == 0
            
        
    

HydrusSerialisable.SERIALISABLE_TYPES_TO_OBJECT_TYPES[ HydrusSerialisable.SERIALISABLE_TYPE_METADATA ] = Metadata

class Petition( HydrusSerialisable.SerialisableBase ):
    
    SERIALISABLE_TYPE = HydrusSerialisable.SERIALISABLE_TYPE_PETITION
    SERIALISABLE_NAME = 'Petition'
    SERIALISABLE_VERSION = 3
    
    def __init__( self, petitioner_account = None, petition_header = None, actions_and_contents = None ):
        
        if actions_and_contents is None:
            
            actions_and_contents = []
            
        
        super().__init__()
        
        self._petitioner_account = petitioner_account
        self._petition_header = petition_header
        self._actions_and_contents = [ ( action, HydrusSerialisable.SerialisableList( contents ) ) for ( action, contents ) in actions_and_contents ]
        
        self._completed_actions_to_contents = collections.defaultdict( list )
        
    
    def __eq__( self, other ):
        
        if isinstance( other, Petition ):
            
            return self.__hash__() == other.__hash__()
            
        
        return NotImplemented
        
    
    def __hash__( self ):
        
        return self._petition_header.__hash__()
        
    
    def _get_serialisable_info( self ):
        
        serialisable_petitioner_account = Account.generate_serialisable_tuple_from_account( self._petitioner_account )
        serialisable_petition_header = self._petition_header.GetSerialisableTuple()
        serialisable_actions_and_contents = [ ( action, contents.get_serialisable_tuple() ) for ( action, contents ) in self._actions_and_contents ]
        
        return ( serialisable_petitioner_account, serialisable_petition_header, serialisable_actions_and_contents )
        
    
    def _initialise_from_serialisable_info( self, serialisable_info ):
        
        ( serialisable_petitioner_account, serialisable_petition_header, serialisable_actions_and_contents ) = serialisable_info
        
        self._petitioner_account = Account.generate_account_from_serialisable_tuple( serialisable_petitioner_account )
        self._petition_header = HydrusSerialisable.create_from_serialisable_tuple( serialisable_petition_header )
        self._actions_and_contents = [ ( action, HydrusSerialisable.create_from_serialisable_tuple( serialisable_contents ) ) for ( action, serialisable_contents ) in serialisable_actions_and_contents ]
        
    
    def _update_serialisable_info( self, version, old_serialisable_info ):
        
        if version == 1:
            
            ( action, serialisable_petitioner_account, reason, serialisable_contents ) = old_serialisable_info
            
            contents = [ HydrusSerialisable.create_from_serialisable_tuple( serialisable_content ) for serialisable_content in serialisable_contents ]
            
            actions_and_contents = [ ( action, HydrusSerialisable.SerialisableList( contents ) ) ]
            
            serialisable_actions_and_contents = [ ( action, contents.get_serialisable_tuple() ) for ( action, contents ) in actions_and_contents ]
            
            new_serialisable_info = ( serialisable_petitioner_account, reason, serialisable_actions_and_contents )
            
            return ( 2, new_serialisable_info )
            
        
        if version == 3:
            
            # we'll dump out, since this code should never be reached. a new client won't be receiving old petitions since an old server won't have the calls
            # it is appropriate to update the version though--that lets an old client talking to a new server get a nicer 'version from the future' error
            
            raise NotImplementedError()
            
        
    
    def approve( self, action, content ):
        
        self._completed_actions_to_contents[ action ].append( content )
        
    
    def approve_all( self ):
        
        for ( action, contents ) in self._actions_and_contents:
            
            for content in contents:
                
                self.approve( action, content )
                
            
        
    
    def deny( self, action, content ):
        
        if action == HC.CONTENT_UPDATE_PEND:
            
            denial_action = HC.CONTENT_UPDATE_DENY_PEND
            
        elif action == HC.CONTENT_UPDATE_PETITION:
            
            denial_action = HC.CONTENT_UPDATE_DENY_PETITION
            
        else:
            
            raise Exception( 'Petition came with unexpected action: {}'.format( action ) )
            
        
        self._completed_actions_to_contents[ denial_action ].append( content )
        
    
    def deny_all( self ):
        
        for ( action, contents ) in self._actions_and_contents:
            
            for content in contents:
                
                self.deny( action, content )
                
            
        
    
    def get_completed_uploadable_client_to_server_updates( self ):
        
        def break_contents_into_chunks( some_contents ):
            
            chunks_of_some_contents = []
            chunk_of_some_contents = []
            
            weight_of_current_chunk = 0
            
            for content in some_contents:
                
                for content_chunk in content.IterateUploadableChunks(): # break 20K-strong mappings petitions into smaller bits to POST back
                    
                    chunk_of_some_contents.append( content_chunk )
                    
                    weight_of_current_chunk += content.GetVirtualWeight()
                    
                    if weight_of_current_chunk > 50:
                        
                        chunks_of_some_contents.append( chunk_of_some_contents )
                        
                        chunk_of_some_contents = []
                        
                        weight_of_current_chunk = 0
                        
                    
                
            
            if len( chunk_of_some_contents ) > 0:
                
                chunks_of_some_contents.append( chunk_of_some_contents )
                
            
            return chunks_of_some_contents
            
        
        updates = []
        
        # make sure you delete before you add
        for action in ( HC.CONTENT_UPDATE_DENY_PETITION, HC.CONTENT_UPDATE_DENY_PEND, HC.CONTENT_UPDATE_PETITION, HC.CONTENT_UPDATE_PEND ):
            
            contents = self._completed_actions_to_contents[ action ]
            
            if len( contents ) == 0:
                
                continue
                
            
            chunks_of_contents = break_contents_into_chunks( contents )
            
            for chunk_of_contents in chunks_of_contents:
                
                update = ClientToServerUpdate()
                
                for content in chunk_of_contents:
                    
                    update.add_content( action, content, self._petition_header.reason )
                    
                
                updates.append( update )
                
            
        
        return updates
        
    
    def get_contents( self, action ):
        
        actions_to_contents = dict( self._actions_and_contents )
        
        if action in actions_to_contents:
            
            return actions_to_contents[ action ]
            
        else:
            
            return []
            
        
    
    def get_actions_and_contents( self ):
        
        return self._actions_and_contents
        
    
    def get_petitioner_account( self ):
        
        return self._petitioner_account
        
    
    def get_petition_header( self ) -> "PetitionHeader":
        
        return self._petition_header
        
    
    def get_reason( self ):
        
        return self._petition_header.reason
        
    
    def get_actual_content_weight( self ) -> int:
        
        total_weight = 0
        
        for ( action, contents ) in self._actions_and_contents:
            
            for content in contents:
                
                total_weight += content.GetActualWeight()
                
            
        
        return total_weight
        
    
    def get_content_summary( self ) -> str:
        
        num_sub_petitions = sum( ( len( contents ) for ( action, contents ) in self._actions_and_contents ) )
        
        if self._petition_header.content_type == HC.CONTENT_TYPE_MAPPINGS and num_sub_petitions > 1:
            
            return '{} mappings in {} petitions'.format( HydrusNumbers.to_human_int( self.get_actual_content_weight() ), HydrusNumbers.to_human_int( num_sub_petitions ) )
            
        else:
            
            return '{} {}'.format( HydrusNumbers.to_human_int( self.get_actual_content_weight() ), HC.content_type_string_lookup[ self._petition_header.content_type ] )
            
        
    

HydrusSerialisable.SERIALISABLE_TYPES_TO_OBJECT_TYPES[ HydrusSerialisable.SERIALISABLE_TYPE_PETITION ] = Petition

class PetitionHeader( HydrusSerialisable.SerialisableBase ):
    
    SERIALISABLE_TYPE = HydrusSerialisable.SERIALISABLE_TYPE_PETITION_HEADER
    SERIALISABLE_NAME = 'Petitions Header'
    SERIALISABLE_VERSION = 1
    
    def __init__( self, content_type = None, status = None, account_key = None, reason = None ):
        
        if content_type is None:
            
            content_type = HC.CONTENT_TYPE_MAPPINGS
            
        
        if status is None:
            
            status = HC.CONTENT_STATUS_PETITIONED
            
        
        if account_key is None:
            
            account_key = b''
            
        
        if reason is None:
            
            reason = ''
            
        
        super().__init__()
        
        self.content_type = content_type
        self.status = status
        self.account_key = account_key
        self.reason = reason
        
    
    def __eq__( self, other ):
        
        if isinstance( other, PetitionHeader ):
            
            return self.__hash__() == other.__hash__()
            
        
        return NotImplemented
        
    
    def __hash__( self ):
        
        return ( self.content_type, self.status, self.account_key, self.reason ).__hash__()
        
    
    def _get_serialisable_info( self ):
        
        serialisable_account_key = self.account_key.hex()
        
        return ( self.content_type, self.status, serialisable_account_key, self.reason )
        
    
    def _initialise_from_serialisable_info( self, serialisable_info ):
        
        ( self.content_type, self.status, serialisable_account_key, self.reason ) = serialisable_info
        
        self.account_key = bytes.fromhex( serialisable_account_key )
        
    

HydrusSerialisable.SERIALISABLE_TYPES_TO_OBJECT_TYPES[ HydrusSerialisable.SERIALISABLE_TYPE_PETITION_HEADER ] = PetitionHeader

class ServerService( object ):
    
    def __init__( self, service_key, service_type, name, port, dictionary ):
        
        self._service_key = service_key
        self._service_type = service_type
        self._name = name
        self._port = port
        
        self._lock = threading.Lock()
        
        self._load_from_dictionary( dictionary )
        
        self._dirty = False
        
    
    def _get_serialisable_dictionary( self ):
        
        dictionary = HydrusSerialisable.SerialisableDictionary()
        
        dictionary[ 'upnp_port' ] = self._upnp_port
        dictionary[ 'bandwidth_tracker' ] = self._bandwidth_tracker
        
        return dictionary
        
    
    def _load_from_dictionary( self, dictionary ):
        
        self._upnp_port = dictionary[ 'upnp_port' ]
        self._bandwidth_tracker = dictionary[ 'bandwidth_tracker' ]
        
    
    def _set_dirty( self ):
        
        self._dirty = True
        
    
    def allows_non_local_connections( self ):
        
        with self._lock:
            
            return True
            
        
    
    def bandwidth_ok( self ):
        
        with self._lock:
            
            return True
            
        
    
    def duplicate( self ):
        
        with self._lock:
            
            dictionary = self._get_serialisable_dictionary()
            
            dictionary = dictionary.duplicate()
            
            duplicate = generate_service( self._service_key, self._service_type, self._name, self._port, dictionary )
            
            return duplicate
            
        
    
    def get_name( self ):
        
        with self._lock:
            
            return self._name
            
        
    
    def get_port( self ):
        
        with self._lock:
            
            return self._port
            
        
    
    def get_upnp_port( self ):
        
        with self._lock:
            
            return self._upnp_port
            
        
    
    def get_service_key( self ):
        
        with self._lock:
            
            return self._service_key
            
        
    
    def get_service_type( self ):
        
        with self._lock:
            
            return self._service_type
            
        
    
    def is_dirty( self ):
        
        with self._lock:
            
            return self._dirty
            
        
    
    def logs_requests( self ):
        
        return True
        
    
    def report_data_used( self, num_bytes ):
        
        with self._lock:
            
            self._bandwidth_tracker.ReportDataUsed( num_bytes )
            
            self._set_dirty()
            
        
    
    def report_request_used( self ):
        
        with self._lock:
            
            self._bandwidth_tracker.ReportRequestUsed()
            
            self._set_dirty()
            
        
    
    def set_clean( self ):
        
        with self._lock:
            
            self._dirty = False
            
        
    
    def set_name( self, name ):
        
        with self._lock:
            
            self._name = name
            
            self._set_dirty()
            
        
    
    def set_port( self, port ):
        
        with self._lock:
            
            self._port = port
            
            self._set_dirty()
            
        
    
    def supports_cors( self ):
        
        return False
        
    
    def to_serialisable_tuple( self ):
        
        with self._lock:
            
            dictionary = self._get_serialisable_dictionary()
            
            dictionary_string = dictionary.dump_to_string()
            
            return ( self._service_key.hex(), self._service_type, self._name, self._port, dictionary_string )
            
        
    
    def to_tuple( self ):
        
        with self._lock:
            
            dictionary = self._get_serialisable_dictionary()
            
            dictionary = dictionary.duplicate()
            
            return ( self._service_key, self._service_type, self._name, self._port, dictionary )
            
        
    
    def use_normie_eris( self ):
        
        with self._lock:
            
            return False
            
        
    
class ServerServiceRestricted( ServerService ):
    
    def _get_serialisable_dictionary( self ):
        
        dictionary = ServerService._get_serialisable_dictionary( self )
        
        dictionary[ 'bandwidth_rules' ] = self._bandwidth_rules
        
        dictionary[ 'service_options' ] = self._service_options
        
        dictionary[ 'server_message' ] = self._server_message
        
        return dictionary
        
    
    def _load_from_dictionary( self, dictionary ):
        
        ServerService._load_from_dictionary( self, dictionary )
        
        if 'service_options' not in dictionary:
            
            dictionary[ 'service_options' ] = HydrusSerialisable.SerialisableDictionary()
            
        
        self._service_options = HydrusSerialisable.SerialisableDictionary( dictionary[ 'service_options' ] )
        
        if 'server_message' not in self._service_options:
            
            self._service_options[ 'server_message' ] = ''
            
        
        self._server_message = self._service_options[ 'server_message' ]
        
        self._bandwidth_rules = dictionary[ 'bandwidth_rules' ]
        
    
    def bandwidth_ok( self ):
        
        with self._lock:
            
            return self._bandwidth_rules.CanStartRequest( self._bandwidth_tracker )
            
        
    
class ServerServiceRepository( ServerServiceRestricted ):
    
    def _get_serialisable_dictionary( self ):
        
        dictionary = ServerServiceRestricted._get_serialisable_dictionary( self )
        
        dictionary[ 'metadata' ] = self._metadata
        dictionary[ 'next_nullification_update_index' ] = self._next_nullification_update_index
        
        return dictionary
        
    
    def _load_from_dictionary( self, dictionary ):
        
        ServerServiceRestricted._load_from_dictionary( self, dictionary )
        
        if 'update_period' not in self._service_options:
            
            self._service_options[ 'update_period' ] = 100000
            
        
        if 'nullification_period' in dictionary:
            
            default_nullification_period = dictionary[ 'nullification_period' ]
            
            del dictionary[ 'nullification_period' ]
            
        else:
            
            default_nullification_period = 90 * 86400
            
        
        if 'nullification_period' not in self._service_options:
            
            self._service_options[ 'nullification_period' ] = default_nullification_period
            
        
        if 'next_nullification_update_index' not in dictionary:
            
            dictionary[ 'next_nullification_update_index' ] = 0
            
        
        self._next_nullification_update_index = dictionary[ 'next_nullification_update_index' ]
        
        self._metadata = dictionary[ 'metadata' ]
        
    
    def get_metadata( self ):
        
        with self._lock:
            
            return self._metadata
            
        
    
    def get_metadata_slice( self, from_update_index ):
        
        with self._lock:
            
            return self._metadata.GetSlice( from_update_index )
            
        
    
    def get_nullification_period( self ) -> int:
        
        with self._lock:
            
            return self._service_options[ 'nullification_period' ]
            
        
    
    def get_update_period( self ) -> int:
        
        with self._lock:
            
            return self._service_options[ 'update_period' ]
            
        
    
    def has_update_hash( self, update_hash ):
        
        with self._lock:
            
            return self._metadata.HasUpdateHash( update_hash )
            
        
    
    def nullify_history( self ):
        
        # when there is a huge amount to catch up on, we don't want to bosh the server for ages
        # instead we'll hammer the server for an hour and then break (for ~three hours, should be)
        
        MAX_WAIT_TIME_WHEN_HEAVY_UPDATES = 120
        
        time_started_nullifying = HydrusTime.get_now()
        time_to_stop_nullifying = time_started_nullifying + 3600
        
        while not HG.started_shutdown:
            
            with self._lock:
                
                next_update_index = self._metadata.GetNextUpdateIndex()
                
                # we are caught up on a server with update times longer than nullification_period
                if self._next_nullification_update_index >= next_update_index:
                    
                    return
                    
                
                ( nullification_begin, nullification_end ) = self._metadata.GetUpdateIndexBeginAndEnd( self._next_nullification_update_index )
                
                nullification_period = self._service_options[ 'nullification_period' ]
                
                # it isn't time to do the next yet!
                if not HydrusTime.time_has_passed( nullification_end + nullification_period ):
                    
                    return
                    
                
                if self._metadata.UpdateIsEmpty( self._next_nullification_update_index ):
                    
                    HydrusData.print_text( 'Account history for "{}" update {} was empty, so nothing to anonymise.'.format( self._name, self._next_nullification_update_index ) )
                    
                    self._next_nullification_update_index += 1
                    
                    self._set_dirty()
                    
                    continue
                    
                
                service_key = self._service_key
                
            
            locked = HG.server_busy.acquire( False ) # pylint: disable=E1111
            
            if not locked:
                
                return
                
            
            try:
                
                HydrusData.print_text( 'Nullifying account history for "{}" update {}.'.format( self._name, self._next_nullification_update_index ) )
                
                update_started = HydrusTime.get_now_float()
                
                HG.controller.write_synchronous( 'nullify_history', service_key, nullification_begin, nullification_end )
                
                update_took = HydrusTime.get_now_float() - update_started
                
                with self._lock:
                    
                    HydrusData.print_text( 'Account history for "{}" update {} was anonymised in {}.'.format( self._name, self._next_nullification_update_index, HydrusTime.timedelta_to_pretty_timedelta( update_took ) ) )
                    
                    self._next_nullification_update_index += 1
                    
                    self._set_dirty()
                    
                
            finally:
                
                HG.server_busy.release()
                
            
            if HydrusTime.time_has_passed( time_to_stop_nullifying ):
                
                return
                
            
            if update_took < 0.5:
                
                continue
                
            
            time_to_wait = min( update_took, MAX_WAIT_TIME_WHEN_HEAVY_UPDATES )
            
            resume_timestamp = HydrusTime.get_now_float() + time_to_wait
            
            while not HG.started_shutdown and not HydrusTime.time_has_passed_float( resume_timestamp ):
                
                time.sleep( 1 )
                
            
        
    
    def set_nullification_period( self, nullification_period: int ):
        
        with self._lock:
            
            self._service_options[ 'nullification_period' ] = nullification_period
            
            self._set_dirty()
            
        
        HG.controller.pub( 'notify_new_nullification' )
        
    
    def set_update_period( self, update_period: int ):
        
        with self._lock:
            
            self._service_options[ 'update_period' ] = update_period
            
            self._metadata.CalculateNewNextUpdateDue( update_period )
            
            self._set_dirty()
            
        
        HG.controller.pub( 'notify_new_repo_sync' )
        
    
    def sync( self ):
        
        with self._lock:
            
            update_due = self._metadata.UpdateDue()
            
        
        update_created = False
        
        if update_due:
            
            locked = HG.server_busy.acquire( False ) # pylint: disable=E1111
            
            if not locked:
                
                return
                
            
            try:
                
                while update_due:
                    
                    with self._lock:
                        
                        service_key = self._service_key
                        
                        begin = self._metadata.GetNextUpdateBegin()
                        
                    
                    update_period = self._service_options[ 'update_period' ]
                    
                    end = begin + update_period
                    
                    update_hashes = HG.controller.write_synchronous( 'create_update', service_key, begin, end )
                    
                    update_created = True
                    
                    next_update_due = end + update_period
                    
                    with self._lock:
                        
                        self._metadata.AppendUpdate( update_hashes, begin, end, next_update_due )
                        
                        update_due = self._metadata.UpdateDue()
                        
                    
                
            finally:
                
                HG.server_busy.release()
                
                if update_created:
                    
                    HG.controller.pub( 'notify_update_created' )
                    
                
            
            with self._lock:
                
                self._set_dirty()
                
            
        
        
    
class ServerServiceRepositoryTag( ServerServiceRepository ):
    
    def _load_from_dictionary( self, dictionary ):
        
        ServerServiceRepository._load_from_dictionary( self, dictionary )
        
        if 'tag_filter' not in self._service_options:
            
            self._service_options[ 'tag_filter' ] = HydrusTags.TagFilter()
            
        
    
    def get_tag_filter( self ) -> HydrusTags.TagFilter:
        
        with self._lock:
            
            return self._service_options[ 'tag_filter' ]
            
        
    
    def set_tag_filter( self, tag_filter: HydrusTags.TagFilter ):
        
        with self._lock:
            
            self._service_options[ 'tag_filter' ] = tag_filter
            
            self._set_dirty()
            
        
    

class ServerServiceRepositoryFile( ServerServiceRepository ):
    
    def _get_serialisable_dictionary( self ):
        
        dictionary = ServerServiceRepository._get_serialisable_dictionary( self )
        
        dictionary[ 'log_uploader_ips' ] = self._log_uploader_ips
        dictionary[ 'max_storage' ] = self._max_storage
        
        return dictionary
        
    
    def _load_from_dictionary( self, dictionary ):
        
        ServerServiceRepository._load_from_dictionary( self, dictionary )
        
        self._log_uploader_ips = dictionary[ 'log_uploader_ips' ]
        self._max_storage = dictionary[ 'max_storage' ]
        
    
    def log_uploader_ips( self ):
        
        with self._lock:
            
            return self._log_uploader_ips
            
        
    
    def get_max_storage( self ):
        
        with self._lock:
            
            return self._max_storage
            
        
    
class ServerServiceAdmin( ServerServiceRestricted ):
    
    def _get_serialisable_dictionary( self ):
        
        dictionary = ServerServiceRestricted._get_serialisable_dictionary( self )
        
        dictionary[ 'server_bandwidth_tracker' ] = self._server_bandwidth_tracker
        dictionary[ 'server_bandwidth_rules' ] = self._server_bandwidth_rules
        
        return dictionary
        
    
    def _load_from_dictionary( self, dictionary ):
        
        ServerServiceRestricted._load_from_dictionary( self, dictionary )
        
        self._server_bandwidth_tracker = dictionary[ 'server_bandwidth_tracker' ]
        self._server_bandwidth_rules = dictionary[ 'server_bandwidth_rules' ]
        
    
    def server_bandwidth_ok( self ):
        
        with self._lock:
            
            return self._server_bandwidth_rules.CanStartRequest( self._server_bandwidth_tracker )
            
        
    
    def server_report_data_used( self, num_bytes ):
        
        with self._lock:
            
            self._server_bandwidth_tracker.ReportDataUsed( num_bytes )
            
            self._set_dirty()
            
        
    
    def server_report_request_used( self ):
        
        with self._lock:
            
            self._server_bandwidth_tracker.ReportRequestUsed()
            
            self._set_dirty()
            
        
    
class UpdateBuilder( object ):
    
    def __init__( self, update_class, max_rows ):
        
        self._update_class = update_class
        self._max_rows = max_rows
        
        self._updates = []
        
        self._current_update = self._update_class()
        self._current_num_rows = 0
        
    
    def add_row( self, row, row_weight = 1 ):
        
        self._current_update.AddRow( row )
        self._current_num_rows += row_weight
        
        if self._current_num_rows > self._max_rows:
            
            self._updates.append( self._current_update )
            
            self._current_update = self._update_class()
            self._current_num_rows = 0
            
        
    
    def finish( self ):
        
        if self._current_update.GetNumRows() > 0:
            
            self._updates.append( self._current_update )
            
        
        self._current_update = None
        
    
    def get_updates( self ):
        
        return self._updates
        
    
