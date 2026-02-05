import collections
import collections.abc
import hashlib
import os
import sqlite3
import sys
import time
import traceback

from hydrus.core import HydrusConstants as HC
from hydrus.core import HydrusData
from hydrus.core import HydrusDB
from hydrus.core import HydrusExceptions
from hydrus.core import HydrusGlobals as HG
from hydrus.core import HydrusLists
from hydrus.core import HydrusNumbers
from hydrus.core import HydrusPaths
from hydrus.core import HydrusSerialisable
from hydrus.core import HydrusTags
from hydrus.core import HydrusTime
from hydrus.core.files import HydrusFilesPhysicalStorage
from hydrus.core.networking import HydrusNetwork

from hydrus.server import ServerFiles
from hydrus.server import ServerGlobals as SG

ALLOWABLE_SERVICE_INFO_TYPES = collections.defaultdict( list )

ALLOWABLE_SERVICE_INFO_TYPES[ HC.FILE_REPOSITORY ] = HC.FILE_REPOSITORY_SERVICE_INFO_TYPES
ALLOWABLE_SERVICE_INFO_TYPES[ HC.TAG_REPOSITORY ] = HC.TAG_REPOSITORY_SERVICE_INFO_TYPES

def generate_repository_master_map_table_names( service_id ):
    
    suffix = str( service_id )
    
    hash_id_map_table_name = 'external_master.repository_hash_id_map_' + suffix
    tag_id_map_table_name = 'external_master.repository_tag_id_map_' + suffix
    
    return ( hash_id_map_table_name, tag_id_map_table_name )
    

def generate_repository_files_table_names( service_id ):
    
    suffix = str( service_id )
    
    current_files_table_name = 'current_files_' + suffix
    deleted_files_table_name = 'deleted_files_' + suffix
    pending_files_table_name = 'pending_files_' + suffix
    petitioned_files_table_name = 'petitioned_files_' + suffix
    ip_addresses_table_name = 'ip_addresses_' + suffix
    
    return ( current_files_table_name, deleted_files_table_name, pending_files_table_name, petitioned_files_table_name, ip_addresses_table_name )
    

def generate_repository_mappings_table_names( service_id ):
    
    suffix = str( service_id )
    
    current_mappings_table_name = 'external_mappings.current_mappings_' + suffix
    deleted_mappings_table_name = 'external_mappings.deleted_mappings_' + suffix
    pending_mappings_table_name = 'external_mappings.pending_mappings_' + suffix
    petitioned_mappings_table_name = 'external_mappings.petitioned_mappings_' + suffix
    
    return ( current_mappings_table_name, deleted_mappings_table_name, pending_mappings_table_name, petitioned_mappings_table_name )
    

def generate_repository_tag_parents_table_names( service_id ):
    
    suffix = str( service_id )
    
    current_tag_parents_table_name = 'current_tag_parents_' + suffix
    deleted_tag_parents_table_name = 'deleted_tag_parents_' + suffix
    pending_tag_parents_table_name = 'pending_tag_parents_' + suffix
    petitioned_tag_parents_table_name = 'petitioned_tag_parents_' + suffix
    
    return ( current_tag_parents_table_name, deleted_tag_parents_table_name, pending_tag_parents_table_name, petitioned_tag_parents_table_name )
    

def generate_repository_tag_siblings_table_names( service_id ):
    
    suffix = str( service_id )
    
    current_tag_siblings_table_name = 'current_tag_siblings_' + suffix
    deleted_tag_siblings_table_name = 'deleted_tag_siblings_' + suffix
    pending_tag_siblings_table_name = 'pending_tag_siblings_' + suffix
    petitioned_tag_siblings_table_name = 'petitioned_tag_siblings_' + suffix
    
    return ( current_tag_siblings_table_name, deleted_tag_siblings_table_name, pending_tag_siblings_table_name, petitioned_tag_siblings_table_name )
    

def generate_repository_update_table_name( service_id ):
    
    return 'updates_' + str( service_id )
    

class DB( HydrusDB.HydrusDB ):
    
    READ_WRITE_ACTIONS = [ 'access_key', 'immediate_content_update', 'registration_keys' ]
    
    def __init__( self, controller, db_dir, db_name ):
        
        self._files_dir = os.path.join( db_dir, 'server_files' )
        
        self._service_ids_to_account_type_ids = collections.defaultdict( set )
        self._service_ids_to_null_account_ids = {}
        self._account_type_ids_to_account_types = {}
        self._service_ids_to_account_type_keys_to_account_type_ids = collections.defaultdict( dict )
        
        super().__init__( controller, db_dir, db_name )
        
    
    def _add_account_type( self, service_id, account_type: HydrusNetwork.AccountType ):
        
        # this does not update the cache. a parent caller has the responsibility
        
        dump = account_type.dump_to_string()
        
        self._execute( 'INSERT INTO account_types ( service_id, dump ) VALUES ( ?, ? );', ( service_id, dump ) )
        
        account_type_id = self._get_last_row_id()
        
        return account_type_id
        
    
    def _add_file( self, file_dict ):
        
        hash = file_dict[ 'hash' ]
        
        master_hash_id = self._get_master_hash_id( hash )
        
        result = self._execute( 'SELECT 1 FROM files_info WHERE master_hash_id = ?;', ( master_hash_id, ) ).fetchone()
        
        if result is None:
            
            size = file_dict[ 'size' ]
            mime = file_dict[ 'mime' ]
            
            if 'width' in file_dict: width = file_dict[ 'width' ]
            else: width = None
            
            if 'height' in file_dict: height = file_dict[ 'height' ]
            else: height = None
            
            if 'duration' in file_dict: duration_ms = file_dict[ 'duration' ]
            else: duration_ms = None
            
            if 'num_frames' in file_dict: num_frames = file_dict[ 'num_frames' ]
            else: num_frames = None
            
            if 'num_words' in file_dict: num_words = file_dict[ 'num_words' ]
            else: num_words = None
            
            self._execute( 'INSERT OR IGNORE INTO files_info ( master_hash_id, size, mime, width, height, duration, num_frames, num_words ) VALUES ( ?, ?, ?, ?, ?, ?, ?, ? );', ( master_hash_id, size, mime, width, height, duration_ms, num_frames, num_words ) )
            
        
        dest_path = ServerFiles.get_expected_file_path( hash )
        
        if not os.path.exists( dest_path ):
            
            source_path = file_dict[ 'path' ]
            
            HydrusPaths.mirror_file( source_path, dest_path )
            
        
        if 'thumbnail' in file_dict:
            
            thumbnail_dest_path = ServerFiles.get_expected_thumbnail_path( hash )
            
            if not os.path.exists( thumbnail_dest_path ):
                
                thumbnail_bytes = file_dict[ 'thumbnail' ]
                
                with open( thumbnail_dest_path, 'wb' ) as f:
                    
                    f.write( thumbnail_bytes )
                    
                
            
        
        return master_hash_id
        
    
    def _add_service( self, service ):
        
        ( service_key, service_type, name, port, dictionary ) = service.to_tuple()
        
        dictionary_string = dictionary.DumpToString()
        
        self._execute( 'INSERT INTO services ( service_key, service_type, name, port, dictionary_string ) VALUES ( ?, ?, ?, ?, ? );', ( sqlite3.Binary( service_key ), service_type, name, port, dictionary_string ) )
        
        service_id = self._get_last_row_id()
        
        #
        
        service_null_account_type = HydrusNetwork.AccountType.generate_null_account_type()
        
        service_null_account_type_id = self._add_account_type( service_id, service_null_account_type )
        
        self._refresh_account_info_cache()
        
        expires = None
        
        [ registration_key ] = self._generate_registration_keys( service_id, 1, service_null_account_type_id, expires )
        
        null_access_key = self._get_access_key( service_key, registration_key )
        
        null_account = self._get_account_key_from_access_key( service_key, null_access_key )
        
        # the null access key disappears in this method, never to be seen again
        
        self._refresh_account_info_cache()
        
        #
        
        service_admin_account_type = HydrusNetwork.AccountType.generate_admin_account_type( service_type )
        
        service_admin_account_type_id = self._add_account_type( service_id, service_admin_account_type )
        
        self._refresh_account_info_cache()
        
        if service_type == HC.SERVER_ADMIN:
            
            force_registration_key = b'init'
            
        else:
            
            force_registration_key = None
            
        
        [ registration_key ] = self._generate_registration_keys( service_id, 1, service_admin_account_type_id, expires, force_registration_key )
        
        admin_access_key = self._get_access_key( service_key, registration_key )
        
        if service_type in HC.REPOSITORIES:
            
            self._repository_create( service_id )
            
        
        return admin_access_key
        
    
    def _add_session( self, session_key, service_key, account_key, expires ):
        
        service_id = self._get_service_id( service_key )
        
        account_id = self._get_account_id( account_key )
        
        self._execute( 'INSERT INTO sessions ( session_key, service_id, account_id, expires ) VALUES ( ?, ?, ?, ? );', ( sqlite3.Binary( session_key ), service_id, account_id, expires ) )
        
    
    def _analyze( self, maintenance_mode = HC.MAINTENANCE_FORCED, stop_time = None ):
        
        stale_time_delta = 30 * 86400
        
        existing_names_to_timestamps = { name : timestamp for ( name, timestamp ) in self._execute( 'SELECT name, timestamp FROM analyze_timestamps;' ).fetchall() }
        
        db_names = [ name for ( index, name, path ) in self._execute( 'PRAGMA database_list;' ) if name not in ( 'mem', 'temp', 'durable_temp' ) ]
        
        all_names = set()
        
        for db_name in db_names:
            
            all_names.update( ( name for ( name, ) in self._execute( 'SELECT name FROM ' + db_name + '.sqlite_master WHERE type = ?;', ( 'table', ) ) ) )
            
        
        all_names.discard( 'sqlite_stat1' )
        
        names_to_analyze = [ name for name in all_names if name not in existing_names_to_timestamps or HydrusTime.time_has_passed( existing_names_to_timestamps[ name ] + stale_time_delta ) ]
        
        if len( names_to_analyze ) > 0:
            
            locked = HG.server_busy.acquire( False ) # pylint: disable=E1111
            
            if not locked:
                
                return
                
            
            try:
                
                for name in HydrusLists.iterate_list_randomly_and_fast( names_to_analyze ):
                    
                    started = HydrusTime.get_now_precise()
                    
                    self._execute( 'ANALYZE ' + name + ';' )
                    
                    self._execute( 'DELETE FROM analyze_timestamps WHERE name = ?;', ( name, ) )
                    
                    self._execute( 'INSERT OR IGNORE INTO analyze_timestamps ( name, timestamp ) VALUES ( ?, ? );', ( name, HydrusTime.get_now() ) )
                    
                    time_took = HydrusTime.get_now_precise() - started
                    
                    if time_took > 1:
                        
                        HydrusData.print_text( 'Analyzed ' + name + ' in ' + HydrusTime.timedelta_to_pretty_timedelta( time_took ) )
                        
                    
                    if SG.server_controller.should_stop_this_work( maintenance_mode, stop_time = stop_time ):
                        
                        break
                        
                    
                
                self._execute( 'ANALYZE sqlite_master;' ) # this reloads the current stats into the query planner
                
            finally:
                
                HG.server_busy.release()
                
            
        
    
    def _backup( self ):
        
        locked = HG.server_busy.acquire( False ) # pylint: disable=E1111
        
        if not locked:
            
            HydrusData.print_text( 'Could not backup because the server was locked.' )
            
            return
            
        
        try:
            
            self._close_db_connection()
            
            backup_path = os.path.join( self._db_dir, 'server_backup' )
            
            HydrusPaths.make_sure_directory_exists( backup_path )
            
            for filename in self._db_filenames.values():
                
                HydrusData.print_text( 'backing up: copying ' + filename )
                
                source = os.path.join( self._db_dir, filename )
                dest = os.path.join( backup_path, filename )
                
                HydrusPaths.mirror_file( source, dest )
                
            
            for filename in [ self._ssl_cert_filename, self._ssl_key_filename ]:
                
                HydrusData.print_text( 'backing up: copying ' + filename )
                
                source = os.path.join( self._db_dir, filename )
                dest = os.path.join( backup_path, filename )
                
                HydrusPaths.mirror_file( source, dest )
                
            
            HydrusData.print_text( 'backing up: copying files' )
            HydrusPaths.mirror_tree( self._files_dir, os.path.join( backup_path, 'server_files' ) )
            
            self._init_db_connection()
            
            HydrusData.print_text( 'backing up: done!' )
            
        finally:
            
            HG.server_busy.release()
            
        
    
    def _clear_deferred_physical_delete( self, file_hash = None, thumbnail_hash = None ):
        
        file_master_hash_id = None if file_hash is None else self._get_master_hash_id( file_hash )
        thumbnail_master_hash_id = None if thumbnail_hash is None else self._get_master_hash_id( thumbnail_hash )
        
        self._clear_deferred_physical_delete_ids( file_master_hash_id = file_master_hash_id, thumbnail_master_hash_id = thumbnail_master_hash_id )
        
    
    def _clear_deferred_physical_delete_ids( self, file_master_hash_id = None, thumbnail_master_hash_id = None ):
        
        if file_master_hash_id is not None:
            
            self._execute( 'DELETE FROM deferred_physical_file_deletes WHERE master_hash_id = ?;', ( file_master_hash_id, ) )
            
        
        if thumbnail_master_hash_id is not None:
            
            self._execute( 'DELETE FROM deferred_physical_thumbnail_deletes WHERE master_hash_id = ?;', ( thumbnail_master_hash_id, ) )
            
        
    
    def _create_db( self ):
        
        HydrusPaths.make_sure_directory_exists( self._files_dir )
        
        for prefix in HydrusFilesPhysicalStorage.iterate_prefixes( '', prefix_length = 2 ):
            
            new_dir = os.path.join( self._files_dir, prefix )
            
            HydrusPaths.make_sure_directory_exists( new_dir )
            
        
        self._execute( 'CREATE TABLE services ( service_id INTEGER PRIMARY KEY, service_key BLOB_BYTES, service_type INTEGER, name TEXT, port INTEGER, dictionary_string TEXT );' )
        
        self._execute( 'CREATE TABLE accounts ( account_id INTEGER PRIMARY KEY, service_id INTEGER, account_key BLOB_BYTES, hashed_access_key BLOB_BYTES, account_type_id INTEGER, created INTEGER, expires INTEGER, dictionary_string TEXT );' )
        self._execute( 'CREATE UNIQUE INDEX accounts_account_key_index ON accounts ( account_key );' )
        self._execute( 'CREATE UNIQUE INDEX accounts_hashed_access_key_index ON accounts ( hashed_access_key );' )
        
        self._execute( 'CREATE TABLE account_scores ( service_id INTEGER, account_id INTEGER, score_type INTEGER, score INTEGER, PRIMARY KEY ( service_id, account_id, score_type ) );' )
        
        self._execute( 'CREATE TABLE account_types ( account_type_id INTEGER PRIMARY KEY, service_id INTEGER, dump TEXT );' )
        
        self._execute( 'CREATE TABLE analyze_timestamps ( name TEXT, timestamp INTEGER );' )
        
        self._execute( 'CREATE TABLE deferred_physical_file_deletes ( master_hash_id INTEGER PRIMARY KEY );' )
        self._execute( 'CREATE TABLE deferred_physical_thumbnail_deletes ( master_hash_id INTEGER PRIMARY KEY );' )
        
        self._execute( 'CREATE TABLE files_info ( master_hash_id INTEGER PRIMARY KEY, size INTEGER, mime INTEGER, width INTEGER, height INTEGER, duration INTEGER, num_frames INTEGER, num_words INTEGER );' )
        
        self._execute( 'CREATE TABLE reasons ( reason_id INTEGER PRIMARY KEY, reason TEXT );' )
        self._execute( 'CREATE UNIQUE INDEX reasons_reason_index ON reasons ( reason );' )
        
        self._execute( 'CREATE TABLE registration_keys ( registration_key BLOB_BYTES PRIMARY KEY, service_id INTEGER, account_type_id INTEGER, account_key BLOB_BYTES, access_key BLOB_BYTES UNIQUE, expires INTEGER );' )
        
        self._execute( 'CREATE TABLE IF NOT EXISTS service_info ( service_id INTEGER, info_type INTEGER, info INTEGER, PRIMARY KEY ( service_id, info_type ) );' )
        
        self._execute( 'CREATE TABLE sessions ( session_key BLOB_BYTES, service_id INTEGER, account_id INTEGER, expires INTEGER );' )
        
        self._execute( 'CREATE TABLE version ( version INTEGER, year INTEGER, month INTEGER );' )
        
        # master
        
        self._execute( 'CREATE TABLE IF NOT EXISTS external_master.hashes ( master_hash_id INTEGER PRIMARY KEY, hash BLOB_BYTES UNIQUE );' )
        
        self._execute( 'CREATE TABLE IF NOT EXISTS external_master.tags ( master_tag_id INTEGER PRIMARY KEY, tag TEXT UNIQUE );' )
        
        # inserts
        
        current_time_struct = time.localtime()
        
        ( current_year, current_month ) = ( current_time_struct.tm_year, current_time_struct.tm_mon )
        
        self._execute( 'INSERT INTO version ( version, year, month ) VALUES ( ?, ?, ? );', ( HC.SOFTWARE_VERSION, current_year, current_month ) )
        
        # set up server admin
        
        admin_service = HydrusNetwork.generate_service( HC.SERVER_ADMIN_KEY, HC.SERVER_ADMIN, 'server admin', HC.DEFAULT_SERVER_ADMIN_PORT )
        
        self._add_service( admin_service ) # this sets up the admin account and a registration token by itself
        
    
    def _defer_files_delete_if_now_orphan( self, master_hash_ids, definitely_no_thumbnails = False, ignore_service_id = None ):
        
        orphan_master_hash_ids = self._filter_orphan_master_hash_ids( master_hash_ids, ignore_service_id = ignore_service_id )
        
        if len( orphan_master_hash_ids ) > 0:
            
            self._execute_many( 'INSERT OR IGNORE INTO deferred_physical_file_deletes ( master_hash_id ) VALUES ( ? );', ( ( master_hash_id, ) for master_hash_id in orphan_master_hash_ids ) )
            
            if not definitely_no_thumbnails:
                
                self._execute_many( 'INSERT OR IGNORE INTO deferred_physical_thumbnail_deletes ( master_hash_id ) VALUES ( ? );', ( ( master_hash_id, ) for master_hash_id in orphan_master_hash_ids ) )
                
            
            self._cursor_transaction_wrapper.pub_after_job( 'notify_new_physical_file_deletes' )
            
        
    
    def _delete_repository_petitions( self, service_id, subject_account_ids ):
        
        ( current_files_table_name, deleted_files_table_name, pending_files_table_name, petitioned_files_table_name, ip_addresses_table_name ) = generate_repository_files_table_names( service_id )
        
        self._execute_many( 'DELETE FROM ' + pending_files_table_name + ' WHERE account_id = ?;', ( ( subject_account_id, ) for subject_account_id in subject_account_ids ) )
        self._execute_many( 'DELETE FROM ' + petitioned_files_table_name + ' WHERE account_id = ?;', ( ( subject_account_id, ) for subject_account_id in subject_account_ids ) )
        
        ( current_mappings_table_name, deleted_mappings_table_name, pending_mappings_table_name, petitioned_mappings_table_name ) = generate_repository_mappings_table_names( service_id )
        
        self._execute_many( 'DELETE FROM ' + pending_mappings_table_name + ' WHERE account_id = ?;', ( ( subject_account_id, ) for subject_account_id in subject_account_ids ) )
        self._execute_many( 'DELETE FROM ' + petitioned_mappings_table_name + ' WHERE account_id = ?;', ( ( subject_account_id, ) for subject_account_id in subject_account_ids ) )
        
        ( current_tag_parents_table_name, deleted_tag_parents_table_name, pending_tag_parents_table_name, petitioned_tag_parents_table_name ) = generate_repository_tag_parents_table_names( service_id )
        
        self._execute_many( 'DELETE FROM ' + pending_tag_parents_table_name + ' WHERE account_id = ?;', ( ( subject_account_id, ) for subject_account_id in subject_account_ids ) )
        self._execute_many( 'DELETE FROM ' + petitioned_tag_parents_table_name + ' WHERE account_id = ?;', ( ( subject_account_id, ) for subject_account_id in subject_account_ids ) )
        
        ( current_tag_siblings_table_name, deleted_tag_siblings_table_name, pending_tag_siblings_table_name, petitioned_tag_siblings_table_name ) = generate_repository_tag_siblings_table_names( service_id )
        
        self._execute_many( 'DELETE FROM ' + pending_tag_siblings_table_name + ' WHERE account_id = ?;', ( ( subject_account_id, ) for subject_account_id in subject_account_ids ) )
        self._execute_many( 'DELETE FROM ' + petitioned_tag_siblings_table_name + ' WHERE account_id = ?;', ( ( subject_account_id, ) for subject_account_id in subject_account_ids ) )
        
    
    def _delete_service( self, service_key ):
        
        service_id = self._get_service_id( service_key )
        service_type = self._get_service_type( service_id )
        
        service_id = self._get_service_id( service_key )
        
        self._execute( 'DELETE FROM services WHERE service_id = ?;', ( service_id, ) )
        
        self._execute( 'DELETE FROM accounts WHERE service_id = ?;', ( service_id, ) )
        self._execute( 'DELETE FROM account_types WHERE service_id = ?;', ( service_id, ) )
        self._execute( 'DELETE FROM account_scores WHERE service_id = ?;', ( service_id, ) )
        self._execute( 'DELETE FROM registration_keys WHERE service_id = ?;', ( service_id, ) )
        self._execute( 'DELETE FROM sessions WHERE service_id = ?;', ( service_id, ) )
        
        if service_type in HC.REPOSITORIES:
            
            self._repository_drop( service_id )
            
        
    
    def _filter_orphan_master_hash_ids( self, master_hash_ids, ignore_service_id = None ):
        
        orphan_master_hash_ids = set( master_hash_ids )
        
        with self._make_temporary_integer_table( master_hash_ids, 'master_hash_id' ) as temp_hash_ids_table_name:
            
            queries = []
            
            for service_id in self._get_service_ids( ( HC.FILE_REPOSITORY, ) ):
                
                if ignore_service_id is not None and service_id == ignore_service_id:
                    
                    continue
                    
                
                ( hash_id_map_table_name, tag_id_map_table_name ) = generate_repository_master_map_table_names( service_id )
                ( current_files_table_name, deleted_files_table_name, pending_files_table_name, petitioned_files_table_name, ip_addresses_table_name ) = generate_repository_files_table_names( service_id )
                
                # temp master files to service to current
                queries.append( 'SELECT master_hash_id FROM {} CROSS JOIN {} USING ( master_hash_id ) CROSS JOIN {} USING ( service_hash_id );'.format( temp_hash_ids_table_name, hash_id_map_table_name, current_files_table_name ) )
                
            
            for service_id in self._get_service_ids( HC.REPOSITORIES ):
                
                if ignore_service_id is not None and service_id == ignore_service_id:
                    
                    continue
                    
                
                update_table_name = generate_repository_update_table_name( service_id )
                
                queries.append( 'SELECT master_hash_id FROM {} CROSS JOIN {} USING ( master_hash_id );'.format( temp_hash_ids_table_name, update_table_name ) )
                
            
            for query in queries:
                
                useful_master_hash_ids = self._sts( self._execute( query ) )
                
                if len( useful_master_hash_ids ) > 0:
                    
                    orphan_master_hash_ids.difference_update( useful_master_hash_ids )
                    
                    if len( orphan_master_hash_ids ) == 0:
                        
                        return orphan_master_hash_ids
                        
                    
                    self._execute_many( 'DELETE FROM {} WHERE master_hash_id = ?;'.format( temp_hash_ids_table_name ), ( ( master_hash_id, ) for master_hash_id in useful_master_hash_ids ) )
                    
                
            
        
        return orphan_master_hash_ids
        
    
    def _generate_registration_keys_from_account( self, service_key, account: HydrusNetwork.Account, num, account_type_key, expires ):
        
        service_id = self._get_service_id( service_key )
        
        account_type_id = self._get_account_type_id( service_id, account_type_key )
        
        return self._generate_registration_keys( service_id, num, account_type_id, expires )
        
    
    def _generate_registration_keys( self, service_id, num, account_type_id, expires, force_registration_key = None ):
        
        account_type = self._get_account_type( service_id, account_type_id )
        
        if account_type.is_null_account():
            
            result = self._execute( 'SELECT 1 FROM accounts WHERE account_type_id = ?;', ( account_type_id, ) ).fetchone()
            
            if result is not None:
                
                # null account already exists
                
                raise HydrusExceptions.BadRequestException( 'You cannot create new null accounts!' )
                
            
        
        if force_registration_key is None:
            
            keys = [ ( os.urandom( HC.HYDRUS_KEY_LENGTH ), os.urandom( HC.HYDRUS_KEY_LENGTH ), os.urandom( HC.HYDRUS_KEY_LENGTH ) ) for i in range( num ) ]
            
        else:
            
            keys = [ ( force_registration_key, os.urandom( HC.HYDRUS_KEY_LENGTH ), os.urandom( HC.HYDRUS_KEY_LENGTH ) ) for i in range( num ) ]
            
        
        self._execute_many( 'INSERT INTO registration_keys ( registration_key, service_id, account_type_id, account_key, access_key, expires ) VALUES ( ?, ?, ?, ?, ?, ? );', [ ( sqlite3.Binary( hashlib.sha256( registration_key ).digest() ), service_id, account_type_id, sqlite3.Binary( account_key ), sqlite3.Binary( access_key ), expires ) for ( registration_key, account_key, access_key ) in keys ] )
        
        return [ registration_key for ( registration_key, account_key, access_key ) in keys ]
        
    
    def _get_access_key( self, service_key, registration_key ):
        
        service_id = self._get_service_id( service_key )
        
        # we generate a new access_key every time this is requested so that no one with access to the registration token can peek at the access_key before the legit user fetches it for real
        # the reg_key is deleted when the last-requested access_key is used to create a session, which calls getaccountkeyfromaccesskey
        
        registration_key_sha256 = hashlib.sha256( registration_key ).digest()
        
        result = self._execute( 'SELECT 1 FROM registration_keys WHERE service_id = ? AND registration_key = ?;', ( service_id, sqlite3.Binary( registration_key_sha256 ) ) ).fetchone()
        
        if result is None:
            
            raise HydrusExceptions.InsufficientCredentialsException( 'The service could not find that registration token in its database.' )
            
        
        new_access_key = os.urandom( HC.HYDRUS_KEY_LENGTH )
        
        self._execute( 'UPDATE registration_keys SET access_key = ? WHERE service_id = ? AND registration_key = ?;', ( sqlite3.Binary( new_access_key ), service_id, sqlite3.Binary( registration_key_sha256 ) ) )
        
        return new_access_key
        
    
    def _get_account( self, service_id, account_id ) -> HydrusNetwork.Account:
        
        ( account_key, account_type_id, created, expires, dictionary_string ) = self._execute( 'SELECT account_key, account_type_id, created, expires, dictionary_string FROM accounts WHERE service_id = ? AND account_id = ?;', ( service_id, account_id ) ).fetchone()
        
        account_type = self._get_account_type( service_id, account_type_id )
        
        dictionary = HydrusSerialisable.create_from_string( dictionary_string )
        
        return HydrusNetwork.Account.generate_account_from_tuple( ( account_key, account_type, created, expires, dictionary ) )
        
    
    def _get_account_key_from_content( self, service_key, content ):
        
        service_id = self._get_service_id( service_key )
        service_type = self._get_service_type( service_id )
        
        content_type = content.GetContentType()
        content_data = content.GetContentData()
        
        if content_type == HC.CONTENT_TYPE_FILES:
            
            if service_type != HC.FILE_REPOSITORY:
                
                raise HydrusExceptions.NotFoundException( 'Only File Repositories support file account lookups!')
                
            
            hash = content_data[0]
            
            if not self._master_hash_exists( hash ):
                
                raise HydrusExceptions.NotFoundException( 'The service could not find that hash in its database.' )
                
            
            master_hash_id = self._get_master_hash_id( hash )
            
            if not self._repository_service_hash_id_exists( service_id, master_hash_id ):
                
                raise HydrusExceptions.NotFoundException( 'The service could not find that service hash in its database.' )
                
            
            service_hash_id = self._repository_get_service_hash_id( service_id, master_hash_id, HydrusTime.get_now() )
            
            ( current_files_table_name, deleted_files_table_name, pending_files_table_name, petitioned_files_table_name, ip_addresses_table_name ) = generate_repository_files_table_names( service_id )
            
            result = self._execute( 'SELECT account_id FROM ' + current_files_table_name + ' WHERE service_hash_id = ?;', ( service_hash_id, ) ).fetchone()
            
            if result is None:
                
                raise HydrusExceptions.NotFoundException( 'The service could not find that hash in its database.' )
                
            
        elif content_type == HC.CONTENT_TYPE_MAPPING:
            
            if service_type != HC.TAG_REPOSITORY:
                
                raise HydrusExceptions.NotFoundException( 'Only Tag Repositories support mapping account lookups!')
                
            
            ( tag, hash ) = content_data
            
            if not self._master_hash_exists( hash ):
                
                raise HydrusExceptions.NotFoundException( 'The service could not find that hash in its database.' )
                
            
            master_hash_id = self._get_master_hash_id( hash )
            
            if not self._repository_service_hash_id_exists( service_id, master_hash_id ):
                
                raise HydrusExceptions.NotFoundException( 'The service could not find that service hash in its database.' )
                
            
            service_hash_id = self._repository_get_service_hash_id( service_id, master_hash_id, HydrusTime.get_now() )
            
            if not self._master_tag_exists( tag ):
                
                raise HydrusExceptions.NotFoundException( 'The service could not find that tag in its database.' )
                
            
            master_tag_id = self._get_master_tag_id( tag )
            
            if not self._repository_service_tag_id_exists( service_id, master_tag_id ):
                
                raise HydrusExceptions.NotFoundException( 'The service could not find that service tag in its database.' )
                
            
            service_tag_id = self._repository_get_service_tag_id( service_id, master_tag_id, HydrusTime.get_now() )
            
            ( current_mappings_table_name, deleted_mappings_table_name, pending_mappings_table_name, petitioned_mappings_table_name ) = generate_repository_mappings_table_names( service_id )
            
            result = self._execute( 'SELECT account_id FROM ' + current_mappings_table_name + ' WHERE service_tag_id = ? AND service_hash_id = ?;', ( service_tag_id, service_hash_id ) ).fetchone()
            
            if result is None:
                
                raise HydrusExceptions.NotFoundException( 'The service could not find that mapping in its database.' )
                
            
        else:
            
            raise HydrusExceptions.NotFoundException( 'The service could not understand the submitted content.' )
            
        
        ( account_id, ) = result
        
        account_key = self._get_account_key_from_account_id( account_id )
        
        return account_key
        
    
    def _get_account_from_account_key( self, service_key, account_key ):
        
        service_id = self._get_service_id( service_key )
        
        account_id = self._get_account_id( account_key )
        
        return self._get_account( service_id, account_id )
        
    
    def _get_account_key_from_access_key( self, service_key, access_key ):
        
        service_id = self._get_service_id( service_key )
        
        result = self._execute( 'SELECT account_key FROM accounts WHERE service_id = ? AND hashed_access_key = ?;', ( service_id, sqlite3.Binary( hashlib.sha256( access_key ).digest() ), ) ).fetchone()
        
        if result is None:
            
            # we do not delete the registration_key (and hence the raw unhashed access_key)
            # until the first attempt to create a session to make sure the user
            # has the access_key saved
            
            try:
                
                ( account_type_id, account_key, expires ) = self._execute( 'SELECT account_type_id, account_key, expires FROM registration_keys WHERE access_key = ?;', ( sqlite3.Binary( access_key ), ) ).fetchone()
                
            except Exception as e:
                
                raise HydrusExceptions.InsufficientCredentialsException( 'The service could not find that account in its database.' )
                
            
            self._execute( 'DELETE FROM registration_keys WHERE access_key = ?;', ( sqlite3.Binary( access_key ), ) )
            
            #
            
            hashed_access_key = hashlib.sha256( access_key ).digest()
            
            account_type = self._get_account_type( service_id, account_type_id )
            
            created = HydrusTime.get_now()
            
            account = HydrusNetwork.Account( account_key, account_type, created, expires )
            
            ( account_key, account_type, created, expires, dictionary ) = HydrusNetwork.Account.generate_tuple_from_account( account )
            
            dictionary_string = dictionary.dump_to_string()
            
            self._execute( 'INSERT INTO accounts ( service_id, account_key, hashed_access_key, account_type_id, created, expires, dictionary_string ) VALUES ( ?, ?, ?, ?, ?, ?, ? );', ( service_id, sqlite3.Binary( account_key ), sqlite3.Binary( hashed_access_key ), account_type_id, created, expires, dictionary_string ) )
            
        else:
            
            ( account_key, ) = result
            
        
        return account_key
        
    
    def _get_account_key_from_account_id( self, account_id ):
        
<<<<<<< HEAD
        try: ( account_key, ) = self._execute( 'SELECT account_key FROM accounts WHERE account_id = ?;', ( account_id, ) ).fetchone()
        except: raise HydrusExceptions.InsufficientCredentialsException( 'The service could not find that account_id in its database.' )
=======
        try: ( account_key, ) = self._Execute( 'SELECT account_key FROM accounts WHERE account_id = ?;', ( account_id, ) ).fetchone()
        except Exception as e: raise HydrusExceptions.InsufficientCredentialsException( 'The service could not find that account_id in its database.' )
>>>>>>> 955f2e8e9df1d901351bb3dcf4c0a50e99048667
        
        return account_key
        
    
    def _get_account_id( self, account_key: bytes ) -> int:
        
        result = self._execute( 'SELECT account_id FROM accounts WHERE account_key = ?;', ( sqlite3.Binary( account_key ), ) ).fetchone()
        
        if result is None:
            
            raise HydrusExceptions.InsufficientCredentialsException( 'The service could not find that account id in its database.' )
            
        
        ( account_id, ) = result
        
        return account_id
        
    
    def _get_account_info( self, service_key, account, subject_account ):
        
        service_id = self._get_service_id( service_key )
        
        subject_account_key = subject_account.GetAccountKey()
        
        subject_account_id = self._get_account_id( subject_account_key )
        
        service_type = self._get_service_type( service_id )
        
        if service_type in HC.REPOSITORIES:
            
            account_info = self._repository_get_account_info( service_id, subject_account_id )
            
        else:
            
            account_info = {}
            
        
        return account_info
        
    
    def _get_account_type_id( self, service_id, account_type_key ):
        
        if account_type_key not in self._service_ids_to_account_type_keys_to_account_type_ids[ service_id ]:
            
            raise HydrusExceptions.DataMissing( 'Could not find the given account type key!' )
            
        
        account_type_id = self._service_ids_to_account_type_keys_to_account_type_ids[ service_id ][ account_type_key ]
        
        if account_type_id not in self._service_ids_to_account_type_ids[ service_id ]:
            
            raise HydrusExceptions.DataMissing( 'Could not find the given account type for that service!' )
            
        
        return account_type_id
        
    
    def _get_account_types( self, service_key, account ):
        
        service_id = self._get_service_id( service_key )
        
        account_types = [ self._account_type_ids_to_account_types[ account_type_id ] for account_type_id in self._service_ids_to_account_type_ids[ service_id ] ]
        
        return account_types
        
    
    def _get_account_type( self, service_id, account_type_id ) -> HydrusNetwork.AccountType:
        
        if account_type_id not in self._service_ids_to_account_type_ids[ service_id ]:
            
            raise HydrusExceptions.DataMissing( 'Could not find the given account type for that service!' )
            
        
        return self._account_type_ids_to_account_types[ account_type_id ]
        
    
    def _get_all_accounts( self, service_key, admin_account ):
        
        service_id = self._get_service_id( service_key )
        
        account_ids = self._stl( self._execute( 'SELECT account_id FROM accounts WHERE service_id = ?;', ( service_id, ) ) )
        
        accounts = [ self._get_account( service_id, account_id ) for account_id in account_ids ]
        
        return accounts
        
    
    def _get_auto_create_account_types( self, service_key ):
        
        service_id = self._get_service_id( service_key )
        
        account_types = [ self._account_type_ids_to_account_types[ account_type_id ] for account_type_id in self._service_ids_to_account_type_ids[ service_id ] ]
        
        auto_create_account_types = [ account_type for account_type in account_types if account_type.SupportsAutoCreateAccount() ]
        
        return auto_create_account_types
        
    
    def _get_auto_create_registration_key( self, service_key, account_type_key ):
        
        service_id = self._get_service_id( service_key )
        
        account_type_id = self._get_account_type_id( service_id, account_type_key )
        
        account_type = self._get_account_type( service_id, account_type_id )
        
        if not account_type.supports_auto_create_account():
            
            raise HydrusExceptions.BadRequestException( '"{}" accounts do not support auto-creation!'.format( account_type.get_title() ) )
            
        
        if not account_type.can_auto_create_account_now():
            
            raise HydrusExceptions.BadRequestException( 'Please wait a bit--there are no new "{}" accounts available for now!'.format( account_type.get_title() ) )
            
        
        num = 1
        expires = None
        
        account_type.report_auto_create_account()
        
        self._execute( 'UPDATE account_types SET dump = ? WHERE service_id = ? AND account_type_id = ?;', ( account_type.dump_to_string(), service_id, account_type_id ) )
        
        return list( self._generate_registration_keys( service_id, num, account_type_id, expires ) )[0]
        
    
    def _get_deferred_physical_delete( self ):
        
        file_result = self._execute( 'SELECT master_hash_id FROM deferred_physical_file_deletes LIMIT 1;' ).fetchone()
        
        if file_result is not None:
            
            ( master_hash_id, ) = file_result
            
            file_result = self._get_hash( master_hash_id )
            
        
        thumbnail_result = self._execute( 'SELECT master_hash_id FROM deferred_physical_thumbnail_deletes LIMIT 1;' ).fetchone()
        
        if thumbnail_result is not None:
            
            ( master_hash_id, ) = thumbnail_result
            
            thumbnail_result = self._get_hash( master_hash_id )
            
        
        return ( file_result, thumbnail_result )
        
    
    def _get_hash( self, master_hash_id ):
        
        result = self._execute( 'SELECT hash FROM hashes WHERE master_hash_id = ?;', ( master_hash_id, ) ).fetchone()
        
        if result is None:
            
            raise Exception( 'File hash error in database' )
            
        
        ( hash, ) = result
        
        return hash
        
    
    def _get_hashes( self, master_hash_ids ):
        
        with self._make_temporary_integer_table( master_hash_ids, 'master_hash_id' ) as temp_hash_ids_table_name:
            
            return self._stl( self._execute( 'SELECT hash FROM {} CROSS JOIN hashes USING ( master_hash_id );'.format( temp_hash_ids_table_name ) ) )
            
        
    
    def _get_master_hash_id( self, hash ):
        
        result = self._execute( 'SELECT master_hash_id FROM hashes WHERE hash = ?;', ( sqlite3.Binary( hash ), ) ).fetchone()
        
        if result is None:
            
            self._execute( 'INSERT INTO hashes ( hash ) VALUES ( ? );', ( sqlite3.Binary( hash ), ) )
            
            master_hash_id = self._get_last_row_id()
            
            return master_hash_id
            
        else:
            
            ( master_hash_id, ) = result
            
            return master_hash_id
            
        
    
    def _get_master_hash_ids( self, hashes ):
        
        master_hash_ids = set()
        hashes_not_in_db = set()
        
        for hash in hashes:
            
            if hash is None:
                
                continue
                
            
            result = self._execute( 'SELECT master_hash_id FROM hashes WHERE hash = ?;', ( sqlite3.Binary( hash ), ) ).fetchone()
            
            if result is None:
                
                hashes_not_in_db.add( hash )
                
            else:
                
                ( master_hash_id, ) = result
                
                master_hash_ids.add( master_hash_id )
                
            
        
        if len( hashes_not_in_db ) > 0:
            
            self._execute_many( 'INSERT INTO hashes ( hash ) VALUES ( ? );', ( ( sqlite3.Binary( hash ), ) for hash in hashes_not_in_db ) )
            
            for hash in hashes_not_in_db:
                
                ( master_hash_id, ) = self._execute( 'SELECT master_hash_id FROM hashes WHERE hash = ?;', ( sqlite3.Binary( hash ), ) ).fetchone()
                
                master_hash_ids.add( master_hash_id )
                
            
        
        return master_hash_ids
        
    
    def _get_master_tag_id( self, tag ):
        
        tag = HydrusTags.clean_tag( tag )
        
        HydrusTags.check_tag_not_empty( tag )
        
        result = self._execute( 'SELECT master_tag_id FROM tags WHERE tag = ?;', ( tag, ) ).fetchone()
        
        if result is None:
            
            self._execute( 'INSERT INTO tags ( tag ) VALUES ( ? );', ( tag, ) )
            
            master_tag_id = self._get_last_row_id()
            
            return master_tag_id
            
        else:
            
            ( master_tag_id, ) = result
            
            return master_tag_id
            
        
    
    def _get_options( self, service_key ):
        
        service_id = self._get_service_id( service_key )
        
        ( options, ) = self._execute( 'SELECT options FROM services WHERE service_id = ?;', ( service_id, ) ).fetchone()
        
        return options
        
    
    def _get_reason( self, reason_id ):
        
        result = self._execute( 'SELECT reason FROM reasons WHERE reason_id = ?;', ( reason_id, ) ).fetchone()
        
        if result is None: raise Exception( 'Reason error in database' )
        
        ( reason, ) = result
        
        return reason
        
    
    def _get_reason_id( self, reason ):
        
        result = self._execute( 'SELECT reason_id FROM reasons WHERE reason = ?;', ( reason, ) ).fetchone()
        
        if result is None:
            
            self._execute( 'INSERT INTO reasons ( reason ) VALUES ( ? );', ( reason, ) )
            
            reason_id = self._get_last_row_id()
            
            return reason_id
            
        else:
            
            ( reason_id, ) = result
            
            return reason_id
            
        
    
    def _get_service_id( self, service_key ):
        
        result = self._execute( 'SELECT service_id FROM services WHERE service_key = ?;', ( sqlite3.Binary( service_key ), ) ).fetchone()
        
        if result is None:
            
            raise HydrusExceptions.DataMissing( 'Service id error in database' )
            
        
        ( service_id, ) = result
        
        return service_id
        
    
    def _get_service_ids( self, limited_types = HC.ALL_SERVICES ):
        
        return [ service_id for ( service_id, ) in self._execute( 'SELECT service_id FROM services WHERE service_type IN ' + HydrusLists.splay_list_for_db( limited_types ) + ';' ) ]
        
    
    def _get_service_info( self, service_key: bytes ):
        
        service_id = self._get_service_id( service_key )
        
        service_info = { info_type : info for ( info_type, info ) in self._execute( 'SELECT info_type, info FROM service_info WHERE service_id = ?;', ( service_id, ) ) }
        
        return service_info
        
    
    def _get_service_key( self, service_id ):
        
        ( service_key, ) = self._execute( 'SELECT service_key FROM services WHERE service_id = ?;', ( service_id, ) ).fetchone()
        
        return service_key
        
    
    def _get_service_keys( self, limited_types = HC.ALL_SERVICES ):
        
        return [ service_key for ( service_key, ) in self._execute( 'SELECT service_key FROM services WHERE service_type IN '+ HydrusLists.splay_list_for_db( limited_types ) + ';' ) ]
        
    
    def _get_service_name( self, service_id ):
        
        result = self._execute( 'SELECT name FROM services WHERE service_id = ?;', ( service_id, ) ).fetchone()
        
        if result is None:
            
            raise Exception( 'Service id error in database' )
            
        
        ( name, ) = result
        
        return name
        
    
    def _get_service_type( self, service_id ):
        
        result = self._execute( 'SELECT service_type FROM services WHERE service_id = ?;', ( service_id, ) ).fetchone()
        
        if result is None:
            
            raise Exception( 'Service id error in database' )
            
        
        ( service_type, ) = result
        
        return service_type
        
    
    def _get_services( self, limited_types = HC.ALL_SERVICES ):
        
        services = []
        
        service_info = self._execute( 'SELECT service_key, service_type, name, port, dictionary_string FROM services WHERE service_type IN ' + HydrusLists.splay_list_for_db( limited_types ) + ';' ).fetchall()
        
        for ( service_key, service_type, name, port, dictionary_string ) in service_info:
            
            dictionary = HydrusSerialisable.create_from_string( dictionary_string )
            
            service = HydrusNetwork.generate_service( service_key, service_type, name, port, dictionary )
            
            services.append( service )
            
        
        return services
        
    
    def _get_services_from_account( self, account ):
        
        return self._get_services()
        
    
    def _get_sessions( self, service_key = None ):
        
        now = HydrusTime.get_now()
        
        self._execute( 'DELETE FROM sessions WHERE ? > expires;', ( now, ) )
        
        sessions = []
        
        if service_key is None:
            
            results = self._execute( 'SELECT session_key, service_id, account_id, expires FROM sessions;' ).fetchall()
            
        else:
            
            service_id = self._get_service_id( service_key)
            
            results = self._execute( 'SELECT session_key, service_id, account_id, expires FROM sessions WHERE service_id = ?;', ( service_id, ) ).fetchall()
            
        
        service_ids_to_service_keys = {}
        
        account_ids_to_accounts = {}
        
        account_ids_to_hashed_access_keys = {}
        
        for ( session_key, service_id, account_id, expires ) in results:
            
            if service_id not in service_ids_to_service_keys:
                
                service_ids_to_service_keys[ service_id ] = self._get_service_key( service_id )
                
            
            service_key = service_ids_to_service_keys[ service_id ]
            
            if account_id not in account_ids_to_accounts:
                
                account = self._get_account( service_id, account_id )
                
                account_ids_to_accounts[ account_id ] = account
                
            
            account = account_ids_to_accounts[ account_id ]
            
            if account_id not in account_ids_to_hashed_access_keys:
                
                ( hashed_access_key, ) = self._execute( 'SELECT hashed_access_key FROM accounts WHERE account_id = ?;', ( account_id, ) ).fetchone()
                
                account_ids_to_hashed_access_keys[ account_id ] = hashed_access_key
                
            
            hashed_access_key = account_ids_to_hashed_access_keys[ account_id ]
            
            sessions.append( ( session_key, service_key, account, hashed_access_key, expires ) )
            
        
        return sessions
        
    
    def _get_tag( self, master_tag_id ):
        
        result = self._execute( 'SELECT tag FROM tags WHERE master_tag_id = ?;', ( master_tag_id, ) ).fetchone()
        
        if result is None:
            
            raise Exception( 'Tag error in database' )
            
        
        ( tag, ) = result
        
        return tag
        
    
    def _hash_exists( self, hash ):
        
        result = self._execute( 'SELECT 1 FROM hashes WHERE hash = ?;', ( sqlite3.Binary( hash ), ) ).fetchone()
        
        if result is None:
            
            return False
            
        else:
            
            return True
            
        
    
    def _init_caches( self ):
        
        self._over_monthly_data = False
        self._services_over_monthly_data = set()
        
        self._refresh_account_info_cache()
        
    
    def _init_commands_to_methods( self ):
        
        super()._init_commands_to_methods()
        
        self._read_commands_to_methods.update(
            {
                'access_key' : self._get_access_key,
                'account' : self._get_account_from_account_key,
                'account_info' : self._get_account_info,
                'account_key_from_access_key' : self._get_account_key_from_access_key,
                'account_key_from_content' : self._get_account_key_from_content,
                'account_types' : self._get_account_types,
                'auto_create_account_types' : self._get_auto_create_account_types,
                'auto_create_registration_key' : self._get_auto_create_registration_key,
                'all_accounts' : self._get_all_accounts,
                'deferred_physical_delete' : self._get_deferred_physical_delete,
                'immediate_update' : self._repository_generate_immediate_update,
                'ip' : self._repository_get_ip_timestamp,
                'is_an_orphan' : self._is_an_orphan,
                'num_petitions' : self._repository_get_num_petitions,
                'petition' : self._repository_get_petition,
                'petitions_summary' : self._repository_get_petitions_summary,
                'registration_keys' : self._generate_registration_keys_from_account,
                'service_has_file' : self._repository_has_file,
                'service_info' : self._get_service_info,
                'service_keys' : self._get_service_keys,
                'services' : self._get_services,
                'services_from_account' : self._get_services_from_account,
                'sessions' : self._get_sessions,
                'verify_access_key' : self._verify_access_key
            }
        )
        
        self._write_commands_to_methods.update(
            {
                'account_types' : self._modify_account_types,
                'analyze' : self._analyze,
                'backup' : self._backup,
                'clear_deferred_physical_delete' : self._clear_deferred_physical_delete,
                'create_update' : self._repository_create_update,
                'dirty_accounts' : self._save_dirty_accounts,
                'dirty_services' : self._save_dirty_services,
                'file' : self._repository_process_add_file,
                'maintenance_regen_service_info' : self._repository_regenerate_service_info_service_key,
                'modify_account_account_type' : self._modify_account_account_type,
                'modify_account_ban' : self._modify_account_ban,
                'modify_account_delete_all_content' : self._modify_account_delete_all_content,
                'modify_account_expires' : self._modify_account_expires,
                'modify_account_set_message' : self._modify_account_set_message,
                'modify_account_unban' : self._modify_account_unban,
                'nullify_history' : self._repository_nullify_history,
                'services' : self._modify_services,
                'session' : self._add_session,
                'update' : self._repository_process_client_to_server_update,
                'vacuum' : self._vacuum
            }
        )
        
    
    def _init_external_databases( self ):
        
        self._db_filenames[ 'external_mappings' ] = 'server.mappings.db'
        self._db_filenames[ 'external_master' ] = 'server.master.db'
        
    
    def _is_an_orphan( self, possible_hash ):
        
        if self._hash_exists( possible_hash ):
            
            hash = possible_hash
            
            master_hash_id = self._get_master_hash_id( hash )
            
            orphan_master_hash_ids = self._filter_orphan_master_hash_ids( ( master_hash_id, ) )
            
            return len( orphan_master_hash_ids ) == 1
            
        else:
            
            return True
            
        
    
    def _is_null_account( self, service_id, account_id ):
        
        return self._service_ids_to_null_account_ids[ service_id ] == account_id
        
    
    def _manage_db_error( self, job, e ):
        
        if isinstance( e, HydrusExceptions.NetworkException ):
            
            job.PutResult( e )
            
        else:
            
            ( exception_type, value, tb ) = sys.exc_info()
            
            new_e = type( e )( '\n'.join( traceback.format_exception( exception_type, value, tb ) ) )
            
            job.PutResult( new_e )
            
        
    
    def _master_hash_exists( self, hash ):
        
        result = self._execute( 'SELECT master_hash_id FROM hashes WHERE hash = ?;', ( sqlite3.Binary( hash ), ) ).fetchone()
        
        if result is None:
            
            return False
            
        else:
            
            return True
            
        
    
    def _master_tag_exists( self, tag ):
        
        result = self._execute( 'SELECT master_tag_id FROM tags WHERE tag = ?;', ( tag, ) ).fetchone()
        
        if result is None:
            
            return False
            
        else:
            
            return True
            
        
    
    def _modify_account_account_type( self, service_key, admin_account, subject_account_key, new_account_type_key ):
        
        service_id = self._get_service_id( service_key )
        
        subject_account_id = self._get_account_id( subject_account_key )
        
        if self._is_null_account( service_id, subject_account_id ):
            
            raise HydrusExceptions.BadRequestException( 'You cannot reassign the null account!' )
            
        
        subject_account = self._get_account( service_id, subject_account_id )
        
        current_account_type_id = self._get_account_type_id( service_id, subject_account.get_account_type().get_account_type_key() )
        new_account_type_id = self._get_account_type_id( service_id, new_account_type_key )
        
        current_account_type = self._get_account_type( service_id, current_account_type_id )
        
        new_account_type = self._get_account_type( service_id, new_account_type_id )
        
        if new_account_type.is_null_account():
            
            raise HydrusExceptions.BadRequestException( 'You cannot reassign anyone to the null account!' )
            
        
        self._execute( 'UPDATE accounts SET account_type_id = ? WHERE account_id = ?;', ( new_account_type_id, subject_account_id ) )
        
        SG.server_controller.pub( 'update_session_accounts', service_key, ( subject_account_key, ) )
        
        HydrusData.print_text(
            'Account {} changed the account type of {} from "{}" to "{}".'.format(
                admin_account.GetAccountKey().hex(),
                subject_account_key.hex(),
                current_account_type.get_title(),
                new_account_type.get_title()
            )
        )
        
    
    def _modify_account_ban( self, service_key, admin_account, subject_account_key, reason, expires ):
        
        service_id = self._get_service_id( service_key )
        
        subject_account_id = self._get_account_id( subject_account_key )
        
        if self._is_null_account( service_id, subject_account_id ):
            
            raise HydrusExceptions.BadRequestException( 'You cannot ban the null account!' )
            
        
        subject_account = self._get_account( service_id, subject_account_id )
        
        now = HydrusTime.get_now()
        
        subject_account.ban( reason, now, expires )
        
        self._save_accounts( service_id, ( subject_account, ) )
        
        service_type = self._get_service_type( service_id )
        
        if service_type in HC.REPOSITORIES:
            
            self._delete_repository_petitions( service_id, ( subject_account_id, ) )
            
        
        SG.server_controller.pub( 'update_session_accounts', service_key, ( subject_account_key, ) )
        
        HydrusData.print_text(
            'Account {} banned {} with reason "{}" until "{}".'.format(
                admin_account.GetAccountKey().hex(),
                subject_account_key.hex(),
                reason,
                HydrusTime.timestamp_to_pretty_expires( expires )
            )
        )
        
    
    def _modify_account_delete_all_content( self, service_key, admin_account: HydrusNetwork.Account, subject_account_key ) -> bool:
        
        service_id = self._get_service_id( service_key )
        
        subject_account_id = self._get_account_id( subject_account_key )
        
        if self._is_null_account( service_id, subject_account_id ):
            
            raise HydrusExceptions.BadRequestException( 'You cannot delete the null account\'s content!' )
            
        
        service_type = self._get_service_type( service_id )
        
        admin_account_key = admin_account.get_account_key()
        
        admin_account_id = self._get_account_id( admin_account_key )
        
        we_deleted_everything = True
        
        if service_type in HC.REPOSITORIES:
            
            self._delete_repository_petitions( service_id, ( subject_account_id, ) )
            
            we_deleted_everything = self._repository_delete_all_current_content( service_id, admin_account_id, subject_account_id )
            
        
        SG.server_controller.pub( 'update_session_accounts', service_key, ( subject_account_key, ) )
        
        HydrusData.print_text(
            'Account {} deleted all content by {}.'.format(
                admin_account_key.hex(),
                subject_account_key.hex()
            )
        )
        
        return we_deleted_everything
        
    
    def _modify_account_expires( self, service_key, admin_account, subject_account_key, new_expires ):
        
        service_id = self._get_service_id( service_key )
        
        subject_account_id = self._get_account_id( subject_account_key )
        
        if self._is_null_account( service_id, subject_account_id ):
            
            raise HydrusExceptions.BadRequestException( 'You cannot modify the null account!' )
            
        
        ( current_expires, ) = self._execute( 'SELECT expires FROM accounts WHERE account_id = ?;', ( subject_account_id, ) ).fetchone()
        
        self._execute( 'UPDATE accounts SET expires = ? WHERE account_id = ?;', ( new_expires, subject_account_id ) )
        
        SG.server_controller.pub( 'update_session_accounts', service_key, ( subject_account_key, ) )
        
        HydrusData.print_text(
            'Account {} changed the expiration of {} from "{}" to "{}".'.format(
                admin_account.GetAccountKey().hex(),
                subject_account_key.hex(),
                HydrusTime.timestamp_to_pretty_expires( current_expires ),
                HydrusTime.timestamp_to_pretty_expires( new_expires )
            )
        )
        
    
    def _modify_account_set_message( self, service_key, admin_account, subject_account_key, message ):
        
        service_id = self._get_service_id( service_key )
        
        subject_account_id = self._get_account_id( subject_account_key )
        
        if self._is_null_account( service_id, subject_account_id ):
            
            raise HydrusExceptions.BadRequestException( 'You cannot tell the null account anything!' )
            
        
        subject_account = self._get_account( service_id, subject_account_id )
        
        now = HydrusTime.get_now()
        
        subject_account.set_message( message, now )
        
        self._save_accounts( service_id, ( subject_account, ) )
        
        SG.server_controller.pub( 'update_session_accounts', service_key, ( subject_account_key, ) )
        
        if message == '':
            
            m = 'Account {} cleared {} of any message.'
            
        else:
            
            m = 'Account {} set {} with a message.'
            
        
        HydrusData.print_text(
            m.format(
                admin_account.GetAccountKey().hex(),
                subject_account_key.hex()
            )
        )
        
    
    def _modify_account_unban( self, service_key, admin_account, subject_account_key ):
        
        service_id = self._get_service_id( service_key )
        
        subject_account_id = self._get_account_id( subject_account_key )
        
        if self._is_null_account( service_id, subject_account_id ):
            
            raise HydrusExceptions.BadRequestException( 'You cannot unban the null account!' )
            
        
        subject_account = self._get_account( service_id, subject_account_id )
        
        subject_account.unban()
        
        self._save_accounts( service_id, ( subject_account, ) )
        
        SG.server_controller.pub( 'update_session_accounts', service_key, ( subject_account_key, ) )
        
        HydrusData.print_text(
            'Account {} unbanned {}.'.format(
                admin_account.GetAccountKey().hex(),
                subject_account_key.hex()
            )
        )
        
    
    def _modify_account_types( self, service_key, admin_account, account_types, deletee_account_type_keys_to_replacement_account_type_keys ):
        
        current_account_types = self._get_account_types( service_key, admin_account )
        
        account_types = [ at for at in account_types if not at.IsNullAccount() ]
        
        account_types.extend( [ at for at in current_account_types if at.IsNullAccount() ] )
        
        #
        
        service_id = self._get_service_id( service_key )
        
        current_account_type_keys_to_account_types = { account_type.GetAccountTypeKey() : account_type for account_type in current_account_types }
        
        current_account_type_keys = set( current_account_type_keys_to_account_types.keys() )
        
        future_account_type_keys_to_account_types = { account_type.GetAccountTypeKey() : account_type for account_type in account_types }
        
        future_account_type_keys = set( future_account_type_keys_to_account_types.keys() )
        
        deletee_account_type_keys = current_account_type_keys.difference( future_account_type_keys )
        
        for deletee_account_type_key in deletee_account_type_keys:
            
            if deletee_account_type_key not in deletee_account_type_keys_to_replacement_account_type_keys:
                
                raise HydrusExceptions.BadRequestException( 'Was missing a replacement account_type_key.' )
                
            
            if deletee_account_type_keys_to_replacement_account_type_keys[ deletee_account_type_key ] not in future_account_type_keys:
                
                raise HydrusExceptions.BadRequestException( 'Was a replacement account_type_key was not in the future account types.' )
                
            
            if future_account_type_keys_to_account_types[ deletee_account_type_keys_to_replacement_account_type_keys[ deletee_account_type_key ] ].IsNullAccount():
                
                raise HydrusExceptions.BadRequestException( 'You cannot assign people to the null account!' )
                
            
        
        # we have a temp lad here, don't want to alter the actual cache structure, just in case of rollback
        modification_account_type_keys_to_account_type_ids = dict( self._service_ids_to_account_type_keys_to_account_type_ids[ service_id ] )
        
        for account_type in account_types:
            
            account_type_key = account_type.GetAccountTypeKey()
            
            if account_type_key not in current_account_type_keys:
                
                account_type_id = self._add_account_type( service_id, account_type )
                
                modification_account_type_keys_to_account_type_ids[ account_type_key ] = account_type_id
                
                HydrusData.print_text(
                    'Account {} added a new account type, "{}".'.format(
                        admin_account.GetAccountKey().hex(),
                        account_type.GetTitle()
                    )
                )
                
            else:
                
                account_type_id = modification_account_type_keys_to_account_type_ids[ account_type_key ]
                
                dump = account_type.DumpToString()
                
                ( existing_dump, ) = self._execute( 'SELECT dump FROM account_types WHERE service_id = ? AND account_type_id = ?;', ( service_id, account_type_id ) ).fetchone()
                
                if dump != existing_dump:
                    
                    self._execute( 'UPDATE account_types SET dump = ? WHERE service_id = ? AND account_type_id = ?;', ( dump, service_id, account_type_id ) )
                    
                    HydrusData.print_text(
                        'Account {} updated the account type, "{}".'.format(
                            admin_account.GetAccountKey().hex(),
                            account_type.GetTitle()
                        )
                    )
                    
                
            
        
        for deletee_account_type_key in deletee_account_type_keys:
            
            new_account_type_key = deletee_account_type_keys_to_replacement_account_type_keys[ deletee_account_type_key ]
            
            deletee_account_type_id = modification_account_type_keys_to_account_type_ids[ deletee_account_type_key ]
            new_account_type_id = modification_account_type_keys_to_account_type_ids[ new_account_type_key ]
            
            self._execute( 'UPDATE accounts SET account_type_id = ? WHERE service_id = ? AND account_type_id = ?;', ( new_account_type_id, service_id, deletee_account_type_id ) )
            self._execute( 'UPDATE registration_keys SET account_type_id = ? WHERE service_id = ? AND account_type_id = ?;', ( new_account_type_id, service_id, deletee_account_type_id ) )
            
            self._execute( 'DELETE FROM account_types WHERE service_id = ? AND account_type_id = ?;', ( service_id, deletee_account_type_id ) )
            
            deletee_account_type = current_account_type_keys_to_account_types[ deletee_account_type_key ]
            new_account_type = future_account_type_keys_to_account_types[ new_account_type_key ]
            
            HydrusData.print_text(
                'Account {} deleted the account type, "{}", replacing them with "{}".'.format(
                    admin_account.GetAccountKey().hex(),
                    deletee_account_type.GetTitle(),
                    new_account_type.GetTitle()
                )
            )
            
        
        # now we are done, no rollback, so let's update the cache
        self._refresh_account_info_cache()
        
        self._cursor_transaction_wrapper.pub_after_job( 'update_all_session_accounts', service_key )
        
    
    def _modify_services( self, account, services ):
        
        current_service_keys = { service_key for ( service_key, ) in self._execute( 'SELECT service_key FROM services;' ) }
        
        future_service_keys = {service.get_service_key() for service in services}
        
        for service_key in current_service_keys:
            
            if service_key not in future_service_keys:
                
                self._delete_service( service_key )
                
            
        
        service_keys_to_access_keys = {}
        
        for service in services:
            
            service_key = service.get_service_key()
            
            if service_key in current_service_keys:
                
                ( service_key, service_type, name, port, dictionary ) = service.to_tuple()
                
                service_id = self._get_service_id( service_key )
                
                dictionary_string = dictionary.DumpToString()
                
                self._execute( 'UPDATE services SET name = ?, port = ?, dictionary_string = ? WHERE service_id = ?;', ( name, port, dictionary_string, service_id ) )
                
            else:
                
                access_key = self._add_service( service )
                
                service_keys_to_access_keys[ service_key ] = access_key
                
            
        
        return service_keys_to_access_keys
        
    
    def _refresh_account_info_cache( self ):
        
        self._service_ids_to_account_type_ids = collections.defaultdict( set )
        self._service_ids_to_null_account_ids = {}
        self._account_type_ids_to_account_types = {}
        self._service_ids_to_account_type_keys_to_account_type_ids = collections.defaultdict( dict )
        
        data = self._execute( 'SELECT account_type_id, service_id, dump FROM account_types;' ).fetchall()
        
        for ( account_type_id, service_id, dump ) in data:
            
            account_type = HydrusSerialisable.create_from_string( dump )
            
            self._service_ids_to_account_type_ids[ service_id ].add( account_type_id )
            self._account_type_ids_to_account_types[ account_type_id ] = account_type
            self._service_ids_to_account_type_keys_to_account_type_ids[ service_id ][ account_type.GetAccountTypeKey() ] = account_type_id
            
            if account_type.IsNullAccount():
                
                result = self._execute( 'SELECT account_id FROM accounts WHERE account_type_id = ?;', ( account_type_id, ) ).fetchone()
                
                if result is not None:
                    
                    ( null_account_id, ) = result
                    
                    self._service_ids_to_null_account_ids[ service_id ] = null_account_id
                    
                
            
        
    
    def _repository_add_file( self, service_id, account_id, file_dict, overwrite_deleted, timestamp ):
        
        master_hash_id = self._add_file( file_dict )
        
        service_hash_id = self._repository_get_service_hash_id( service_id, master_hash_id, timestamp )
        
        ( current_files_table_name, deleted_files_table_name, pending_files_table_name, petitioned_files_table_name, ip_addresses_table_name ) = generate_repository_files_table_names( service_id )
        
        if 'ip' in file_dict:
            
            ip = file_dict[ 'ip' ]
            
            self._execute( 'INSERT INTO ' + ip_addresses_table_name + ' ( master_hash_id, ip, ip_timestamp ) VALUES ( ?, ?, ? );', ( master_hash_id, ip, timestamp ) )
            
        
        result = self._execute( 'SELECT 1 FROM ' + current_files_table_name + ' WHERE service_hash_id = ?;', ( service_hash_id, ) ).fetchone()
        
        if result is not None:
            
            return
            
        
        if overwrite_deleted:
            
            self._execute( 'DELETE FROM ' + deleted_files_table_name + ' WHERE service_hash_id = ?;', ( service_hash_id, ) )
            
            self._repository_update_service_info( service_id, HC.SERVICE_INFO_NUM_DELETED_FILES, - self._get_row_count() )
            
        else:
            
            result = self._execute( 'SELECT 1 FROM ' + deleted_files_table_name + ' WHERE service_hash_id = ?;', ( service_hash_id, ) ).fetchone()
            
            if result is not None:
                
                return
                
            
        
        # if we ever do 'pending files', delete from that table here, it'll use master ids I think
        
        self._execute( 'INSERT INTO ' + current_files_table_name + ' ( service_hash_id, account_id, file_timestamp ) VALUES ( ?, ?, ? );', ( service_hash_id, account_id, timestamp ) )
        
        self._repository_update_service_info( service_id, HC.SERVICE_INFO_NUM_FILES, 1 )
        
        self._clear_deferred_physical_delete_ids( file_master_hash_id = master_hash_id, thumbnail_master_hash_id = master_hash_id )
        
    
    def _repository_add_mappings( self, service_id, account_id, master_tag_id, master_hash_ids, overwrite_deleted, timestamp ):
        
        service_tag_id = self._repository_get_service_tag_id( service_id, master_tag_id, timestamp )
        service_hash_ids = self._repository_get_service_hash_ids( service_id, master_hash_ids, timestamp )
        
        ( current_mappings_table_name, deleted_mappings_table_name, pending_mappings_table_name, petitioned_mappings_table_name ) = generate_repository_mappings_table_names( service_id )
        
        if overwrite_deleted:
            
            self._execute_many( 'DELETE FROM ' + deleted_mappings_table_name + ' WHERE service_tag_id = ? AND service_hash_id = ?;', ( ( service_tag_id, service_hash_id ) for service_hash_id in service_hash_ids ) )
            
            self._repository_update_service_info( service_id, HC.SERVICE_INFO_NUM_DELETED_MAPPINGS, - self._get_row_count() )
            
        else:
            
            with self._make_temporary_integer_table( service_hash_ids, 'service_hash_id' ) as temp_hash_ids_table_name:
                
                deleted_service_hash_ids = self._sts( self._execute( 'SELECT service_hash_id FROM {} CROSS JOIN {} USING ( service_hash_id ) WHERE service_tag_id = ?;'.format( temp_hash_ids_table_name, deleted_mappings_table_name ), ( service_tag_id, ) ) )
                
            
            service_hash_ids = set( service_hash_ids ).difference( deleted_service_hash_ids )
            
        
        # if we ever do pending mappings, delete from pending with the master ids here
        
        self._execute_many( 'INSERT OR IGNORE INTO ' + current_mappings_table_name + ' ( service_tag_id, service_hash_id, account_id, mapping_timestamp ) VALUES ( ?, ?, ?, ? );', [ ( service_tag_id, service_hash_id, account_id, timestamp ) for service_hash_id in service_hash_ids ] )
        
        self._repository_update_service_info( service_id, HC.SERVICE_INFO_NUM_MAPPINGS, self._get_row_count() )
        
    
    def _repository_add_tag_parent( self, service_id, account_id, child_master_tag_id, parent_master_tag_id, overwrite_deleted, timestamp ):
        
        child_service_tag_id = self._repository_get_service_tag_id( service_id, child_master_tag_id, timestamp )
        parent_service_tag_id = self._repository_get_service_tag_id( service_id, parent_master_tag_id, timestamp )
        
        ( current_tag_parents_table_name, deleted_tag_parents_table_name, pending_tag_parents_table_name, petitioned_tag_parents_table_name ) = generate_repository_tag_parents_table_names( service_id )
        
        if overwrite_deleted:
            
            self._repository_reward_tag_parent_penders( service_id, child_master_tag_id, parent_master_tag_id, 1 )
            
            self._execute( 'DELETE FROM ' + deleted_tag_parents_table_name + ' WHERE child_service_tag_id = ? AND parent_service_tag_id = ?;', ( child_service_tag_id, parent_service_tag_id ) )
            
            self._repository_update_service_info( service_id, HC.SERVICE_INFO_NUM_DELETED_TAG_PARENTS, - self._get_row_count() )
            
        else:
            
            result = self._execute( 'SELECT 1 FROM ' + deleted_tag_parents_table_name + ' WHERE child_service_tag_id = ? AND parent_service_tag_id = ?;', ( child_service_tag_id, parent_service_tag_id ) ).fetchone()
            
            if result is not None:
                
                return
                
            
        
        self._repository_delete_raw_pending_tag_parent_rows( service_id, child_master_tag_id, parent_master_tag_id )
        
        self._execute( 'INSERT OR IGNORE INTO ' + current_tag_parents_table_name + ' ( child_service_tag_id, parent_service_tag_id, account_id, parent_timestamp ) VALUES ( ?, ?, ?, ? );', ( child_service_tag_id, parent_service_tag_id, account_id, timestamp ) )
        
        self._repository_update_service_info( service_id, HC.SERVICE_INFO_NUM_TAG_PARENTS, self._get_row_count() )
        
    
    def _repository_add_tag_sibling( self, service_id, account_id, bad_master_tag_id, good_master_tag_id, overwrite_deleted, timestamp ):
        
        bad_service_tag_id = self._repository_get_service_tag_id( service_id, bad_master_tag_id, timestamp )
        good_service_tag_id = self._repository_get_service_tag_id( service_id, good_master_tag_id, timestamp )
        
        ( current_tag_siblings_table_name, deleted_tag_siblings_table_name, pending_tag_siblings_table_name, petitioned_tag_siblings_table_name ) = generate_repository_tag_siblings_table_names( service_id )
        
        if overwrite_deleted:
            
            self._repository_reward_tag_sibling_penders( service_id, bad_master_tag_id, good_master_tag_id, 1 )
            
            self._execute( 'DELETE FROM ' + deleted_tag_siblings_table_name + ' WHERE bad_service_tag_id = ? AND good_service_tag_id = ?;', ( bad_service_tag_id, good_service_tag_id ) )
            
            self._repository_update_service_info( service_id, HC.SERVICE_INFO_NUM_DELETED_TAG_SIBLINGS, - self._get_row_count() )
            
        else:
            
            result = self._execute( 'SELECT 1 FROM ' + deleted_tag_siblings_table_name + ' WHERE bad_service_tag_id = ? AND good_service_tag_id = ?;', ( bad_service_tag_id, good_service_tag_id ) ).fetchone()
            
            if result is not None:
                
                return
                
            
        
        self._repository_delete_raw_pending_tag_sibling_rows( service_id, bad_master_tag_id, good_master_tag_id )
        
        self._execute( 'INSERT OR IGNORE INTO ' + current_tag_siblings_table_name + ' ( bad_service_tag_id, good_service_tag_id, account_id, sibling_timestamp ) VALUES ( ?, ?, ?, ? );', ( bad_service_tag_id, good_service_tag_id, account_id, timestamp ) )
        
        self._repository_update_service_info( service_id, HC.SERVICE_INFO_NUM_TAG_SIBLINGS, self._get_row_count() )
        
    
    def _repository_convert_petition_ids_to_summary( self, content_type: int, status: int, petition_ids: collections.abc.Collection[ tuple[ int, int ] ], limit: int ) -> list[ HydrusNetwork.PetitionHeader ]:
        
        if len( petition_ids ) > limit:
            
            # we don't want a random sample, we want to sample grouped by account id, so let's be a bit more clever about it
            
            petitioner_account_ids_to_reason_ids = HydrusData.build_key_to_list_dict( petition_ids )
            
            petition_ids = []
            
            num_rows = 0
            
            petition_account_ids = list( petitioner_account_ids_to_reason_ids.keys() )
            
            for petition_account_id in HydrusLists.iterate_list_randomly_and_fast( petition_account_ids ):
                
                reason_ids = petitioner_account_ids_to_reason_ids[ petition_account_id ]
                
                if num_rows + len( reason_ids ) > limit:
                    
                    num_to_add = limit - num_rows
                    
                    reason_ids = reason_ids[ : num_to_add ]
                    
                
                petition_ids.extend( ( ( petition_account_id, reason_id ) for reason_id in reason_ids ) )
                
                num_rows += len( reason_ids )
                
                if num_rows >= limit:
                    
                    break
                    
                
            
        
        petitioner_account_ids = { petitioner_account_id for ( petitioner_account_id, reason_id ) in petition_ids }
        reason_ids = { reason_id for ( petitioner_account_id, reason_id ) in petition_ids }
        
        petitioner_account_ids_to_account_keys = { petitioner_account_id : self._get_account_key_from_account_id( petitioner_account_id ) for petitioner_account_id in petitioner_account_ids }
        reason_ids_to_reasons = { reason_id : self._get_reason( reason_id ) for reason_id in reason_ids }
        
        return HydrusSerialisable.SerialisableList( [ HydrusNetwork.PetitionHeader( content_type = content_type, status = status, account_key = petitioner_account_ids_to_account_keys[ petitioner_account_id ], reason = reason_ids_to_reasons[ reason_id ] ) for ( petitioner_account_id, reason_id ) in petition_ids ] )
        
    
    def _repository_create( self, service_id ):
        
        ( hash_id_map_table_name, tag_id_map_table_name ) = generate_repository_master_map_table_names( service_id )
        
        self._execute( 'CREATE TABLE ' + hash_id_map_table_name + ' ( service_hash_id INTEGER PRIMARY KEY, master_hash_id INTEGER UNIQUE, hash_id_timestamp INTEGER );' )
        self._create_index( hash_id_map_table_name, [ 'hash_id_timestamp' ] )
        
        self._execute( 'CREATE TABLE ' + tag_id_map_table_name + ' ( service_tag_id INTEGER PRIMARY KEY, master_tag_id INTEGER UNIQUE, tag_id_timestamp INTEGER );' )
        self._create_index( tag_id_map_table_name, [ 'tag_id_timestamp' ] )
        
        #
        
        ( current_files_table_name, deleted_files_table_name, pending_files_table_name, petitioned_files_table_name, ip_addresses_table_name ) = generate_repository_files_table_names( service_id )
        
        self._execute( 'CREATE TABLE ' + current_files_table_name + ' ( service_hash_id INTEGER PRIMARY KEY, account_id INTEGER, file_timestamp INTEGER );' )
        self._create_index( current_files_table_name, [ 'account_id' ] )
        self._create_index( current_files_table_name, [ 'file_timestamp' ] )
        
        self._execute( 'CREATE TABLE ' + deleted_files_table_name + ' ( service_hash_id INTEGER PRIMARY KEY, account_id INTEGER, file_timestamp INTEGER );' )
        self._create_index( deleted_files_table_name, [ 'account_id' ] )
        self._create_index( deleted_files_table_name, [ 'file_timestamp' ] )
        
        self._execute( 'CREATE TABLE ' + pending_files_table_name + ' ( master_hash_id INTEGER, account_id INTEGER, reason_id INTEGER, PRIMARY KEY ( master_hash_id, account_id ) ) WITHOUT ROWID;' )
        self._create_index( pending_files_table_name, [ 'account_id', 'reason_id' ] )
        
        self._execute( 'CREATE TABLE ' + petitioned_files_table_name + ' ( service_hash_id INTEGER, account_id INTEGER, reason_id INTEGER, PRIMARY KEY ( service_hash_id, account_id ) ) WITHOUT ROWID;' )
        self._create_index( petitioned_files_table_name, [ 'account_id', 'reason_id' ] )
        
        self._execute( 'CREATE TABLE ' + ip_addresses_table_name + ' ( master_hash_id INTEGER, ip TEXT, ip_timestamp INTEGER );' )
        
        #
        
        ( current_mappings_table_name, deleted_mappings_table_name, pending_mappings_table_name, petitioned_mappings_table_name ) = generate_repository_mappings_table_names( service_id )
        
        self._execute( 'CREATE TABLE ' + current_mappings_table_name + ' ( service_tag_id INTEGER, service_hash_id INTEGER, account_id INTEGER, mapping_timestamp INTEGER, PRIMARY KEY ( service_tag_id, service_hash_id ) ) WITHOUT ROWID;' )
        self._create_index( current_mappings_table_name, [ 'account_id' ] )
        self._create_index( current_mappings_table_name, [ 'mapping_timestamp' ] )
        
        self._execute( 'CREATE TABLE ' + deleted_mappings_table_name + ' ( service_tag_id INTEGER, service_hash_id INTEGER, account_id INTEGER, mapping_timestamp INTEGER, PRIMARY KEY ( service_tag_id, service_hash_id ) ) WITHOUT ROWID;' )
        self._create_index( deleted_mappings_table_name, [ 'account_id' ] )
        self._create_index( deleted_mappings_table_name, [ 'mapping_timestamp' ] )
        
        self._execute( 'CREATE TABLE ' + pending_mappings_table_name + ' ( master_tag_id INTEGER, master_hash_id INTEGER, account_id INTEGER, reason_id INTEGER, PRIMARY KEY ( master_tag_id, master_hash_id, account_id ) ) WITHOUT ROWID;' )
        self._create_index( pending_mappings_table_name, [ 'account_id', 'reason_id' ] )
        
        self._execute( 'CREATE TABLE ' + petitioned_mappings_table_name + ' ( service_tag_id INTEGER, service_hash_id INTEGER, account_id INTEGER, reason_id INTEGER, PRIMARY KEY ( service_tag_id, service_hash_id, account_id ) ) WITHOUT ROWID;' )
        self._create_index( petitioned_mappings_table_name, [ 'account_id', 'reason_id' ] )
        
        #
        
        ( current_tag_parents_table_name, deleted_tag_parents_table_name, pending_tag_parents_table_name, petitioned_tag_parents_table_name ) = generate_repository_tag_parents_table_names( service_id )
        
        self._execute( 'CREATE TABLE ' + current_tag_parents_table_name + ' ( child_service_tag_id INTEGER, parent_service_tag_id INTEGER, account_id INTEGER, parent_timestamp INTEGER, PRIMARY KEY ( child_service_tag_id, parent_service_tag_id ) ) WITHOUT ROWID;' )
        self._create_index( current_tag_parents_table_name, [ 'account_id' ] )
        self._create_index( current_tag_parents_table_name, [ 'parent_timestamp' ] )
        
        self._execute( 'CREATE TABLE ' + deleted_tag_parents_table_name + ' ( child_service_tag_id INTEGER, parent_service_tag_id INTEGER, account_id INTEGER, parent_timestamp INTEGER, PRIMARY KEY ( child_service_tag_id, parent_service_tag_id ) ) WITHOUT ROWID;' )
        self._create_index( deleted_tag_parents_table_name, [ 'account_id' ] )
        self._create_index( deleted_tag_parents_table_name, [ 'parent_timestamp' ] )
        
        self._execute( 'CREATE TABLE ' + pending_tag_parents_table_name + ' ( child_master_tag_id INTEGER, parent_master_tag_id INTEGER, account_id INTEGER, reason_id INTEGER, PRIMARY KEY ( child_master_tag_id, parent_master_tag_id, account_id ) ) WITHOUT ROWID;' )
        self._create_index( pending_tag_parents_table_name, [ 'account_id', 'reason_id' ] )
        
        self._execute( 'CREATE TABLE ' + petitioned_tag_parents_table_name + ' ( child_service_tag_id INTEGER, parent_service_tag_id INTEGER, account_id INTEGER, reason_id INTEGER, PRIMARY KEY ( child_service_tag_id, parent_service_tag_id, account_id ) ) WITHOUT ROWID;' )
        self._create_index( petitioned_tag_parents_table_name, [ 'account_id', 'reason_id' ] )
        
        #
        
        ( current_tag_siblings_table_name, deleted_tag_siblings_table_name, pending_tag_siblings_table_name, petitioned_tag_siblings_table_name ) = generate_repository_tag_siblings_table_names( service_id )
        
        self._execute( 'CREATE TABLE ' + current_tag_siblings_table_name + ' ( bad_service_tag_id INTEGER PRIMARY KEY, good_service_tag_id INTEGER, account_id INTEGER, sibling_timestamp INTEGER );' )
        self._create_index( current_tag_siblings_table_name, [ 'account_id' ] )
        self._create_index( current_tag_siblings_table_name, [ 'sibling_timestamp' ] )
        
        self._execute( 'CREATE TABLE ' + deleted_tag_siblings_table_name + ' ( bad_service_tag_id INTEGER PRIMARY KEY, good_service_tag_id INTEGER, account_id INTEGER, sibling_timestamp INTEGER );' )
        self._create_index( deleted_tag_siblings_table_name, [ 'account_id' ] )
        self._create_index( deleted_tag_siblings_table_name, [ 'sibling_timestamp' ] )
        
        self._execute( 'CREATE TABLE ' + pending_tag_siblings_table_name + ' ( bad_master_tag_id INTEGER, good_master_tag_id INTEGER, account_id INTEGER, reason_id INTEGER, PRIMARY KEY ( bad_master_tag_id, account_id ) ) WITHOUT ROWID;' )
        self._create_index( pending_tag_siblings_table_name, [ 'account_id', 'reason_id' ] )
        
        self._execute( 'CREATE TABLE ' + petitioned_tag_siblings_table_name + ' ( bad_service_tag_id INTEGER, good_service_tag_id INTEGER, account_id INTEGER, reason_id INTEGER, PRIMARY KEY ( bad_service_tag_id, account_id ) ) WITHOUT ROWID;' )
        self._create_index( petitioned_tag_siblings_table_name, [ 'account_id', 'reason_id' ] )
        
        #
        
        ( update_table_name ) = generate_repository_update_table_name( service_id )
        
        self._execute( 'CREATE TABLE ' + update_table_name + ' ( master_hash_id INTEGER PRIMARY KEY );' )
        
        self._repository_regenerate_service_info( service_id = service_id )
        
    
    def _repository_create_update( self, service_key, begin, end ):
        
        service_id = self._get_service_id( service_key )
        
        ( name, ) = self._execute( 'SELECT name FROM services WHERE service_id = ?;', ( service_id, ) ).fetchone()
        
        HydrusData.print_text( 'Creating update for ' + repr( name ) + ' from ' + HydrusTime.timestamp_to_pretty_time( begin, in_utc = True ) + ' to ' + HydrusTime.timestamp_to_pretty_time( end, in_utc = True ) )
        
        updates = self._repository_generate_updates( service_id, begin, end )
        
        update_hashes = []
        
        total_definition_rows = 0
        total_content_rows = 0
        
        if len( updates ) > 0:
            
            for update in updates:
                
                num_rows = update.GetNumRows()
                
                if isinstance( update, HydrusNetwork.DefinitionsUpdate ):
                    
                    total_definition_rows += num_rows
                    
                elif isinstance( update, HydrusNetwork.ContentUpdate ):
                    
                    total_content_rows += num_rows
                    
                
                update_bytes = update.dump_to_network_bytes()
                
                update_hash = hashlib.sha256( update_bytes ).digest()
                
                dest_path = ServerFiles.get_expected_file_path( update_hash )
                
                with open( dest_path, 'wb' ) as f:
                    
                    f.write( update_bytes )
                    
                
                update_hashes.append( update_hash )
                
            
            update_table_name = generate_repository_update_table_name( service_id )
            
            master_hash_ids = self._get_master_hash_ids( update_hashes )
            
            self._execute_many( 'INSERT OR IGNORE INTO ' + update_table_name + ' ( master_hash_id ) VALUES ( ? );', ( ( master_hash_id, ) for master_hash_id in master_hash_ids ) )
            
            for master_hash_id in master_hash_ids:
                
                self._clear_deferred_physical_delete_ids( file_master_hash_id = master_hash_id )
                
            
        
        HydrusData.print_text( 'Update OK. ' + HydrusNumbers.to_human_int( total_definition_rows ) + ' definition rows and ' + HydrusNumbers.to_human_int( total_content_rows ) + ' content rows in ' + HydrusNumbers.to_human_int( len( updates ) ) + ' update files.' )
        
        return update_hashes
        
    
    def _repository_delete_all_current_content( self, service_id, admin_account_id, subject_account_id ):
        
        we_deleted_everything = False
        
        time_started = HydrusTime.get_now_float()
        time_to_stop = time_started + 20
        
        num_rows_do_delete_at_a_time = 500
        
        now = HydrusTime.get_now()
        
        ( current_files_table_name, deleted_files_table_name, pending_files_table_name, petitioned_files_table_name, ip_addresses_table_name ) = generate_repository_files_table_names( service_id )
        
        query = 'SELECT service_hash_id FROM {} WHERE account_id = ? LIMIT {};'.format( current_files_table_name, num_rows_do_delete_at_a_time )
        
        service_hash_ids = self._stl( self._execute( query, ( subject_account_id, ) ) )
        
        while len( service_hash_ids ) > 0:
            
            self._repository_delete_files( service_id, admin_account_id, service_hash_ids, now )
            
            if HydrusTime.time_has_passed_float( time_to_stop ):
                
                return we_deleted_everything
                
            
            service_hash_ids = self._stl( self._execute( query, ( subject_account_id, ) ) )
            
        
        ( current_mappings_table_name, deleted_mappings_table_name, pending_mappings_table_name, petitioned_mappings_table_name ) = generate_repository_mappings_table_names( service_id )
        
        query = 'SELECT service_tag_id, service_hash_id FROM {} WHERE account_id = ? LIMIT {};'.format( current_mappings_table_name, num_rows_do_delete_at_a_time )
        
        mappings_dict = HydrusData.build_key_to_list_dict( self._execute( query, ( subject_account_id, ) ) )
        
        while len( mappings_dict ) > 0:
            
            for ( service_tag_id, service_hash_ids ) in mappings_dict.items():
                
                self._repository_Delete_mappings( service_id, admin_account_id, service_tag_id, service_hash_ids, now )
                
            
            if HydrusTime.time_has_passed_float( time_to_stop ):
                
                return we_deleted_everything
                
            
            mappings_dict = HydrusData.build_key_to_list_dict( self._execute( query, ( subject_account_id, ) ) )
            
        
        ( current_tag_parents_table_name, deleted_tag_parents_table_name, pending_tag_parents_table_name, petitioned_tag_parents_table_name ) = generate_repository_tag_parents_table_names( service_id )
        
        query = 'SELECT child_service_tag_id, parent_service_tag_id FROM {} WHERE account_id = ? LIMIT {};'.format( current_tag_parents_table_name, num_rows_do_delete_at_a_time )
        
        pairs = self._execute( query, ( subject_account_id, ) ).fetchall()
        
        while len( pairs ) > 0:
            
            for ( child_service_tag_id, parent_service_tag_id ) in pairs:
                
                self._repository_delete_tag_parent( service_id, admin_account_id, child_service_tag_id, parent_service_tag_id, now )
                
            
            if HydrusTime.time_has_passed_float( time_to_stop ):
                
                return we_deleted_everything
                
            
            pairs = self._execute( query, ( subject_account_id, ) ).fetchall()
            
        
        ( current_tag_siblings_table_name, deleted_tag_siblings_table_name, pending_tag_siblings_table_name, petitioned_tag_siblings_table_name ) = generate_repository_tag_siblings_table_names( service_id )
        
        query = 'SELECT bad_service_tag_id, good_service_tag_id FROM {} WHERE account_id = ? LIMIT {};'.format( current_tag_siblings_table_name, num_rows_do_delete_at_a_time )
        
        pairs = self._execute( query, ( subject_account_id, ) ).fetchall()
        
        while len( pairs ) > 0:
            
            for ( bad_service_tag_id, good_service_tag_id ) in pairs:
                
                self._repository_delete_tag_sibling( service_id, admin_account_id, bad_service_tag_id, good_service_tag_id, now )
                
            
            if HydrusTime.time_has_passed_float( time_to_stop ):
                
                return we_deleted_everything
                
            
            pairs = self._execute( query, ( subject_account_id, ) ).fetchall()
            
        
        we_deleted_everything = True
        
        return we_deleted_everything
        
    
    def _repository_delete_files( self, service_id, account_id, service_hash_ids, timestamp ):
        
        ( current_files_table_name, deleted_files_table_name, pending_files_table_name, petitioned_files_table_name, ip_addresses_table_name ) = generate_repository_files_table_names( service_id )
        
        with self._make_temporary_integer_table( service_hash_ids, 'service_hash_id' ) as temp_hash_ids_table_name:
            
            valid_service_hash_ids = self._stl( self._execute( 'SELECT service_hash_id FROM {} CROSS JOIN {} USING ( service_hash_id );'.format( temp_hash_ids_table_name, current_files_table_name ) ) )
            
        
        self._repository_reward_file_petitioners( service_id, valid_service_hash_ids, 1 )
        
        self._execute_many( 'DELETE FROM ' + current_files_table_name + ' WHERE service_hash_id = ?', ( ( service_hash_id, ) for service_hash_id in valid_service_hash_ids ) )
        
        self._repository_update_service_info( service_id, HC.SERVICE_INFO_NUM_FILES, - self._get_row_count() )
        
        self._repository_delete_raw_petitioned_file_rows( service_id, valid_service_hash_ids )
        
        self._execute_many( 'INSERT OR IGNORE INTO ' + deleted_files_table_name + ' ( service_hash_id, account_id, file_timestamp ) VALUES ( ?, ?, ? );', ( ( service_hash_id, account_id, timestamp ) for service_hash_id in valid_service_hash_ids ) )
        
        self._repository_update_service_info( service_id, HC.SERVICE_INFO_NUM_DELETED_FILES, self._get_row_count() )
        
        master_hash_ids = self._repository_get_master_hash_ids( service_id, valid_service_hash_ids )
        
        self._defer_files_delete_if_now_orphan( master_hash_ids )
        
    
    def _repository_Delete_mappings( self, service_id, account_id, service_tag_id, service_hash_ids, timestamp ):
        
        ( current_mappings_table_name, deleted_mappings_table_name, pending_mappings_table_name, petitioned_mappings_table_name ) = generate_repository_mappings_table_names( service_id )
        
        with self._make_temporary_integer_table( service_hash_ids, 'service_hash_id' ) as temp_hash_ids_table_name:
            
            valid_service_hash_ids = self._stl( self._execute( 'SELECT service_hash_id FROM {} CROSS JOIN {} USING ( service_hash_id ) WHERE service_tag_id = ?;'.format( temp_hash_ids_table_name, current_mappings_table_name ), ( service_tag_id, ) ) )
            
        
        self._repository_reward_mapping_petitioners( service_id, service_tag_id, valid_service_hash_ids, 1 )
        
        self._execute_many( 'DELETE FROM ' + current_mappings_table_name + ' WHERE service_tag_id = ? AND service_hash_id = ?;', ( ( service_tag_id, service_hash_id ) for service_hash_id in valid_service_hash_ids ) )
        
        self._repository_update_service_info( service_id, HC.SERVICE_INFO_NUM_MAPPINGS, - self._get_row_count() )
        
        self._repository_delete_raw_petitioned_mapping_rows( service_id, service_tag_id, valid_service_hash_ids )
        
        self._execute_many( 'INSERT OR IGNORE INTO ' + deleted_mappings_table_name + ' ( service_tag_id, service_hash_id, account_id, mapping_timestamp ) VALUES ( ?, ?, ?, ? );', ( ( service_tag_id, service_hash_id, account_id, timestamp ) for service_hash_id in valid_service_hash_ids ) )
        
        self._repository_update_service_info( service_id, HC.SERVICE_INFO_NUM_DELETED_MAPPINGS, self._get_row_count() )
        
    
    def _repository_delete_raw_pending_tag_parent_rows( self, service_id: int, child_master_tag_id: int, parent_master_tag_id: int ):
        
        ( current_tag_parents_table_name, deleted_tag_parents_table_name, pending_tag_parents_table_name, petitioned_tag_parents_table_name ) = generate_repository_tag_parents_table_names( service_id )
        
        account_ids = self._repository_get_account_ids_with_probable_actionable_add_tag_parent_petitions( service_id, child_master_tag_id, parent_master_tag_id )
        
        pre_change_count = self._repository_get_count_of_actionable_add_tag_parent_petitions_for_accounts( service_id, account_ids )
        
        self._execute( 'DELETE FROM {} WHERE child_master_tag_id = ? AND parent_master_tag_id = ?;'.format( pending_tag_parents_table_name ), ( child_master_tag_id, parent_master_tag_id ) )
        
        self._repository_update_service_info( service_id, HC.SERVICE_INFO_NUM_PENDING_TAG_PARENTS, - self._get_row_count() )
        
        post_change_count = self._repository_get_count_of_actionable_add_tag_parent_petitions_for_accounts( service_id, account_ids )
        
        self._repository_update_service_info( service_id, HC.SERVICE_INFO_NUM_ACTIONABLE_PARENT_ADD_PETITIONS, post_change_count - pre_change_count )
        
    
    def _repository_delete_raw_pending_tag_sibling_rows( self, service_id: int, bad_master_tag_id: int, good_master_tag_id: int ):
        
        ( current_tag_siblings_table_name, deleted_tag_siblings_table_name, pending_tag_siblings_table_name, petitioned_tag_siblings_table_name ) = generate_repository_tag_siblings_table_names( service_id )
        
        account_ids = self._repository_get_account_ids_with_probable_actionable_add_tag_sibling_petitions( service_id, bad_master_tag_id, good_master_tag_id )
        
        pre_change_count = self._repository_get_count_of_actionable_add_tag_sibling_petitions_for_accounts( service_id, account_ids )
        
        self._execute( 'DELETE FROM {} WHERE bad_master_tag_id = ? AND good_master_tag_id = ?;'.format( pending_tag_siblings_table_name ), ( bad_master_tag_id, good_master_tag_id ) )
        
        self._repository_update_service_info( service_id, HC.SERVICE_INFO_NUM_PENDING_TAG_SIBLINGS, - self._get_row_count() )
        
        post_change_count = self._repository_get_count_of_actionable_add_tag_sibling_petitions_for_accounts( service_id, account_ids )
        
        self._repository_update_service_info( service_id, HC.SERVICE_INFO_NUM_ACTIONABLE_SIBLING_ADD_PETITIONS, post_change_count - pre_change_count )
        
    
    def _repository_delete_raw_petitioned_file_rows( self, service_id: int, service_hash_ids: collections.abc.Collection[ int ] ):
        
        ( current_files_table_name, deleted_files_table_name, pending_files_table_name, petitioned_files_table_name, ip_addresses_table_name ) = generate_repository_files_table_names( service_id )
        
        with self._make_temporary_integer_table( service_hash_ids, 'service_hash_id' ) as temp_hash_ids_table_name:
            
            account_ids = self._repository_get_account_ids_with_actionable_delete_file_petitions( service_id, temp_hash_ids_table_name )
            
        
        pre_change_count = self._repository_get_count_of_actionable_delete_file_petitions_for_accounts( service_id, account_ids )
        
        self._execute_many( 'DELETE FROM {} WHERE service_hash_id = ?'.format( petitioned_files_table_name ), ( ( service_hash_id, ) for service_hash_id in service_hash_ids ) )
        
        self._repository_update_service_info( service_id, HC.SERVICE_INFO_NUM_PETITIONED_FILES, - self._get_row_count() )
        
        post_change_count = self._repository_get_count_of_actionable_delete_file_petitions_for_accounts( service_id, account_ids )
        
        self._repository_update_service_info( service_id, HC.SERVICE_INFO_NUM_ACTIONABLE_FILE_DELETE_PETITIONS, post_change_count - pre_change_count )
        
    
    def _repository_delete_raw_petitioned_mapping_rows( self, service_id: int, service_tag_id: int, service_hash_ids: collections.abc.Collection[ int ] ):
        
        ( current_mappings_table_name, deleted_mappings_table_name, pending_mappings_table_name, petitioned_mappings_table_name ) = generate_repository_mappings_table_names( service_id )
        
        with self._make_temporary_integer_table( service_hash_ids, 'service_hash_id' ) as temp_hash_ids_table_name:
            
            account_ids = self._repository_get_account_ids_with_actionable_delete_mapping_petitions( service_id, service_tag_id, temp_hash_ids_table_name )
            
        
        pre_change_count = self._repository_get_count_of_actionable_delete_mapping_petitions_for_accounts( service_id, service_tag_id, account_ids )
        
        self._execute_many( 'DELETE FROM {} WHERE service_tag_id = ? AND service_hash_id = ?;'.format( petitioned_mappings_table_name ), ( ( service_tag_id, service_hash_id ) for service_hash_id in service_hash_ids ) )
        
        self._repository_update_service_info( service_id, HC.SERVICE_INFO_NUM_PETITIONED_MAPPINGS, - self._get_row_count() )
        
        post_change_count = self._repository_get_count_of_actionable_delete_mapping_petitions_for_accounts( service_id, service_tag_id, account_ids )
        
        self._repository_update_service_info( service_id, HC.SERVICE_INFO_NUM_ACTIONABLE_MAPPING_DELETE_PETITIONS, post_change_count - pre_change_count )
        
    
    def _repository_delete_raw_petitioned_tag_parent_rows( self, service_id: int, child_service_tag_id: int, parent_service_tag_id: int ):
        
        ( current_tag_parents_table_name, deleted_tag_parents_table_name, pending_tag_parents_table_name, petitioned_tag_parents_table_name ) = generate_repository_tag_parents_table_names( service_id )
        
        account_ids = self._repository_get_account_ids_with_actionable_delete_tag_parent_petitions( service_id, child_service_tag_id, parent_service_tag_id )
        
        pre_add_change_count = self._repository_get_count_of_actionable_add_tag_parent_petitions_for_accounts( service_id, account_ids )
        
        pre_delete_change_count = self._repository_get_count_of_actionable_delete_tag_parent_petitions_for_accounts( service_id, account_ids )
        
        self._execute( 'DELETE FROM {} WHERE child_service_tag_id = ? AND parent_service_tag_id = ?;'.format( petitioned_tag_parents_table_name ), ( child_service_tag_id, parent_service_tag_id ) )
        
        self._repository_update_service_info( service_id, HC.SERVICE_INFO_NUM_PETITIONED_TAG_PARENTS, - self._get_row_count() )
        
        post_add_change_count = self._repository_get_count_of_actionable_add_tag_parent_petitions_for_accounts( service_id, account_ids )
        
        post_delete_change_count = self._repository_get_count_of_actionable_delete_tag_parent_petitions_for_accounts( service_id, account_ids )
        
        self._repository_update_service_info( service_id, HC.SERVICE_INFO_NUM_ACTIONABLE_PARENT_DELETE_PETITIONS, post_delete_change_count - pre_delete_change_count )
        
        self._repository_update_service_info( service_id, HC.SERVICE_INFO_NUM_ACTIONABLE_PARENT_ADD_PETITIONS, post_add_change_count - pre_add_change_count )
        
    
    def _repository_delete_raw_petitioned_tag_sibling_rows( self, service_id: int, bad_service_tag_id: int, good_service_tag_id: int ):
        
        ( current_tag_siblings_table_name, deleted_tag_siblings_table_name, pending_tag_siblings_table_name, petitioned_tag_siblings_table_name ) = generate_repository_tag_siblings_table_names( service_id )
        
        account_ids = self._repository_get_account_ids_with_actionable_delete_tag_sibling_petitions( service_id, bad_service_tag_id, good_service_tag_id )
        
        pre_add_change_count = self._repository_get_count_of_actionable_add_tag_sibling_petitions_for_accounts( service_id, account_ids )
        
        pre_delete_change_count = self._repository_get_count_of_actionable_delete_tag_sibling_petitions_for_accounts( service_id, account_ids )
        
        self._execute( 'DELETE FROM {} WHERE bad_service_tag_id = ? AND good_service_tag_id = ?;'.format( petitioned_tag_siblings_table_name ), ( bad_service_tag_id, good_service_tag_id ) )
        
        self._repository_update_service_info( service_id, HC.SERVICE_INFO_NUM_PETITIONED_TAG_SIBLINGS, - self._get_row_count() )
        
        post_add_change_count = self._repository_get_count_of_actionable_add_tag_sibling_petitions_for_accounts( service_id, account_ids )
        
        post_delete_change_count = self._repository_get_count_of_actionable_delete_tag_sibling_petitions_for_accounts( service_id, account_ids )
        
        self._repository_update_service_info( service_id, HC.SERVICE_INFO_NUM_ACTIONABLE_SIBLING_DELETE_PETITIONS, post_delete_change_count - pre_delete_change_count )
        
        self._repository_update_service_info( service_id, HC.SERVICE_INFO_NUM_ACTIONABLE_SIBLING_ADD_PETITIONS, post_add_change_count - pre_add_change_count )
        
    
    def _repository_delete_tag_parent( self, service_id, account_id, child_service_tag_id, parent_service_tag_id, timestamp ):
        
        ( current_tag_parents_table_name, deleted_tag_parents_table_name, pending_tag_parents_table_name, petitioned_tag_parents_table_name ) = generate_repository_tag_parents_table_names( service_id )
        
        self._repository_reward_tag_parent_petitioners( service_id, child_service_tag_id, parent_service_tag_id, 1 )
        
        self._execute( 'DELETE FROM ' + current_tag_parents_table_name + ' WHERE child_service_tag_id = ? AND parent_service_tag_id = ?;', ( child_service_tag_id, parent_service_tag_id ) )
        
        self._repository_update_service_info( service_id, HC.SERVICE_INFO_NUM_TAG_PARENTS, - self._get_row_count() )
        
        self._repository_delete_raw_petitioned_tag_parent_rows( service_id, child_service_tag_id, parent_service_tag_id )
        
        self._execute( 'INSERT OR IGNORE INTO ' + deleted_tag_parents_table_name + ' ( child_service_tag_id, parent_service_tag_id, account_id, parent_timestamp ) VALUES ( ?, ?, ?, ? );', ( child_service_tag_id, parent_service_tag_id, account_id, timestamp ) )
        
        self._repository_update_service_info( service_id, HC.SERVICE_INFO_NUM_DELETED_TAG_PARENTS, self._get_row_count() )
        
    
    def _repository_delete_tag_sibling( self, service_id, account_id, bad_service_tag_id, good_service_tag_id, timestamp ):
        
        ( current_tag_siblings_table_name, deleted_tag_siblings_table_name, pending_tag_siblings_table_name, petitioned_tag_siblings_table_name ) = generate_repository_tag_siblings_table_names( service_id )
        
        self._repository_reward_tag_sibling_petitioners( service_id, bad_service_tag_id, good_service_tag_id, 1 )
        
        self._execute( 'DELETE FROM ' + current_tag_siblings_table_name + ' WHERE bad_service_tag_id = ? AND good_service_tag_id = ?;', ( bad_service_tag_id, good_service_tag_id ) )
        
        self._repository_update_service_info( service_id, HC.SERVICE_INFO_NUM_TAG_SIBLINGS, - self._get_row_count() )
        
        self._repository_delete_raw_petitioned_tag_sibling_rows( service_id, bad_service_tag_id, good_service_tag_id )
        
        self._execute( 'INSERT OR IGNORE INTO ' + deleted_tag_siblings_table_name + ' ( bad_service_tag_id, good_service_tag_id, account_id, sibling_timestamp ) VALUES ( ?, ?, ?, ? );', ( bad_service_tag_id, good_service_tag_id, account_id, timestamp ) )
        
        self._repository_update_service_info( service_id, HC.SERVICE_INFO_NUM_DELETED_TAG_SIBLINGS, self._get_row_count() )
        
    
    def _repository_deny_file_petition( self, service_id, service_hash_ids ):
        
        self._repository_reward_file_petitioners( service_id, service_hash_ids, -1 )
        
        self._repository_delete_raw_petitioned_file_rows( service_id, service_hash_ids )
        
    
    def _repository_deny_mapping_petition( self, service_id, service_tag_id, service_hash_ids ):
        
        self._repository_reward_mapping_petitioners( service_id, service_tag_id, service_hash_ids, -1 )
        
        self._repository_delete_raw_petitioned_mapping_rows( service_id, service_tag_id, service_hash_ids )
        
    
    def _repository_deny_tag_parent_pend( self, service_id, child_master_tag_id, parent_master_tag_id ):
        
        self._repository_reward_tag_parent_penders( service_id, child_master_tag_id, parent_master_tag_id, -1 )
        
        self._repository_delete_raw_pending_tag_parent_rows( service_id, child_master_tag_id, parent_master_tag_id )
        
    
    def _repository_deny_tag_parent_petition( self, service_id, child_service_tag_id, parent_service_tag_id ):
        
        self._repository_reward_tag_parent_petitioners( service_id, child_service_tag_id, parent_service_tag_id, -1 )
        
        self._repository_delete_raw_petitioned_tag_parent_rows( service_id, child_service_tag_id, parent_service_tag_id )
        
    
    def _repository_deny_tag_sibling_pend( self, service_id, bad_master_tag_id, good_master_tag_id ):
        
        self._repository_reward_tag_sibling_penders( service_id, bad_master_tag_id, good_master_tag_id, -1 )
        
        self._repository_delete_raw_pending_tag_sibling_rows( service_id, bad_master_tag_id, good_master_tag_id )
        
    
    def _repository_deny_tag_sibling_petition( self, service_id, bad_service_tag_id, good_service_tag_id ):
        
        self._repository_reward_tag_sibling_petitioners( service_id, bad_service_tag_id, good_service_tag_id, -1 )
        
        self._repository_delete_raw_petitioned_tag_sibling_rows( service_id, bad_service_tag_id, good_service_tag_id )
        
    
    def _repository_drop( self, service_id ):
        
        ( hash_id_map_table_name, tag_id_map_table_name ) = generate_repository_master_map_table_names( service_id )
        ( current_files_table_name, deleted_files_table_name, pending_files_table_name, petitioned_files_table_name, ip_addresses_table_name ) = generate_repository_files_table_names( service_id )
        
        for ( block_of_master_hash_ids, num_done, num_to_do ) in HydrusDB.read_large_id_query_in_separate_chunks( self._c, 'SELECT master_hash_id FROM {} CROSS JOIN {} USING ( service_hash_id );'.format( current_files_table_name, hash_id_map_table_name ), 1024 ):
            
            self._defer_files_delete_if_now_orphan( block_of_master_hash_ids, ignore_service_id = service_id )
            
        
        update_table_name = generate_repository_update_table_name( service_id )
        
        for ( block_of_master_hash_ids, num_done, num_to_do ) in HydrusDB.read_large_id_query_in_separate_chunks( self._c, 'SELECT master_hash_id FROM {};'.format( update_table_name ), 1024 ):
            
            self._defer_files_delete_if_now_orphan( block_of_master_hash_ids, definitely_no_thumbnails = True, ignore_service_id = service_id )
            
        
        #
        
        table_names = []
        
        table_names.extend( generate_repository_master_map_table_names( service_id ) )
        
        table_names.extend( generate_repository_files_table_names( service_id ) )
        
        table_names.extend( generate_repository_mappings_table_names( service_id ) )
        
        table_names.extend( generate_repository_tag_parents_table_names( service_id ) )
        
        table_names.extend( generate_repository_tag_siblings_table_names( service_id ) )
        
        table_names.append( generate_repository_update_table_name( service_id ) )
        
        for table_name in table_names:
            
            self._execute( 'DROP TABLE ' + table_name + ';' )
            
        
        self._execute( 'DELETE FROM service_info WHERE service_id = ?;', ( service_id, ) )
        
    
    def _repository_generate_immediate_update( self, service_key, account, begin, end ):
        
        service_id = self._get_service_id( service_key )
        
        updates = self._repository_generate_updates( service_id, begin, end )
        
        return updates
        
    
    def _repository_generate_updates( self, service_id, begin, end ):
        
        MAX_DEFINITIONS_ROWS = 50000
        MAX_CONTENT_ROWS = 250000
        
        MAX_CONTENT_CHUNK = 25000
        
        updates = []
        
        definitions_update_builder = HydrusNetwork.UpdateBuilder( HydrusNetwork.DefinitionsUpdate, MAX_DEFINITIONS_ROWS )
        content_update_builder = HydrusNetwork.UpdateBuilder( HydrusNetwork.ContentUpdate, MAX_CONTENT_ROWS )
        
        ( service_hash_ids_table_name, service_tag_ids_table_name ) = generate_repository_master_map_table_names( service_id )
        
        for ( service_hash_id, hash ) in self._execute( 'SELECT service_hash_id, hash FROM ' + service_hash_ids_table_name + ' NATURAL JOIN hashes WHERE hash_id_timestamp BETWEEN ? AND ?;', ( begin, end ) ):
            
            row = ( HC.DEFINITIONS_TYPE_HASHES, service_hash_id, hash )
            
            definitions_update_builder.add_row( row )
            
        
        for ( service_tag_id, tag ) in self._execute( 'SELECT service_tag_id, tag FROM ' + service_tag_ids_table_name + ' NATURAL JOIN tags WHERE tag_id_timestamp BETWEEN ? AND ?;', ( begin, end ) ):
            
            row = ( HC.DEFINITIONS_TYPE_TAGS, service_tag_id, tag )
            
            definitions_update_builder.add_row( row )
            
        
        definitions_update_builder.finish()
        
        updates.extend( definitions_update_builder.get_updates() )
        
        #
        
        ( current_files_table_name, deleted_files_table_name, pending_files_table_name, petitioned_files_table_name, ip_addresses_table_name ) = generate_repository_files_table_names( service_id )
        
        table_join = self._repository_get_files_info_files_table_join( service_id, HC.CONTENT_STATUS_CURRENT )
        
        for ( service_hash_id, size, mime, timestamp, width, height, duration_ms, num_frames, num_words ) in self._execute( 'SELECT service_hash_id, size, mime, file_timestamp, width, height, duration, num_frames, num_words FROM ' + table_join + ' WHERE file_timestamp BETWEEN ? AND ?;', ( begin, end ) ):
            
            file_row = ( service_hash_id, size, mime, timestamp, width, height, duration_ms, num_frames, num_words )
            
            content_update_builder.add_row( ( HC.CONTENT_TYPE_FILES, HC.CONTENT_UPDATE_ADD, file_row ) )
            
        
        service_hash_ids = [ service_hash_id for ( service_hash_id, ) in self._execute( 'SELECT service_hash_id FROM ' + deleted_files_table_name + ' WHERE file_timestamp BETWEEN ? AND ?;', ( begin, end ) ) ]
        
        for service_hash_id in service_hash_ids:
            
            content_update_builder.add_row( ( HC.CONTENT_TYPE_FILES, HC.CONTENT_UPDATE_DELETE, service_hash_id ) )
            
        
        #
        
        ( current_mappings_table_name, deleted_mappings_table_name, pending_mappings_table_name, petitioned_mappings_table_name ) = generate_repository_mappings_table_names( service_id )
        
        service_tag_ids_to_service_hash_ids = HydrusData.build_key_to_list_dict( self._execute( 'SELECT service_tag_id, service_hash_id FROM ' + current_mappings_table_name + ' WHERE mapping_timestamp BETWEEN ? AND ?;', ( begin, end ) ) )
        
        for ( service_tag_id, service_hash_ids ) in list(service_tag_ids_to_service_hash_ids.items()):
            
            for block_of_service_hash_ids in HydrusLists.split_list_into_chunks( service_hash_ids, MAX_CONTENT_CHUNK ):
                
                row_weight = len( block_of_service_hash_ids )
                
                content_update_builder.add_row( ( HC.CONTENT_TYPE_MAPPINGS, HC.CONTENT_UPDATE_ADD, ( service_tag_id, block_of_service_hash_ids ) ), row_weight )
                
            
        
        service_tag_ids_to_service_hash_ids = HydrusData.build_key_to_list_dict( self._execute( 'SELECT service_tag_id, service_hash_id FROM ' + deleted_mappings_table_name + ' WHERE mapping_timestamp BETWEEN ? AND ?;', ( begin, end ) ) )
        
        for ( service_tag_id, service_hash_ids ) in list(service_tag_ids_to_service_hash_ids.items()):
            
            for block_of_service_hash_ids in HydrusLists.split_list_into_chunks( service_hash_ids, MAX_CONTENT_CHUNK ):
                
                row_weight = len( block_of_service_hash_ids )
                
                content_update_builder.add_row( ( HC.CONTENT_TYPE_MAPPINGS, HC.CONTENT_UPDATE_DELETE, ( service_tag_id, block_of_service_hash_ids ) ), row_weight )
                
            
        
        #
        
        ( current_tag_parents_table_name, deleted_tag_parents_table_name, pending_tag_parents_table_name, petitioned_tag_parents_table_name ) = generate_repository_tag_parents_table_names( service_id )
        
        pairs = self._execute( 'SELECT child_service_tag_id, parent_service_tag_id FROM ' + current_tag_parents_table_name + ' WHERE parent_timestamp BETWEEN ? AND ?;', ( begin, end ) ).fetchall()
        
        for pair in pairs:
            
            content_update_builder.add_row( ( HC.CONTENT_TYPE_TAG_PARENTS, HC.CONTENT_UPDATE_ADD, pair ) )
            
        
        pairs = self._execute( 'SELECT child_service_tag_id, parent_service_tag_id FROM ' + deleted_tag_parents_table_name + ' WHERE parent_timestamp BETWEEN ? AND ?;', ( begin, end ) ).fetchall()
        
        for pair in pairs:
            
            content_update_builder.add_row( ( HC.CONTENT_TYPE_TAG_PARENTS, HC.CONTENT_UPDATE_DELETE, pair ) )
            
        
        #
        
        ( current_tag_siblings_table_name, deleted_tag_siblings_table_name, pending_tag_siblings_table_name, petitioned_tag_siblings_table_name ) = generate_repository_tag_siblings_table_names( service_id )
        
        pairs = self._execute( 'SELECT bad_service_tag_id, good_service_tag_id FROM ' + current_tag_siblings_table_name + ' WHERE sibling_timestamp BETWEEN ? AND ?;', ( begin, end ) ).fetchall()
        
        for pair in pairs:
            
            content_update_builder.add_row( ( HC.CONTENT_TYPE_TAG_SIBLINGS, HC.CONTENT_UPDATE_ADD, pair ) )
            
        
        pairs = self._execute( 'SELECT bad_service_tag_id, good_service_tag_id FROM ' + deleted_tag_siblings_table_name + ' WHERE sibling_timestamp BETWEEN ? AND ?;', ( begin, end ) ).fetchall()
        
        for pair in pairs:
            
            content_update_builder.add_row( ( HC.CONTENT_TYPE_TAG_SIBLINGS, HC.CONTENT_UPDATE_DELETE, pair ) )
            
        
        #
        
        content_update_builder.finish()
        
        updates.extend( content_update_builder.get_updates() )
        
        return updates
        
    
    def _repository_get_account_ids_with_probable_actionable_add_tag_sibling_petitions( self, service_id: int, bad_master_tag_id: int, good_master_tag_id: int ):
        
        # this isn't precise, but being precise takes a bunch of work, you have to do SELECT DISTINCT account_id, reason_id from pending ... EXCEPT SELECT DISTINCT account_id, reason_id from petitioned
        
        ( current_tag_siblings_table_name, deleted_tag_siblings_table_name, pending_tag_siblings_table_name, petitioned_tag_siblings_table_name ) = generate_repository_tag_siblings_table_names( service_id )
        
        account_ids = self._sts( self._execute( 'SELECT DISTINCT account_id FROM {} WHERE bad_master_tag_id = ? AND good_master_tag_id = ?;'.format( pending_tag_siblings_table_name ), ( bad_master_tag_id, good_master_tag_id ) ) )
        
        return account_ids
        
    
    def _repository_get_account_ids_with_probable_actionable_add_tag_parent_petitions( self, service_id: int, child_master_tag_id: int, parent_master_tag_id: int ):
        
        # this isn't precise, but being precise takes a bunch of work, you have to do SELECT DISTINCT account_id, reason_id from pending ... EXCEPT SELECT DISTINCT account_id, reason_id from petitioned
        
        ( current_tag_parents_table_name, deleted_tag_parents_table_name, pending_tag_parents_table_name, petitioned_tag_parents_table_name ) = generate_repository_tag_parents_table_names( service_id )
        
        account_ids = self._sts( self._execute( 'SELECT DISTINCT account_id FROM {} WHERE child_master_tag_id = ? AND parent_master_tag_id = ?;'.format( pending_tag_parents_table_name ), ( child_master_tag_id, parent_master_tag_id ) ) )
        
        return account_ids
        
    
    def _repository_get_account_ids_with_actionable_delete_file_petitions( self, service_id, service_hash_ids_table_name ):
        
        ( current_files_table_name, deleted_files_table_name, pending_files_table_name, petitioned_files_table_name, ip_addresses_table_name ) = generate_repository_files_table_names( service_id )
        
        account_ids = self._sts( self._execute( 'SELECT DISTINCT account_id FROM {} CROSS JOIN {} USING ( service_hash_id );'.format( petitioned_files_table_name, service_hash_ids_table_name ) ) )
        
        return account_ids
        
    
    def _repository_get_account_ids_with_actionable_delete_mapping_petitions( self, service_id, service_tag_id, service_hash_ids_table_name ):
        
        ( current_mappings_table_name, deleted_mappings_table_name, pending_mappings_table_name, petitioned_mappings_table_name ) = generate_repository_mappings_table_names( service_id )
        
        account_ids = self._sts( self._execute( 'SELECT DISTINCT account_id FROM {} CROSS JOIN {} USING ( service_hash_id ) WHERE service_tag_id = ?;'.format( petitioned_mappings_table_name, service_hash_ids_table_name ), ( service_tag_id, ) ) )
        
        return account_ids
        
    
    def _repository_get_account_ids_with_actionable_delete_tag_parent_petitions( self, service_id: int, child_service_tag_id: int, parent_service_tag_id: int ):
        
        ( current_tag_parents_table_name, deleted_tag_parents_table_name, pending_tag_parents_table_name, petitioned_tag_parents_table_name ) = generate_repository_tag_parents_table_names( service_id )
        
        account_ids = self._sts( self._execute( 'SELECT DISTINCT account_id FROM {} WHERE child_service_tag_id = ? AND parent_service_tag_id = ?;'.format( petitioned_tag_parents_table_name ), ( child_service_tag_id, parent_service_tag_id ) ) )
        
        return account_ids
        
    
    def _repository_get_account_ids_with_actionable_delete_tag_sibling_petitions( self, service_id: int, bad_service_tag_id: int, good_service_tag_id: int ):
        
        ( current_tag_siblings_table_name, deleted_tag_siblings_table_name, pending_tag_siblings_table_name, petitioned_tag_siblings_table_name ) = generate_repository_tag_siblings_table_names( service_id )
        
        account_ids = self._sts( self._execute( 'SELECT DISTINCT account_id FROM {} WHERE bad_service_tag_id = ? AND good_service_tag_id = ?;'.format( petitioned_tag_siblings_table_name ), ( bad_service_tag_id, good_service_tag_id ) ) )
        
        return account_ids
        
    
    def _repository_get_account_info( self, service_id, account_id ):
        
        service_type = self._get_service_type( service_id )
        
        ( hash_id_map_table_name, tag_id_map_table_name ) = generate_repository_master_map_table_names( service_id )
        
        account_info = {}
        
        if service_type == HC.FILE_REPOSITORY:
            
            ( current_files_table_name, deleted_files_table_name, pending_files_table_name, petitioned_files_table_name, ip_addresses_table_name ) = generate_repository_files_table_names( service_id )
            
            table_join = 'files_info NATURAL JOIN {} NATURAL JOIN {}'.format( hash_id_map_table_name, current_files_table_name )
            
            ( num_files, num_files_bytes ) = self._execute( 'SELECT COUNT( * ), SUM( size ) FROM ' + table_join + ' WHERE account_id = ?;', ( account_id, ) ).fetchone()
            
            if num_files_bytes is None:
                
                num_files_bytes = 0
                
            
            account_info[ 'num_files' ] = num_files
            account_info[ 'num_files_bytes' ] = num_files_bytes
            
        elif service_type == HC.TAG_REPOSITORY:
            
            ( current_mappings_table_name, deleted_mappings_table_name, pending_mappings_table_name, petitioned_mappings_table_name ) = generate_repository_mappings_table_names( service_id )
            
            num_mappings = len( self._execute( 'SELECT 1 FROM {} WHERE account_id = ? LIMIT 5000;'.format( current_mappings_table_name ), ( account_id, ) ).fetchall() )
            
            account_info[ 'num_mappings' ] = num_mappings
            
            num_petitioned_mappings = len( self._execute( 'SELECT 1 FROM {} WHERE account_id = ? LIMIT 5000;'.format( petitioned_mappings_table_name ), ( account_id, ) ).fetchall() )
            
            account_info[ 'num_mappings_petitioned' ] = num_petitioned_mappings
            
            ( current_tag_siblings_table_name, deleted_tag_siblings_table_name, pending_tag_siblings_table_name, petitioned_tag_siblings_table_name ) = generate_repository_tag_siblings_table_names( service_id )

            ( num_siblings, ) = self._execute( 'SELECT COUNT( * ) FROM {} WHERE account_id = ?;'.format( current_tag_siblings_table_name ), ( account_id, ) ).fetchone()
            
            account_info[ 'num_siblings' ] = num_siblings
            
            ( num_pending_siblings, ) = self._execute( 'SELECT COUNT( * ) FROM {} WHERE account_id = ?;'.format( pending_tag_siblings_table_name ), ( account_id, ) ).fetchone()
            
            account_info[ 'num_siblings_pending' ] = num_pending_siblings
            
            ( num_petitioned_siblings, ) = self._execute( 'SELECT COUNT( * ) FROM {} WHERE account_id = ?;'.format( petitioned_tag_siblings_table_name ), ( account_id, ) ).fetchone()
            
            account_info[ 'num_siblings_petitioned' ] = num_petitioned_siblings
            
            ( current_tag_parents_table_name, deleted_tag_parents_table_name, pending_tag_parents_table_name, petitioned_tag_parents_table_name ) = generate_repository_tag_parents_table_names( service_id )
            
            ( num_parents, ) = self._execute( 'SELECT COUNT( * ) FROM {} WHERE account_id = ?;'.format( current_tag_parents_table_name ), ( account_id, ) ).fetchone()
            
            account_info[ 'num_parents' ] = num_parents
            
            ( num_pending_parents, ) = self._execute( 'SELECT COUNT( * ) FROM {} WHERE account_id = ?;'.format( pending_tag_parents_table_name ), ( account_id, ) ).fetchone()
            
            account_info[ 'num_parents_pending' ] = num_pending_parents
            
            ( num_petitioned_parents, ) = self._execute( 'SELECT COUNT( * ) FROM {} WHERE account_id = ?;'.format( petitioned_tag_parents_table_name ), ( account_id, ) ).fetchone()
            
            account_info[ 'num_parents_petitioned' ] = num_petitioned_parents
            
        
        #
        
        result = self._execute( 'SELECT score FROM account_scores WHERE service_id = ? AND account_id = ? AND score_type = ?;', ( service_id, account_id, HC.SCORE_PETITION ) ).fetchone()
        
        if result is None: petition_score = 0
        else: ( petition_score, ) = result
        
        account_info[ 'petition_score' ] = petition_score
        
        return account_info
        
    
    def _repository_get_count_of_actionable_add_tag_parent_petitions_for_accounts( self, service_id: int, account_ids: collections.abc.Collection[ int ] ):
        
        ( current_tag_parents_table_name, deleted_tag_parents_table_name, pending_tag_parents_table_name, petitioned_tag_parents_table_name ) = generate_repository_tag_parents_table_names( service_id )
        
        total = 0
        
        for account_id in account_ids:
            
            ( count, ) = self._execute( 'SELECT COUNT( * ) FROM ( SELECT DISTINCT reason_id FROM {} WHERE account_id = ? EXCEPT SELECT DISTINCT reason_id FROM {} WHERE account_id = ? );'.format( pending_tag_parents_table_name, petitioned_tag_parents_table_name ), ( account_id, account_id ) ).fetchone()
            
            total += count
            
        
        return total
        
    
    def _repository_get_count_of_actionable_add_tag_sibling_petitions_for_accounts( self, service_id: int, account_ids: collections.abc.Collection[ int ] ):
        
        ( current_tag_siblings_table_name, deleted_tag_siblings_table_name, pending_tag_siblings_table_name, petitioned_tag_siblings_table_name ) = generate_repository_tag_siblings_table_names( service_id )
        
        total = 0
        
        for account_id in account_ids:
            
            ( count, ) = self._execute( 'SELECT COUNT( * ) FROM ( SELECT DISTINCT reason_id FROM {} WHERE account_id = ? EXCEPT SELECT DISTINCT reason_id FROM {} WHERE account_id = ? );'.format( pending_tag_siblings_table_name, petitioned_tag_siblings_table_name ), ( account_id, account_id ) ).fetchone()
            
            total += count
            
        
        return total
        
    
    def _repository_get_count_of_actionable_delete_file_petitions_for_accounts( self, service_id: int, account_ids: collections.abc.Collection[ int ] ):
        
        ( current_files_table_name, deleted_files_table_name, pending_files_table_name, petitioned_files_table_name, ip_addresses_table_name ) = generate_repository_files_table_names( service_id )
        
        total = 0
        
        for account_id in account_ids:
            
            ( count, ) = self._execute( 'SELECT COUNT( * ) FROM ( SELECT DISTINCT reason_id FROM {} WHERE account_id = ? );'.format( petitioned_files_table_name ), ( account_id, ) ).fetchone()
            
            total += count
            
        
        return total
        
    
    def _repository_get_count_of_actionable_delete_mapping_petitions_for_accounts( self, service_id: int, service_tag_id: int, account_ids: collections.abc.Collection[ int ] ):
        
        ( current_mappings_table_name, deleted_mappings_table_name, pending_mappings_table_name, petitioned_mappings_table_name ) = generate_repository_mappings_table_names( service_id )
        
        total = 0
        
        for account_id in account_ids:
            
            ( count, ) = self._execute( 'SELECT COUNT( * ) FROM ( SELECT DISTINCT reason_id FROM {} WHERE account_id = ? AND service_tag_id = ? );'.format( petitioned_mappings_table_name ), ( account_id, service_tag_id ) ).fetchone()
            
            total += count
            
        
        return total
        
    
    def _repository_get_count_of_actionable_delete_tag_parent_petitions_for_accounts( self, service_id: int, account_ids: collections.abc.Collection[ int ] ):
        
        ( current_tag_parents_table_name, deleted_tag_parents_table_name, pending_tag_parents_table_name, petitioned_tag_parents_table_name ) = generate_repository_tag_parents_table_names( service_id )
        
        total = 0
        
        for account_id in account_ids:
            
            ( count, ) = self._execute( 'SELECT COUNT( * ) FROM ( SELECT DISTINCT reason_id FROM {} WHERE account_id = ? );'.format( petitioned_tag_parents_table_name ), ( account_id, ) ).fetchone()
            
            total += count
            
        
        return total
        
    
    def _repository_get_count_of_actionable_delete_tag_sibling_petitions_for_accounts( self, service_id: int, account_ids: collections.abc.Collection[ int ] ):
        
        ( current_tag_siblings_table_name, deleted_tag_siblings_table_name, pending_tag_siblings_table_name, petitioned_tag_siblings_table_name ) = generate_repository_tag_siblings_table_names( service_id )
        
        total = 0
        
        for account_id in account_ids:
            
            ( count, ) = self._execute( 'SELECT COUNT( * ) FROM ( SELECT DISTINCT reason_id FROM {} WHERE account_id = ? );'.format( petitioned_tag_siblings_table_name ), ( account_id, ) ).fetchone()
            
            total += count
            
        
        return total
        
    
    def _repository_get_current_mappings_count( self, service_id, service_tag_id ):
        
        ( current_mappings_table_name, deleted_mappings_table_name, pending_mappings_table_name, petitioned_mappings_table_name ) = generate_repository_mappings_table_names( service_id )
        
        ( count, ) = self._execute( 'SELECT COUNT( * ) FROM ' + current_mappings_table_name + ' WHERE service_tag_id = ?;', ( service_tag_id, ) ).fetchone()
        
        return count
        
    
    def _repository_get_files_info_files_table_join( self, service_id, content_status ):
        
        ( hash_id_map_table_name, tag_id_map_table_name ) = generate_repository_master_map_table_names( service_id )
        
        ( current_files_table_name, deleted_files_table_name, pending_files_table_name, petitioned_files_table_name, ip_addresses_table_name ) = generate_repository_files_table_names( service_id )
        
        if content_status == HC.CONTENT_STATUS_CURRENT:
            
            return 'files_info NATURAL JOIN ' + hash_id_map_table_name + ' NATURAL JOIN ' + current_files_table_name
            
        elif content_status == HC.CONTENT_STATUS_DELETED:
            
            return 'files_info NATURAL JOIN ' + hash_id_map_table_name + ' NATURAL JOIN ' + deleted_files_table_name
            
        elif content_status == HC.CONTENT_STATUS_PENDING:
            
            return 'files_info NATURAL JOIN ' + hash_id_map_table_name + ' NATURAL JOIN ' + pending_files_table_name
            
        elif content_status == HC.CONTENT_STATUS_PETITIONED:
            
            return 'files_info NATURAL JOIN ' + hash_id_map_table_name + ' NATURAL JOIN ' + petitioned_files_table_name
            
        
    
    def _repository_get_file_petition( self, service_id, petitioner_account_id, reason_id ) -> HydrusNetwork.Petition:
        
        ( current_files_table_name, deleted_files_table_name, pending_files_table_name, petitioned_files_table_name, ip_addresses_table_name ) = generate_repository_files_table_names( service_id )
        
        petitioner_account = self._get_account( service_id, petitioner_account_id )
        
        reason = self._get_reason( reason_id )
        
        service_hash_ids = [ service_hash_id for ( service_hash_id, ) in self._execute( f'SELECT service_hash_id FROM {petitioned_files_table_name} WHERE account_id = ? AND reason_id = ?;', ( petitioner_account_id, reason_id ) ) ]
        
        if len( service_hash_ids ) == 0:
            
            raise HydrusExceptions.NotFoundException( 'Sorry, did not find a petition for that account and reason!' )
            
        
        master_hash_ids = self._repository_get_master_hash_ids( service_id, service_hash_ids )
        
        hashes = self._get_hashes( master_hash_ids )
        
        content_type = HC.CONTENT_TYPE_FILES
        
        contents = [ HydrusNetwork.Content( content_type, hashes ) ]
        
        action = HC.CONTENT_UPDATE_PETITION
        
        actions_and_contents = [ ( action, contents ) ]
        
        petition_header = HydrusNetwork.PetitionHeader(
            content_type = HC.CONTENT_TYPE_FILES,
            status = HC.CONTENT_STATUS_PETITIONED,
            account_key = petitioner_account.get_account_key(),
            reason = reason
        )
        
        return HydrusNetwork.Petition( petitioner_account = petitioner_account, petition_header = petition_header, actions_and_contents = actions_and_contents )
        
    
    def _repository_get_file_petitions_summary( self, service_id, limit = 100, account_id = None, reason_id = None ) -> list:
        
        ( current_files_table_name, deleted_files_table_name, pending_files_table_name, petitioned_files_table_name, ip_addresses_table_name ) = generate_repository_files_table_names( service_id )
        
        petition_search_limit = max( limit * 5, 100 )
        
        preds = []
        
        if account_id is not None:
            
            preds.append( f'account_id = {account_id}' )
            
        
        if reason_id is not None:
            
            preds.append( f'reason_id = {reason_id}' )
            
        
        pred_string = ''
        
        if len( preds ) > 0:
            
            pred_string = ' WHERE {}'.format( ' AND '.join( preds ) )
            
        
        potential_petition_ids = self._execute( f'SELECT DISTINCT account_id, reason_id FROM {petitioned_files_table_name}{pred_string} ORDER BY account_id LIMIT ?;', ( petition_search_limit, ) ).fetchall()
        
        return self._repository_convert_petition_ids_to_summary( HC.CONTENT_TYPE_FILES, HC.CONTENT_STATUS_PETITIONED, potential_petition_ids, limit )
        
    
    def _repository_get_ip_timestamp( self, service_key, account, hash ):
        
        service_id = self._get_service_id( service_key )
        
        ( current_files_table_name, deleted_files_table_name, pending_files_table_name, petitioned_files_table_name, ip_addresses_table_name ) = generate_repository_files_table_names( service_id )
        
        master_hash_id = self._get_master_hash_id( hash )
        
        result = self._execute( 'SELECT ip, ip_timestamp FROM ' + ip_addresses_table_name + ' WHERE master_hash_id = ?;', ( master_hash_id, ) ).fetchone()
        
        if result is None:
            
            raise HydrusExceptions.NotFoundException( 'Did not find ip information for that hash.' )
            
        
        return result
        
    
    def _repository_get_mapping_petition( self, service_id, petitioner_account_id, reason_id ) -> HydrusNetwork.Petition:
        
        # we had a user petition 250k 'hash:abcdef...' tags with the same reason, and it overloaded the petition system trying to present them all
        MAX_MAPPINGS_PER_PETITION = 500000
        MAX_UNIQUE_TAG_IDS_PER_PETITION = 10000
        TIME_ALLOWED = 10
        
        time_started = HydrusTime.get_now_float()
        
        ( current_mappings_table_name, deleted_mappings_table_name, pending_mappings_table_name, petitioned_mappings_table_name ) = generate_repository_mappings_table_names( service_id )
        
        petitioner_account = self._get_account( service_id, petitioner_account_id )
        
        reason = self._get_reason( reason_id )
        
        tag_ids_to_hash_ids = HydrusData.build_key_to_list_dict( self._execute( f'SELECT service_tag_id, service_hash_id FROM {petitioned_mappings_table_name} WHERE account_id = ? AND reason_id = ? LIMIT ?;', ( petitioner_account_id, reason_id, MAX_MAPPINGS_PER_PETITION ) ) )
        
        if len( tag_ids_to_hash_ids ) == 0:
            
            raise HydrusExceptions.NotFoundException( 'Sorry, did not find a petition for that account and reason!' )
            
        
        contents = []
        
        # if this is a giganto petition, let's serve the large-count tags first
        petition_pairs = sorted( tag_ids_to_hash_ids.items(), key = lambda t_and_h: len( t_and_h[1] ), reverse = True )
        
        for ( service_tag_id, service_hash_ids ) in petition_pairs[ : MAX_UNIQUE_TAG_IDS_PER_PETITION ]:
            
            master_tag_id = self._repository_get_master_tag_id( service_id, service_tag_id )
            
            tag = self._get_tag( master_tag_id )
            
            master_hash_ids = self._repository_get_master_hash_ids( service_id, service_hash_ids )
            
            hashes = self._get_hashes( master_hash_ids )
            
            content = HydrusNetwork.Content( HC.CONTENT_TYPE_MAPPINGS, ( tag, hashes ) )
            
            contents.append( content )
            
            if HydrusTime.time_has_passed_float( time_started + TIME_ALLOWED ):
                
                break
                
            
        
        action = HC.CONTENT_UPDATE_PETITION
        
        actions_and_contents = [ ( action, contents ) ]
        
        petition_header = HydrusNetwork.PetitionHeader(
            content_type = HC.CONTENT_TYPE_MAPPINGS,
            status = HC.CONTENT_STATUS_PETITIONED,
            account_key = petitioner_account.get_account_key(),
            reason = reason
        )
        
        return HydrusNetwork.Petition( petitioner_account = petitioner_account, petition_header = petition_header, actions_and_contents = actions_and_contents )
        
    
    def _repository_get_mapping_petitions_summary( self, service_id, limit = 100, account_id = None, reason_id = None ) -> list:
        
        ( current_mappings_table_name, deleted_mappings_table_name, pending_mappings_table_name, petitioned_mappings_table_name ) = generate_repository_mappings_table_names( service_id )
        
        petition_search_limit = max( limit * 5, 100 )
        
        preds = []
        
        if account_id is not None:
            
            preds.append( f'account_id = {account_id}' )
            
        
        if reason_id is not None:
            
            preds.append( f'reason_id = {reason_id}' )
            
        
        pred_string = ''
        
        if len( preds ) > 0:
            
            pred_string = ' WHERE {}'.format( ' AND '.join( preds ) )
            
        
        potential_petition_ids = self._execute( f'SELECT DISTINCT account_id, reason_id FROM {petitioned_mappings_table_name}{pred_string} ORDER BY account_id LIMIT ?;', ( petition_search_limit, ) ).fetchall()
        
        return self._repository_convert_petition_ids_to_summary( HC.CONTENT_TYPE_MAPPINGS, HC.CONTENT_STATUS_PETITIONED, potential_petition_ids, limit )
        
    
    def _repository_get_master_hash_ids( self, service_id, service_hash_ids ):
        
        with self._make_temporary_integer_table( service_hash_ids, 'service_hash_id' ) as temp_service_hash_ids_table_name:
            
            ( hash_id_map_table_name, tag_id_map_table_name ) = generate_repository_master_map_table_names( service_id )
            
            master_hash_ids = self._stl( self._execute( 'SELECT master_hash_id FROM {} CROSS JOIN {} USING ( service_hash_id );'.format( temp_service_hash_ids_table_name, hash_id_map_table_name ) ) )
            
            if len( service_hash_ids ) != len( master_hash_ids ):
                
                raise HydrusExceptions.DataMissing( 'Missing master_hash_id map error!' )
                
            
        
        return master_hash_ids
        
    
    def _repository_get_master_tag_id( self, service_id, service_tag_id ):
        
        ( hash_id_map_table_name, tag_id_map_table_name ) = generate_repository_master_map_table_names( service_id )
        
        result = self._execute( 'SELECT master_tag_id FROM ' + tag_id_map_table_name + ' WHERE service_tag_id = ?;', ( service_tag_id, ) ).fetchone()
        
        if result is None:
            
            raise HydrusExceptions.DataMissing( 'Missing master_tag_id map error!' )
            
        
        ( master_tag_id, ) = result
        
        return master_tag_id
        
    
    def _repository_get_num_petitions( self, service_key, account, subject_account_key = None ):
        
        service_id = self._get_service_id( service_key )
        
        petition_count_info = []
        
        if account.has_permission(HC.CONTENT_TYPE_FILES, HC.PERMISSION_ACTION_MODERATE):
            
            petition_count_info.append( ( HC.CONTENT_TYPE_FILES, HC.CONTENT_STATUS_PETITIONED, HC.SERVICE_INFO_NUM_ACTIONABLE_FILE_DELETE_PETITIONS ) )
            
        
        if account.has_permission(HC.CONTENT_TYPE_MAPPINGS, HC.PERMISSION_ACTION_MODERATE):
            
            petition_count_info.append( ( HC.CONTENT_TYPE_MAPPINGS, HC.CONTENT_STATUS_PETITIONED, HC.SERVICE_INFO_NUM_ACTIONABLE_MAPPING_DELETE_PETITIONS ) )
            
        
        if account.has_permission(HC.CONTENT_TYPE_TAG_PARENTS, HC.PERMISSION_ACTION_MODERATE):
            
            petition_count_info.append( ( HC.CONTENT_TYPE_TAG_PARENTS, HC.CONTENT_STATUS_PENDING, HC.SERVICE_INFO_NUM_ACTIONABLE_PARENT_ADD_PETITIONS ) )
            
            petition_count_info.append( ( HC.CONTENT_TYPE_TAG_PARENTS, HC.CONTENT_STATUS_PETITIONED, HC.SERVICE_INFO_NUM_ACTIONABLE_PARENT_DELETE_PETITIONS ) )
            
        
        if account.has_permission(HC.CONTENT_TYPE_TAG_SIBLINGS, HC.PERMISSION_ACTION_MODERATE):
            
            petition_count_info.append( ( HC.CONTENT_TYPE_TAG_SIBLINGS, HC.CONTENT_STATUS_PENDING, HC.SERVICE_INFO_NUM_ACTIONABLE_SIBLING_ADD_PETITIONS ) )
            
            petition_count_info.append( ( HC.CONTENT_TYPE_TAG_SIBLINGS, HC.CONTENT_STATUS_PETITIONED, HC.SERVICE_INFO_NUM_ACTIONABLE_SIBLING_DELETE_PETITIONS ) )
            
        
        final_petition_count_info = []
        
        if subject_account_key is None:
            
            for ( content_type, content_status, info_type ) in petition_count_info:
                
                result = self._execute( 'SELECT info FROM service_info WHERE service_id = ? AND info_type = ?;', ( service_id, info_type ) ).fetchone()
                
                if result is None:
                    
                    self._repository_regenerate_service_info_specific( service_id, ( info_type, ) )
                    
                    result = self._execute( 'SELECT info FROM service_info WHERE service_id = ? AND info_type = ?;', ( service_id, info_type ) ).fetchone()
                    
                
                ( count, ) = result
                
                final_petition_count_info.append( ( content_type, content_status, count ) )
                
            
        else:
            
            try:
                
                subject_account_id = self._get_account_id( subject_account_key )
                
            except HydrusExceptions.InsufficientCredentialsException:
                
                raise HydrusExceptions.NotFoundException( 'That subject account id was not found on this service!' )
                
            
            for ( content_type, content_status, info_type ) in petition_count_info:
                
                count = self._repository_get_service_info_specific_for_account( service_id, info_type, subject_account_id )
                
                final_petition_count_info.append( ( content_type, content_status, count ) )
                
            
        
        return final_petition_count_info
        
    
    def _repository_get_petition( self, service_key, account, content_type, status, subject_account_key, reason ) -> HydrusNetwork.Petition:
        
        service_id = self._get_service_id( service_key )
        
        subject_account_id = self._get_account_id( subject_account_key )
        
        reason_id = self._get_reason_id( reason )
        
        if content_type == HC.CONTENT_TYPE_FILES:
            
            petition = self._repository_get_file_petition( service_id, subject_account_id, reason_id )
            
        elif content_type == HC.CONTENT_TYPE_MAPPINGS:
            
            petition = self._repository_get_mapping_petition( service_id, subject_account_id, reason_id )
            
        elif content_type == HC.CONTENT_TYPE_TAG_PARENTS:
            
            if status == HC.CONTENT_STATUS_PENDING:
                
                petition = self._repository_get_tag_parent_pend( service_id, subject_account_id, reason_id )
                
            else:
                
                petition = self._repository_get_tag_parent_petition( service_id, subject_account_id, reason_id )
                
            
        elif content_type == HC.CONTENT_TYPE_TAG_SIBLINGS:
            
            if status == HC.CONTENT_STATUS_PENDING:
                
                petition = self._repository_get_tag_sibling_pend( service_id, subject_account_id, reason_id )
                
            else:
                
                petition = self._repository_get_tag_sibling_petition( service_id, subject_account_id, reason_id )
                
            
        else:
            
            raise HydrusExceptions.BadRequestException( 'Unknown content type!' )
            
        
        
        return petition
        
    
    def _repository_get_petitions_summary( self, service_key, account, content_type, status, limit = 100, subject_account_key = None, reason = None ) -> list:
        
        service_id = self._get_service_id( service_key )
        
        if subject_account_key is None:
            
            subject_account_id = None
            
        else:
            
            subject_account_id = self._get_account_id( subject_account_key )
            
        
        if reason is None:
            
            reason_id = None
            
        else:
            
            reason_id = self._get_reason_id( reason )
            
        
        if content_type == HC.CONTENT_TYPE_FILES:
            
            petitions_summary = self._repository_get_file_petitions_summary( service_id, limit = limit, account_id = subject_account_id, reason_id = reason_id )
            
        elif content_type == HC.CONTENT_TYPE_MAPPINGS:
            
            petitions_summary = self._repository_get_mapping_petitions_summary( service_id, limit = limit, account_id = subject_account_id, reason_id = reason_id )
            
        elif content_type == HC.CONTENT_TYPE_TAG_PARENTS:
            
            if status == HC.CONTENT_STATUS_PENDING:
                
                petitions_summary = self._repository_get_tag_parent_pends_summary( service_id, limit = limit, account_id = subject_account_id, reason_id = reason_id )
                
            else:
                
                petitions_summary = self._repository_get_tag_parent_petitions_summary( service_id, limit = limit, account_id = subject_account_id, reason_id = reason_id )
                
            
        elif content_type == HC.CONTENT_TYPE_TAG_SIBLINGS:
            
            if status == HC.CONTENT_STATUS_PENDING:
                
                petitions_summary = self._repository_get_tag_sibling_pends_summary( service_id, limit = limit, account_id = subject_account_id, reason_id = reason_id )
                
            else:
                
                petitions_summary = self._repository_get_tag_sibling_petitions_summary( service_id, limit = limit, account_id = subject_account_id, reason_id = reason_id )
                
            
        else:
            
            raise HydrusExceptions.BadRequestException( 'Unknown content type!' )
            
        
        if len( petitions_summary ) == 0:
            
            if subject_account_key is None and reason is None:
                
                info_type = None
                
                if content_type == HC.CONTENT_TYPE_FILES:
                    
                    info_type = HC.SERVICE_INFO_NUM_ACTIONABLE_FILE_DELETE_PETITIONS
                    
                elif content_type == HC.CONTENT_TYPE_MAPPINGS:
                    
                    info_type = HC.SERVICE_INFO_NUM_ACTIONABLE_MAPPING_DELETE_PETITIONS
                    
                elif content_type == HC.CONTENT_TYPE_TAG_PARENTS:
                    
                    if status == HC.CONTENT_STATUS_PENDING:
                        
                        info_type = HC.SERVICE_INFO_NUM_ACTIONABLE_PARENT_ADD_PETITIONS
                        
                    else:
                        
                        info_type = HC.SERVICE_INFO_NUM_ACTIONABLE_PARENT_DELETE_PETITIONS
                        
                    
                elif content_type == HC.CONTENT_TYPE_TAG_SIBLINGS:
                    
                    if status == HC.CONTENT_STATUS_PENDING:
                        
                        info_type = HC.SERVICE_INFO_NUM_ACTIONABLE_SIBLING_ADD_PETITIONS
                        
                    else:
                        
                        info_type = HC.SERVICE_INFO_NUM_ACTIONABLE_SIBLING_DELETE_PETITIONS
                        
                    
                
                if info_type is not None:
                    
                    self._execute( 'DELETE FROM service_info WHERE service_id = ? AND info_type = ?;', ( service_id, info_type ) ).fetchone()
                    
                
            
        
        return petitions_summary
        
    
    def _repository_get_service_hash_id( self, service_id, master_hash_id, timestamp ):
        
        ( hash_id_map_table_name, tag_id_map_table_name ) = generate_repository_master_map_table_names( service_id )
        
        result = self._execute( 'SELECT service_hash_id FROM ' + hash_id_map_table_name + ' WHERE master_hash_id = ?;', ( master_hash_id, ) ).fetchone()
        
        if result is None:
            
            self._execute( 'INSERT INTO ' + hash_id_map_table_name + ' ( master_hash_id, hash_id_timestamp ) VALUES ( ?, ? );', ( master_hash_id, timestamp ) )
            
            service_hash_id = self._get_last_row_id()
            
            self._repository_update_service_info( service_id, HC.SERVICE_INFO_NUM_FILE_HASHES, 1 )
            
            return service_hash_id
            
        else:
            
            ( service_hash_id, ) = result
            
            return service_hash_id
            
        
    
    def _repository_get_service_hash_ids( self, service_id, master_hash_ids, timestamp ):
        
        ( hash_id_map_table_name, tag_id_map_table_name ) = generate_repository_master_map_table_names( service_id )
        
        service_hash_ids = set()
        master_hash_ids_not_in_table = set()
        
        for master_hash_id in master_hash_ids:
            
            result = self._execute( 'SELECT service_hash_id FROM ' + hash_id_map_table_name + ' WHERE master_hash_id = ?;', ( master_hash_id, ) ).fetchone()
            
            if result is None:
                
                master_hash_ids_not_in_table.add( master_hash_id )
                
            else:
                
                ( service_hash_id, ) = result
                
                service_hash_ids.add( service_hash_id )
                
            
        
        if len( master_hash_ids_not_in_table ) > 0:
            
            self._execute_many( 'INSERT INTO ' + hash_id_map_table_name + ' ( master_hash_id, hash_id_timestamp ) VALUES ( ?, ? );', ( ( master_hash_id, timestamp ) for master_hash_id in master_hash_ids_not_in_table ) )
            
            for master_hash_id in master_hash_ids_not_in_table:
                
                ( service_hash_id, ) = self._execute( 'SELECT service_hash_id FROM ' + hash_id_map_table_name + ' WHERE master_hash_id = ?;', ( master_hash_id, ) ).fetchone()
                
                service_hash_ids.add( service_hash_id )
                
            
            self._repository_update_service_info( service_id, HC.SERVICE_INFO_NUM_FILE_HASHES, len( master_hash_ids_not_in_table ) )
            
        
        return service_hash_ids
        
    
    def _repository_get_service_info_specific_for_account( self, service_id: int, info_type: int, account_id: int ):
        
        ( current_files_table_name, deleted_files_table_name, pending_files_table_name, petitioned_files_table_name, ip_addresses_table_name ) = generate_repository_files_table_names( service_id )
        ( current_mappings_table_name, deleted_mappings_table_name, pending_mappings_table_name, petitioned_mappings_table_name ) = generate_repository_mappings_table_names( service_id )
        ( current_tag_siblings_table_name, deleted_tag_siblings_table_name, pending_tag_siblings_table_name, petitioned_tag_siblings_table_name ) = generate_repository_tag_siblings_table_names( service_id )
        ( current_tag_parents_table_name, deleted_tag_parents_table_name, pending_tag_parents_table_name, petitioned_tag_parents_table_name ) = generate_repository_tag_parents_table_names( service_id )
        
        if info_type == HC.SERVICE_INFO_NUM_FILES:
            
            ( info, ) = self._execute( 'SELECT COUNT( * ) FROM {} WHERE account_id = ?;'.format( current_files_table_name ), ( account_id, ) ).fetchone()
            
        elif info_type == HC.SERVICE_INFO_NUM_DELETED_FILES:
            
            ( info, ) = self._execute( 'SELECT COUNT( * ) FROM {} WHERE account_id = ?;'.format( deleted_files_table_name ), ( account_id, ) ).fetchone()
            
        elif info_type == HC.SERVICE_INFO_NUM_PENDING_FILES:
            
            ( info, ) = self._execute( 'SELECT COUNT( * ) FROM {} WHERE account_id = ?;'.format( pending_files_table_name ), ( account_id, ) ).fetchone()
            
        elif info_type == HC.SERVICE_INFO_NUM_PETITIONED_FILES:
            
            ( info, ) = self._execute( 'SELECT COUNT( * ) FROM {} WHERE account_id = ?;'.format( petitioned_files_table_name ), ( account_id, ) ).fetchone()
            
        elif info_type == HC.SERVICE_INFO_NUM_ACTIONABLE_FILE_ADD_PETITIONS:
            
            ( info, ) = self._execute( 'SELECT COUNT( * ) FROM ( SELECT DISTINCT reason_id FROM {} WHERE account_id = ? );'.format( pending_files_table_name ), ( account_id, ) ).fetchone()
            
        elif info_type == HC.SERVICE_INFO_NUM_ACTIONABLE_FILE_DELETE_PETITIONS:
            
            ( info, ) = self._execute( 'SELECT COUNT( * ) FROM ( SELECT DISTINCT reason_id FROM {} WHERE account_id = ? );'.format( petitioned_files_table_name ), ( account_id, ) ).fetchone()
            
        elif info_type == HC.SERVICE_INFO_NUM_MAPPINGS:
            
            ( info, ) = self._execute( 'SELECT COUNT( * ) FROM {} WHERE account_id = ?;'.format( current_mappings_table_name ), ( account_id, ) ).fetchone()
            
        elif info_type == HC.SERVICE_INFO_NUM_DELETED_MAPPINGS:
            
            ( info, ) = self._execute( 'SELECT COUNT( * ) FROM {} WHERE account_id = ?;'.format( deleted_mappings_table_name ), ( account_id, ) ).fetchone()
            
        elif info_type == HC.SERVICE_INFO_NUM_PENDING_MAPPINGS:
            
            ( info, ) = self._execute( 'SELECT COUNT( * ) FROM {} WHERE account_id = ?;'.format( pending_mappings_table_name ), ( account_id, ) ).fetchone()
            
        elif info_type == HC.SERVICE_INFO_NUM_PETITIONED_MAPPINGS:
            
            ( info, ) = self._execute( 'SELECT COUNT( * ) FROM {} WHERE account_id = ?;'.format( petitioned_mappings_table_name ), ( account_id, ) ).fetchone()
            
        elif info_type == HC.SERVICE_INFO_NUM_ACTIONABLE_MAPPING_ADD_PETITIONS:
            
            ( info, ) = self._execute( 'SELECT COUNT( * ) FROM ( SELECT DISTINCT master_tag_id, reason_id FROM {} WHERE account_id = ? );'.format( pending_mappings_table_name ), ( account_id, ) ).fetchone()
            
        elif info_type == HC.SERVICE_INFO_NUM_ACTIONABLE_MAPPING_DELETE_PETITIONS:
            
            ( info, ) = self._execute( 'SELECT COUNT( * ) FROM ( SELECT DISTINCT service_tag_id, reason_id FROM {} WHERE account_id = ? );'.format( petitioned_mappings_table_name ), ( account_id, ) ).fetchone()
            
        elif info_type == HC.SERVICE_INFO_NUM_TAG_SIBLINGS:
            
            ( info, ) = self._execute( 'SELECT COUNT( * ) FROM {} WHERE account_id = ?;'.format( current_tag_siblings_table_name ), ( account_id, ) ).fetchone()
            
        elif info_type == HC.SERVICE_INFO_NUM_DELETED_TAG_SIBLINGS:
            
            ( info, ) = self._execute( 'SELECT COUNT( * ) FROM {} WHERE account_id = ?;'.format( deleted_tag_siblings_table_name ), ( account_id, ) ).fetchone()
            
        elif info_type == HC.SERVICE_INFO_NUM_PENDING_TAG_SIBLINGS:
            
            ( info, ) = self._execute( 'SELECT COUNT( * ) FROM {} WHERE account_id = ?;'.format( pending_tag_siblings_table_name ), ( account_id, ) ).fetchone()
            
        elif info_type == HC.SERVICE_INFO_NUM_PETITIONED_TAG_SIBLINGS:
            
            ( info, ) = self._execute( 'SELECT COUNT( * ) FROM {} WHERE account_id = ?;'.format( petitioned_tag_siblings_table_name ), ( account_id, ) ).fetchone()
            
        elif info_type == HC.SERVICE_INFO_NUM_ACTIONABLE_SIBLING_ADD_PETITIONS:
            
            ( info, ) = self._execute( 'SELECT COUNT( * ) FROM ( SELECT DISTINCT reason_id FROM {} WHERE account_id = ? EXCEPT SELECT DISTINCT reason_id FROM {} WHERE account_id = ? );'.format( pending_tag_siblings_table_name, petitioned_tag_siblings_table_name ), ( account_id, account_id ) ).fetchone()
            
        elif info_type == HC.SERVICE_INFO_NUM_ACTIONABLE_SIBLING_DELETE_PETITIONS:
            
            ( info, ) = self._execute( 'SELECT COUNT( * ) FROM ( SELECT DISTINCT reason_id FROM {} WHERE account_id = ? );'.format( petitioned_tag_siblings_table_name ), ( account_id, ) ).fetchone()
            
        elif info_type == HC.SERVICE_INFO_NUM_TAG_PARENTS:
            
            ( info, ) = self._execute( 'SELECT COUNT( * ) FROM {} WHERE account_id = ?;'.format( current_tag_parents_table_name ), ( account_id, ) ).fetchone()
            
        elif info_type == HC.SERVICE_INFO_NUM_DELETED_TAG_PARENTS:
            
            ( info, ) = self._execute( 'SELECT COUNT( * ) FROM {} WHERE account_id = ?;'.format( deleted_tag_parents_table_name ), ( account_id, ) ).fetchone()
            
        elif info_type == HC.SERVICE_INFO_NUM_PENDING_TAG_PARENTS:
            
            ( info, ) = self._execute( 'SELECT COUNT( * ) FROM {} WHERE account_id = ?;'.format( pending_tag_parents_table_name ), ( account_id, ) ).fetchone()
            
        elif info_type == HC.SERVICE_INFO_NUM_PETITIONED_TAG_PARENTS:
            
            ( info, ) = self._execute( 'SELECT COUNT( * ) FROM {} WHERE account_id = ?;'.format( petitioned_tag_parents_table_name ), ( account_id, ) ).fetchone()
            
        elif info_type == HC.SERVICE_INFO_NUM_ACTIONABLE_PARENT_ADD_PETITIONS:
            
            ( info, ) = self._execute( 'SELECT COUNT( * ) FROM ( SELECT DISTINCT reason_id FROM {} WHERE account_id = ? EXCEPT SELECT DISTINCT reason_id FROM {} WHERE account_id = ? );'.format( pending_tag_parents_table_name, petitioned_tag_parents_table_name ), ( account_id, account_id ) ).fetchone()
            
        elif info_type == HC.SERVICE_INFO_NUM_ACTIONABLE_PARENT_DELETE_PETITIONS:
            
            ( info, ) = self._execute( 'SELECT COUNT( * ) FROM ( SELECT DISTINCT reason_id FROM {} WHERE account_id = ? );'.format( petitioned_tag_parents_table_name ), ( account_id, ) ).fetchone()
            
        else:
            
            raise Exception( 'Was asked to generate account-specific service info for an unsupported type: {}'.format( info_type ) )
            
        
        return info
        
    
    def _repository_get_service_tag_id( self, service_id, master_tag_id, timestamp ):
        
        ( hash_id_map_table_name, tag_id_map_table_name ) = generate_repository_master_map_table_names( service_id )
        
        result = self._execute( 'SELECT service_tag_id FROM ' + tag_id_map_table_name + ' WHERE master_tag_id = ?;', ( master_tag_id, ) ).fetchone()
        
        if result is None:
            
            self._execute( 'INSERT INTO ' + tag_id_map_table_name + ' ( master_tag_id, tag_id_timestamp ) VALUES ( ?, ? );', ( master_tag_id, timestamp ) )
            
            service_tag_id = self._get_last_row_id()
            
            self._repository_update_service_info( service_id, HC.SERVICE_INFO_NUM_TAGS, 1 )
            
            return service_tag_id
            
        else:
            
            ( service_tag_id, ) = result
            
            return service_tag_id
            
        
    
    def _repository_get_tag_parent_pend( self, service_id, petitioner_account_id, reason_id ) -> HydrusNetwork.Petition:
        
        ( current_tag_parents_table_name, deleted_tag_parents_table_name, pending_tag_parents_table_name, petitioned_tag_parents_table_name ) = generate_repository_tag_parents_table_names( service_id )
        
        petitioner_account = self._get_account( service_id, petitioner_account_id )
        
        reason = self._get_reason( reason_id )
        
        pairs = self._execute( f'SELECT child_master_tag_id, parent_master_tag_id FROM {pending_tag_parents_table_name} WHERE account_id = ? AND reason_id = ?;', ( petitioner_account_id, reason_id ) ).fetchall()
        
        if len( pairs ) == 0:
            
            raise HydrusExceptions.NotFoundException( 'Sorry, did not find a petition for that account and reason!' )
            
        
        contents = []
        
        for ( child_master_tag_id, parent_master_tag_id ) in pairs:
            
            parent_tag = self._get_tag( parent_master_tag_id )
            
            child_tag = self._get_tag( child_master_tag_id )
            
            content = HydrusNetwork.Content( HC.CONTENT_TYPE_TAG_PARENTS, ( child_tag, parent_tag ) )
            
            contents.append( content )
            
        
        action = HC.CONTENT_UPDATE_PEND
        
        actions_and_contents = [ ( action, contents ) ]
        
        petition_header = HydrusNetwork.PetitionHeader(
            content_type = HC.CONTENT_TYPE_TAG_PARENTS,
            status = HC.CONTENT_STATUS_PENDING,
            account_key = petitioner_account.get_account_key(),
            reason = reason
        )
        
        return HydrusNetwork.Petition( petitioner_account = petitioner_account, petition_header = petition_header, actions_and_contents = actions_and_contents )
        
    
    def _repository_get_tag_parent_pends_summary( self, service_id, limit = 100, account_id = None, reason_id = None ) -> list:
        
        ( current_tag_parents_table_name, deleted_tag_parents_table_name, pending_tag_parents_table_name, petitioned_tag_parents_table_name ) = generate_repository_tag_parents_table_names( service_id )
        
        petition_search_limit = max( limit * 5, 100 )
        
        preds = [ f'1 NOT IN ( SELECT 1 FROM {petitioned_tag_parents_table_name} WHERE account_id = a1 AND reason_id = r1 )' ]
        
        if account_id is not None:
            
            preds.append( f'account_id = {account_id}' )
            
        
        if reason_id is not None:
            
            preds.append( f'reason_id = {reason_id}' )
            
        
        pred_string = ' WHERE {}'.format( ' AND '.join( preds ) )
        
        potential_petition_ids = self._execute( f'SELECT DISTINCT account_id as a1, reason_id as r1 FROM {pending_tag_parents_table_name}{pred_string} ORDER BY account_id LIMIT ?;', ( petition_search_limit, ) ).fetchall()
        
        return self._repository_convert_petition_ids_to_summary( HC.CONTENT_TYPE_TAG_PARENTS, HC.CONTENT_STATUS_PENDING, potential_petition_ids, limit )
        
    
    def _repository_get_tag_parent_petition( self, service_id, petitioner_account_id, reason_id ) -> HydrusNetwork.Petition:
        
        ( current_tag_parents_table_name, deleted_tag_parents_table_name, pending_tag_parents_table_name, petitioned_tag_parents_table_name ) = generate_repository_tag_parents_table_names( service_id )
        
        petitioner_account = self._get_account( service_id, petitioner_account_id )
        
        reason = self._get_reason( reason_id )
        
        actions_and_contents = []
        
        #
        
        pairs = self._execute( f'SELECT child_service_tag_id, parent_service_tag_id FROM {petitioned_tag_parents_table_name} WHERE account_id = ? AND reason_id = ?;', ( petitioner_account_id, reason_id ) ).fetchall()
        
        if len( pairs ) == 0:
            
            raise HydrusExceptions.NotFoundException( 'Sorry, did not find a petition for that account and reason!' )
            
        
        contents = []
        
        for ( child_service_tag_id, parent_service_tag_id ) in pairs:
            
            child_master_tag_id = self._repository_get_master_tag_id( service_id, child_service_tag_id )
            parent_master_tag_id = self._repository_get_master_tag_id( service_id, parent_service_tag_id )
            
            parent_tag = self._get_tag( parent_master_tag_id )
            
            child_tag = self._get_tag( child_master_tag_id )
            
            content = HydrusNetwork.Content( HC.CONTENT_TYPE_TAG_PARENTS, ( child_tag, parent_tag ) )
            
            contents.append( content )
            
        
        action = HC.CONTENT_UPDATE_PETITION
        
        actions_and_contents.append( ( action, contents ) )
        
        #
        
        pairs = self._execute( f'SELECT child_master_tag_id, parent_master_tag_id FROM {pending_tag_parents_table_name} WHERE account_id = ? AND reason_id = ?;', ( petitioner_account_id, reason_id ) ).fetchall()
        
        contents = []
        
        for ( child_master_tag_id, parent_master_tag_id ) in pairs:
            
            parent_tag = self._get_tag( parent_master_tag_id )
            
            child_tag = self._get_tag( child_master_tag_id )
            
            content = HydrusNetwork.Content( HC.CONTENT_TYPE_TAG_PARENTS, ( child_tag, parent_tag ) )
            
            contents.append( content )
            
        
        action = HC.CONTENT_UPDATE_PEND
        
        actions_and_contents.append( ( action, contents ) )
        
        #
        
        petition_header = HydrusNetwork.PetitionHeader(
            content_type = HC.CONTENT_TYPE_TAG_PARENTS,
            status = HC.CONTENT_STATUS_PETITIONED,
            account_key = petitioner_account.get_account_key(),
            reason = reason
        )
        
        return HydrusNetwork.Petition( petitioner_account = petitioner_account, petition_header = petition_header, actions_and_contents = actions_and_contents )
        
    
    def _repository_get_tag_parent_petitions_summary( self, service_id, limit = 100, account_id = None, reason_id = None ) -> list:
        
        ( current_tag_parents_table_name, deleted_tag_parents_table_name, pending_tag_parents_table_name, petitioned_tag_parents_table_name ) = generate_repository_tag_parents_table_names( service_id )
        
        petition_search_limit = max( limit * 5, 100 )
        
        preds = []
        
        if account_id is not None:
            
            preds.append( f'account_id = {account_id}' )
            
        
        if reason_id is not None:
            
            preds.append( f'reason_id = {reason_id}' )
            
        
        pred_string = ''
        
        if len( preds ) > 0:
            
            pred_string = ' WHERE {}'.format( ' AND '.join( preds ) )
            
        
        potential_petition_ids = self._execute( f'SELECT DISTINCT account_id, reason_id FROM {petitioned_tag_parents_table_name}{pred_string} ORDER BY account_id LIMIT ?;', ( petition_search_limit, ) ).fetchall()
        
        return self._repository_convert_petition_ids_to_summary( HC.CONTENT_TYPE_TAG_PARENTS, HC.CONTENT_STATUS_PETITIONED, potential_petition_ids, limit )
        
    
    def _repository_get_tag_sibling_pend( self, service_id, petitioner_account_id, reason_id ) -> HydrusNetwork.Petition:
        
        ( current_tag_siblings_table_name, deleted_tag_siblings_table_name, pending_tag_siblings_table_name, petitioned_tag_siblings_table_name ) = generate_repository_tag_siblings_table_names( service_id )
        
        petitioner_account = self._get_account( service_id, petitioner_account_id )
        
        reason = self._get_reason( reason_id )
        
        pairs = self._execute( f'SELECT bad_master_tag_id, good_master_tag_id FROM {pending_tag_siblings_table_name} WHERE account_id = ? AND reason_id = ?;', ( petitioner_account_id, reason_id ) ).fetchall()
        
        if len( pairs ) == 0:
            
            raise HydrusExceptions.NotFoundException( 'Sorry, did not find a petition for that account and reason!' )
            
        
        contents = []
        
        for ( bad_master_tag_id, good_master_tag_id ) in pairs:
            
            good_tag = self._get_tag( good_master_tag_id )
            
            bad_tag = self._get_tag( bad_master_tag_id )
            
            content = HydrusNetwork.Content( HC.CONTENT_TYPE_TAG_SIBLINGS, ( bad_tag, good_tag ) )
            
            contents.append( content )
            
        
        action = HC.CONTENT_UPDATE_PEND
        
        actions_and_contents = [ ( action, contents ) ]
        
        petition_header = HydrusNetwork.PetitionHeader(
            content_type = HC.CONTENT_TYPE_TAG_SIBLINGS,
            status = HC.CONTENT_STATUS_PENDING,
            account_key = petitioner_account.get_account_key(),
            reason = reason
        )
        
        return HydrusNetwork.Petition( petitioner_account = petitioner_account, petition_header = petition_header, actions_and_contents = actions_and_contents )
        
    
    def _repository_get_tag_sibling_pends_summary( self, service_id, limit = 100, account_id = None, reason_id = None ) -> list:
        
        ( current_tag_siblings_table_name, deleted_tag_siblings_table_name, pending_tag_siblings_table_name, petitioned_tag_siblings_table_name ) = generate_repository_tag_siblings_table_names( service_id )
        
        petition_search_limit = max( limit * 5, 100 )
        
        preds = [ f'1 NOT IN ( SELECT 1 FROM {petitioned_tag_siblings_table_name} WHERE account_id = a1 AND reason_id = r1 )' ]
        
        if account_id is not None:
            
            preds.append( f'account_id = {account_id}' )
            
        
        if reason_id is not None:
            
            preds.append( f'reason_id = {reason_id}' )
            
        
        pred_string = ' WHERE {}'.format( ' AND '.join( preds ) )
        
        potential_petition_ids = self._execute( f'SELECT DISTINCT account_id as a1, reason_id as r1 FROM {pending_tag_siblings_table_name}{pred_string} ORDER BY account_id LIMIT ?;', ( petition_search_limit, ) ).fetchall()
        
        return self._repository_convert_petition_ids_to_summary( HC.CONTENT_TYPE_TAG_SIBLINGS, HC.CONTENT_STATUS_PENDING, potential_petition_ids, limit )
        
    
    def _repository_get_tag_sibling_petition( self, service_id, petitioner_account_id, reason_id ) -> HydrusNetwork.Petition:
        
        ( current_tag_siblings_table_name, deleted_tag_siblings_table_name, pending_tag_siblings_table_name, petitioned_tag_siblings_table_name ) = generate_repository_tag_siblings_table_names( service_id )
        
        petitioner_account = self._get_account( service_id, petitioner_account_id )
        
        reason = self._get_reason( reason_id )
        
        actions_and_contents = []
        
        #
        
        pairs = self._execute( f'SELECT bad_service_tag_id, good_service_tag_id FROM {petitioned_tag_siblings_table_name} WHERE account_id = ? AND reason_id = ?;', ( petitioner_account_id, reason_id ) ).fetchall()
        
        if len( pairs ) == 0:
            
            raise HydrusExceptions.NotFoundException( 'Sorry, did not find a petition for that account and reason!' )
            
        
        contents = []
        
        for ( bad_service_tag_id, good_service_tag_id ) in pairs:
            
            bad_master_tag_id = self._repository_get_master_tag_id( service_id, bad_service_tag_id )
            good_master_tag_id = self._repository_get_master_tag_id( service_id, good_service_tag_id )
            
            good_tag = self._get_tag( good_master_tag_id )
            
            bad_tag = self._get_tag( bad_master_tag_id )
            
            content = HydrusNetwork.Content( HC.CONTENT_TYPE_TAG_SIBLINGS, ( bad_tag, good_tag ) )
            
            contents.append( content )
            
        
        action = HC.CONTENT_UPDATE_PETITION
        
        actions_and_contents.append( ( action, contents ) )
        
        #
        
        pairs = self._execute( f'SELECT bad_master_tag_id, good_master_tag_id FROM {pending_tag_siblings_table_name} WHERE account_id = ? AND reason_id = ?;', ( petitioner_account_id, reason_id ) ).fetchall()
        
        contents = []
        
        for ( bad_master_tag_id, good_master_tag_id ) in pairs:
            
            good_tag = self._get_tag( good_master_tag_id )
            
            bad_tag = self._get_tag( bad_master_tag_id )
            
            content = HydrusNetwork.Content( HC.CONTENT_TYPE_TAG_SIBLINGS, ( bad_tag, good_tag ) )
            
            contents.append( content )
            
        
        action = HC.CONTENT_UPDATE_PEND
        
        actions_and_contents.append( ( action, contents ) )
        
        #
        
        petition_header = HydrusNetwork.PetitionHeader(
            content_type = HC.CONTENT_TYPE_TAG_SIBLINGS,
            status = HC.CONTENT_STATUS_PETITIONED,
            account_key = petitioner_account.get_account_key(),
            reason = reason
        )
        
        return HydrusNetwork.Petition( petitioner_account = petitioner_account, petition_header = petition_header, actions_and_contents = actions_and_contents )
        
    
    def _repository_get_tag_sibling_petitions_summary( self, service_id, limit = 100, account_id = None, reason_id = None ) -> list:
        
        ( current_tag_siblings_table_name, deleted_tag_siblings_table_name, pending_tag_siblings_table_name, petitioned_tag_siblings_table_name ) = generate_repository_tag_siblings_table_names( service_id )
        
        petition_search_limit = max( limit * 5, 100 )
        
        preds = []
        
        if account_id is not None:
            
            preds.append( f'account_id = {account_id}' )
            
        
        if reason_id is not None:
            
            preds.append( f'reason_id = {reason_id}' )
            
        
        pred_string = ''
        
        if len( preds ) > 0:
            
            pred_string = ' WHERE {}'.format( ' AND '.join( preds ) )
            
        
        potential_petition_ids = self._execute( f'SELECT DISTINCT account_id, reason_id FROM {petitioned_tag_siblings_table_name}{pred_string} ORDER BY account_id LIMIT ?;', ( petition_search_limit, ) ).fetchall()
        
        return self._repository_convert_petition_ids_to_summary( HC.CONTENT_TYPE_TAG_SIBLINGS, HC.CONTENT_STATUS_PETITIONED, potential_petition_ids, limit )
        
    
    def _repository_has_file( self, service_key, hash ):
        
        if not self._master_hash_exists( hash ):
            
            return ( False, None )
            
        
        service_id = self._get_service_id( service_key )
        
        master_hash_id = self._get_master_hash_id( hash )
        
        table_join = self._repository_get_files_info_files_table_join( service_id, HC.CONTENT_STATUS_CURRENT )
        
        result = self._execute( 'SELECT mime FROM ' + table_join + ' WHERE master_hash_id = ?;', ( master_hash_id, ) ).fetchone()
        
        if result is None:
            
            return ( False, None )
            
        
        ( mime, ) = result
        
        return ( True, mime )
        
    
    def _repository_nullify_history( self, service_key, begin, end ):
        
        service_id = self._get_service_id( service_key )
        
        self._repository_nullify_history_files( service_id, begin, end )
        self._repository_nullify_history_tag_parents( service_id, begin, end )
        self._repository_nullify_history_tag_siblings( service_id, begin, end )
        self._repository_nullify_history_mappings( service_id, begin, end )
        
    
    def _repository_nullify_history_files( self, service_id, begin, end ):
        
        null_account_id = self._service_ids_to_null_account_ids[ service_id ]
        
        ( current_files_table_name, deleted_files_table_name, pending_files_table_name, petitioned_files_table_name, ip_addresses_table_name ) = generate_repository_files_table_names( service_id )
        
        self._execute( 'UPDATE {} SET account_id = ? WHERE file_timestamp BETWEEN ? AND ?;'.format( current_files_table_name ), ( null_account_id, begin, end ) )
        self._execute( 'UPDATE {} SET account_id = ? WHERE file_timestamp BETWEEN ? AND ?;'.format( deleted_files_table_name ), ( null_account_id, begin, end ) )
        
    
    def _repository_nullify_history_mappings( self, service_id, begin, end ):
        
        null_account_id = self._service_ids_to_null_account_ids[ service_id ]
        
        ( current_mappings_table_name, deleted_mappings_table_name, pending_mappings_table_name, petitioned_mappings_table_name ) = generate_repository_mappings_table_names( service_id )
        
        self._execute( 'UPDATE {} SET account_id = ? WHERE mapping_timestamp BETWEEN ? AND ?;'.format( current_mappings_table_name ), ( null_account_id, begin, end ) )
        self._execute( 'UPDATE {} SET account_id = ? WHERE mapping_timestamp BETWEEN ? AND ?;'.format( deleted_mappings_table_name ), ( null_account_id, begin, end ) )
        
    
    def _repository_nullify_history_tag_parents( self, service_id, begin, end ):
        
        null_account_id = self._service_ids_to_null_account_ids[ service_id ]
        
        ( current_tag_parents_table_name, deleted_tag_parents_table_name, pending_tag_parents_table_name, petitioned_tag_parents_table_name ) = generate_repository_tag_parents_table_names( service_id )
        
        self._execute( 'UPDATE {} SET account_id = ? WHERE parent_timestamp BETWEEN ? AND ?;'.format( current_tag_parents_table_name ), ( null_account_id, begin, end ) )
        self._execute( 'UPDATE {} SET account_id = ? WHERE parent_timestamp BETWEEN ? AND ?;'.format( deleted_tag_parents_table_name ), ( null_account_id, begin, end ) )
        
    
    def _repository_nullify_history_tag_siblings( self, service_id, begin, end ):
        
        null_account_id = self._service_ids_to_null_account_ids[ service_id ]
        
        ( current_tag_siblings_table_name, deleted_tag_siblings_table_name, pending_tag_siblings_table_name, petitioned_tag_siblings_table_name ) = generate_repository_tag_siblings_table_names( service_id )
        
        self._execute( 'UPDATE {} SET account_id = ? WHERE sibling_timestamp BETWEEN ? AND ?;'.format( current_tag_siblings_table_name ), ( null_account_id, begin, end ) )
        self._execute( 'UPDATE {} SET account_id = ? WHERE sibling_timestamp BETWEEN ? AND ?;'.format( deleted_tag_siblings_table_name ), ( null_account_id, begin, end ) )
        
    
    def _repository_pend_tag_parent( self, service_id, account_id, child_master_tag_id, parent_master_tag_id, reason_id ):
        
        ( current_tag_parents_table_name, deleted_tag_parents_table_name, pending_tag_parents_table_name, petitioned_tag_parents_table_name ) = generate_repository_tag_parents_table_names( service_id )
        
        child_exists = self._repository_service_tag_id_exists( service_id, child_master_tag_id )
        parent_exists = self._repository_service_tag_id_exists( service_id, parent_master_tag_id )
        
        if child_exists and parent_exists:
            
            child_service_tag_id = self._repository_get_service_tag_id( service_id, child_master_tag_id, HydrusTime.get_now() )
            parent_service_tag_id = self._repository_get_service_tag_id( service_id, parent_master_tag_id, HydrusTime.get_now() )
            
            result = self._execute( 'SELECT 1 FROM ' + current_tag_parents_table_name + ' WHERE child_service_tag_id = ? AND parent_service_tag_id = ?;', ( child_service_tag_id, parent_service_tag_id ) ).fetchone()
            
            if result is not None:
                
                return
                
            
        
        pre_change_count = self._repository_get_count_of_actionable_add_tag_parent_petitions_for_accounts( service_id, ( account_id, ) )
        
        self._execute( 'DELETE FROM {} WHERE child_master_tag_id = ? AND parent_master_tag_id = ? AND account_id = ?;'.format( pending_tag_parents_table_name ), ( child_master_tag_id, parent_master_tag_id, account_id ) )
        
        num_raw_deleted = self._get_row_count()
        
        self._execute( 'INSERT OR IGNORE INTO {} ( child_master_tag_id, parent_master_tag_id, account_id, reason_id ) VALUES ( ?, ?, ?, ? );'.format( pending_tag_parents_table_name ), ( child_master_tag_id, parent_master_tag_id, account_id, reason_id ) )
        
        num_raw_added = self._get_row_count()
        
        self._repository_update_service_info( service_id, HC.SERVICE_INFO_NUM_PENDING_TAG_PARENTS, num_raw_added - num_raw_deleted )
        
        post_change_count = self._repository_get_count_of_actionable_add_tag_parent_petitions_for_accounts( service_id, ( account_id, ) )
        
        self._repository_update_service_info( service_id, HC.SERVICE_INFO_NUM_ACTIONABLE_PARENT_ADD_PETITIONS, post_change_count - pre_change_count )
        
    
    def _repository_pend_tag_sibling( self, service_id, account_id, bad_master_tag_id, good_master_tag_id, reason_id ):
        
        ( current_tag_siblings_table_name, deleted_tag_siblings_table_name, pending_tag_siblings_table_name, petitioned_tag_siblings_table_name ) = generate_repository_tag_siblings_table_names( service_id )
        
        bad_exists = self._repository_service_tag_id_exists( service_id, bad_master_tag_id )
        good_exists = self._repository_service_tag_id_exists( service_id, good_master_tag_id )
        
        if bad_exists and good_exists:
            
            bad_service_tag_id = self._repository_get_service_tag_id( service_id, bad_master_tag_id, HydrusTime.get_now() )
            good_service_tag_id = self._repository_get_service_tag_id( service_id, good_master_tag_id, HydrusTime.get_now() )
            
            result = self._execute( 'SELECT 1 FROM ' + current_tag_siblings_table_name + ' WHERE bad_service_tag_id = ? AND good_service_tag_id = ?;', ( bad_service_tag_id, good_service_tag_id ) ).fetchone()
            
            if result is not None:
                
                return
                
            
        
        pre_change_count = self._repository_get_count_of_actionable_add_tag_sibling_petitions_for_accounts( service_id, ( account_id, ) )
        
        self._execute( 'DELETE FROM {} WHERE bad_master_tag_id = ? AND good_master_tag_id = ? AND account_id = ?;'.format( pending_tag_siblings_table_name ), ( bad_master_tag_id, good_master_tag_id, account_id ) )
        
        num_raw_deleted = self._get_row_count()
        
        self._execute( 'INSERT OR IGNORE INTO {} ( bad_master_tag_id, good_master_tag_id, account_id, reason_id ) VALUES ( ?, ?, ?, ? );'.format( pending_tag_siblings_table_name ), ( bad_master_tag_id, good_master_tag_id, account_id, reason_id ) )
        
        num_raw_added = self._get_row_count()
        
        self._repository_update_service_info( service_id, HC.SERVICE_INFO_NUM_PENDING_TAG_SIBLINGS, num_raw_added - num_raw_deleted )
        
        post_change_count = self._repository_get_count_of_actionable_add_tag_sibling_petitions_for_accounts( service_id, ( account_id, ) )
        
        self._repository_update_service_info( service_id, HC.SERVICE_INFO_NUM_ACTIONABLE_SIBLING_ADD_PETITIONS, post_change_count - pre_change_count )
        
    
    def _repository_petition_files( self, service_id, account_id, service_hash_ids, reason_id ):
        
        ( current_files_table_name, deleted_files_table_name, pending_files_table_name, petitioned_files_table_name, ip_addresses_table_name ) = generate_repository_files_table_names( service_id )
        
        with self._make_temporary_integer_table( service_hash_ids, 'service_hash_id' ) as temp_hash_ids_table_name:
            
            valid_service_hash_ids = self._stl( self._execute( 'SELECT service_hash_id FROM {} CROSS JOIN {} USING ( service_hash_id );'.format( temp_hash_ids_table_name, current_files_table_name ) ) )
            
        
        pre_change_count = self._repository_get_count_of_actionable_delete_file_petitions_for_accounts( service_id, ( account_id, ) )
        
        self._execute_many( 'DELETE FROM {} WHERE service_hash_id = ? AND account_id = ?;'.format( petitioned_files_table_name ), ( ( service_hash_id, account_id ) for service_hash_id in valid_service_hash_ids ) )
        
        num_raw_deleted = self._get_row_count()
        
        self._execute_many( 'INSERT OR IGNORE INTO {} ( service_hash_id, account_id, reason_id ) VALUES ( ?, ?, ? );'.format( petitioned_files_table_name ), ( ( service_hash_id, account_id, reason_id ) for service_hash_id in valid_service_hash_ids ) )
        
        num_raw_added = self._get_row_count()
        
        self._repository_update_service_info( service_id, HC.SERVICE_INFO_NUM_PETITIONED_FILES, num_raw_added - num_raw_deleted )
        
        post_change_count = self._repository_get_count_of_actionable_delete_file_petitions_for_accounts( service_id, ( account_id, ) )
        
        self._repository_update_service_info( service_id, HC.SERVICE_INFO_NUM_ACTIONABLE_FILE_DELETE_PETITIONS, post_change_count - pre_change_count )
        
    
    def _repository_petition_mappings( self, service_id, account_id, service_tag_id, service_hash_ids, reason_id ):
        
        ( current_mappings_table_name, deleted_mappings_table_name, pending_mappings_table_name, petitioned_mappings_table_name ) = generate_repository_mappings_table_names( service_id )
        
        with self._make_temporary_integer_table( service_hash_ids, 'service_hash_id' ) as temp_hash_ids_table_name:
            
            valid_service_hash_ids = self._stl( self._execute( 'SELECT service_hash_id FROM {} CROSS JOIN {} USING ( service_hash_id ) WHERE service_tag_id = ?;'.format( temp_hash_ids_table_name, current_mappings_table_name ), ( service_tag_id, ) ) )
            
        
        pre_change_count = self._repository_get_count_of_actionable_delete_mapping_petitions_for_accounts( service_id, service_tag_id, ( account_id, ) )
        
        self._execute_many( 'DELETE FROM {} WHERE service_tag_id = ? AND service_hash_id = ? AND account_id = ?;'.format( petitioned_mappings_table_name ), ( ( service_tag_id, service_hash_id, account_id ) for service_hash_id in valid_service_hash_ids ) )
        
        num_raw_deleted = self._get_row_count()
        
        self._execute_many( 'INSERT OR IGNORE INTO {} ( service_tag_id, service_hash_id, account_id, reason_id ) VALUES ( ?, ?, ?, ? );'.format( petitioned_mappings_table_name ), ( ( service_tag_id, service_hash_id, account_id, reason_id ) for service_hash_id in valid_service_hash_ids ) )
        
        num_raw_added = self._get_row_count()
        
        self._repository_update_service_info( service_id, HC.SERVICE_INFO_NUM_PETITIONED_MAPPINGS, num_raw_added - num_raw_deleted )
        
        post_change_count = self._repository_get_count_of_actionable_delete_mapping_petitions_for_accounts( service_id, service_tag_id, ( account_id, ) )
        
        self._repository_update_service_info( service_id, HC.SERVICE_INFO_NUM_ACTIONABLE_MAPPING_DELETE_PETITIONS, post_change_count - pre_change_count )
        
    
    def _repository_petition_tag_parent( self, service_id, account_id, child_service_tag_id, parent_service_tag_id, reason_id ):
        
        ( current_tag_parents_table_name, deleted_tag_parents_table_name, pending_tag_parents_table_name, petitioned_tag_parents_table_name ) = generate_repository_tag_parents_table_names( service_id )
        
        result = self._execute( 'SELECT 1 FROM ' + current_tag_parents_table_name + ' WHERE child_service_tag_id = ? AND parent_service_tag_id = ?;', ( child_service_tag_id, parent_service_tag_id ) ).fetchone()
        
        if result is None:
            
            return
            
        
        pre_add_change_count = self._repository_get_count_of_actionable_add_tag_parent_petitions_for_accounts( service_id, ( account_id, ) )
        
        pre_delete_change_count = self._repository_get_count_of_actionable_delete_tag_parent_petitions_for_accounts( service_id, ( account_id, ) )
        
        self._execute( 'DELETE FROM {} WHERE child_service_tag_id = ? AND parent_service_tag_id = ? AND account_id = ?;'.format( petitioned_tag_parents_table_name ), ( child_service_tag_id, parent_service_tag_id, account_id ) )
        
        num_raw_deleted = self._get_row_count()
        
        self._execute( 'INSERT OR IGNORE INTO {} ( child_service_tag_id, parent_service_tag_id, account_id, reason_id ) VALUES ( ?, ?, ?, ? );'.format( petitioned_tag_parents_table_name ), ( child_service_tag_id, parent_service_tag_id, account_id, reason_id ) )
        
        num_raw_added = self._get_row_count()
        
        self._repository_update_service_info( service_id, HC.SERVICE_INFO_NUM_PETITIONED_TAG_PARENTS, num_raw_added - num_raw_deleted )
        
        post_add_change_count = self._repository_get_count_of_actionable_add_tag_parent_petitions_for_accounts( service_id, ( account_id, ) )
        
        post_delete_change_count = self._repository_get_count_of_actionable_delete_tag_parent_petitions_for_accounts( service_id, ( account_id, ) )
        
        self._repository_update_service_info( service_id, HC.SERVICE_INFO_NUM_ACTIONABLE_PARENT_DELETE_PETITIONS, post_delete_change_count - pre_delete_change_count )
        
        self._repository_update_service_info( service_id, HC.SERVICE_INFO_NUM_ACTIONABLE_PARENT_ADD_PETITIONS, post_add_change_count - pre_add_change_count )
        
    
    def _repository_petition_tag_sibling( self, service_id, account_id, bad_service_tag_id, good_service_tag_id, reason_id ):
        
        ( current_tag_siblings_table_name, deleted_tag_siblings_table_name, pending_tag_siblings_table_name, petitioned_tag_siblings_table_name ) = generate_repository_tag_siblings_table_names( service_id )
        
        result = self._execute( 'SELECT 1 FROM ' + current_tag_siblings_table_name + ' WHERE bad_service_tag_id = ? AND good_service_tag_id = ?;', ( bad_service_tag_id, good_service_tag_id ) ).fetchone()
        
        if result is None:
            
            return
            
        
        pre_add_change_count = self._repository_get_count_of_actionable_add_tag_sibling_petitions_for_accounts( service_id, ( account_id, ) )
        
        pre_delete_change_count = self._repository_get_count_of_actionable_delete_tag_sibling_petitions_for_accounts( service_id, ( account_id, ) )
        
        self._execute( 'DELETE FROM {} WHERE bad_service_tag_id = ? AND good_service_tag_id = ? AND account_id = ?;'.format( petitioned_tag_siblings_table_name ), ( bad_service_tag_id, good_service_tag_id, account_id ) )
        
        num_raw_deleted = self._get_row_count()
        
        self._execute( 'INSERT OR IGNORE INTO {} ( bad_service_tag_id, good_service_tag_id, account_id, reason_id ) VALUES ( ?, ?, ?, ? );'.format( petitioned_tag_siblings_table_name ), ( bad_service_tag_id, good_service_tag_id, account_id, reason_id ) )
        
        num_raw_added = self._get_row_count()
        
        self._repository_update_service_info( service_id, HC.SERVICE_INFO_NUM_PETITIONED_TAG_SIBLINGS, num_raw_added - num_raw_deleted )
        
        post_add_change_count = self._repository_get_count_of_actionable_add_tag_sibling_petitions_for_accounts( service_id, ( account_id, ) )
        
        post_delete_change_count = self._repository_get_count_of_actionable_delete_tag_sibling_petitions_for_accounts( service_id, ( account_id, ) )
        
        self._repository_update_service_info( service_id, HC.SERVICE_INFO_NUM_ACTIONABLE_SIBLING_DELETE_PETITIONS, post_delete_change_count - pre_delete_change_count )
        
        self._repository_update_service_info( service_id, HC.SERVICE_INFO_NUM_ACTIONABLE_SIBLING_ADD_PETITIONS, post_add_change_count - pre_add_change_count )
        
    
    def _repository_process_add_file( self, service, account, file_dict, timestamp ):
        
        service_key = service.get_service_key()
        
        service_id = self._get_service_id( service_key )
        
        account_key = account.GetAccountKey()
        
        account_id = self._get_account_id( account_key )
        
        can_create_files = account.has_permission(HC.CONTENT_TYPE_FILES, HC.PERMISSION_ACTION_CREATE)
        can_moderate_files = account.has_permission(HC.CONTENT_TYPE_FILES, HC.PERMISSION_ACTION_MODERATE)
        
        # later add pend file here however that is neat
        
        if can_create_files or can_moderate_files:
            
            if not can_moderate_files:
                
                max_storage = service.GetMaxStorage()
                
                if max_storage is not None:
                    
                    ( current_files_table_name, deleted_files_table_name, pending_files_table_name, petitioned_files_table_name, ip_addresses_table_name ) = generate_repository_files_table_names( service_id )
                    
                    table_join = self._repository_get_files_info_files_table_join( service_id, HC.CONTENT_STATUS_CURRENT )
                    
                    result = self._execute( 'SELECT SUM( size ) FROM ' + table_join + ';' ).fetchone()
                    
                    total_current_storage = self._get_sum_result( result )
                    
                    table_join = self._repository_get_files_info_files_table_join( service_id, HC.CONTENT_STATUS_PENDING )
                    
                    result = self._execute( 'SELECT SUM( size ) FROM ' + table_join + ';' ).fetchone()
                    
                    total_pending_storage = self._get_sum_result( result )
                    
                    if total_current_storage + total_pending_storage + file_dict[ 'size' ] > max_storage:
                        
                        raise HydrusExceptions.ConflictException( 'This repository is full up and cannot take any more files!' )
                        
                    
                
            
            overwrite_deleted = can_moderate_files
            
            self._repository_add_file( service_id, account_id, file_dict, overwrite_deleted, timestamp )
            
        
    
    def _repository_process_client_to_server_update( self, service_key: bytes, account: HydrusNetwork.Account, client_to_server_update: HydrusNetwork.ClientToServerUpdate, timestamp: int ):
        
        service_id = self._get_service_id( service_key )
        
        account_key = account.get_account_key()
        
        account_id = self._get_account_id( account_key )
        
        can_petition_files = account.has_permission( HC.CONTENT_TYPE_FILES, HC.PERMISSION_ACTION_PETITION )
        can_moderate_files = account.has_permission( HC.CONTENT_TYPE_FILES, HC.PERMISSION_ACTION_MODERATE )
        
        can_petition_mappings = account.has_permission( HC.CONTENT_TYPE_MAPPINGS, HC.PERMISSION_ACTION_PETITION )
        can_create_mappings = account.has_permission( HC.CONTENT_TYPE_MAPPINGS, HC.PERMISSION_ACTION_CREATE )
        can_moderate_mappings = account.has_permission( HC.CONTENT_TYPE_MAPPINGS, HC.PERMISSION_ACTION_MODERATE )
        
        can_petition_tag_parents = account.has_permission( HC.CONTENT_TYPE_TAG_PARENTS, HC.PERMISSION_ACTION_PETITION )
        can_create_tag_parents = account.has_permission( HC.CONTENT_TYPE_TAG_PARENTS, HC.PERMISSION_ACTION_CREATE )
        can_moderate_tag_parents = account.has_permission( HC.CONTENT_TYPE_TAG_PARENTS, HC.PERMISSION_ACTION_MODERATE )
        
        can_petition_tag_siblings = account.has_permission( HC.CONTENT_TYPE_TAG_SIBLINGS, HC.PERMISSION_ACTION_PETITION )
        can_create_tag_siblings = account.has_permission( HC.CONTENT_TYPE_TAG_SIBLINGS, HC.PERMISSION_ACTION_CREATE )
        can_moderate_tag_siblings = account.has_permission( HC.CONTENT_TYPE_TAG_SIBLINGS, HC.PERMISSION_ACTION_MODERATE )
        
        if can_moderate_files or can_petition_files:
            
            for ( hashes, reason ) in client_to_server_update.get_content_data_iterator( HC.CONTENT_TYPE_FILES, HC.CONTENT_UPDATE_PETITION ):
                
                master_hash_ids = self._get_master_hash_ids( hashes )
                
                service_hash_ids = self._repository_get_service_hash_ids( service_id, master_hash_ids, timestamp )
                
                if can_moderate_files:
                    
                    self._repository_delete_files( service_id, account_id, service_hash_ids, timestamp )
                    
                elif can_petition_files:
                    
                    reason_id = self._get_reason_id( reason )
                    
                    self._repository_petition_files( service_id, account_id, service_hash_ids, reason_id )
                    
                
            
        
        if can_moderate_files:
            
            for ( hashes, reason ) in client_to_server_update.get_content_data_iterator( HC.CONTENT_TYPE_FILES, HC.CONTENT_UPDATE_DENY_PETITION ):
                
                master_hash_ids = self._get_master_hash_ids( hashes )
                
                service_hash_ids = self._repository_get_service_hash_ids( service_id, master_hash_ids, timestamp )
                
                self._repository_deny_file_petition( service_id, service_hash_ids )
                
            
        
        #
        
        # later add pend mappings here however that is neat
        
        if can_create_mappings or can_moderate_mappings:
            
            for ( ( tag, hashes ), reason ) in client_to_server_update.get_content_data_iterator( HC.CONTENT_TYPE_MAPPINGS, HC.CONTENT_UPDATE_PEND ):
                
                try:
                    
                    master_tag_id = self._get_master_tag_id( tag )
                    
                except Exception as e:
                    
                    continue
                    
                
                master_hash_ids = self._get_master_hash_ids( hashes )
                
                overwrite_deleted = can_moderate_mappings
                
                self._repository_add_mappings( service_id, account_id, master_tag_id, master_hash_ids, overwrite_deleted, timestamp )
                
            
        
        if can_moderate_mappings or can_petition_mappings:
            
            for ( ( tag, hashes ), reason ) in client_to_server_update.get_content_data_iterator( HC.CONTENT_TYPE_MAPPINGS, HC.CONTENT_UPDATE_PETITION ):
                
                try:
                    
                    master_tag_id = self._get_master_tag_id( tag )
                    
                except Exception as e:
                    
                    continue
                    
                
                service_tag_id = self._repository_get_service_tag_id( service_id, master_tag_id, timestamp )
                
                master_hash_ids = self._get_master_hash_ids( hashes )
                
                service_hash_ids = self._repository_get_service_hash_ids( service_id, master_hash_ids, timestamp )
                
                if can_moderate_mappings:
                    
                    self._repository_Delete_mappings( service_id, account_id, service_tag_id, service_hash_ids, timestamp )
                    
                elif can_petition_mappings:
                    
                    reason_id = self._get_reason_id( reason )
                    
                    self._repository_petition_mappings( service_id, account_id, service_tag_id, service_hash_ids, reason_id )
                    
                
            
        
        if can_moderate_mappings:
            
            for ( ( tag, hashes ), reason ) in client_to_server_update.get_content_data_iterator( HC.CONTENT_TYPE_MAPPINGS, HC.CONTENT_UPDATE_DENY_PETITION ):
                
                try:
                    
                    master_tag_id = self._get_master_tag_id( tag )
                    
                except Exception as e:
                    
                    continue
                    
                
                service_tag_id = self._repository_get_service_tag_id( service_id, master_tag_id, timestamp )
                
                master_hash_ids = self._get_master_hash_ids( hashes )
                
                service_hash_ids = self._repository_get_service_hash_ids( service_id, master_hash_ids, timestamp )
                
                self._repository_deny_mapping_petition( service_id, service_tag_id, service_hash_ids )
                
            
        
        #
        
        if can_create_tag_parents or can_moderate_tag_parents or can_petition_tag_parents:
            
            for ( ( child_tag, parent_tag ), reason ) in client_to_server_update.get_content_data_iterator( HC.CONTENT_TYPE_TAG_PARENTS, HC.CONTENT_UPDATE_PEND ):
                
                try:
                    
                    child_master_tag_id = self._get_master_tag_id( child_tag )
                    parent_master_tag_id = self._get_master_tag_id( parent_tag )
                    
                except Exception as e:
                    
                    continue
                    
                
                if can_create_tag_parents or can_moderate_tag_parents:
                    
                    overwrite_deleted = can_moderate_tag_parents
                    
                    self._repository_add_tag_parent( service_id, account_id, child_master_tag_id, parent_master_tag_id, overwrite_deleted, timestamp )
                    
                elif can_petition_tag_parents:
                    
                    reason_id = self._get_reason_id( reason )
                    
                    self._repository_pend_tag_parent( service_id, account_id, child_master_tag_id, parent_master_tag_id, reason_id )
                    
                
            
            for ( ( child_tag, parent_tag ), reason ) in client_to_server_update.get_content_data_iterator( HC.CONTENT_TYPE_TAG_PARENTS, HC.CONTENT_UPDATE_PETITION ):
                
                try:
                    
                    child_master_tag_id = self._get_master_tag_id( child_tag )
                    parent_master_tag_id = self._get_master_tag_id( parent_tag )
                    
                except Exception as e:
                    
                    continue
                    
                
                child_service_tag_id = self._repository_get_service_tag_id( service_id, child_master_tag_id, timestamp )
                parent_service_tag_id = self._repository_get_service_tag_id( service_id, parent_master_tag_id, timestamp )
                
                if can_moderate_tag_parents:
                    
                    self._repository_delete_tag_parent( service_id, account_id, child_service_tag_id, parent_service_tag_id, timestamp )
                    
                elif can_petition_tag_parents:
                    
                    reason_id = self._get_reason_id( reason )
                    
                    self._repository_petition_tag_parent( service_id, account_id, child_service_tag_id, parent_service_tag_id, reason_id )
                    
                
            
        
        if can_moderate_tag_parents:
            
            for ( ( child_tag, parent_tag ), reason ) in client_to_server_update.get_content_data_iterator( HC.CONTENT_TYPE_TAG_PARENTS, HC.CONTENT_UPDATE_DENY_PEND ):
                
                try:
                    
                    child_master_tag_id = self._get_master_tag_id( child_tag )
                    parent_master_tag_id = self._get_master_tag_id( parent_tag )
                    
                except Exception as e:
                    
                    continue
                    
                
                self._repository_deny_tag_parent_pend( service_id, child_master_tag_id, parent_master_tag_id )
                
            
            for ( ( child_tag, parent_tag ), reason ) in client_to_server_update.get_content_data_iterator( HC.CONTENT_TYPE_TAG_PARENTS, HC.CONTENT_UPDATE_DENY_PETITION ):
                
                try:
                    
                    child_master_tag_id = self._get_master_tag_id( child_tag )
                    parent_master_tag_id = self._get_master_tag_id( parent_tag )
                    
                except Exception as e:
                    
                    continue
                    
                
                child_service_tag_id = self._repository_get_service_tag_id( service_id, child_master_tag_id, timestamp )
                parent_service_tag_id = self._repository_get_service_tag_id( service_id, parent_master_tag_id, timestamp )
                
                self._repository_deny_tag_parent_petition( service_id, child_service_tag_id, parent_service_tag_id )
                
            
        
        #
        
        if can_create_tag_siblings or can_moderate_tag_siblings or can_petition_tag_siblings:
            
            # do delete before pend for petitions, since bad_tag is primary key!
            
            for ( ( bad_tag, good_tag ), reason ) in client_to_server_update.get_content_data_iterator( HC.CONTENT_TYPE_TAG_SIBLINGS, HC.CONTENT_UPDATE_PETITION ):
                
                try:
                    
                    bad_master_tag_id = self._get_master_tag_id( bad_tag )
                    good_master_tag_id = self._get_master_tag_id( good_tag )
                    
                except Exception as e:
                    
                    continue
                    
                
                bad_service_tag_id = self._repository_get_service_tag_id( service_id, bad_master_tag_id, timestamp )
                good_service_tag_id = self._repository_get_service_tag_id( service_id, good_master_tag_id, timestamp )
                
                if can_moderate_tag_siblings:
                    
                    self._repository_delete_tag_sibling( service_id, account_id, bad_service_tag_id, good_service_tag_id, timestamp )
                    
                elif can_petition_tag_siblings:
                    
                    reason_id = self._get_reason_id( reason )
                    
                    self._repository_petition_tag_sibling( service_id, account_id, bad_service_tag_id, good_service_tag_id, reason_id )
                    
                
            
            for ( ( bad_tag, good_tag ), reason ) in client_to_server_update.get_content_data_iterator( HC.CONTENT_TYPE_TAG_SIBLINGS, HC.CONTENT_UPDATE_PEND ):
                
                try:
                    
                    bad_master_tag_id = self._get_master_tag_id( bad_tag )
                    good_master_tag_id = self._get_master_tag_id( good_tag )
                    
                except Exception as e:
                    
                    continue
                    
                
                if can_create_tag_siblings or can_moderate_tag_siblings:
                    
                    overwrite_deleted = can_moderate_tag_siblings
                    
                    self._repository_add_tag_sibling( service_id, account_id, bad_master_tag_id, good_master_tag_id, overwrite_deleted, timestamp )
                    
                elif can_petition_tag_siblings:
                    
                    reason_id = self._get_reason_id( reason )
                    
                    self._repository_pend_tag_sibling( service_id, account_id, bad_master_tag_id, good_master_tag_id, reason_id )
                    
                
            
        
        if can_moderate_tag_siblings:
            
            for ( ( bad_tag, good_tag ), reason ) in client_to_server_update.get_content_data_iterator( HC.CONTENT_TYPE_TAG_SIBLINGS, HC.CONTENT_UPDATE_DENY_PEND ):
                
                try:
                    
                    bad_master_tag_id = self._get_master_tag_id( bad_tag )
                    good_master_tag_id = self._get_master_tag_id( good_tag )
                    
                except Exception as e:
                    
                    continue
                    
                
                self._repository_deny_tag_sibling_pend( service_id, bad_master_tag_id, good_master_tag_id )
                
            
            for ( ( bad_tag, good_tag ), reason ) in client_to_server_update.get_content_data_iterator( HC.CONTENT_TYPE_TAG_SIBLINGS, HC.CONTENT_UPDATE_DENY_PETITION ):
                
                try:
                    
                    bad_master_tag_id = self._get_master_tag_id( bad_tag )
                    good_master_tag_id = self._get_master_tag_id( good_tag )
                    
                except Exception as e:
                    
                    continue
                    
                
                bad_service_tag_id = self._repository_get_service_tag_id( service_id, bad_master_tag_id, timestamp )
                good_service_tag_id = self._repository_get_service_tag_id( service_id, good_master_tag_id, timestamp )
                
                self._repository_deny_tag_sibling_petition( service_id, bad_service_tag_id, good_service_tag_id )
                
            
        
    
    def _repository_regenerate_service_info( self, service_id = None, info_type = None ):
        
        if service_id is None:
            
            service_ids = self._get_service_ids( HC.REPOSITORIES )
            
        else:
            
            service_ids = ( service_id, )
            
        
        for service_id in service_ids:
            
            allowable_service_info_types = ALLOWABLE_SERVICE_INFO_TYPES[ self._get_service_type( service_id ) ]
            
            if info_type is None:
                
                self._execute( 'DELETE FROM service_info WHERE service_id = ?;', ( service_id, ) )
                
                info_types = allowable_service_info_types
                
            else:
                
                if info_type in allowable_service_info_types:
                    
                    info_types = ( info_type, )
                    
                else:
                    
                    return
                    
                
            
            self._repository_regenerate_service_info_specific( service_id, info_types )
            
        
    
    def _repository_regenerate_service_info_service_key( self, service_key ):
        
        service_id = self._get_service_id( service_key )
        
        if self._get_service_type( service_id ) not in HC.REPOSITORIES:
            
            return
            
        
        locked = HG.server_busy.acquire( False )
        
        if not locked:
            
            raise HydrusExceptions.ServerBusyException( 'Sorry, server is busy and cannot do maintenance right now!' )
            
        
        self._repository_regenerate_service_info( service_id = service_id )
        
        HG.server_busy.release()
        
    
    def _repository_regenerate_service_info_specific( self, service_id: int, info_types: collections.abc.Collection[ int ] ):
        
        self._execute_many( 'DELETE FROM service_info WHERE service_id = ? AND info_type = ?;', ( ( service_id, info_type ) for info_type in info_types ) )
        
        service_name = self._get_service_name( service_id )
        
        ( hash_id_map_table_name, tag_id_map_table_name ) = generate_repository_master_map_table_names( service_id )
        ( current_files_table_name, deleted_files_table_name, pending_files_table_name, petitioned_files_table_name, ip_addresses_table_name ) = generate_repository_files_table_names( service_id )
        ( current_mappings_table_name, deleted_mappings_table_name, pending_mappings_table_name, petitioned_mappings_table_name ) = generate_repository_mappings_table_names( service_id )
        ( current_tag_siblings_table_name, deleted_tag_siblings_table_name, pending_tag_siblings_table_name, petitioned_tag_siblings_table_name ) = generate_repository_tag_siblings_table_names( service_id )
        ( current_tag_parents_table_name, deleted_tag_parents_table_name, pending_tag_parents_table_name, petitioned_tag_parents_table_name ) = generate_repository_tag_parents_table_names( service_id )
        
        for info_type in info_types:
            
            if info_type == HC.SERVICE_INFO_NUM_FILE_HASHES:
                
                ( info, ) = self._execute( 'SELECT COUNT( * ) FROM {};'.format( hash_id_map_table_name ) ).fetchone()
                
            elif info_type == HC.SERVICE_INFO_NUM_FILES:
                
                ( info, ) = self._execute( 'SELECT COUNT( * ) FROM {};'.format( current_files_table_name ) ).fetchone()
                
            elif info_type == HC.SERVICE_INFO_NUM_DELETED_FILES:
                
                ( info, ) = self._execute( 'SELECT COUNT( * ) FROM {};'.format( deleted_files_table_name ) ).fetchone()
                
            elif info_type == HC.SERVICE_INFO_NUM_PENDING_FILES:
                
                ( info, ) = self._execute( 'SELECT COUNT( * ) FROM {};'.format( pending_files_table_name ) ).fetchone()
                
            elif info_type == HC.SERVICE_INFO_NUM_PETITIONED_FILES:
                
                ( info, ) = self._execute( 'SELECT COUNT( * ) FROM {};'.format( petitioned_files_table_name ) ).fetchone()
                
            elif info_type == HC.SERVICE_INFO_NUM_ACTIONABLE_FILE_ADD_PETITIONS:
                
                ( info, ) = self._execute( 'SELECT COUNT( * ) FROM ( SELECT DISTINCT account_id, reason_id FROM {} );'.format( pending_files_table_name ) ).fetchone()
                
            elif info_type == HC.SERVICE_INFO_NUM_ACTIONABLE_FILE_DELETE_PETITIONS:
                
                ( info, ) = self._execute( 'SELECT COUNT( * ) FROM ( SELECT DISTINCT account_id, reason_id FROM {} );'.format( petitioned_files_table_name ) ).fetchone()
                
            elif info_type == HC.SERVICE_INFO_NUM_TAGS:
                
                ( info, ) = self._execute( 'SELECT COUNT( * ) FROM {};'.format( tag_id_map_table_name ) ).fetchone()
                
            elif info_type == HC.SERVICE_INFO_NUM_MAPPINGS:
                
                ( info, ) = self._execute( 'SELECT COUNT( * ) FROM {};'.format( current_mappings_table_name ) ).fetchone()
                
            elif info_type == HC.SERVICE_INFO_NUM_DELETED_MAPPINGS:
                
                ( info, ) = self._execute( 'SELECT COUNT( * ) FROM {};'.format( deleted_mappings_table_name ) ).fetchone()
                
            elif info_type == HC.SERVICE_INFO_NUM_PENDING_MAPPINGS:
                
                ( info, ) = self._execute( 'SELECT COUNT( * ) FROM {};'.format( pending_mappings_table_name ) ).fetchone()
                
            elif info_type == HC.SERVICE_INFO_NUM_PETITIONED_MAPPINGS:
                
                ( info, ) = self._execute( 'SELECT COUNT( * ) FROM {};'.format( petitioned_mappings_table_name ) ).fetchone()
                
            elif info_type == HC.SERVICE_INFO_NUM_ACTIONABLE_MAPPING_ADD_PETITIONS:
                
                ( info, ) = self._execute( 'SELECT COUNT( * ) FROM ( SELECT DISTINCT master_tag_id, account_id, reason_id FROM {} );'.format( pending_mappings_table_name ) ).fetchone()
                
            elif info_type == HC.SERVICE_INFO_NUM_ACTIONABLE_MAPPING_DELETE_PETITIONS:
                
                ( info, ) = self._execute( 'SELECT COUNT( * ) FROM ( SELECT DISTINCT service_tag_id, account_id, reason_id FROM {} );'.format( petitioned_mappings_table_name ) ).fetchone()
                
            elif info_type == HC.SERVICE_INFO_NUM_TAG_SIBLINGS:
                
                ( info, ) = self._execute( 'SELECT COUNT( * ) FROM {};'.format( current_tag_siblings_table_name ) ).fetchone()
                
            elif info_type == HC.SERVICE_INFO_NUM_DELETED_TAG_SIBLINGS:
                
                ( info, ) = self._execute( 'SELECT COUNT( * ) FROM {};'.format( deleted_tag_siblings_table_name ) ).fetchone()
                
            elif info_type == HC.SERVICE_INFO_NUM_PENDING_TAG_SIBLINGS:
                
                ( info, ) = self._execute( 'SELECT COUNT( * ) FROM {};'.format( pending_tag_siblings_table_name ) ).fetchone()
                
            elif info_type == HC.SERVICE_INFO_NUM_PETITIONED_TAG_SIBLINGS:
                
                ( info, ) = self._execute( 'SELECT COUNT( * ) FROM {};'.format( petitioned_tag_siblings_table_name ) ).fetchone()
                
            elif info_type == HC.SERVICE_INFO_NUM_ACTIONABLE_SIBLING_ADD_PETITIONS:
                
                ( info, ) = self._execute( 'SELECT COUNT( * ) FROM ( SELECT DISTINCT account_id, reason_id FROM {} EXCEPT SELECT DISTINCT account_id, reason_id FROM {} );'.format( pending_tag_siblings_table_name, petitioned_tag_siblings_table_name ) ).fetchone()
                
            elif info_type == HC.SERVICE_INFO_NUM_ACTIONABLE_SIBLING_DELETE_PETITIONS:
                
                ( info, ) = self._execute( 'SELECT COUNT( * ) FROM ( SELECT DISTINCT account_id, reason_id FROM {} );'.format( petitioned_tag_siblings_table_name ) ).fetchone()
                
            elif info_type == HC.SERVICE_INFO_NUM_TAG_PARENTS:
                
                ( info, ) = self._execute( 'SELECT COUNT( * ) FROM {};'.format( current_tag_parents_table_name ) ).fetchone()
                
            elif info_type == HC.SERVICE_INFO_NUM_DELETED_TAG_PARENTS:
                
                ( info, ) = self._execute( 'SELECT COUNT( * ) FROM {};'.format( deleted_tag_parents_table_name ) ).fetchone()
                
            elif info_type == HC.SERVICE_INFO_NUM_PENDING_TAG_PARENTS:
                
                ( info, ) = self._execute( 'SELECT COUNT( * ) FROM {};'.format( pending_tag_parents_table_name ) ).fetchone()
                
            elif info_type == HC.SERVICE_INFO_NUM_PETITIONED_TAG_PARENTS:
                
                ( info, ) = self._execute( 'SELECT COUNT( * ) FROM {};'.format( petitioned_tag_parents_table_name ) ).fetchone()
                
            elif info_type == HC.SERVICE_INFO_NUM_ACTIONABLE_PARENT_ADD_PETITIONS:
                
                ( info, ) = self._execute( 'SELECT COUNT( * ) FROM ( SELECT DISTINCT account_id, reason_id FROM {} EXCEPT SELECT DISTINCT account_id, reason_id FROM {} );'.format( pending_tag_parents_table_name, petitioned_tag_parents_table_name ) ).fetchone()
                
            elif info_type == HC.SERVICE_INFO_NUM_ACTIONABLE_PARENT_DELETE_PETITIONS:
                
                ( info, ) = self._execute( 'SELECT COUNT( * ) FROM ( SELECT DISTINCT account_id, reason_id FROM {} );'.format( petitioned_tag_parents_table_name ) ).fetchone()
                
            else:
                
                raise Exception( 'Was asked to generate service info for an unknown type: {}'.format( info_type ) )
                
            
            HydrusData.print_text( 'Regenerated a service info number: {} - {} - {}'.format( service_name, HC.service_info_enum_str_lookup[ info_type ], HydrusNumbers.to_human_int( info ) ) )
            
            self._execute( 'INSERT OR IGNORE INTO service_info ( service_id, info_type, info ) VALUES ( ?, ?, ? )', ( service_id, info_type, info ) )
            
        
    
    def _repository_reward_file_petitioners( self, service_id, service_hash_ids, multiplier ):
        
        ( current_files_table_name, deleted_files_table_name, pending_files_table_name, petitioned_files_table_name, ip_addresses_table_name ) = generate_repository_files_table_names( service_id )
        
        counter = collections.Counter()
        
        with self._make_temporary_integer_table( service_hash_ids, 'service_hash_id' ) as temp_hash_ids_table_name:
            
            for ( account_id, count ) in self._execute( 'SELECT account_id, COUNT( * ) FROM {} CROSS JOIN {} USING ( service_hash_id ) GROUP BY account_id;'.format( temp_hash_ids_table_name, petitioned_files_table_name ) ):
                
                counter[ account_id ] += count
                
            
        
        scores = [ ( account_id, count * multiplier ) for ( account_id, count ) in counter.items() ]
        
        self._reward_accounts( service_id, HC.SCORE_PETITION, scores )
        
    
    def _repository_reward_mapping_petitioners( self, service_id, service_tag_id, service_hash_ids, multiplier ):
        
        ( current_mappings_table_name, deleted_mappings_table_name, pending_mappings_table_name, petitioned_mappings_table_name ) = generate_repository_mappings_table_names( service_id )
        
        counter = collections.Counter()
        
        with self._make_temporary_integer_table( service_hash_ids, 'service_hash_id' ) as temp_hash_ids_table_name:
            
            for ( account_id, count ) in self._execute( 'SELECT account_id, COUNT( * ) FROM {} CROSS JOIN {} USING ( service_hash_id ) WHERE service_tag_id = ? GROUP BY account_id;'.format( temp_hash_ids_table_name, petitioned_mappings_table_name ), ( service_tag_id, ) ):
                
                counter[ account_id ] += count
                
            
        
        scores = [ ( account_id, count * multiplier ) for ( account_id, count ) in counter.items() ]
        
        self._reward_accounts( service_id, HC.SCORE_PETITION, scores )
        
    
    def _repository_reward_tag_parent_penders( self, service_id, child_master_tag_id, parent_master_tag_id, multiplier ):
        
        if self._repository_service_tag_id_exists( service_id, child_master_tag_id ):
            
            child_service_tag_id = self._repository_get_service_tag_id( service_id, child_master_tag_id, HydrusTime.get_now() )
            
            score = self._repository_get_current_mappings_count( service_id, child_service_tag_id )
            
        else:
            
            score = 1
            
        
        score = max( score, 1 )
        
        weighted_score = score * multiplier
        
        ( current_tag_parents_table_name, deleted_tag_parents_table_name, pending_tag_parents_table_name, petitioned_tag_parents_table_name ) = generate_repository_tag_parents_table_names( service_id )
        
        account_ids = [ account_id for ( account_id, ) in self._execute( 'SELECT account_id FROM ' + pending_tag_parents_table_name + ' WHERE child_master_tag_id = ? AND parent_master_tag_id = ?;', ( child_master_tag_id, parent_master_tag_id ) ) ]
        
        scores = [ ( account_id, weighted_score ) for account_id in account_ids ]
        
        self._reward_accounts( service_id, HC.SCORE_PETITION, scores )
        
    
    def _repository_reward_tag_parent_petitioners( self, service_id, child_service_tag_id, parent_service_tag_id, multiplier ):
        
        score = self._repository_get_current_mappings_count( service_id, child_service_tag_id )
        
        score = max( score, 1 )
        
        weighted_score = score * multiplier
        
        ( current_tag_parents_table_name, deleted_tag_parents_table_name, pending_tag_parents_table_name, petitioned_tag_parents_table_name ) = generate_repository_tag_parents_table_names( service_id )
        
        account_ids = [ account_id for ( account_id, ) in self._execute( 'SELECT account_id FROM ' + petitioned_tag_parents_table_name + ' WHERE child_service_tag_id = ? AND parent_service_tag_id = ?;', ( child_service_tag_id, parent_service_tag_id ) ) ]
        
        scores = [ ( account_id, weighted_score ) for account_id in account_ids ]
        
        self._reward_accounts( service_id, HC.SCORE_PETITION, scores )
        
    
    def _repository_reward_tag_sibling_penders( self, service_id, bad_master_tag_id, good_master_tag_id, multiplier ):
        
        if self._repository_service_tag_id_exists( service_id, bad_master_tag_id ):
            
            bad_service_tag_id = self._repository_get_service_tag_id( service_id, bad_master_tag_id, HydrusTime.get_now() )
            
            score = self._repository_get_current_mappings_count( service_id, bad_service_tag_id )
            
        else:
            
            score = 1
            
        
        score = max( score, 1 )
        
        weighted_score = score * multiplier
        
        ( current_tag_siblings_table_name, deleted_tag_siblings_table_name, pending_tag_siblings_table_name, petitioned_tag_siblings_table_name ) = generate_repository_tag_siblings_table_names( service_id )
        
        account_ids = [ account_id for ( account_id, ) in self._execute( 'SELECT account_id FROM ' + pending_tag_siblings_table_name + ' WHERE bad_master_tag_id = ? AND good_master_tag_id = ?;', ( bad_master_tag_id, good_master_tag_id ) ) ]
        
        scores = [ ( account_id, weighted_score ) for account_id in account_ids ]
        
        self._reward_accounts( service_id, HC.SCORE_PETITION, scores )
        
    
    def _repository_reward_tag_sibling_petitioners( self, service_id, bad_service_tag_id, good_service_tag_id, multiplier ):
        
        score = self._repository_get_current_mappings_count( service_id, bad_service_tag_id )
        
        score = max( score, 1 )
        
        weighted_score = score * multiplier
        
        ( current_tag_siblings_table_name, deleted_tag_siblings_table_name, pending_tag_siblings_table_name, petitioned_tag_siblings_table_name ) = generate_repository_tag_siblings_table_names( service_id )
        
        account_ids = [ account_id for ( account_id, ) in self._execute( 'SELECT account_id FROM ' + petitioned_tag_siblings_table_name + ' WHERE bad_service_tag_id = ? AND good_service_tag_id = ?;', ( bad_service_tag_id, good_service_tag_id ) ) ]
        
        scores = [ ( account_id, weighted_score ) for account_id in account_ids ]
        
        self._reward_accounts( service_id, HC.SCORE_PETITION, scores )
        
    
    def _repository_service_hash_id_exists( self, service_id, master_hash_id ):
        
        ( hash_id_map_table_name, tag_id_map_table_name ) = generate_repository_master_map_table_names( service_id )
        
        result = self._execute( 'SELECT 1 FROM ' + hash_id_map_table_name + ' WHERE master_hash_id = ?;', ( master_hash_id, ) ).fetchone()
        
        if result is None:
            
            return False
            
        else:
            
            return True
            
        
    
    def _repository_service_tag_id_exists( self, service_id, master_tag_id ):
        
        ( hash_id_map_table_name, tag_id_map_table_name ) = generate_repository_master_map_table_names( service_id )
        
        result = self._execute( 'SELECT 1 FROM ' + tag_id_map_table_name + ' WHERE master_tag_id = ?;', ( master_tag_id, ) ).fetchone()
        
        if result is None:
            
            return False
            
        else:
            
            return True
            
        
    
    def _repository_update_service_info( self, service_id: int, info_type: int, delta: int ):
        
        if delta == 0:
            
            return
            
        
        self._execute( 'UPDATE service_info SET info = info + ? WHERE service_id = ? AND info_type = ?;', ( delta, service_id, info_type ) )
        
    
    def _reward_accounts( self, service_id, score_type, scores ):
        
        self._execute_many( 'INSERT OR IGNORE INTO account_scores ( service_id, account_id, score_type, score ) VALUES ( ?, ?, ?, ? );', [ ( service_id, account_id, score_type, 0 ) for ( account_id, score ) in scores ] )
        
        self._execute_many( 'UPDATE account_scores SET score = score + ? WHERE service_id = ? AND account_id = ? and score_type = ?;', [ ( score, service_id, account_id, score_type ) for ( account_id, score ) in scores ] )
        
    
    def _save_accounts( self, service_id, accounts ):
        
        for account in accounts:
            
            ( account_key, account_type, created, expires, dictionary ) = HydrusNetwork.Account.generate_tuple_from_account( account )
            
            dictionary_string = dictionary.dump_to_string()
            
            self._execute( 'UPDATE accounts SET dictionary_string = ? WHERE account_key = ?;', ( dictionary_string, sqlite3.Binary( account_key ) ) )
            
            account.set_clean()
            
        
    
    def _save_dirty_accounts( self, service_keys_to_dirty_accounts ):
        
        for ( service_key, dirty_accounts ) in service_keys_to_dirty_accounts.items():
            
            service_id = self._get_service_id( service_key )
            
            self._save_accounts( service_id, dirty_accounts )
            
        
    
    def _save_dirty_services( self, dirty_services ):
        
        self._save_services( dirty_services )
        
    
    def _save_services( self, services ):
        
        for service in services:
            
            ( service_key, service_type, name, port, dictionary ) = service.to_tuple()
            
            dictionary_string = dictionary.DumpToString()
            
            self._execute( 'UPDATE services SET dictionary_string = ? WHERE service_key = ?;', ( dictionary_string, sqlite3.Binary( service_key ) ) )
            
            service.set_clean()
            
        
    
    def _update_db( self, version ):
        
        HydrusData.print_text( 'The server is updating to version ' + str( version + 1 ) )
        
        if version == 433:
            
            old_data = self._execute( 'SELECT account_type_id, service_id, account_type_key, title, dictionary_string FROM account_types;' ).fetchall()
            
            self._execute( 'DROP TABLE account_types;' )
            
            from hydrus.core.networking import HydrusNetworkLegacy
            
            self._execute( 'CREATE TABLE account_types ( account_type_id INTEGER PRIMARY KEY, service_id INTEGER, dump TEXT );' )
            
            for ( account_type_id, service_id, account_type_key, title, dictionary_string ) in old_data:
                
                account_type = HydrusNetworkLegacy.convert_to_new_account_type( account_type_key, title, dictionary_string )
                
                dump = account_type.dump_to_string()
                
                self._execute( 'INSERT INTO account_types ( account_type_id, service_id, dump ) VALUES ( ?, ?, ? );', ( account_type_id, service_id, dump ) )
                
            
        
        if version == 445:
            
            # ok, time for null account!
            
            service_ids = self._get_service_ids()
            
            for service_id in service_ids:
                
                service_key = self._get_service_key( service_id )
                
                service_null_account_type = HydrusNetwork.AccountType.generate_null_account_type()
                
                service_null_account_type_id = self._add_account_type( service_id, service_null_account_type )
                
                self._refresh_account_info_cache()
                
                expires = None
                
                [ registration_key ] = self._generate_registration_keys( service_id, 1, service_null_account_type_id, expires )
                
                null_access_key = self._get_access_key( service_key, registration_key )
                
                null_account = self._get_account_key_from_access_key( service_key, null_access_key )
                
                self._refresh_account_info_cache()
                
            
        
        if version == 463:
            
            result = self._execute( 'SELECT 1 FROM sqlite_master WHERE name = ?;', ( 'deferred_physical_file_deletes', ) ).fetchone()
            
            if result is None:
                
                self._execute( 'CREATE TABLE deferred_physical_file_deletes ( master_hash_id INTEGER PRIMARY KEY );' )
                self._execute( 'CREATE TABLE deferred_physical_thumbnail_deletes ( master_hash_id INTEGER PRIMARY KEY );' )
                
                HydrusData.print_text( 'Populating deferred physical file delete tables' + HC.UNICODE_ELLIPSIS )
                
                for service_id in self._get_service_ids( ( HC.FILE_REPOSITORY, ) ):
                    
                    ( hash_id_map_table_name, tag_id_map_table_name ) = generate_repository_master_map_table_names( service_id )
                    ( current_files_table_name, deleted_files_table_name, pending_files_table_name, petitioned_files_table_name, ip_addresses_table_name ) = generate_repository_files_table_names( service_id )
                    
                    for ( block_of_master_hash_ids, num_done, num_to_do ) in HydrusDB.read_large_id_query_in_separate_chunks( self._c, 'SELECT master_hash_id FROM {} CROSS JOIN {} USING ( service_hash_id );'.format( deleted_files_table_name, hash_id_map_table_name ), 1024 ):
                        
                        self._defer_files_delete_if_now_orphan( block_of_master_hash_ids )
                        
                    
                
            
        
        if version == 497:
            
            self._execute( 'CREATE TABLE IF NOT EXISTS service_info ( service_id INTEGER, info_type INTEGER, info INTEGER, PRIMARY KEY ( service_id, info_type ) );' )
            
            HydrusData.print_text( 'Populating new cached counts table' + HC.UNICODE_ELLIPSIS )
            
            self._repository_regenerate_service_info()
            
        
        HydrusData.print_text( 'The server has updated to version ' + str( version + 1 ) )
        
        self._execute( 'UPDATE version SET version = ?;', ( version + 1, ) )
        
    
    def _vacuum( self ):
        
        locked = HG.server_busy.acquire( False ) # pylint: disable=E1111
        
        if not locked:
            
            HydrusData.print_text( 'Could not vacuum because the server was locked!' )
            
            return
            
        
        try:
            
            db_names = [ name for ( index, name, path ) in self._execute( 'PRAGMA database_list;' ) if name not in ( 'mem', 'temp', 'durable_temp' ) ]
            
            db_names = [ name for name in db_names if name in self._db_filenames ]
            
            ok_db_names = []
            
            for name in db_names:
                
                db_path = os.path.join( self._db_dir, self._db_filenames[ name ] )
                
                try:
                    
                    HydrusDB.check_can_vacuum_into_cursor( db_path, self._c )
                    
                except Exception as e:
                    
                    HydrusData.print_text( 'Cannot vacuum "{}": {}'.format( db_path, e ) )
                    
                    continue
                    
                
                ok_db_names.append( name )
                
            
            db_names = ok_db_names
            
            if len( db_names ) > 0:
                
                self._close_db_connection()
                
                try:
                    
                    names_done = []
                    
                    for name in db_names:
                        
                        try:
                            
                            db_path = os.path.join( self._db_dir, self._db_filenames[ name ] )
                            
                            started = HydrusTime.get_now_precise()
                            
                            HydrusDB.vacuum_db_into( db_path )
                            
                            time_took = HydrusTime.get_now_precise() - started
                            
                            HydrusData.print_text( 'Vacuumed ' + db_path + ' in ' + HydrusTime.timedelta_to_pretty_timedelta( time_took ) )
                            
                            names_done.append( name )
                            
                        except Exception as e:
                            
                            HydrusData.print_text( 'vacuum failed:' )
                            
                            HydrusData.ShowException( e )
                            
                            return
                            
                        
                    
                finally:
                    
                    self._init_db_connection()
                    
                
            
        finally:
            
            HG.server_busy.release()
            
        
    
    def _verify_access_key( self, service_key, access_key ):
        
        service_id = self._get_service_id( service_key )
        
        result = self._execute( 'SELECT 1 FROM accounts WHERE service_id = ? AND hashed_access_key = ?;', ( service_id, sqlite3.Binary( hashlib.sha256( access_key ).digest() ) ) ).fetchone()
        
        if result is None:
            
            result = self._execute( 'SELECT 1 FROM registration_keys WHERE service_id = ? AND access_key = ?;', ( service_id, sqlite3.Binary( access_key ) ) ).fetchone()
            
            if result is None:
                
                return False
                
            
        
        return True
        
    
    def get_files_dir( self ):
        
        return self._files_dir
        
    
