import collections.abc
import sqlite3

from hydrus.core import HydrusDBBase
from hydrus.core import HydrusExceptions

class HydrusDBModule( HydrusDBBase.DBBase ):
    
    CAN_REPOPULATE_ALL_MISSING_DATA = False
    
    def __init__( self, name, cursor: sqlite3.Cursor ):
        
        super().__init__()
        
        self.name = name
        
        self._set_cursor( cursor )
        
    
    def _flatten_index_generation_dict( self, index_generation_dict: dict ):
        
        tuples = []
        
        for ( table_name, index_rows ) in index_generation_dict.items():
            
            tuples.extend( ( ( table_name, columns, unique, version_added ) for ( columns, unique, version_added ) in index_rows ) )
            
        
        return tuples
        
    
    def _create_table( self, create_query_without_name: str, table_name: str ):
        
        if 'fts4(' in create_query_without_name.lower():
            
            # when we want to repair a missing fts4 table, the damaged old virtual table sometimes still has some sub-tables hanging around, which breaks the new create
            # so, let's route all table creation through here and check for and clear any subtables beforehand!
            
            if '.' in table_name:
                
                ( schema, raw_table_name ) = table_name.split( '.', 1 )
                
                sqlite_master_table = '{}.sqlite_master'.format( schema )
                
            else:
                
                raw_table_name = table_name
                sqlite_master_table = 'sqlite_master'
                
            
            # little test here to make sure we stay idempotent if the primary table actually already exists--don't want to delete things that are actually good!
            if self._execute( 'SELECT 1 FROM {} WHERE name = ?;'.format( sqlite_master_table ), ( raw_table_name, ) ).fetchone() is None:
                
                possible_suffixes = [ '_content', '_docsize', '_segdir', '_segments', '_stat' ]
                
                possible_subtable_names = [ '{}{}'.format( raw_table_name, suffix ) for suffix in possible_suffixes ]
                
                for possible_subtable_name in possible_subtable_names:
                    
                    if self._execute( 'SELECT 1 FROM {} WHERE name = ?;'.format( sqlite_master_table ), ( possible_subtable_name, ) ).fetchone() is not None:
                        
                        self._execute( 'DROP TABLE {};'.format( possible_subtable_name ) )
                        
                    
                
            
        
        self._execute( create_query_without_name.format( table_name ) )
        
    
    def _do_last_shutdown_was_bad_work( self ):
        
        pass
        
    
    def _get_critical_table_names( self ) -> collections.abc.Collection[ str ]:
        
        return set()
        
    
    def _get_initial_index_generation_dict( self ) -> dict:
        
        return {}
        
    
    def _get_initial_table_generation_dict( self ) -> dict:
        
        return {}
        
    
    def _get_service_index_generation_dict( self, service_id ) -> dict:
        
        return {}
        
    
    def _get_service_table_generation_dict( self, service_id ) -> dict:
        
        return {}
        
    
    def _get_services_index_generation_dict( self ) -> dict:
        
        index_generation_dict = {}
        
        for service_id in self._get_service_ids_we_generate_dynamic_tables_for():
            
            index_generation_dict.update( self._get_service_index_generation_dict( service_id ) )
            
        
        return index_generation_dict
        
    
    def _get_services_table_generation_dict( self ) -> dict:
        
        table_generation_dict = {}
        
        for service_id in self._get_service_ids_we_generate_dynamic_tables_for():
            
            table_generation_dict.update( self._get_service_table_generation_dict( service_id ) )
            
        
        return table_generation_dict
        
    
    def _get_service_table_prefixes( self ) -> collections.abc.Collection[ str ]:
        
        return set()
        
    
    def _get_service_ids_we_generate_dynamic_tables_for( self ):
        
        return []
        
    
    def _present_missing_indices_warning_to_user( self, index_names ):
        
        raise NotImplementedError()
        
    
    def _present_missing_tables_warning_to_user( self, table_names ):
        
        raise NotImplementedError()
        
    
    def _repair_repopulate_tables( self, table_names, cursor_transaction_wrapper: HydrusDBBase.DBCursorTransactionWrapper ):
        
        pass
        
    
    def create_initial_indices( self ):
        
        index_generation_dict = self._get_initial_index_generation_dict()
        
        for ( table_name, columns, unique, version_added ) in self._flatten_index_generation_dict( index_generation_dict ):
            
            self._create_index( table_name, columns, unique = unique )
            
        
    
    def create_initial_tables( self ):
        
        table_generation_dict = self._get_initial_table_generation_dict()
        
        for ( table_name, ( create_query_without_name, version_added ) ) in table_generation_dict.items():
            
            self._create_table( create_query_without_name, table_name )
            
        
    
    def do_last_shutdown_was_bad_work( self ):
        
        self._do_last_shutdown_was_bad_work()
        
    
    def get_expected_service_table_names( self ) -> collections.abc.Collection[ str ]:
        
        table_generation_dict = self._get_services_table_generation_dict()
        
        return list( table_generation_dict.keys() )
        
    
    def get_expected_initial_table_names( self ) -> collections.abc.Collection[ str ]:
        
        table_generation_dict = self._get_initial_table_generation_dict()
        
        return list( table_generation_dict.keys() )
        
    
    def get_surplus_service_table_names( self, all_table_names ) -> set[ str ]:
        
        prefixes = self._get_service_table_prefixes()
        
        if len( prefixes ) == 0:
            
            return set()
            
        
        # careful about the flattening here. after adding more tables here, I ran into issues with db tables either being main.current_files_x or just current_files_x and got (dangerous!!) false positives on the test
        # so, to failsafe, let's merge everything down to just the table name, no db schema, and then we'll catch all possible collisions no matter what the calls are actually giving us here
        
        all_service_table_names = { name if '.' not in name else name.split( '.', 1 )[1] for name in all_table_names }
        
        all_service_table_names = { table_name for table_name in all_service_table_names if True in ( table_name.startswith( prefix ) for prefix in prefixes ) }
        
        good_service_table_names = self.get_expected_service_table_names()
        
        good_service_table_names = { name if '.' not in name else name.split( '.', 1 )[1] for name in good_service_table_names }
        
        surplus_table_names = all_service_table_names.difference( good_service_table_names )
        
        return surplus_table_names
        
    
    def get_tables_and_columns_that_use_definitions( self, content_type: int ) -> list[ tuple[ str, str ] ]:
        
        # could also do another one of these for orphan tables that have service id in the name.
        
        raise NotImplementedError()
        
    
    def repair( self, current_db_version, cursor_transaction_wrapper: HydrusDBBase.DBCursorTransactionWrapper ):
        
        # core, initial tables first
        
        table_generation_dict = self._get_initial_table_generation_dict()
        
        missing_table_rows = [ ( table_name, create_query_without_name ) for ( table_name, ( create_query_without_name, version_added ) ) in table_generation_dict.items() if version_added <= current_db_version and not self._table_exists( table_name ) ]
        
        if len( missing_table_rows ) > 0:
            
            missing_table_names = sorted( [ missing_table_row[0] for missing_table_row in missing_table_rows ] )
            
            critical_table_names = self._get_critical_table_names()
            
            missing_critical_table_names = set( missing_table_names ).intersection( critical_table_names )
            
            if len( missing_critical_table_names ) > 0:
                
                message = 'Unfortunately, this database is missing one or more critical tables! This database is non functional and cannot be repaired. Please check out "install_dir/db/help my db is broke.txt" for the next steps. The table names are:\n\n' + '\n'.join( missing_critical_table_names )
                
                raise HydrusExceptions.DBAccessException( message )
                
            
            self._present_missing_tables_warning_to_user( missing_table_names )
            
            for ( table_name, create_query_without_name ) in missing_table_rows:
                
                self._create_table( create_query_without_name, table_name )
                
                cursor_transaction_wrapper.commit_and_begin()
                
            
            self._repair_repopulate_tables( missing_table_names, cursor_transaction_wrapper )
            
            cursor_transaction_wrapper.commit_and_begin()
            
        
        # now indices for those tables
        
        index_generation_dict = self._get_initial_index_generation_dict()
        
        missing_index_rows = [ ( self._generate_ideal_index_name( table_name, columns ), table_name, columns, unique ) for ( table_name, columns, unique, version_added ) in self._flatten_index_generation_dict( index_generation_dict ) if version_added <= current_db_version and not self._ideal_index_exists( table_name, columns ) ]
        
        if len( missing_index_rows ):
            
            self._present_missing_indices_warning_to_user( sorted( [ index_name for ( index_name, table_name, columns, unique ) in missing_index_rows ] ) )
            
            for ( index_name, table_name, columns, unique ) in missing_index_rows:
                
                self._create_index( table_name, columns, unique = unique )
                
                cursor_transaction_wrapper.commit_and_begin()
                
            
        
        # now do service tables, same thing over again
        
        table_generation_dict = self._get_services_table_generation_dict()
        
        missing_table_rows = [ ( table_name, create_query_without_name ) for ( table_name, ( create_query_without_name, version_added ) ) in table_generation_dict.items() if version_added <= current_db_version and not self._table_exists( table_name ) ]
        
        if len( missing_table_rows ) > 0:
            
            missing_table_names = sorted( [ missing_table_row[0] for missing_table_row in missing_table_rows ] )
            
            self._present_missing_tables_warning_to_user( missing_table_names )
            
            for ( table_name, create_query_without_name ) in missing_table_rows:
                
                self._create_table( create_query_without_name, table_name )
                
                cursor_transaction_wrapper.commit_and_begin()
                
            
            self._repair_repopulate_tables( missing_table_names, cursor_transaction_wrapper )
            
            cursor_transaction_wrapper.commit_and_begin()
            
        
        # now indices for those tables
        
        index_generation_dict = self._get_services_index_generation_dict()
        
        missing_index_rows = [ ( self._generate_ideal_index_name( table_name, columns ), table_name, columns, unique ) for ( table_name, columns, unique, version_added ) in self._flatten_index_generation_dict( index_generation_dict ) if version_added <= current_db_version and not self._ideal_index_exists( table_name, columns ) ]
        
        if len( missing_index_rows ):
            
            self._present_missing_indices_warning_to_user( sorted( [ index_name for ( index_name, table_name, columns, unique ) in missing_index_rows ] ) )
            
            for ( index_name, table_name, columns, unique ) in missing_index_rows:
                
                self._create_index( table_name, columns, unique = unique )
                
                cursor_transaction_wrapper.commit_and_begin()
                
            
        
