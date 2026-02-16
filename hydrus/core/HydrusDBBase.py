import collections
import collections.abc
import threading
import time

import sqlite3

from hydrus.core import HydrusData
from hydrus.core import HydrusExceptions
from hydrus.core import HydrusPaths
from hydrus.core import HydrusProfiling
from hydrus.core import HydrusPSUtil
from hydrus.core import HydrusGlobals as HG
from hydrus.core import HydrusTemp
from hydrus.core import HydrusTime

def check_has_space_for_db_transaction(db_dir, num_bytes, no_temp_needed = False):
    
    if no_temp_needed:
        
        temp_space_needed = 0
        
        destination_space_needed = int( num_bytes * 1.1 )
        
    else:
        
        temp_space_needed = int( num_bytes * 1.1 )
        
        destination_space_needed = temp_space_needed * 2 # not only do we need the space on disk, we'll have a very brief WAL copy!
        
    
    if HG.no_db_temp_files:
        
        if HydrusPSUtil.PSUTIL_OK:
            
            approx_available_memory = HydrusPSUtil.psutil.virtual_memory().available * 4 / 5
            
            if approx_available_memory < num_bytes:
                
                raise Exception( f'I believe you need about {HydrusData.to_human_bytes(temp_space_needed)} available memory, since you are running in no_db_temp_files mode, but you only seem to have {HydrusData.to_human_bytes(approx_available_memory)}.')
                
            
        
        db_disk_free_space = HydrusPaths.get_free_space(db_dir)
        
        if db_disk_free_space is not None and db_disk_free_space < destination_space_needed:
            
            raise Exception( f'I believe you need about {HydrusData.to_human_bytes(destination_space_needed)} on your db\'s disk partition (perhaps only temporarily), but you only seem to have {HydrusData.to_human_bytes(db_disk_free_space)}.')
            
        
    else:
        
        temp_dir = HydrusTemp.get_current_sq_lite_temp_dir()
        
        temp_disk_free_space = HydrusPaths.get_free_space(temp_dir)
        
        temp_and_db_on_same_device = HydrusPaths.get_device(temp_dir) == HydrusPaths.get_device(db_dir)
        
        if temp_and_db_on_same_device and temp_space_needed > 0:
            
            space_needed = temp_space_needed + destination_space_needed
            
            if temp_disk_free_space is not None and temp_disk_free_space < space_needed:
                
                raise Exception( f'I believe you need about {HydrusData.to_human_bytes(space_needed)} on your db\'s disk partition (perhaps only temporarily), which I think also holds your SQLite temporary path ({temp_dir}), but you only seem to have {HydrusData.to_human_bytes(temp_disk_free_space)}.')
                
            
        else:
            
            if temp_disk_free_space is not None and temp_disk_free_space < temp_space_needed:
                
                message = f'I believe you need about {HydrusData.to_human_bytes(temp_space_needed)} free on the disk partition holding your SQLite temporary path, which I think is "{temp_dir}", but you only seem to have {HydrusData.to_human_bytes(temp_disk_free_space)}.'
                
                temp_total_space = HydrusPaths.get_total_space(temp_dir)
                
                if temp_total_space is not None and temp_total_space <= 4 * 1024 * 1024 * 1024:
                    
                    message += ' I think you might be using a ramdisk! You may want to instead launch hydrus with a different temp dir. Please check the "launch arguments" section of the help.'
                    
                else:
                    
                    message += ' Please note that temporary paths can be complicated, and if you have a ramdisk or OS settings limiting how large it can get, or you simply cannot free space on your system drive, you may want to instead launch hydrus with a different temp directory. Please check the "launch arguments" section of the help.'
                    
                
                raise Exception( message )
                
            
            db_disk_free_space = HydrusPaths.get_free_space(db_dir)
            
            if db_disk_free_space is not None and db_disk_free_space < destination_space_needed:
                
                raise Exception( f'I believe you need about {HydrusData.to_human_bytes(destination_space_needed)} on your db\'s disk partition (perhaps only temporarily), but you only seem to have {HydrusData.to_human_bytes(db_disk_free_space)}.')
                
            
        
    

def read_from_cancellable_cursor(cursor, largest_group_size, cancelled_hook = None):
    
    if cancelled_hook is None:
        
        return cursor.fetchall()
        
    
    results = []
    
    if cancelled_hook():
        
        return results
        
    
    NUM_TO_GET = 1
    
    group_of_results = cursor.fetchmany( NUM_TO_GET )
    
    while len( group_of_results ) > 0:
        
        results.extend( group_of_results )
        
        if cancelled_hook():
            
            break
            
        
        if NUM_TO_GET < largest_group_size:
            
            NUM_TO_GET *= 2
            
        
        group_of_results = cursor.fetchmany( NUM_TO_GET )
        
    
    return results
    

class TemporaryIntegerTableNameCache( object ):
    
    my_instance = None
    
    def __init__( self ):
        
        TemporaryIntegerTableNameCache.my_instance = self
        
        self._column_name_tuples_to_table_names = collections.defaultdict( collections.deque )
        self._column_name_tuples_counter = collections.Counter()
        
    
    @staticmethod
    def instance() -> 'TemporaryIntegerTableNameCache':
        
        if TemporaryIntegerTableNameCache.my_instance is None:
            
            raise Exception( 'TemporaryIntegerTableNameCache is not yet initialised!' )
            
        else:
            
            return TemporaryIntegerTableNameCache.my_instance
            
        
    
    def clear(self):
        
        self._column_name_tuples_to_table_names = collections.defaultdict( collections.deque )
        self._column_name_tuples_counter = collections.Counter()
        
    
    def get_name(self, column_names: tuple[ str]):
        
        if isinstance( column_names, str ):
            
            column_names = ( column_names, )
            
        
        table_names = self._column_name_tuples_to_table_names[ column_names ]
        
        initialised = True
        
        if len( table_names ) == 0:
            
            initialised = False
            
            i = self._column_name_tuples_counter[ column_names ]
            
            table_name = f'mem.temp_{len(column_names)}_int_{"_".join(column_names)}_{i}'
            
            table_names.append( table_name )
            
            self._column_name_tuples_counter[ column_names ] += 1
            
        
        table_name = table_names.pop()
        
        return ( initialised, table_name )
        
    
    def release_name(self, column_names: tuple[ str], table_name: str):
        
        if isinstance( column_names, str ):
            
            column_names = ( column_names, )
            
        
        self._column_name_tuples_to_table_names[ column_names ].append( table_name )
        
    

class TemporaryIntegerTable( object ):
    
    def __init__( self, cursor: sqlite3.Cursor, integers_iterable, column_names ):
        
        if not isinstance( integers_iterable, set ):
            
            integers_iterable = set( integers_iterable )
            
        
        if isinstance( column_names, str ):
            
            column_names = ( column_names, )
            
        
        self._cursor = cursor
        self._integers_iterable = integers_iterable
        self._column_names = column_names
        
        ( self._initialised, self._table_name ) = TemporaryIntegerTableNameCache.instance().get_name(self._column_names)
        
    
    def __enter__( self ):
        
        if len( self._column_names ) == 1:
            
            ( column_name, ) = self._column_names
            
            if not self._initialised:
                
                self._cursor.execute( 'CREATE TABLE IF NOT EXISTS {} ( {} INTEGER PRIMARY KEY );'.format( self._table_name, column_name ) )
                
            
            self._cursor.executemany( 'INSERT INTO {} ( {} ) VALUES ( ? );'.format( self._table_name, column_name ), ( ( i, ) for i in self._integers_iterable ) )
            
        else:
            
            if not self._initialised:
                
                column_defs = ', '.join( ( f'{column_name} INTEGER' for column_name in self._column_names ) )
                
                columns_listed = ', '.join( self._column_names )
                
                column_defs += f', PRIMARY KEY ( {columns_listed} )'
                
                self._cursor.execute( f'CREATE TABLE IF NOT EXISTS {self._table_name} ( {column_defs} );' )
                
                if '.' in self._table_name:
                    
                    table_name_simple = self._table_name.split( '.' )[1]
                    
                else:
                    
                    table_name_simple = self._table_name
                    
                
                for column_name in self._column_names:
                    
                    index_name = f'{self._table_name}_{column_name}_index'
                    
                    self._cursor.execute( f'CREATE INDEX IF NOT EXISTS {index_name} ON {table_name_simple} ({column_name});' )
                    
                
            
            column_tup = ', '.join( self._column_names )
            qmark_tup = ', '.join( ( '?' for i in self._column_names ) )
            
            self._cursor.executemany( f'INSERT INTO {self._table_name} ( {column_tup} ) VALUES ( {qmark_tup} );', self._integers_iterable )
            
        
        return self._table_name
        
    
    def __exit__( self, exc_type, exc_val, exc_tb ):
        
        self._cursor.execute( 'DELETE FROM {};'.format( self._table_name ) )
        
        TemporaryIntegerTableNameCache.instance().release_name(self._column_names, self._table_name)
        
        return False
        
    

class JobDatabase( object ):
    
    def __init__( self, job_type, synchronous, action, *args, **kwargs ):
        
        self._type = job_type
        self._synchronous = synchronous
        self._action = action
        self._args = args
        self._kwargs = kwargs
        
        self._result_ready = threading.Event()
        
    
    def __str__( self ):
        
        return 'DB Job: {}'.format(self.to_string())
        
    
    def _do_delayed_result_relief(self):
        
        pass
        
    
    def get_callable_tuple(self):
        
        return ( self._action, self._args, self._kwargs )
        
    
    def get_result(self):
        
        time.sleep( 0.00001 ) # this one neat trick can save hassle on superquick jobs as event.wait can be laggy
        
        while True:
            
            result_was_ready = self._result_ready.wait( 2 )
            
            if result_was_ready:
                
                break
                
            
            if HG.model_shutdown:
                
                raise HydrusExceptions.ShutdownException( 'Application quit before db could serve result!' )
                
            
            self._do_delayed_result_relief()
            
        
        if isinstance( self._result, Exception ):
            
            e = self._result
            
            raise e
            
        else:
            
            return self._result
            
        
    
    def get_type(self):
        
        return self._type
        
    
    def is_synchronous(self):
        
        return self._synchronous
        
    
    def put_result(self, result):
        
        self._result = result
        
        self._result_ready.set()
        
    
    def to_string(self):
        
        return '{} {}'.format( self._type, self._action )
        
    

class DBBase( object ):
    
    def __init__( self ):
        
        self._c = None
        
    
    def _analyze_temp_table(self, temp_table_name):
        
        # this is useful to do after populating a temp table so the query planner can decide which index to use in a big join that uses it
        
        self._execute('ANALYZE {};'.format(temp_table_name))
        self._execute('ANALYZE mem.sqlite_master;') # this reloads the current stats into the query planner, may no longer be needed
        
    
    def _close_cursor(self):
        
        if self._c is not None:
            
            self._c.close()
            
            del self._c
            
            self._c = None
            
        
    
    def _create_index(self, table_name, columns, unique = False):
        
        if unique:
            
            create_phrase = 'CREATE UNIQUE INDEX IF NOT EXISTS'
            
        else:
            
            create_phrase = 'CREATE INDEX IF NOT EXISTS'
            
        
        ideal_index_name = self._generate_ideal_index_name(table_name, columns)
        
        index_name = ideal_index_name
        
        i = 0
        
        while self._actual_index_exists(index_name):
            
            index_name = f'{ideal_index_name}_{i}'
            
            i += 1
            
        
        if '.' in table_name:
            
            table_name_simple = table_name.split( '.' )[1]
            
        else:
            
            table_name_simple = table_name
            
        
        statement = '{} {} ON {} ({});'.format( create_phrase, index_name, table_name_simple, ', '.join( columns ) )
        
        self._execute(statement)
        
    
    def _execute(self, query, *query_args) -> sqlite3.Cursor:
        
        if HydrusProfiling.query_planner_mode and query not in HydrusProfiling.queries_planned:
            
            plan_lines = self._c.execute( 'EXPLAIN QUERY PLAN {}'.format( query ), *query_args ).fetchall()
            
            HydrusProfiling.print_query_plan(query, plan_lines)
            
        
        return self._c.execute( query, *query_args )
        
    
    def _execute_cancellable(self, query, query_args, cancelled_hook: collections.abc.Callable[ [], bool]):
        
        if cancelled_hook is not None and cancelled_hook():
            
            return []
            
        
        cursor = self._execute(query, query_args)
        
        return read_from_cancellable_cursor(cursor, 1024, cancelled_hook = cancelled_hook)
        
    
    def _execute_many(self, query, args_iterator):
        
        if HydrusProfiling.query_planner_mode and query not in HydrusProfiling.queries_planned:
            
            args_iterator = list( args_iterator )
            
            if len( args_iterator ) > 0:
                
                plan_lines = self._c.execute( 'EXPLAIN QUERY PLAN {}'.format( query ), args_iterator[0] ).fetchall()
                
                HydrusProfiling.print_query_plan(query, plan_lines)
                
            
        
        self._c.executemany( query, args_iterator )
        
    
    def _generate_ideal_index_name(self, table_name, columns):
        
        return '{}_{}_index'.format( table_name, '_'.join( columns ) )
        
    
    def _get_attached_database_names(self, include_temp = False):
        
        if include_temp:
            
            f = lambda schema_name, path: True
            
        else:
            
            f = lambda schema_name, path: schema_name != 'temp' and path != ''
            
        
        names = [schema_name for ( number, schema_name, path ) in self._execute('PRAGMA database_list;') if f(schema_name, path)]
        
        return names
        
    
    def _get_last_row_id(self) -> int:
        
        return self._c.lastrowid
        
    
    def _get_row_count(self):
        
        row_count = self._c.rowcount
        
        if row_count == -1:
            
            return 0
            
        else:
            
            return row_count
            
        
    
    def _get_sum_result(self, result: tuple[int | None] | None) -> int:
        
        if result is None or result[0] is None:
            
            sum_value = 0
            
        else:
            
            ( sum_value, ) = result
            
        
        return sum_value
        
    
    def _actual_index_exists(self, index_name):
        
        if '.' in index_name:
            
            ( schema, index_name ) = index_name.split( '.', 1 )
            
            search_schemas = [ schema ]
            
        else:
            
            search_schemas = self._get_attached_database_names()
            
        
        for schema in search_schemas:
            
            result = self._execute(f'SELECT 1 FROM {schema}.sqlite_master WHERE name = ? and type = ?;', (index_name, 'index')).fetchone()
            
            if result is not None:
                
                return True
                
            
        
        return False
        
    
    def _ideal_index_exists(self, table_name, columns):
        
        # ok due to deferred delete gubbins, we have overlapping index names. therefore this has to be more flexible than a static name
        # we'll search based on tbl_name in sqlite_master
        
        ideal_index_name = self._generate_ideal_index_name(table_name, columns)
        
        if '.' in ideal_index_name:
            
            ( schema, ideal_index_name ) = ideal_index_name.split( '.', 1 )
            
            search_schemas = [ schema ]
            
        else:
            
            search_schemas = self._get_attached_database_names()
            
        
        if '.' in table_name:
            
            table_name = table_name.split( '.', 1 )[1]
            
        
        for schema in search_schemas:
            
            table_result = self._execute(f'SELECT 1 FROM {schema}.sqlite_master WHERE name = ?;', (table_name,)).fetchone()
            
            if table_result is None:
                
                continue
                
            
            # ok the table exists on this db, so let's see if it has our index, whatever its actual name
            
            all_indices_of_this_table = self._stl(self._execute(f'SELECT name FROM {schema}.sqlite_master WHERE tbl_name = ? AND type = ?;', (table_name, 'index')))
            
            for index_name in all_indices_of_this_table:
                
                if ideal_index_name in index_name:
                    
                    return True
                    
                
            
        
        return False
        
    
    def _make_temporary_integer_table(self, integers_iterable, column_names):
        
        return TemporaryIntegerTable( self._c, integers_iterable, column_names )
        
    
    def _set_cursor(self, c: sqlite3.Cursor):
        
        self._c = c
        
    
    def _sti(self, iterable_cursor):
        
        # strip singleton tuples to an iterator
        
        return ( item for ( item, ) in iterable_cursor )
        
    
    def _stl(self, iterable_cursor):
        
        # strip singleton tuples to a list
        
        return [ item for ( item, ) in iterable_cursor ]
        
    
    def _sts(self, iterable_cursor):
        
        # strip singleton tuples to a set
        
        return { item for ( item, ) in iterable_cursor }
        
    
    def _table_exists(self, table_name):
        
        if '.' in table_name:
            
            ( schema, table_name ) = table_name.split( '.', 1 )
            
            search_schemas = [ schema ]
            
        else:
            
            search_schemas = self._get_attached_database_names()
            
        
        for schema in search_schemas:
            
            result = self._execute(f'SELECT 1 FROM {schema}.sqlite_master WHERE name = ? AND type = ?;', (table_name, 'table')).fetchone()
            
            if result is not None:
                
                return True
                
            
        
        return False
        
    

JOURNAL_SIZE_LIMIT = 128 * 1024 * 1024
JOURNAL_ZERO_PERIOD = 900
MEM_REFRESH_PERIOD = 600
WAL_PASSIVE_CHECKPOINT_PERIOD = 300
WAL_TRUNCATE_CHECKPOINT_PERIOD = 900

class DBCursorTransactionWrapper( DBBase ):
    
    def __init__( self, c: sqlite3.Cursor, transaction_commit_period: int ):
        
        super().__init__()
        
        self._set_cursor(c)
        
        self._transaction_commit_period = transaction_commit_period
        
        self._transaction_start_time = 0
        self._in_transaction = False
        self._transaction_contains_writes = False
        
        self._committing_as_soon_as_possible = False
        
        self._last_mem_refresh_time = HydrusTime.get_now()
        self._last_wal_passive_checkpoint_time = HydrusTime.get_now()
        self._last_wal_truncate_checkpoint_time = HydrusTime.get_now()
        self._last_journal_zero_time = HydrusTime.get_now()
        
        self._pubsubs = []
        
    
    def _zero_journal(self):
        
        if HG.db_journal_mode not in ( 'PERSIST', 'WAL' ):
            
            return
            
        
        self._execute('BEGIN IMMEDIATE;')
        
        # durable_temp is not excluded here
        db_names = [name for ( index, name, path ) in self._execute('PRAGMA database_list;') if name not in ('mem', 'temp')]
        
        for db_name in db_names:
            
            self._execute('PRAGMA {}.journal_size_limit = {};'.format(db_name, 0))
            
        
        self._execute('COMMIT;')
        
        for db_name in db_names:
            
            self._execute('PRAGMA {}.journal_size_limit = {};'.format(db_name, JOURNAL_SIZE_LIMIT))
            
        
    
    def begin_immediate(self):
        
        if not self._in_transaction:
            
            self._execute('BEGIN IMMEDIATE;')
            self._execute('SAVEPOINT hydrus_savepoint;')
            
            self._transaction_start_time = HydrusTime.get_now()
            self._in_transaction = True
            self._transaction_contains_writes = False
            
        
    
    def clean_pub_subs(self):
        
        self._pubsubs = []
        
    
    def commit(self):
        
        if self._in_transaction:
            
            self.do_pub_subs()
            
            self.clean_pub_subs()
            
            self._execute('COMMIT;')
            
            self._in_transaction = False
            self._transaction_contains_writes = False
            
            if HG.db_journal_mode == 'WAL' and HydrusTime.time_has_passed(self._last_wal_passive_checkpoint_time + WAL_PASSIVE_CHECKPOINT_PERIOD):
                
                if HydrusTime.time_has_passed(self._last_wal_truncate_checkpoint_time + WAL_TRUNCATE_CHECKPOINT_PERIOD):
                    
                    self._execute('PRAGMA wal_checkpoint(TRUNCATE);')
                    
                    self._last_wal_truncate_checkpoint_time = HydrusTime.get_now()
                    
                else:
                    
                    self._execute('PRAGMA wal_checkpoint(PASSIVE);')
                    
                
                self._last_wal_passive_checkpoint_time = HydrusTime.get_now()
                
            
            if HydrusTime.time_has_passed(self._last_mem_refresh_time + MEM_REFRESH_PERIOD):
                
                self._execute('DETACH mem;')
                self._execute('ATTACH ":memory:" AS mem;')
                
                TemporaryIntegerTableNameCache.instance().clear()
                
                self._last_mem_refresh_time = HydrusTime.get_now()
                
            
            if HG.db_journal_mode == 'PERSIST' and HydrusTime.time_has_passed(self._last_journal_zero_time + JOURNAL_ZERO_PERIOD):
                
                self._zero_journal()
                
                self._last_journal_zero_time = HydrusTime.get_now()
                
            
        else:
            
            HydrusData.print_text('Received a call to commit, but was not in a transaction!')
            
        
        self._committing_as_soon_as_possible = False
        
    
    def commit_and_begin(self):
        
        if self._in_transaction:
            
            self.commit()
            
            self.begin_immediate()
            
        
    
    def do_a_commit_as_soon_as_possible(self):
        
        self._committing_as_soon_as_possible = True
        
    
    def do_pub_subs(self):
        
        for ( topic, args, kwargs ) in self._pubsubs:
            
            HG.controller.pub( topic, *args, **kwargs )
            
        
    
    def is_committing_as_soon_as_possible(self) -> bool:
        
        return self._committing_as_soon_as_possible
        
    
    def in_transaction(self):
        
        return self._in_transaction
        
    
    def notify_write_occuring(self):
        
        self._transaction_contains_writes = True
        
    
    def pub_after_job( self, topic, *args, **kwargs ):
        
        if len( args ) == 0 and len( kwargs ) == 0:
            
            if ( topic, args, kwargs ) in self._pubsubs:
                
                return
                
            
        
        self._pubsubs.append( ( topic, args, kwargs ) )
        
    
    def rollback(self):
        
        if self._in_transaction:
            
            self._execute('ROLLBACK TO hydrus_savepoint;')
            
            # any temp int tables created in this lad will be rolled back, so 'initialised' can't be trusted. just reset, no big deal
            TemporaryIntegerTableNameCache.instance().clear()
            
            # still in transaction
            # transaction may no longer contain writes, but it isn't important to figure out that it doesn't
            
        else:
            
            HydrusData.print_text('Received a call to rollback, but was not in a transaction!')
            
        
    
    def save(self):
        
        if self._in_transaction:
            
            try:
                
                self._execute('RELEASE hydrus_savepoint;')
                
            except sqlite3.OperationalError:
                
                HydrusData.print_text('Tried to release a database savepoint, but failed!')
                
            
            self._execute('SAVEPOINT hydrus_savepoint;')
            
        else:
            
            HydrusData.print_text('Received a call to save, but was not in a transaction!')
            
        
    
    def time_to_commit(self):
        
        p1 = self._committing_as_soon_as_possible
        p2 = self._transaction_contains_writes and HydrusTime.time_has_passed(self._transaction_start_time + self._transaction_commit_period)
        
        return self._in_transaction and ( p1 or p2 )
        
    
