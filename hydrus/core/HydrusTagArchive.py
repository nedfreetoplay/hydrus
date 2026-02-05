import os
import sqlite3

# Please feel free to use this file however you wish.
# None of this is thread-safe, though, so don't try to do anything clever.


# If you want to make a new tag archive for use in hydrus, you want to do something like:

# import HydrusTagArchive
# hta = HydrusTagArchive.HydrusTagArchive( 'my_little_archive.db' )
# hta.SetHashType( HydrusTagArchive.HASH_TYPE_MD5 )
# hta.BeginBigJob()
# for ( hash, tags ) in my_complex_mappings_generator: hta.AddMappings( hash, tags )
  # -or-
# for ( hash, tag ) in my_simple_mapping_generator: hta.AddMapping( hash, tag )
# hta.CommitBigJob()
# hta.Optimise() # if you made a lot of changes and want to complete your archive for sharing
# hta.Close()
# del hta


# If you are only adding a couple tags, you can exclude the BigJob stuff. It just makes millions of sequential writes more efficient.


# Also, this manages hashes as bytes, not hex, so if you have something like:

# hash = ab156e87c5d6e215ab156e87c5d6e215

# Then go hash = bytes.fromhex( hash ) before you pass it to Add/Get/Has/SetMappings


# If you have tags that are namespaced like hydrus (e.g. series:ghost in the shell), then check out:
# GetNamespaces
# DeleteNamespaces
# and
# RebuildNamespaces

# RebuildNamespaces takes namespaces_to_exclude, if you want to curate your namespaces a little better.

# If your GetNamespaces gives garbage, then just hit DeleteNamespaces. I'll be using the result of GetNamespaces to populate
# the tag import options widget when people sync with these archives.


# And also feel free to contact me directly at hydrus_dev@proton.me if you need help.

HASH_TYPE_MD5 = 0 # 16 bytes long
HASH_TYPE_SHA1 = 1 # 20 bytes long
HASH_TYPE_SHA256 = 2 # 32 bytes long
HASH_TYPE_SHA512 = 3 # 64 bytes long

hash_type_to_str_lookup = {}

hash_type_to_str_lookup[ HASH_TYPE_MD5 ] = 'md5'
hash_type_to_str_lookup[ HASH_TYPE_SHA1 ] = 'sha1'
hash_type_to_str_lookup[ HASH_TYPE_SHA256 ] = 'sha256'
hash_type_to_str_lookup[ HASH_TYPE_SHA512 ] = 'sha512'

hash_str_to_type_lookup = {}

hash_str_to_type_lookup[ 'md5' ] = HASH_TYPE_MD5
hash_str_to_type_lookup[ 'sha1' ] = HASH_TYPE_SHA1
hash_str_to_type_lookup[ 'sha256' ] = HASH_TYPE_SHA256
hash_str_to_type_lookup[ 'sha512' ] = HASH_TYPE_SHA512

def read_large_id_query_in_separate_chunks( cursor, select_statement, chunk_size ):
    
    table_name = 'tempbigread' + os.urandom( 32 ).hex()
    
    cursor.execute( 'CREATE TEMPORARY TABLE ' + table_name + ' ( job_id INTEGER PRIMARY KEY AUTOINCREMENT, temp_id INTEGER );' )
    
    cursor.execute( 'INSERT INTO ' + table_name + ' ( temp_id ) ' + select_statement ) # given statement should end in semicolon, so we are good
    
    num_to_do = cursor.rowcount
    
    if num_to_do is None or num_to_do == -1:
        
        num_to_do = 0
        
    
    i = 0
    
    while i < num_to_do:
        
        chunk = [ temp_id for ( temp_id, ) in cursor.execute( 'SELECT temp_id FROM ' + table_name + ' WHERE job_id BETWEEN ? AND ?;', ( i, i + chunk_size - 1 ) ) ]
        
        yield chunk
        
        i += chunk_size
        
    
    cursor.execute( 'DROP TABLE ' + table_name + ';' )
    
class HydrusTagArchive( object ):
    
    def __init__( self, path ):
        
        self._path = path
        
        if not os.path.exists( self._path ): create_db = True
        else: create_db = False
        
        self._init_db_connection()
        
        if create_db: self._init_db()
        
        self._namespaces = { namespace for ( namespace, ) in self._c.execute( 'SELECT namespace FROM namespaces;' ) }
        self._namespaces.add( '' )
        
    
    def _add_mappings( self, hash_id, tag_ids ):
        
        self._c.executemany( 'INSERT OR IGNORE INTO mappings ( hash_id, tag_id ) VALUES ( ?, ? );', ( ( hash_id, tag_id ) for tag_id in tag_ids ) )
        
    
    def _init_db( self ):
        
        self._c.execute( 'CREATE TABLE hash_type ( hash_type INTEGER );' )
        
        self._c.execute( 'CREATE TABLE hashes ( hash_id INTEGER PRIMARY KEY, hash BLOB_BYTES );' )
        self._c.execute( 'CREATE UNIQUE INDEX hashes_hash_index ON hashes ( hash );' )
        
        self._c.execute( 'CREATE TABLE mappings ( hash_id INTEGER, tag_id INTEGER, PRIMARY KEY ( hash_id, tag_id ) );' )
        self._c.execute( 'CREATE INDEX mappings_hash_id_index ON mappings ( hash_id );' )
        
        self._c.execute( 'CREATE TABLE namespaces ( namespace TEXT );' )
        
        self._c.execute( 'CREATE TABLE tags ( tag_id INTEGER PRIMARY KEY, tag TEXT );' )
        self._c.execute( 'CREATE UNIQUE INDEX tags_tag_index ON tags ( tag );' )
        
    
    def _init_db_connection( self ):
        
        self._db = sqlite3.connect( self._path, isolation_level = None, detect_types = sqlite3.PARSE_DECLTYPES )
        
        self._c = self._db.cursor()
        
    
    def _get_hashes( self, tag_id ):
        
        result = { hash for ( hash, ) in self._c.execute( 'SELECT hash FROM mappings NATURAL JOIN hashes WHERE tag_id = ?;', ( tag_id, ) ) }
        
        return result
        
    
    def _get_hash_id( self, hash, read_only = False ):
        
        result = self._c.execute( 'SELECT hash_id FROM hashes WHERE hash = ?;', ( sqlite3.Binary( hash ), ) ).fetchone()
        
        if result is None:
            
            if read_only:
                
                raise Exception()
                
            
            self._c.execute( 'INSERT INTO hashes ( hash ) VALUES ( ? );', ( sqlite3.Binary( hash ), ) )
            
            hash_id = self._c.lastrowid
            
        else:
            
            ( hash_id, ) = result
            
        
        return hash_id
        
    
    def _get_tags( self, hash_id ):
        
        result = { tag for ( tag, ) in self._c.execute( 'SELECT tag FROM mappings NATURAL JOIN tags WHERE hash_id = ?;', ( hash_id, ) ) }
        
        return result
        
    
    def _get_tag_id( self, tag, read_only = False ):
        
        result = self._c.execute( 'SELECT tag_id FROM tags WHERE tag = ?;', ( tag, ) ).fetchone()
        
        if result is None:
            
            if read_only:
                
                raise Exception()
                
            
            self._c.execute( 'INSERT INTO tags ( tag ) VALUES ( ? );', ( tag, ) )
            
            tag_id = self._c.lastrowid
            
        else:
            
            ( tag_id, ) = result
            
        
        if ':' in tag:
            
            ( namespace, subtag ) = tag.split( ':', 1 )
            
            if namespace != '' and namespace not in self._namespaces:
                
                self._c.execute( 'INSERT INTO namespaces ( namespace ) VALUES ( ? );', ( namespace, ) )
                
                self._namespaces.add( namespace )
                
            
        
        return tag_id
        
    
    def begin_big_job( self ):
        
        self._c.execute( 'BEGIN IMMEDIATE;' )
        
    
    def commit_big_job( self ):
        
        self._c.execute( 'COMMIT;' )
        
    
    def add_mapping( self, hash, tag ):
        
        hash_id = self._get_hash_id( hash )
        tag_id = self._get_tag_id( tag )
        
        self._c.execute( 'INSERT OR IGNORE INTO mappings ( hash_id, tag_id ) VALUES ( ?, ? );', ( hash_id, tag_id ) )
        
    
    def add_mappings( self, hash, tags ):
        
        hash_id = self._get_hash_id( hash )
        
        tag_ids = [ self._get_tag_id( tag ) for tag in tags ]
        
        self._add_mappings( hash_id, tag_ids )
        
    
    def close( self ):
        
        self._c.close()
        self._db.close()
        
        self._db = None
        self._c = None
        
    
    def delete_mapping( self, hash, tag ):
        
        hash_id = self._get_hash_id( hash )
        tag_id = self._get_tag_id( tag )
        
        self._c.execute( 'DELETE FROM mappings WHERE hash_id = ? AND tag_id = ?;', ( hash_id, tag_id ) )
        
    
    def delete_mappings( self, hash ): self.delete_tags( hash )
    
    def delete_tags( self, hash ):
        
<<<<<<< HEAD
        try: hash_id = self._get_hash_id( hash, read_only = True )
        except: return
=======
        try:
            
            hash_id = self._GetHashId( hash, read_only = True )
            
        except Exception as e:
            
            return
            
>>>>>>> 955f2e8e9df1d901351bb3dcf4c0a50e99048667
        
        self._c.execute( 'DELETE FROM mappings WHERE hash_id = ?;', ( hash_id, ) )
        
    
    def delete_namespaces( self ):
        
        self._namespaces = set()
        self._namespaces.add( '' )
        
        self._c.execute( 'DELETE FROM namespaces;' )
        
    
    def get_hashes( self, tag ):
        
        try:
            
            tag_id = self._get_tag_id( tag, read_only = True )
            
        except Exception as e:
            
            return set()
            
        
        return self._get_hashes( tag_id )
        
    
    def get_hash_type( self ):
        
        result = self._c.execute( 'SELECT hash_type FROM hash_type;' ).fetchone()
        
        if result is None:
            
            result = self._c.execute( 'SELECT hash FROM hashes;' ).fetchone()
            
            if result is None:
                
                raise Exception( 'This archive has no hash type set, and as it has no files, no hash type guess can be made.' )
                
            
            ( hash, ) = result
            
            hash_len = len( hash )
            
            len_to_hash_type = {}
            
            len_to_hash_type[ 16 ] = HASH_TYPE_MD5
            len_to_hash_type[ 20 ] = HASH_TYPE_SHA1
            len_to_hash_type[ 32 ] = HASH_TYPE_SHA256
            len_to_hash_type[ 64 ] = HASH_TYPE_SHA512
            
            if hash_len in len_to_hash_type:
                
                self.set_hash_type( len_to_hash_type[ hash_len ] )
                
            else:
                
                raise Exception()
                
            
            return self.get_hash_type()
            
        else:
            
            ( hash_type, ) = result
            
            return hash_type
            
        
    
    def get_mappings( self, hash ): return self.get_tags( hash )
    
    def get_name( self ):
        
        filename = os.path.basename( self._path )
        
        if '.' in filename:
            
            filename = filename.split( '.', 1 )[0]
            
        
        return filename
        
    
    def get_namespaces( self ): return self._namespaces
    
    def get_tags( self, hash ):
        
        try:
            
            hash_id = self._get_hash_id( hash, read_only = True )
            
        except Exception as e:
            
            return set()
            
        
        return self._get_tags( hash_id )
        
    
    def has_hash( self, hash ):
        
        try:
            
            hash_id = self._get_hash_id( hash, read_only = True )
            
            return True
            
        except Exception as e:
            
            return False
            
        
    
    def has_hash_type_set( self ):
        
        result = self._c.execute( 'SELECT hash_type FROM hash_type;' ).fetchone()
        
        return result is not None
        
    
    def iterate_hashes( self ):
        
        for ( hash, ) in self._c.execute( 'SELECT hash FROM hashes;' ):
            
            yield hash
            
        
    
    def iterate_mappings( self ):
        
        for group_of_hash_ids in read_large_id_query_in_separate_chunks( self._c, 'SELECT hash_id FROM hashes;', 256 ):
            
            for hash_id in group_of_hash_ids:
                
                tags = self._get_tags( hash_id )
                
                if len( tags ) > 0:
                    
                    ( hash, ) = self._c.execute( 'SELECT hash FROM hashes WHERE hash_id = ?;', ( hash_id, ) ).fetchone()
                    
                    yield ( hash, tags )
                    
                
            
        
    
    def iterate_mappings_tag_first( self ):
        
        for group_of_tag_ids in read_large_id_query_in_separate_chunks( self._c, 'SELECT tag_id FROM tags;', 256 ):
            
            for tag_id in group_of_tag_ids:
                
                hashes = self._get_hashes( tag_id )
                
                if len( hashes ) > 0:
                    
                    ( tag, ) = self._c.execute( 'SELECT tag FROM tags WHERE tag_id = ?;', ( tag_id, ) ).fetchone()
                    
                    yield ( tag, hashes )
                    
                
            
        
    
    def optimise( self ):
        
        self._c.execute( 'VACUUM;' )
        self._c.execute( 'ANALYZE;' )
        
    
    def rebuild_namespaces( self, namespaces_to_exclude = None ):
        
        if namespaces_to_exclude is None:
            
            namespaces_to_exclude = set()
            
        
        self._namespaces = set()
        self._namespaces.add( '' )
        
        self._c.execute( 'DELETE FROM namespaces;' )
        
        for ( tag, ) in self._c.execute( 'SELECT tag FROM tags;' ):
            
            if ':' in tag:
                
                ( namespace, subtag ) = tag.split( ':', 1 )
                
                if namespace != '' and namespace not in self._namespaces and namespace not in namespaces_to_exclude:
                    
                    self._namespaces.add( namespace )
                    
                
            
        
        self._c.executemany( 'INSERT INTO namespaces ( namespace ) VALUES ( ? );', ( ( namespace, ) for namespace in self._namespaces ) )
        
    
    def set_hash_type( self, hash_type ):
        
        self._c.execute( 'DELETE FROM hash_type;' )
        
        self._c.execute( 'INSERT INTO hash_type ( hash_type ) VALUES ( ? );', ( hash_type, ) )
        
    
    def set_mappings( self, hash, tags ):
        
        hash_id = self._get_hash_id( hash )
        
        self._c.execute( 'DELETE FROM mappings WHERE hash_id = ?;', ( hash_id, ) )
        
        tag_ids = [ self._get_tag_id( tag ) for tag in tags ]
        
        self._add_mappings( hash_id, tag_ids )
        

TAG_PAIR_TYPE_SIBLINGS = 0
TAG_PAIR_TYPE_PARENTS = 1

# This is similar, but it only has pairs of tags. Do something like:

# import HydrusTagArchive
# hta = HydrusTagArchive.HydrusTagPairArchive( 'my_little_archive.db' )
# hta.SetPairType( HydrusTagArchive.TAG_PAIR_TYPE_SIBLINGS )
# hta.BeginBigJob()
# for ( bad_tag, better_tag ) in my_tag_pairs_generator: hta.AddPair( bad_tag, better_tag )
  # -or-
# hta.AddPairs( my_tag_pairs_generator )
# hta.CommitBigJob()
# hta.Optimise() # if you made a lot of changes and want to complete your archive for sharing
# hta.Close()
# del hta

# It does not enforce hydrus sibling or parent rules, so you can add loops here.

class HydrusTagPairArchive( object ):
    
    def __init__( self, path ):
        
        self._path = path
        
        is_new_db = not os.path.exists( self._path )
        
        self._init_db_connection()
        
        if is_new_db:
            
            self._init_db()
            
        
        result = self._c.execute( 'SELECT pair_type FROM pair_type;' ).fetchone()
        
        if result is None:
            
            self._pair_type = None
            
        else:
            
            ( self._pair_type, ) = result
            
        
    
    def _init_db( self ):
        
        self._c.execute( 'CREATE TABLE pair_type ( pair_type INTEGER );', )
        
        self._c.execute( 'CREATE TABLE pairs ( tag_id_1 INTEGER, tag_id_2 INTEGER, PRIMARY KEY ( tag_id_1, tag_id_2 ) );' )
        
        self._c.execute( 'CREATE TABLE tags ( tag_id INTEGER PRIMARY KEY, tag TEXT );' )
        self._c.execute( 'CREATE UNIQUE INDEX tags_tag_index ON tags ( tag );' )
        
    
    def _init_db_connection( self ):
        
        self._db = sqlite3.connect( self._path, isolation_level = None, detect_types = sqlite3.PARSE_DECLTYPES )
        
        self._c = self._db.cursor()
        
    
    def _get_tag_id( self, tag ):
        
        result = self._c.execute( 'SELECT tag_id FROM tags WHERE tag = ?;', ( tag, ) ).fetchone()
        
        if result is None:
            
            self._c.execute( 'INSERT INTO tags ( tag ) VALUES ( ? );', ( tag, ) )
            
            tag_id = self._c.lastrowid
            
        else:
            
            ( tag_id, ) = result
            
        
        return tag_id
        
    
    def begin_big_job( self ):
        
        self._c.execute( 'BEGIN IMMEDIATE;' )
        
    
    def close( self ):
        
        self._c.close()
        self._db.close()
        
        self._db = None
        self._c = None
        
    
    def commit_big_job( self ):
        
        self._c.execute( 'COMMIT;' )
        
    
    def add_pair( self, tag_1, tag_2 ):
        
        self.add_pairs( [ ( tag_1, tag_2 ) ] )
        
    
    def add_pairs( self, pairs ):
        
        if self._pair_type is None:
            
            raise Exception( 'Please set the pair type first before you start populating the database!' )
            
        
        pair_id_inserts = [ ( self._get_tag_id( tag_1 ), self._get_tag_id( tag_2 ) ) for ( tag_1, tag_2 ) in pairs ]
        
        self._c.executemany( 'INSERT OR IGNORE INTO pairs ( tag_id_1, tag_id_2 ) VALUES ( ?, ? );', pair_id_inserts )
        
    
    def delete_pair( self, tag_1, tag_2 ):
        
        self.delete_pairs( [ ( tag_1, tag_2 ) ] )
        
    
    def delete_pairs( self, pairs ):
        
        pair_id_deletees = [ ( self._get_tag_id( tag_1 ), self._get_tag_id( tag_2 ) ) for ( tag_1, tag_2 ) in pairs ]
        
        self._c.executemany( 'DELETE FROM pairs WHERE tag_id_1 = ? AND tag_id_2 = ?;', pair_id_deletees )
        
    
    def get_pair_type( self ):
        
        return self._pair_type
        
    
    def get_name( self ):
        
        filename = os.path.basename( self._path )
        
        if '.' in filename:
            
            filename = filename.split( '.', 1 )[0]
            
        
        return filename
        
    
    def has_pair( self, tag_1, tag_2 ):
        
        tag_id_1 = self._get_tag_id( tag_1 )
        tag_id_2 = self._get_tag_id( tag_2 )
        
        result  = self._c.execute( 'SELECT 1 FROM pairs WHERE tag_id_1 = ? AND tag_id_2 = ?;', ( tag_id_1, tag_id_2 ) ).fetchone()
        
        return result is not None
        
    
    def has_pair_type_set( self ):
        
        return self._pair_type is not None
        
    
    def iterate_pairs( self ):
        
        query = 'SELECT t1.tag, t2.tag FROM pairs, tags AS t1, tags as t2 ON ( pairs.tag_id_1 = t1.tag_id AND pairs.tag_id_2 = t2.tag_id );'
        
        for pair in self._c.execute( query ):
            
            yield pair
            
        
    
    def optimise( self ):
        
        self._c.execute( 'VACUUM;' )
        self._c.execute( 'ANALYZE;' )
        
    
    def set_pair_type( self, pair_type ):
        
        self._c.execute( 'DELETE FROM pair_type;' )
        
        self._c.execute( 'INSERT INTO pair_type ( pair_type ) VALUES ( ? );', ( pair_type, ) )
        
        self._pair_type = pair_type
        
    
