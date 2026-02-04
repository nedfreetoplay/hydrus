import os

from hydrus.core import HydrusExceptions
from hydrus.core.files import HydrusFilesPhysicalStorage

from hydrus.server import ServerGlobals as SG

def get_all_hashes( file_type ):
    
    return { bytes.fromhex( os.path.split( path )[1] ) for path in iterate_all_paths( file_type ) }
    
def get_expected_file_path( hash ):
    
    files_dir = SG.server_controller.get_files_dir()
    
    hash_encoded = hash.hex()
    
    first_two_chars = hash_encoded[:2]
    
    path = os.path.join( files_dir, first_two_chars, hash_encoded )
    
    return path
    
def get_expected_thumbnail_path( hash ):
    
    files_dir = SG.server_controller.get_files_dir()
    
    hash_encoded = hash.hex()
    
    first_two_chars = hash_encoded[:2]
    
    path = os.path.join( files_dir, first_two_chars, hash_encoded + '.thumbnail' )
    
    return path
    
def get_file_path( hash ):
    
    path = get_expected_file_path( hash )
    
    if not os.path.exists( path ):
        
        raise HydrusExceptions.NotFoundException( 'File not found!' )
        
    
    return path
    
def get_thumbnail_path( hash ):
    
    path = get_expected_thumbnail_path( hash )
    
    if not os.path.exists( path ):
        
        raise HydrusExceptions.NotFoundException( 'Thumbnail not found!' )
        
    
    return path
    
def iterate_all_paths( file_type ):
    
    files_dir = SG.server_controller.get_files_dir()
    
    for prefix in HydrusFilesPhysicalStorage.iterate_prefixes( '', prefix_length = 2 ):
        
        dir = os.path.join( files_dir, prefix )
        
        filenames = os.listdir( dir )
        
        for filename in filenames:
            
            if file_type == 'file' and filename.endswith( '.thumbnail' ):
                
                continue
                
            elif file_type == 'thumbnail' and not filename.endswith( '.thumbnail' ):
                
                continue
                
            
            yield os.path.join( dir, filename )
            
        
    
