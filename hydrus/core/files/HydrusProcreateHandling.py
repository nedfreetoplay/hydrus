from hydrus.core import HydrusExceptions
from hydrus.core.files import HydrusArchiveHandling
import plistlib

# Mostly based on https://github.com/jaromvogel/ProcreateViewer/blob/master/ProcreatePython/ProcreateImageData.py

PROCREATE_THUMBNAIL_FILE_PATH = 'QuickLook/Thumbnail.png'
PROCREATE_DOCUMENT_ARCHIVE = 'Document.archive'
# object key in plist to start from (trunk)
PROCREATE_PROJECT_KEY = 1

def extract_zipped_thumbnail_to_path(path_to_zip, temp_path_file):
    
    try:
        
        HydrusArchiveHandling.extract_single_file_from_zip(path_to_zip, PROCREATE_THUMBNAIL_FILE_PATH, temp_path_file)
        
    except KeyError:
        
        raise HydrusExceptions.NoThumbnailFileException( 'This procreate file had no thumbnail file!' )
        
    

def get_procreate_plist(path):
    
    plist_file = HydrusArchiveHandling.get_zip_as_path(path, PROCREATE_DOCUMENT_ARCHIVE)

    if not plist_file.exists():
        
        raise HydrusExceptions.DamagedOrUnusualFileException( 'Procreate file has no plist!' )
        
    
    with HydrusArchiveHandling.get_zip_as_path(path, PROCREATE_DOCUMENT_ARCHIVE).open('rb') as document:
        
        return plistlib.load( document )
        
    

def zip_looks_like_procreate(path) -> bool:
    
    try:
        
        document = get_procreate_plist(path)
        
        objects = document['$objects']
        
        class_pointer = objects[PROCREATE_PROJECT_KEY]['$class']
        
        class_name = objects[class_pointer]['$classname']
        
        return class_name == 'SilicaDocument'
        
    except Exception as e:
        
        return False
        
    

def get_procreate_resolution(path): 
    
    # TODO: animation stuff from plist
    
    try:
        
        document = get_procreate_plist(path)
        
        objects = document['$objects']
        
        dimension_pointer = objects[PROCREATE_PROJECT_KEY]['size'].data
        
        # eg '{2894, 4093}'
        size_string = objects[dimension_pointer]
        
        size = size_string.strip('{').strip('}').split(', ')
        
        orientation = objects[PROCREATE_PROJECT_KEY]['orientation']
        
        if orientation in [3,4]:
            
            # canvas is rotated 90 or -90 degrees
            
            width = size[1]
            height = size[0]
            
        else:
            
            width = size[0]
            height = size[1]
            
        
    except Exception as e:
        
        raise HydrusExceptions.NoResolutionFileException()
        
    
    return ( int( width ), int( height ) )
    
