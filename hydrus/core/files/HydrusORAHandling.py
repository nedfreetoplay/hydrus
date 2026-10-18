from hydrus.core import HydrusExceptions
from hydrus.core.files import HydrusArchiveHandling
from hydrus.core.files.images import HydrusImageHandling

import numpy

from PIL import Image as PILImage
import xml.etree.ElementTree as ET

ORA_FILE_THUMB = "Thumbnails/thumbnail.png"
ORA_FILE_MERGED = "mergedimage.png"

def merged_pil_image_from_ora(path: str):
    
    try:
        
        zip_path_file_obj = HydrusArchiveHandling.get_zip_as_path( path, ORA_FILE_MERGED ).open( 'rb' )
        
        return HydrusImageHandling.generate_pil_image( zip_path_file_obj )
        
    except FileNotFoundError:
        
        raise HydrusExceptions.NoRenderFileException( f'Could not read {ORA_FILE_MERGED} from this ora file' )

def thumbnail_pil_image_from_ora(path: str):
    
    try:
        
        zip_path_file_obj = HydrusArchiveHandling.get_zip_as_path( path, ORA_FILE_THUMB ).open( 'rb' )
        
        return HydrusImageHandling.generate_pil_image( zip_path_file_obj )
        
    except FileNotFoundError:
        
        raise HydrusExceptions.NoThumbnailFileException( f'Could not read {ORA_FILE_THUMB} from this ora file' )

def generate_thumbnail_numpy_from_ora_path(path: str, target_resolution: tuple[int, int]) -> numpy.ndarray:
    
    try:
        
        pil_image = merged_pil_image_from_ora( path )
        
    except Exception as e:
        
        pil_image = thumbnail_pil_image_from_ora( path )
        
    
    try:
        
        thumbnail_pil_image = pil_image.resize( target_resolution, PILImage.Resampling.LANCZOS )
        
    finally:
        
        pil_image.close()
        
    
    try:
        
        numpy_image = HydrusImageHandling.generate_numpy_image_from_pil_image( thumbnail_pil_image )
        
    finally:
        
        thumbnail_pil_image.close()
        
    
    return numpy_image

def get_ora_properties(path: str):
    
    DOCUMENT_INFO_FILE = "stack.xml"
    
    try:
        
        data_file = HydrusArchiveHandling.get_zip_as_path( path, DOCUMENT_INFO_FILE ).open( 'rb' )
        
        tree = ET.parse( data_file )
        
        root = tree.getroot()
        
        width = int( root.get( 'w' ) )
        
        height = int( root.get( 'h' ) )
        
        return ( width, height )
        
    except Exception as e:
        
        raise HydrusExceptions.NoResolutionFileException( f'This ora file had no {DOCUMENT_INFO_FILE} or it contains no resolution!' )
