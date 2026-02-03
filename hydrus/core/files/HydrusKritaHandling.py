from hydrus.core import HydrusExceptions
from hydrus.core.files import HydrusArchiveHandling
from hydrus.core.files.images import HydrusImageHandling

import numpy

from PIL import Image as PILImage
import xml.etree.ElementTree as ET

KRITA_FILE_THUMB = "preview.png"
KRITA_FILE_MERGED = "mergedimage.png"

def merged_pil_image_from_kra( path ):
    
    try:
        
        zip_path_file_obj = HydrusArchiveHandling.get_zip_as_path( path, KRITA_FILE_MERGED ).open( 'rb' )
        
        return HydrusImageHandling.generate_pil_image( zip_path_file_obj )
        
    except FileNotFoundError:
        
        raise HydrusExceptions.NoRenderFileException( f'Could not read {KRITA_FILE_MERGED} from this Krita file' )
        
    

def thumbnail_pil_image_from_kra( path ):
    
    try:
        
        zip_path_file_obj = HydrusArchiveHandling.get_zip_as_path( path, KRITA_FILE_THUMB ).open( 'rb' )
        
        return HydrusImageHandling.generate_pil_image( zip_path_file_obj )
        
    except FileNotFoundError:
        
        raise HydrusExceptions.NoThumbnailFileException( f'Could not read {KRITA_FILE_THUMB} from this Krita file' )
        
    

def generate_thumbnail_numpy_from_kra_path( path: str, target_resolution: tuple[ int, int ] ) -> numpy.ndarray:
    
    try:
        
        pil_image = merged_pil_image_from_kra( path )
        
    except:
        
        pil_image = thumbnail_pil_image_from_kra( path )
        
    
    # noinspection PyUnresolvedReferences
    thumbnail_pil_image = pil_image.resize( target_resolution, PILImage.Resampling.LANCZOS )
    
    numpy_image = HydrusImageHandling.generate_numpy_image_from_pil_image( thumbnail_pil_image )
    
    return numpy_image
    

# TODO: animation and frame stuff which is also in the maindoc.xml
def get_kra_properties( path ):
    
    DOCUMENT_INFO_FILE = "maindoc.xml"
    
    try:
        
        data_file = HydrusArchiveHandling.get_zip_as_path( path, DOCUMENT_INFO_FILE ).open( 'rb' )
        
        root = ET.parse( data_file )
        
        image_tag = root.find( '{http://www.calligra.org/DTD/krita}IMAGE' )
        
        width = int( image_tag.attrib[ 'width' ] )
        
        height = int( image_tag.attrib[ 'height' ] )
        
        return ( width, height )
        
    except:
        
        raise HydrusExceptions.NoResolutionFileException( f'This krita file had no {DOCUMENT_INFO_FILE} or it contains no resolution!' )
        
    
