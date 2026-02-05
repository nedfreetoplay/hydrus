import base64
import numpy
import struct

from io import BytesIO
from PIL import Image as PILImage

from hydrus.core import HydrusExceptions
from hydrus.core.files.images import HydrusImageHandling

import xml.etree.ElementTree as ET

def generate_thumbnail_numpy_from_paint_net( path: str, target_resolution: tuple[ int, int ] ) -> numpy.ndarray:
    
    pil_image = thumbnail_pil_image_from_paint_net( path )
    
    # noinspection PyUnresolvedReferences
    thumbnail_pil_image = pil_image.resize( target_resolution, PILImage.Resampling.LANCZOS )
    
    numpy_image = HydrusImageHandling.generate_numpy_image_from_pil_image( thumbnail_pil_image )
    
    return numpy_image
    

def get_paint_net_resolution( path: str ):
    
    try:
        
        xml_header = get_paint_net_xml_header( path )
        
        return get_paint_net_resolution_from_xml_header( xml_header )
        
    except Exception as e:
        
        raise HydrusExceptions.NoThumbnailFileException( f'Could not read resolution bytes from this Paint.NET!' )
        
    

def get_paint_net_resolution_from_xml_header( xml_header: str ):
    
    try:
        
        root = ET.fromstring( xml_header )
        
        width = int( root.attrib[ 'width' ] )
        height = int( root.attrib[ 'height' ] )
        
    except Exception as e:
        
        raise HydrusExceptions.DamagedOrUnusualFileException( 'Cannot parse the XML from this Paint.NET file!' )
        
    
    return ( width, height )
    

def get_paint_net_xml_header( path:str ):
    
    with open( path, 'rb' ) as f:
        
        try:
            
            f.read( 4 )
            header_length_bytes = f.read( 3 ) + b'\x00'
            header_length = struct.unpack( '<i', header_length_bytes )[0]
            
            xml_header = f.read( header_length ).decode( 'utf-8' )
            
        except Exception as e:
            
            raise HydrusExceptions.DamagedOrUnusualFileException( 'Cannot read the XML from this Paint.NET file!' )
            
        
    
    return xml_header
    

def thumbnail_pil_image_from_paint_net( path: str ):
    
    try:
        
        xml_header = get_paint_net_xml_header( path )
        
        root = ET.fromstring( xml_header )
        
        thumb_tag = root.find( './custom/thumb' )
        png_b64 = thumb_tag.attrib[ 'png' ]
        
    except Exception as e:
        
        raise HydrusExceptions.NoThumbnailFileException( f'Could not read thumb bytes from this Paint.NET xml!' )
        
    
    try:
        
        png_bytes = base64.b64decode( png_b64 )
        
    except Exception as e:
        
        raise HydrusExceptions.NoThumbnailFileException( f'Could not decode thumb bytes from this Paint.NET xml!' )
        
    
    return HydrusImageHandling.generate_pil_image( BytesIO( png_bytes ), human_file_description = f'Preview image inside Paint.NET "{path}"' )
    
