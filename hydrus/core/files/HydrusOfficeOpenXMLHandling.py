from hydrus.core import HydrusConstants as HC
from hydrus.core import HydrusExceptions
from hydrus.core.files.HydrusArchiveHandling import get_zip_as_path
from hydrus.core.files.images import HydrusImageHandling

import xml.etree.ElementTree as ET

from PIL import Image as PILImage

DOCX_XPATH = ".//{*}Override[@PartName='/word/document.xml'][@ContentType='application/vnd.openxmlformats-officedocument.wordprocessingml.document.main+xml']"
XLSX_XPATH = ".//{*}Override[@PartName='/xl/workbook.xml'][@ContentType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml']"
PPTX_XPATH = ".//{*}Override[@PartName='/ppt/presentation.xml'][@ContentType='application/vnd.openxmlformats-officedocument.presentationml.presentation.main+xml']"

DOCX_XPATH_DEFAULT = ".//{*}Default[@Extension='xml'][@ContentType='application/vnd.openxmlformats-officedocument.wordprocessingml.document.main+xml']"
XLSX_XPATH_DEFAULT = ".//{*}Default[@Extension='xml'][@ContentType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml']"
PPTX_XPATH_DEFAULT = ".//{*}Default[@Extension='xml'][@ContentType='application/vnd.openxmlformats-officedocument.presentationml.presentation.main+xml']"

def mime_from_microsoft_open_xml_document(path: str):
    
    try:
        
        file = get_zip_as_path( path, '[Content_Types].xml' ).open( 'rb' )
        
        root = ET.parse( file )
        
        if root.find(DOCX_XPATH) is not None:
            
            return HC.APPLICATION_DOCX
            
        elif root.find(XLSX_XPATH) is not None:
            
            return HC.APPLICATION_XLSX
            
        elif root.find(PPTX_XPATH) is not None:
            
            return HC.APPLICATION_PPTX
        
        if root.find(DOCX_XPATH_DEFAULT) is not None:
            
            return HC.APPLICATION_DOCX
            
        elif root.find(XLSX_XPATH_DEFAULT) is not None:
            
            return HC.APPLICATION_XLSX
            
        elif root.find(PPTX_XPATH_DEFAULT) is not None:
            
            return HC.APPLICATION_PPTX
            
        else:
            
            return None
            
        
    except Exception as e:
        
        return None
        
    

def generate_thumbnail_numpy_from_office_path( path: str, target_resolution: tuple[ int, int ] ) -> bytes:
    
    try:
        
        zip_path_file_obj = get_zip_as_path( path, 'docProps/thumbnail.jpeg' ).open( 'rb' )
        
    except FileNotFoundError:
        
        raise HydrusExceptions.NoThumbnailFileException( 'No thumbnail.jpeg file!' )
        
    
    pil_image = HydrusImageHandling.generate_pil_image( zip_path_file_obj )
    
    thumbnail_pil_image = pil_image.resize( target_resolution, PILImage.Resampling.LANCZOS )
    
    numpy_image = HydrusImageHandling.generate_numpy_image_from_pil_image( thumbnail_pil_image )
    
    return numpy_image
    

PPTX_ASSUMED_DPI = 300

# https://startbigthinksmall.wordpress.com/2010/01/04/points-inches-and-emus-measuring-units-in-office-open-xml/
# PowerPoint uses English Metric Unit (EMU) for vector coordinates
# 1 inch = 914400 EMU

PPTX_PIXEL_PER_EMU = PPTX_ASSUMED_DPI / 914400

def power_point_resolution( path: str ):
    
    file = get_zip_as_path( path, 'ppt/presentation.xml' ).open( 'rb' )
    
    root = ET.parse( file )
    
    sldSz = root.find('./p:sldSz', {'p': 'http://schemas.openxmlformats.org/presentationml/2006/main'})
    
    x_emu = int(sldSz.get('cx'))
    
    y_emu = int(sldSz.get('cy'))
    
    width = round(x_emu * PPTX_PIXEL_PER_EMU)
    
    height = round(y_emu * PPTX_PIXEL_PER_EMU)
    
    return ( width, height) 
    

def office_document_word_count( path: str ):
    
    file = get_zip_as_path( path, 'docProps/app.xml' ).open( 'rb' )
    
    root = ET.parse( file )
    
    words = root.findtext('./ep:Words', namespaces = {'ep' : 'http://schemas.openxmlformats.org/officeDocument/2006/extended-properties'})
    
    num_words = int(words)
    
    return num_words
    

def get_pptx_info( path: str ):
    
    try:
        
        ( width, height ) = power_point_resolution( path )
        
    except Exception as e:
        
        ( width, height ) = ( None, None )
    
    try:
        
        num_words = office_document_word_count( path )
        
    except Exception as e:
        
        num_words = None
        
    return ( num_words, ( width, height ) )
    

def get_docx_info( path:str ):
    
    try:
        
        num_words = office_document_word_count( path )
        
    except Exception as e:
        
        num_words = None
        
    
    return num_words
    
