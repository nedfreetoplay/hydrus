import zipfile
import json
import typing
import functools

from hydrus.core import HydrusConstants as HC
from hydrus.core import HydrusExceptions
from hydrus.core.files import HydrusArchiveHandling
from hydrus.core.files.images import HydrusImageHandling

from PIL import Image as PILImage

# handle getting a list of frame paths from a ugoira without json metadata:
def get_frame_paths_from_ugoira_zip( path ):
    
    with zipfile.ZipFile( path ) as zip_handle:
        
        paths = [ zip_info.filename for zip_info in zip_handle.infolist() if (not zip_info.is_dir()) and HydrusArchiveHandling.filename_has_image_ext(zip_info.filename) ]
        
        if len( paths ) == 0:
            
            raise HydrusExceptions.DamagedOrUnusualFileException( 'This Ugoira seems to be empty! It has probably been corrupted!' )
            
        
        paths.sort()
        
        return paths
        
    

def get_ugoira_properties( path_to_zip ):
    
    # try to get properties from json file first:
    try:
        
        return get_ugoira_properties_from_json( path_to_zip )
        
    except:
        
        pass
        
    
    try:
        
        pil_image = get_ugoira_frame_pil( path_to_zip, 0 )
        
        ( width, height ) = pil_image.size
        
    except:
        
        ( width, height ) = ( 100, 100 )
        
    
    try:
        
        num_frames = len(get_frame_paths_from_ugoira_zip( path_to_zip ))
        
    except:
        
        num_frames = None
        
    
    return ( ( width, height ), None, num_frames )
    

def zip_looks_like_ugoira( path_to_zip ):
    
    # Check zip for valid ugoira json first:
    try:
        
        frames = get_ugoira_frame_data_json( path_to_zip )
        
        if frames is not None and len( frames ) > 0 and all(('delay' in frame and 'file' in frame) for frame in frames):
            
            return True
            
        
    except:
        
        pass
        

    # what does an Ugoira look like? it has a standard, but this is not always followed, so be somewhat forgiving
    # it is a list of images named in the format 000123.jpg. this is 6-figure, starting at 000000
    # I have seen 'Ugoiras' that are zero-padded with 4 digits and/or 1-indexed instead of 0-indexed, but this is so atypical we are assuming these are ancient handmade artifacts and actually incorrect
    # no directories
    # we can forgive a .json or .js file, nothing else
    
    our_image_ext = None
    
    with zipfile.ZipFile( path_to_zip ) as zip_handle:
        
        zip_infos = zip_handle.infolist()
        
        if True in ( zip_info.is_dir() for zip_info in zip_infos ):
            
            return False
            
        
        image_number_strings = []
        
        filenames = [ zip_info.filename for zip_info in zip_infos ]
        
        for filename in filenames:
            
            if '.' not in filename:
                
                return False
                
            
            number = '.'.join( filename.split( '.' )[:-1] )
            ext = '.' + filename.split( '.' )[-1]
            
            if ext in ( '.js', '.json' ):
                
                continue
                
            
            if ext not in HC.IMAGE_FILE_EXTS:
                
                return False
                
            
            if our_image_ext is None:
                
                our_image_ext = ext
                
            
            if ext != our_image_ext:
                
                return False
                
            
            image_number_strings.append( number )
            
        
        if len( image_number_strings ) <= 1:
            
            return False
            
        
        number_of_digits = 6
        
        for ( expected_image_number, image_number_string ) in enumerate( image_number_strings ):
            
            string_we_expect = str( expected_image_number ).zfill( number_of_digits )
            
            if image_number_string != string_we_expect:
                
                return False
                
            
        
        try:
            
            path = HydrusArchiveHandling.get_cover_page_path( zip_handle )
            
            with zip_handle.open( path ) as reader:
                
                reader.read()
                
            
        except:
            
            return False
            
        
    
    return True
    




### Handling ugoira files with frame data json:

def get_ugoira_json( path: str ):
    
    jsonFile = HydrusArchiveHandling.get_zip_as_path( path, 'animation.json' )

    if not jsonFile.exists():
        
        raise HydrusExceptions.LimitedSupportFileException( 'Zip file has no animation.json!' )
        
    
    with jsonFile.open('rb') as jsonData:
        
        return json.load( jsonData )
        
    


# {file: "000000.jpg", "delay": 100} where delay is in ms
UgoiraFrame = typing.TypedDict('UgoiraFrame', {'file': str, 'delay': int})

# this function is called multiple times for a single ugoira file 
# and involves opening and parsing JSON so let's cache it
@functools.lru_cache( maxsize = 8 )
def get_ugoira_frame_data_json( path: str ) -> list[ UgoiraFrame ] | None:
    
    try:
        
        ugoiraJson = get_ugoira_json( path )
        
        # JSON from gallery-dl is just the array
        if isinstance(ugoiraJson, list):
            
            return ugoiraJson
            
        else:
            
            return ugoiraJson['frames']
            
        
    except:
        
        return None
        
    

def get_ugoira_properties_from_json( path ):
    
    frameData = get_ugoira_frame_data_json( path )
    
    if frameData is None:
        
        raise HydrusExceptions.LimitedSupportFileException( 'Zip file has no animation.json or it cannot be parsed' )
        
    
    frame_durations_ms = [data['delay'] for data in frameData]
    
    duration_ms = sum( frame_durations_ms )
    num_frames = len( frame_durations_ms )
    
    firstFrame = get_ugoira_frame_pil( path, 0 )
    
    return ( firstFrame.size, duration_ms, num_frames )
    

# Combined Ugoira functions:

def get_frame_paths_ugoira( path ): 
    
    try:
        
        frameData = get_ugoira_frame_data_json( path )
        
        if frameData is not None:
            
            return [data['file'] for data in frameData]
            
        
    except:
        
        pass
        
    
    return get_frame_paths_from_ugoira_zip( path )
    

def get_ugoira_frame_pil( path: str, frameIndex: int ) -> PILImage.Image:
    
    framePaths = get_frame_paths_ugoira( path )
    
    frameName = framePaths[frameIndex]
    
    frameFromZip = HydrusArchiveHandling.get_zip_as_path( path, frameName ).open( 'rb' )
    
    return HydrusImageHandling.generate_pil_image( frameFromZip )
    

def generate_thumbnail_numpy_from_ugoira_path( path: str, target_resolution: tuple[int, int], frame_index: int ):
    
    pil_image = get_ugoira_frame_pil( path, frame_index )
    
    thumbnail_pil_image = pil_image.resize( target_resolution, PILImage.Resampling.LANCZOS )
    
    numpy_image = HydrusImageHandling.generate_numpy_image_from_pil_image( thumbnail_pil_image )
    
    return numpy_image
    
