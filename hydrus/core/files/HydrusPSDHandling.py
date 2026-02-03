from io import BytesIO
import re

import numpy

from hydrus.core import HydrusData
from hydrus.core import HydrusExceptions
from hydrus.core.files import HydrusFFMPEG
from hydrus.core.files.images import HydrusImageHandling
from hydrus.core.processes import HydrusSubprocess

def read_uint16(f):
    
    return int.from_bytes( f.read( 2 ), byteorder = 'big' )
    

def read_uint32(f):
    
    return int.from_bytes( f.read( 4 ), byteorder = 'big' )
    

def get_psd_image_resources( path ):
    
    with open(path, 'rb') as f:
        
        # Skip PSD Header: 26 bytes
        f.seek( 26 )
        
        # Skip Color Mode Data section
        color_mode_data_len = read_uint32( f )
        f.seek( color_mode_data_len, 1 )

        # Read Image Resources section length
        image_resources_len = read_uint32( f )
        image_resources_start = f.tell()
        image_resources_end = image_resources_start + image_resources_len
        
        resources = []
        
        while f.tell() < image_resources_end:
            
            sig = f.read( 4 )
            
            if sig != b'8BIM':
                
                raise ValueError(f'Invalid resource signature at {f.tell()-4}: {sig}')
                
            
            resource_id = read_uint16( f )
            
            # Read Pascal string name (padded to even size)
            name_len = f.read( 1 )[ 0 ]
            
            f.read( name_len )
            
            if ( name_len + 1 ) % 2 == 1:
                
                f.read( 1 )  # padding
                
            
            # Read resource data size and offset
            size = read_uint32( f )
            data_offset = f.tell()
            
            resources.append( ( resource_id, size, data_offset ) )

            # Skip the actual data, with even-length padding
            f.seek( size + ( size % 2 ), 1 )
            

        return resources
        
    

def get_psd_image_resource_ids( image_resources ):
    
    return { resource_id for ( resource_id, size, data_offset ) in image_resources }
    

def get_ffmpeg_psd_lines( path ):
    
    # open the file in a pipe, provoke an error, read output
    
    cmd = [ HydrusFFMPEG.FFMPEG_PATH, "-xerror", "-i", path ]
    
    HydrusData.check_program_is_not_shutting_down()
    
    try:
        
        ( stdout, stderr ) = HydrusSubprocess.run_subprocess( cmd, bufsize = 1024 * 512 )
        
    except HydrusExceptions.SubprocessTimedOut:
        
        raise HydrusExceptions.DamagedOrUnusualFileException( 'ffmpeg could not get PSD data quick enough!' )
        
    except FileNotFoundError as e:
        
        raise HydrusFFMPEG.handle_ffmpeg_file_not_found_and_generate_exception( e, path )
        
    
    if stderr is None or len( stderr ) == 0:
        
        raise HydrusFFMPEG.handle_ffmpeg_no_content_and_generate_exception( path, stdout, stderr )
        
    
    lines = stderr.splitlines()
    
    HydrusFFMPEG.check_ffmpeg_error( lines )
    
    return lines
    

def ParseFFMPEGPSDLine( lines ) -> str:
    
    # get the output line that speaks about PSD. something like this:
    # Stream #0:0: Video: psd, rgb24, 1920x1080
    # the ^\sStream is to exclude the 'title' line, when it exists, includes the string 'Video: ', ha ha
    lines_video = [ line for line in lines if re.search( r'^\s*Stream', line ) is not None and 'Video: ' in line and 'psd' in line ]
    
    if len( lines_video ) == 0:
        
        raise HydrusExceptions.DamagedOrUnusualFileException( 'Could not find PSD information!' )
        
    
    line = lines_video[0]
    
    return line
    

def parse_ffmpeg_psd_resolution( lines ) -> tuple[ int, int ]:
    
    try:
        
        line = ParseFFMPEGPSDLine( lines )
        
        # get the size, of the form 460x320 (w x h)
        match = re.search(" [0-9]*x[0-9]*([, ])", line)
        
        resolution_string = line[match.start():match.end()-1]
        
        ( width_string, height_string ) = resolution_string.split( 'x' )
        
        width = int( width_string )
        height = int( height_string )
        
        return ( width, height )
        
    except:
        
        raise HydrusExceptions.NoResolutionFileException( 'Error parsing resolution!' )
        
    

def psd_has_icc_profile( path: str ):
    
    image_resources = get_psd_image_resources( path )
    
    resource_ids = get_psd_image_resource_ids( image_resources )
    
    return 1039 in resource_ids
    

def generate_pil_image_from_psd( path ):
    
    # could faff around with getting raw bytes and reshaping, but let's KISS for now
    png_bytes = HydrusFFMPEG.render_image_to_png_bytes( path )
    
    if len( png_bytes ) == 0:
        
        raise HydrusExceptions.NoRenderFileException( 'This PSD has no embedded Preview file that FFMPEG can read!' )
        
    
    return HydrusImageHandling.generate_pil_image( BytesIO( png_bytes ), human_file_description = f'Preview image inside PSD "{path}"' )
    

def generate_thumbnail_numpy_from_psd_path( path: str, target_resolution: tuple[int, int] ) -> numpy.ndarray:
    
    try:
        
        pil_image = generate_pil_image_from_psd( path )
        
    except HydrusExceptions.LimitedSupportFileException as e:
        
        raise HydrusExceptions.NoThumbnailFileException( str( e ) )
        
    
    # convert to numpy rather than doing pil_image.resize because psd previews sometimes have 100% alpha and the resize applies that
    numpy_image = HydrusImageHandling.generate_numpy_image_from_pil_image( pil_image )
    
    thumbnail_numpy_image = HydrusImageHandling.resize_numpy_image( numpy_image, target_resolution )
    
    return thumbnail_numpy_image
    

def get_psd_resolution( path: str ):
    
    with open( path, 'rb' ) as f:
        
        f.seek( 14 )
        
        height_bytes = f.read( 4 )
        width_bytes = f.read( 4 )
        
    
    height = int.from_bytes( height_bytes, 'big' )
    width = int.from_bytes( width_bytes, 'big' )
    
    return ( width, height )
    
