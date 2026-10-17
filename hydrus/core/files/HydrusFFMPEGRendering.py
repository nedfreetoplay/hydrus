from hydrus.core import HydrusData
from hydrus.core import HydrusExceptions
from hydrus.core.processes import HydrusSubprocess

from hydrus.core.files import HydrusFFMPEG
from hydrus.core.files import HydrusFFMPEGParsing

def RenderAnyAttachedStillImageToPath( file_path, output_path ):
    
    lines = HydrusFFMPEG.get_ffmpeg_info_lines( file_path )
    
    image_lines = HydrusFFMPEGParsing.get_image_stream_lines( lines )
    
    image_stream_mapping = None
    
    if len( image_lines ) > 0:
        
        image_line = image_lines[0]
        
        try:
            
            image_stream_mapping = HydrusFFMPEGParsing.parse_stream_mapping_from_stream_line( image_line )
            
        except Exception as e:
            
            return False
            
        
    
    if image_stream_mapping is None:
        
        return False
        
    
    ffmpeg_path = HydrusFFMPEG.get_current_ffmpeg_path()
    
    # no dimensions here, so called is responsible for reshaping numpy array or whatever
    
    # ffmpeg -xerror -y -i m.mp3 -map 0:1 -frames:v 1 -update 1 -f image2 -c:v png output.png
    # note the "-update 1" says "keep overwriting every frame onto the output" just to suppress a warning; -frames:v 1 makes it moot
    cmd = [ ffmpeg_path, "-xerror", '-y', '-i', file_path, '-map', image_stream_mapping, '-frames:v', '1', '-update', '1', '-f', 'image2', '-c:v', 'png', output_path ]
    
    HydrusData.check_program_is_not_shutting_down()
    
    try:
        
        HydrusSubprocess.run_subprocess( cmd, timeout = HydrusFFMPEG.FFMPEG_SUBPROCESS_TIMEOUT, bufsize = 1024 * 512, text = False )
        
    except HydrusExceptions.SubprocessTimedOut:
        
        raise HydrusExceptions.DamagedOrUnusualFileException( 'ffmpeg could not render it quick enough!' )
        
    except FileNotFoundError as e:
        
        raise HydrusFFMPEG.handle_ffmpeg_file_not_found_and_generate_exception( e, file_path )
        
    
    return True
    

def RenderImageToImagePath( path, temp_image_path ):
    
    ffmpeg_path = HydrusFFMPEG.get_current_ffmpeg_path()
    
    # -y to overwrite the temp path
    
    if temp_image_path.endswith( '.jpg' ):
        
        # '-q:v 1' does high quality
        cmd = [ ffmpeg_path, "-xerror", '-y', "-i", path, "-q:v", "1", temp_image_path ]
        
    else:
        
        cmd = [ ffmpeg_path, "-xerror", '-y', "-i", path, temp_image_path ]
        
    
    HydrusData.check_program_is_not_shutting_down()
    
    try:
        
        HydrusSubprocess.run_subprocess( cmd, timeout = HydrusFFMPEG.FFMPEG_SUBPROCESS_TIMEOUT )
        
    except HydrusExceptions.SubprocessTimedOut:
        
        raise HydrusExceptions.DamagedOrUnusualFileException( 'ffmpeg could not render it quick enough!' )
        
    except FileNotFoundError as e:
        
        raise HydrusFFMPEG.handle_ffmpeg_file_not_found_and_generate_exception( e, path )
        
    

def RenderImageToRawRGBABytes( path ):
    
    ffmpeg_path = HydrusFFMPEG.get_current_ffmpeg_path()
    
    # no dimensions here, so called is responsible for reshaping numpy array or whatever
    
    cmd = [ ffmpeg_path, "-xerror", '-i', path, '-f', 'rawvideo', '-pix_fmt', 'rgba', '-' ]
    
    HydrusData.check_program_is_not_shutting_down()
    
    try:
        
        ( stdout, stderr, returncode ) = HydrusSubprocess.run_subprocess( cmd, timeout = HydrusFFMPEG.FFMPEG_SUBPROCESS_TIMEOUT, bufsize = 1024 * 512, text = False )
        
    except HydrusExceptions.SubprocessTimedOut:
        
        raise HydrusExceptions.DamagedOrUnusualFileException( 'ffmpeg could not render it quick enough!' )
        
    except FileNotFoundError as e:
        
        raise HydrusFFMPEG.handle_ffmpeg_file_not_found_and_generate_exception( e, path )
        
    
    return stdout
    

def RenderImageToPNGBytes( path ):
    
    ffmpeg_path = HydrusFFMPEG.get_current_ffmpeg_path()
    
    cmd = [ ffmpeg_path, "-xerror", '-i', path, '-f', 'image2pipe', '-vcodec', 'png', '-' ]
    
    HydrusData.check_program_is_not_shutting_down()
    
    try:
        
        ( stdout, stderr, returncode ) = HydrusSubprocess.run_subprocess( cmd, timeout = HydrusFFMPEG.FFMPEG_SUBPROCESS_TIMEOUT, bufsize = 1024 * 512, text = False )
        
    except HydrusExceptions.SubprocessTimedOut:
        
        raise HydrusExceptions.DamagedOrUnusualFileException( 'ffmpeg could not render it quick enough!' )
        
    except FileNotFoundError as e:
        
        raise HydrusFFMPEG.handle_ffmpeg_file_not_found_and_generate_exception( e, path )
        
    
    return stdout
    
