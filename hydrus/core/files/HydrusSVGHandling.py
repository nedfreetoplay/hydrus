from hydrus.core import HydrusExceptions

def base_generate_thumbnail_numpy_from_svg_path( path: str, target_resolution: tuple[int, int] ) -> bytes:
    
    raise HydrusExceptions.NoThumbnailFileException()
    

def base_get_svg_resolution( path: str ):
    
    raise HydrusExceptions.NoResolutionFileException()
    

generate_thumbnail_numpy_from_svg_path = base_generate_thumbnail_numpy_from_svg_path
get_svg_resolution = base_get_svg_resolution
