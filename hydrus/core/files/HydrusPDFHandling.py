from hydrus.core import HydrusExceptions

def base_generate_thumbnail_numpy_from_pdf_path( path: str, target_resolution: tuple[int, int] ) -> bytes:
    
    raise HydrusExceptions.NoThumbnailFileException()
    

def base_get_pdf_info( path: str ):
    
    raise HydrusExceptions.LimitedSupportFileException()
    

generate_thumbnail_numpy_from_pdf_path = base_generate_thumbnail_numpy_from_pdf_path
get_pdf_info = base_get_pdf_info
