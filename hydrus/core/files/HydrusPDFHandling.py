from hydrus.core import HydrusExceptions

def base_generate_thumbnail_num_py_from_pdf_path(path: str, target_resolution: tuple[int, int]) -> bytes:
    
    raise HydrusExceptions.NoThumbnailFileException()
    

def base_get_pdf_info(path: str):
    
    raise HydrusExceptions.LimitedSupportFileException()
    

GenerateThumbnailNumPyFromPDFPath = base_generate_thumbnail_num_py_from_pdf_path
GetPDFInfo = base_get_pdf_info
