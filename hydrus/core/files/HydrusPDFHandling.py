from hydrus.core import HydrusExceptions

def BaseGenerateThumbnailNumPyFromPDFPath( path: str, target_resolution: tuple[int, int] ) -> bytes:
    
    """Executes `BaseGenerateThumbnailNumPyFromPDFPath`."""
    raise HydrusExceptions.NoThumbnailFileException()
    

def BaseGetPDFInfo( path: str ):
    
    """Executes `BaseGetPDFInfo`."""
    raise HydrusExceptions.LimitedSupportFileException()
    

GenerateThumbnailNumPyFromPDFPath = BaseGenerateThumbnailNumPyFromPDFPath
GetPDFInfo = BaseGetPDFInfo
