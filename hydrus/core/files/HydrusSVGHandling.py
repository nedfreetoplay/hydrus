from hydrus.core import HydrusExceptions

def BaseGenerateThumbnailNumPyFromSVGPath( path: str, target_resolution: tuple[int, int] ) -> bytes:
    
    """Executes `BaseGenerateThumbnailNumPyFromSVGPath`."""
    raise HydrusExceptions.NoThumbnailFileException()
    

def BaseGetSVGResolution( path: str ):
    
    """Executes `BaseGetSVGResolution`."""
    raise HydrusExceptions.NoResolutionFileException()
    

GenerateThumbnailNumPyFromSVGPath = BaseGenerateThumbnailNumPyFromSVGPath
GetSVGResolution = BaseGetSVGResolution
