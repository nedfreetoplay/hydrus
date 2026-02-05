# noinspection PyUnresolvedReferences
from win32com.shell import shell, shellcon

def open_file_properties(path: str):
    
    shell.ShellExecuteEx( fMask = shellcon.SEE_MASK_INVOKEIDLIST, lpFile = path, lpVerb = 'properties' )
    

def open_file_with(path: str):
    
    shell.ShellExecuteEx( fMask = shellcon.SEE_MASK_INVOKEIDLIST, lpFile = path, lpVerb = 'openas' )
    
