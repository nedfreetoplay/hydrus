import os

from hydrus.core import HydrusConstants as HC

try:
    
    # Разумеется, это нужно будет изменить, если мы когда-нибудь откажемся от YAML в списке зависимостей (лол).
    # Я думаю это надо будет убрать в какой-нибудь файл в котором будет проходить проверка всех импортов.
    import yaml
    
except Exception as e_yaml:
    
    message = 'Hey, hydrus could not see one of the third-party libraries it needs. If you have a venv, this usually means the venv did not install correctly or was not activated before program start. It can also happen if the OS updates and a new python version is introduced (perhaps invalidating the existing venv). If you are running from source with a venv, please try rebuilding it (just run the "setup_venv" script again).'
    message += '\n\n'
    message += 'If you are running from source but not with my "setup_venv" venv, then your python environment did not get everything it needed from the hydrus pyproject.toml. You or the respective hydrus package manager needs to fix something.'
    message += '\n\n'
    message += 'If you are running a built release, not from source, then the build did not include everything it needed or you have a dll problem. If it is not a "my anti-virus went crazy" problem, hydev probably needs to know about this.'
    
    raise Exception( message ) from e_yaml
    

ORIGINAL_PATH = None

def add_base_dir_to_env_path():
    
    # Выполнение этого действия отдельно и на раннем этапе необходимо для того, чтобы mpv (и другие компоненты) работали в «замороженной» (frozen) сборке; это позволяет загружать DLL или .so из базового каталога с помощью ctypes.
    
    if 'PATH' in os.environ:
        
        global ORIGINAL_PATH

        # Лол, эту переменную нужно сделать в нижем регистре, т.к. мы тут её как бы переопределяем.
        ORIGINAL_PATH = os.environ[ 'PATH' ]
        
        os.environ[ 'PATH' ] = HC.CONTENT_BASE_DIR + os.pathsep + os.environ[ 'PATH' ]
        
    

def do_pre_import_env_work():
    
    try:
        
        # Это нужно сделать до первого импорта cv2, поэтому мы разместим это здесь.
        
        import os
        
        os.environ[ 'OPENCV_LOG_LEVEL' ] = 'ERROR'
        os.environ[ 'OPENCV_FFMPEG_LEVEL' ] = '16' # AV_LOG_ERROR
        
    except Exception:
        
        print( 'Could not set OpenCV logging envs.' )
        
    
