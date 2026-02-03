import os

from hydrus.core import HydrusConstants as HC
from hydrus.core import HydrusGlobals as HG

INSTALL_STATIC_DIR = os.path.join( HC.BASE_DIR, 'static' )

USE_USER_STATIC_DIR = True

def get_static_icon_path( name: str, force_install_dir = False ):
    
    ( svg_path, was_userdir ) = get_static_path_with_result( name + '.svg', force_install_dir = force_install_dir )
    
    if not was_userdir:
        
        # a little ugly, but user png has preference over install svg
        ( png_path, was_userdir ) = get_static_path_with_result( name + '.png', force_install_dir = force_install_dir )
        
        if was_userdir:
            
            return png_path
            
        
    
    if os.path.exists( svg_path ):
        
        path = svg_path
        
    else:
        
        png_path = get_static_path( name + '.png', force_install_dir = force_install_dir )
        
        path = png_path
        
    
    return path
    

def get_static_path( sub_path: str, force_install_dir = False ):
    
    ( path, was_userdir ) = get_static_path_with_result( sub_path, force_install_dir = force_install_dir )
    
    return path
    

def get_static_path_with_result( sub_path: str, force_install_dir = False ):
    
    if not force_install_dir and USE_USER_STATIC_DIR and HG.controller is not None:
        
        user_path = os.path.join( HG.controller.get_db_dir(), 'static', sub_path )
        
        if os.path.exists( user_path ):
            
            return ( user_path, True )
            
        
    
    return ( os.path.join( INSTALL_STATIC_DIR, sub_path ), False )
    

def get_svg_path( name, userdir = HC.USERPATH_SVG_ICON ):
    
    svg_path = os.path.join( INSTALL_STATIC_DIR, userdir, name + '.svg' ) 
    
    if os.path.exists( svg_path ):
        
        return svg_path
        
    
    svg_path = get_static_icon_path( name )
    
    if os.path.exists( svg_path ):
        
        return svg_path
        
    

def list_static_dir_file_paths( sub_dir_path: str ):
    
    user_path = get_static_path( sub_dir_path )
    install_path = get_static_path( sub_dir_path, force_install_dir = True )
    
    dirs_to_do = []
    
    if user_path != install_path:
        
        dirs_to_do.append( user_path )
        
    
    dirs_to_do.append( install_path )
    
    file_paths = []
    
    for dir_to_do in dirs_to_do:
        
        if not os.path.exists( dir_to_do ):
            
            continue
            
        
        for filename in list( os.listdir( dir_to_do ) ):
            
            path = os.path.join( dir_to_do, filename )
            
            if os.path.isfile( path ):
                
                if path in file_paths: # prefer user dir to overwrite
                    
                    continue
                    
                
                file_paths.append( path )
                
            
        
    
    return file_paths
    
