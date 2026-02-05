def float_to_percentage( f ):
    
    percent = f * 100
    
    if percent == int( percent ):
        
        return f'{int( percent )}%'
        
    else:
        
        return f'{percent:.1f}%'
        
    

def index_to_pretty_ordinal_string( index: int ):
    
    if index >= 0:
        
        return int_to_pretty_ordinal_string( index + 1 )
        
    else:
        
        return int_to_pretty_ordinal_string( index )
        
    

def int_to_pixels( i ):
    
    if i == 1: return 'pixels'
    elif i == 1000: return 'kilopixels'
    elif i == 1000000: return 'megapixels'
    else: return 'megapixels'
    

def int_to_unit( unit ):
    
    if unit == 1: return 'B'
    elif unit == 1024: return 'KB'
    elif unit == 1048576: return 'MB'
    elif unit == 1073741824: return 'GB'
    

def int_to_pretty_ordinal_string( num: int ):
    
    if num == 0:
        
        return 'unknown position'
        
    
    tens = ( abs( num ) % 100 ) // 10
    
    if tens == 1:
        
        ordinal = 'th'
        
    else:
        
        remainder = abs( num ) % 10
        
        if remainder == 1:
            
            ordinal = 'st'
            
        elif remainder == 2:
            
            ordinal = 'nd'
            
        elif remainder == 3:
            
            ordinal = 'rd'
            
        else:
            
            ordinal = 'th'
            
        
    
    s = '{}{}'.format( to_human_int( abs( num ) ), ordinal )
    
    if num < 0:
        
        if num == -1:
            
            s = 'last'
            
        else:
            
            s = '{} from last'.format( s )
            
        
    
    return s
    

def pixels_to_int( unit ):
    
    if unit == 'pixels': return 1
    elif unit == 'kilopixels': return 1000
    elif unit == 'megapixels': return 1000000
    

def to_human_int( num ):
    
    try:
        
        num = int( num )
        
    except Exception as e:
        
        return 'unknown'
        
    
    # this got stomped on by mpv, which resets locale
    #text = locale.format_string( '%d', num, grouping = True )
    
    text = '{:,}'.format( num )
    
    return text
    

def unit_to_int( unit ):
    
    if unit == 'B': return 1
    elif unit == 'KB': return 1024
    elif unit == 'MB': return 1024 ** 2
    elif unit == 'GB': return 1024 ** 3
    elif unit == 'TB': return 1024 ** 4
    

def value_range_to_pretty_string( value, range ):
    
    if value is not None and range is not None:
        
        value = min( value, range )
        
    
    return to_human_int( value ) + '/' + to_human_int( range )
    
