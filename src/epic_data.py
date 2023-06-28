


class EpicDelta(): 
    def __init__(self, path, **kwargs): 
        self.path = path
        
        for kwd in ['file_regex', 'schema', 'meta_cols']: 
            setattr(self, kwd, kwargs[kwd]) if kwd in kwargs else None
            # self.file_regex = kwargs['file_regex']   ... y similares.               

    def read_by_keys(self, **regex_keys): 
        if not hasattr(self, 'file_regex'): 
            raise Exception("Delta must have 'FILE_REGEX'.")
        
        file_keys = keys_from_regex(self.file_regex)

    
def keys_from_regex(a_regex: str): 
    key_regex = r"\(\?P\<(.+?)\>.+?\)"
    keys = []
    key_match = re.search(key_regex, a_regex)
    while key_match:  
        keys.append(key_match.group(1))
        a_regex = a_regex.replace(key_match.group(0), '')
        key_match = re.search(key_regex, a_regex)    
    return keys
    
    