import base64


encode64 = (lambda a_str: 
    base64.b64encode(a_str.encode('ascii')).decode('ascii'))


def dict_minus(a_dict, key_ls, copy=True): 
    b_dict = a_dict.copy() if copy else a_dict
    for key in key_ls: 
        b_dict.pop(key, None)
    return b_dict
