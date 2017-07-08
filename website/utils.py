'''
Created on Jul 8, 2017

@author: dvanaken
'''

import re

def parse_dbms_version_string(dbms_type, version_string):
    if dbms_type == 'POSTGRES':
        dbms_version = version_string.split(',')[0]
        dbms_version = re.search("\d+\.\d+(?=\.\d+)", dbms_version).group(0)
    else:
        raise NotImplementedError('Implement me!')
    return dbms_version