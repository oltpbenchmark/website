'''
Created on Jul 9, 2017

@author: dvanaken
'''

class BaseType(object):
    TYPE_NAMES = {}
    
    @classmethod
    def choices(cls):
        return list(cls.TYPE_NAMES.iteritems())
    
    @classmethod
    def name(cls, ctype):
        return cls.TYPE_NAMES[ctype]
    
    @classmethod
    def type(cls, name):
        return [k for k,v in cls.TYPE_NAMES.iteritems() if v.lower() == name.lower()][0]
    

class DBMSType(BaseType):
    MYSQL     = 1
    POSTGRES  = 2
    DB2       = 3
    ORACLE    = 4
    SQLSERVER = 5
    SQLITE    = 6
    HSTORE    = 7
    VECTOR    = 8

    TYPE_NAMES = {
        MYSQL:     'MySQL',
        POSTGRES:  'Postgres',
        DB2:       'Db2',
        ORACLE:    'Oracle',
        SQLITE:    'SQLite',
        HSTORE:    'HStore',
        VECTOR:    'Vector',
        SQLSERVER: 'SQL Server',
    }

class MetricType(BaseType):
    COUNTER = 1
    INFO    = 2
    
    TYPE_NAMES = {
        COUNTER: 'COUNTER',
        INFO:    'INFO',
    }

class VarType(BaseType):
    STRING    = 1
    INTEGER   = 2
    REAL      = 3
    BOOL      = 4
    ENUM      = 5
    TIMESTAMP = 6

    TYPE_NAMES = {
        STRING:    'STRING',
        INTEGER:   'INTEGER',
        REAL:      'REAL',
        BOOL:      'BOOL',
        ENUM:      'ENUM',
        TIMESTAMP: 'TIMESTAMP',
    }


class HardwareType(BaseType):

    TYPE_NAMES = {1: 'GENERIC', 2: 't2.nano', 3: 't2.micro', 4: 't2.small', 5: 't2.medium', 6: 't2.large', 7: 't2.xlarge', 8: 't2.2xlarge', 9: 'm4.large', 10: 'm4.xlarge', 11: 'm4.2xlarge', 12: 'm4.4xlarge', 13: 'm4.10xlarge', 14: 'm4.16xlarge', 15: 'm3.medium', 16: 'm3.large', 17: 'm3.xlarge', 18: 'm3.2xlarge', 19: 'c4.large', 20: 'c4.xlarge', 21: 'c4.2xlarge', 22: 'c4.4xlarge', 23: 'c4.8xlarge', 24: 'c3.large', 25: 'c3.xlarge', 26: 'c3.2xlarge', 27: 'c3.4xlarge', 28: 'c3.8xlarge', 29: 'p2.xlarge', 30: 'p2.8xlarge', 31: 'p2.16xlarge', 32: 'g2.2xlarge', 33: 'g2.8xlarge', 34: 'x1.16large', 35: 'x1.32xlarge', 36: 'r4.large', 37: 'r4.xlarge', 38: 'r4.2xlarge', 39: 'r4.4xlarge', 40: 'r4.8xlarge', 41: 'r4.16xlarge', 42: 'r3.large', 43: 'r3.xlarge', 44: 'r3.2xlarge', 45: 'r3.4xlarge', 46: 'r3.8xlarge', 47: 'i3.large', 48: 'i3.xlarge', 49: 'i3.2xlarge', 50: 'i3.4xlarge', 51: 'i3.8xlarge', 52: 'i3.16large', 53: 'd2.xlarge', 54: 'd2.2xlarge', 55: 'd2.4xlarge', 56: 'd2.8xlarge', 57: 'f1.2xlarge', 58: 'f1.16xlarge'}

for k,v in HardwareType.TYPE_NAMES.iteritems():
    if v == 'GENERIC':
        attr_name = v
    else:
        attr_name = 'EC2_{}'.format(v.upper().replace('.', ''))
    setattr(HardwareType, attr_name, k)
            
    