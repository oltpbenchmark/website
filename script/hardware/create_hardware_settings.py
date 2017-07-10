import sys
import csv
import json
import shutil
from collections import OrderedDict
    

HW = OrderedDict()
with open('ec2_instance_types.csv', 'r') as f:
    reader = csv.reader(f)
    for i,row in enumerate(reader):
        if i == 0:
            header = row
        else:
            print row[0]
            entry = {}
            entry['type'] = i+1
            entry['name'] = row[0]
            entry['cpu'] = int(row[1])
            entry['memory'] = float(row[2].replace(',', ''))
            storage_str = row[3]
            print "STORAGE_STR: {}".format(storage_str)
            storage_type = None
            if 'EBS' in storage_str:
                storage_type = 'EBS'
            elif 'NVMe' in storage_str:
                storage_type = 'NVMe'
            elif 'SSD' in storage_str:
                storage_type = 'SSD'
            elif entry['name'].startswith('r4'):
                    storage_type = 'EBS'
            elif entry['name'].startswith('d2'):
                storage_type = 'HDD'
            elif entry['name'] == 'f1.16xlarge':
                storage_type = 'SSD'
            else:
                raise Exception('Unknown storage type for {}'.format(entry['name']))
            storage_list = None
            print "STORAGE_TYPE = {}".format(storage_type)
            if storage_type == 'EBS':
                entry['storage'] = '40,40'
            elif entry['name'] == 'f1.2xlarge':
                entry['storage'] = storage_str.split(' ')[0]
            else:
                parts = storage_str.split(' ')
                num_devices = 4 if int(parts[0]) > 4 else int(parts[0])
                size = parts[2].replace(',', '')
                entry['storage'] = ','.join([size for _ in range(num_devices)])

            entry['storage_type'] = storage_type
            entry['additional_specs'] = json.dumps(OrderedDict(zip(header[4:], row[4:])), encoding='utf-8')
            HW[entry['name']] = entry

# For types.HardwareTypes
type_names = {v['type']: k for k,v in HW.iteritems()}
type_names[1] = 'GENERIC'
with open('type_names.txt', 'w') as f:
    f.write(str(type_names))

entries = []
for k,v in HW.iteritems():
    entries.append({
        "model": "website.Hardware",
        'fields': v
    })

with open('hardware.json', 'w') as f:
    json.dump(entries, f, encoding='utf-8', indent=4)

shutil.copy('hardware.json', '../preload/hardware.json')

maxx = ''
maxlen = 0
for k,v in HW.iteritems():
    if len(v['storage']) > maxlen:
        print k,len(v['storage']), v['storage']
        maxlen = len(v['storage'])
        
