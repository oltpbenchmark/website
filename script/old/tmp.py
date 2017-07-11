import json

with open('mysql_knobs.json', 'r') as f:
    data = json.load(f)

maxlen = 0
for entry in data:
    name = entry['fields']['params']
    if len(name) > maxlen:
        maxlen = len(name)
print "max name length: {}".format(maxlen)
