from poster.encode import multipart_encode
from poster.streaminghttp import register_openers
import urllib2

register_openers()

db="mysql_"
prefix=3

params = {
'summary_data': open(db + str(prefix) + ".summary", "r"),
'db_conf_data':open(db + str(prefix) + ".db.cnf","r"),
'db_status':open(db + str(prefix) + ".db.status",'r'),
'sample_data':open(db + str(prefix)+".res","r"),
'raw_data': open(db + str(prefix)+".res","r"),
'benchmark_conf_data': open(db + str(prefix)+".ben.cnf","r"),
'upload_code':  '14414QYEORPR4CE52X20',
'upload_use':'compute',
#'store',
'hardware':'m3.xlarge',
'cluster':'exps_mysql_5.6_m3.xlarge_ycsb_rr_sf18000_tr50_t300_runlimited_w0-0-0-100-0-0_s0.6' #unknown

}

datagen, headers = multipart_encode(params)

#request = urllib2.Request("http://127.0.0.1:8000/new_result/", datagen, headers)
request = urllib2.Request("http://52.26.247.195:8000/new_result/", datagen, headers)

print urllib2.urlopen(request).read()
