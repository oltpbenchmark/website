TARGET_CONF="mysql.conf"
BENCH_CONF="tpcc_config_mysql_ysu1.xml"
SAMPLE_RESULT="bench_result.res"
TEMP_FILE="/tmp/$(basename $0).$RANDOM.txt"

mysqladmin variables | grep "|.*|.*|" | sed s/"|\s*\(\S*\)\s*|\s*\(.*\)\s*|"/"\1:\2"/ | sed s/"\s*$"// > $TARGET_CONF

wc -l < $TARGET_CONF > $TEMP_FILE
wc -l < $BENCH_CONF >> $TEMP_FILE
wc -l < $SAMPLE_RESULT >> $TEMP_FILE

cat $TARGET_CONF >> $TEMP_FILE
cat $BENCH_CONF >> $TEMP_FILE
cat $SAMPLE_RESULT >> $TEMP_FILE

FORM_OPT="--form upload_code=`cat code`"

curl $FORM_OPT --form "data=@"$TEMP_FILE http://127.0.0.1:8000/new_result/

rm $TEMP_FILE
