HOST=$1
CODE=$2
RESULT_BASE=$3

curl --form upload_code=$2 \
    --form "sample_data=@"$RESULT_BASE.res \
    --form "raw_data=@"$RESULT_BASE.raw.gz \
    --form "db_conf_data=@"$RESULT_BASE.db.cnf \
    --form "benchmark_conf_data=@"$RESULT_BASE.ben.cnf \
    --form "summary_data=@"$RESULT_BASE.summary \
    $1
