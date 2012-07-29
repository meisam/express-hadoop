#!/bin/bash
#Horizontal partitioning Versus Vertical partitioning

source `dirname $0`/../conf/express-env.sh;

#$TESTDIR/ex_HV.sh 16384,512,2048 8,512,1024 32,1,1 "16384,512,16 256,512,1024" "2" HV_1G_N16_C_ 'true' '.16'
echo "64MB";

python $TOOLDIR/orc-xonf.py -f "$CONF_DIR/mapred-site.xml" -k 'hdf.reduce.write' -v 'true';

#$TESTDIR/ex_HV.sh 8192,512,2048 8,512,1024 16,1,1 "8192,512,16" "1" $TAG 'true' '.8';
#$TESTDIR/ex_HV.sh 8192,512,2048 8,512,1024 16,1,1 "128,512,1024" "1" $TAG;
TAG=REPGEN_4G_N8_C_T2_R1;
$TESTDIR/ex_HV.sh 8192,512,1024 8,512,1024 16,1,1 "8192,512,16 128,512,1024" "1" $TAG 'true' '.8';

TAG=REPGEN_4G_N8_C_T2_R2;
$TESTDIR/ex_HV.sh 8192,512,1024 8,512,1024 16,1,1 "8192,512,16 128,512,1024" "2" $TAG;

TAG=REPGEN_4G_N8_C_T2_R3;
$TESTDIR/ex_HV.sh 8192,512,1024 8,512,1024 16,1,1 "8192,512,16 128,512,1024" "3" $TAG '.8';

TAG=REPGEN_4G_N8_C_T2_R4;
$TESTDIR/ex_HV.sh 8192,512,1024 8,512,1024 16,1,1 "8192,512,16 128,512,1024" "4" $TAG '.8';
python $TOOLDIR/orc-xonf.py -f "$CONF_DIR/mapred-site.xml" -k 'hdf.reduce.write' -v 'false';
