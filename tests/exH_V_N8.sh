#!/bin/bash
#Horizontal partitioning Versus Vertical partitioning

TESTDIR=$(dirname $0);
TOOLDIR=$TESTDIR/../tools;
HADOOP=/mnt/common/siyuan/src/hadoop-1.0.1;
BIN=$HADOOP/bin;
CONF=$HADOOP/conf/slaves;
EXEC=$HADOOP/bin/hadoop;
JAR=$HADOOP/express-hadoop.jar;
LOGDIR=/mnt/common/siyuan/log;

format() {
	local NEWCONF=$1;
	$BIN/stop-all.sh;
	cp $NEWCONF $CONF;
	NODES=$(cat $CONF);
	$TOOLDIR/_quickinit.sh "$NODES" "/home/siyuan_hadoop10" "/mnt/common/siyuan/src/hadoop-1.0.1/logs/";
	$EXEC namenode -format;
	$BIN/start-all.sh;
	$EXEC dfsadmin -safemode wait;
}

echo "@@reformat to condunct Tests on 8 nodes";
#format $CONF.8;
echo "Tests run...";
PREFIX=HV_1G_N8_;
LDIR=$LOGDIR/$PREFIX;
mkdir $LDIR;
touch $LOGDIR/${PREFIX}s;

for rep in $(seq 1 5); do
	$EXEC dfs -setrep -R -w $rep ${PREFIX}hdf-data;
	$TESTDIR/_exH_V.sh 1024,512,2048 8,512,2048 8,1,1 "1024,512,128 64,512,2048" $rep $PREFIX $LOGDIR;
done
