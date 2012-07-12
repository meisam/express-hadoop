#!/bin/bash
#Horizontal partitioning Versus Vertical partitioning

TESTDIR=$(dirname $0);
TOOLDIR=$TESTDIR/../tools;
HADOOP=/mnt/common/siyuan/src/hadoop-1.0.1;
BIN=$HADOOP/bin;
CONF=$HADOOP/conf/slaves;
CONF_DIR=$HADOOP/conf;
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
	$BIN/stop-all.sh;
	$BIN/start-all.sh;
}

hdfgen() {
	local DIR=$1;
    echo "stop all";
    $BIN/stop-all.sh;

    python $TOOLDIR/orc-xonf.py -f "$CONF_DIR/mapred-site.xml" -k 'mapred.tasktracker.map.tasks.maximum' -v 1;
    $BIN/start-all.sh;

    echo "wait safe mode";
    $EXEC dfsadmin -safemode wait;

    echo "@clear existing data"
    $EXEC fs -rmr $DIR;

    echo "@write test";
    $HADOOP/bin/hadoop jar $JAR express.hdd.HDFGen 1024,128,128 0,0,0 8,128,128 16,1,1 $DIR;
    $EXEC fs -rmr $DIR;

    echo "@generate test data";
    $HADOOP/bin/hadoop jar $JAR express.hdd.HDFGen 16384,512,2048 0,0,0 8,512,1024 32,1,1 $DIR;
}

reset4test() {
    echo "reset conf for tests";
    $BIN/stop-all.sh;
    python $TOOLDIR/orc-xonf.py -f "$CONF_DIR/mapred-site.xml" -k 'mapred.tasktracker.map.tasks.maximum' -v 8;
    python $TOOLDIR/orc-xonf.py -f "$CONF_DIR/mapred-site.xml" -k 'mapred.tasktracker.reduce.tasks.maximum' -v 8;
    python $TOOLDIR/orc-xonf.py -f "$CONF_DIR/hdfs-site.xml" -k 'dfs.block.size' -v 268435456;
    $BIN/start-all.sh;
    echo "wait safe mode";
    $EXEC dfsadmin -safemode wait;
}

echo "@@reformat to condunct Tests on 8 nodes";
echo "Tests run...";
PREFIX=HV_1G_N16_C_;
LDIR=$LOGDIR/$PREFIX;
mkdir $LDIR;
touch $LOGDIR/${PREFIX}s;
echo "generate data";
format $CONF.16;
hdfgen ${PREFIX}hdf-data;
reset4test;

for rep in $(seq 1 5); do
	$EXEC dfs -setrep -R -w $rep ${PREFIX}hdf-data;
	$TESTDIR/_exH_V.sh 16384,512,2048 8,512,1024 32,1,1 "16384,512,16 256,512,1024" $rep $PREFIX $LOGDIR;
done
