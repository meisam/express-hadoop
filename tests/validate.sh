#!/bin/bash

TESTDIR=$(dirname $0);
TOOLDIR=$TESTDIR/../tools;
source $TESTDIR/env.sh;

NODES=$(cat $CONF);

flush() {
	local _NODES=$1;
	echo "clear cache on nodes {${_NODES}}"
	for node in $_NODES; do 
		ssh $node cat /etc/hostname; 
		ssh $node $HOME/exec/sbin/cache-cleanup.sh -f; 
	done
}

hdfgen() {
	echo "stop all";
	$BIN/stop-all.sh;
	
	echo "@Test conducts on 8 nodes";
	cp $CONF.8 $CONF;
	python $TOOLDIR/orc-xonf.py -f "$CONF_DIR/mapred-site.xml" -k 'mapred.tasktracker.map.tasks.maximum' -v 1;
	$BIN/start-all.sh;
	
	echo "wait safe mode";
	$EXEC dfsadmin -safemode wait;
	
	echo "@clear existing data"
	$EXEC fs -rmr hdf-test;
	
	echo "@generate test data";
	$HADOOP/bin/hadoop jar $JAR express.hdd.HDFGen 1024,512,2048 0,0,0 8,512,2048 8,1,1 hdf-test;
}

reset4test() {
	echo "reset conf for tests";
	$BIN/stop-all.sh;
	python $TOOLDIR/orc-xonf.py -f "$CONF_DIR/mapred-site.xml" -k 'mapred.tasktracker.map.tasks.maximum' -v 7;
	$BIN/start-all.sh;
	echo "wait safe mode";
	$EXEC dfsadmin -safemode wait;
}

hdfgen;
reset4test;

$EXEC fs -rmr mismatch.r1;
flush "$NODES";
$EXEC jar $JAR express.hdd.HDFMicroBenchmark 1024,512,2048 0,0,0 8,512,2048 8,1,1 0,0,0 1024,512,128 hdf-test mismatch.r1 'true';

$EXEC fs -rmr match.r1;
flush "$NODES";
$EXEC jar $JAR express.hdd.HDFMicroBenchmark 1024,512,2048 0,0,0 8,512,2048 8,1,1 0,0,0 64,512,2048 hdf-test match.r1 'true';

