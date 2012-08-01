#!/bin/bash

source `dirname $0`/../conf/express-env.sh;

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

mediumTest() {
	hdfgen;
	reset4test;
	
	$EXEC fs -rmr mismatch.r1;
	flush "$NODES";
	$EXEC jar $JAR express.hdd.HDFMicroBenchmark 1024,512,2048 0,0,0 8,512,2048 8,1,1 0,0,0 1024,512,128 hdf-test mismatch.r1 'true';
	
	$EXEC fs -rmr match.r1;
	flush "$NODES";
	$EXEC jar $JAR express.hdd.HDFMicroBenchmark 1024,512,2048 0,0,0 8,512,2048 8,1,1 0,0,0 64,512,2048 hdf-test match.r1 'true';
}

pureBM(){
	local dataSize=$1;
	local sampleOffset=$2;
	local sampleSliceSize=$3;
	local sliceInSample=$4;
	local OSampleOffest=$5;
	local OSampleSize=$6;
	local inputDir=$7;
	local outputDir=$8;

	$EXEC jar $JAR express.hdd.HDFMicroBenchmark $dataSize $sampleOffset $sampleSliceSize $sliceInSample $OSampleOffest $OSampleSize $inputDir $outputDir 'true';
}

smallTest(){
	echo "smallTest $@";
	$TESTDIR/genBase.sh testGen;
	$EXEC fs -rmr hdf-test;
	
	$TESTDIR/genBase.sh pureGen 128,512,512 0,0,0 8,512,512 1,1,1 hdf-test;
	$EXEC fs -rmr match.r1;
	pureBM 128,512,512 0,0,0 8,512,512 1,1,1 0,0,0 8,512,512 hdf-test match.r1;
	$EXEC fs -rmr mismatch.r1;
	pureBM 128,512,512 0,0,0 8,512,512 1,1,1 0,0,0 128,512,32 hdf-test mismatch.r1;
}

pipeTest(){
	echo "pipeTest $@";
	$TESTDIR/genBase.sh testGen;
	$EXEC fs -rmr hdf-test;

	$TESTDIR/genBase.sh pureGen 128,512,512 0,0,0 8,512,512 1,1,1 hdf-pipeFile;
	$EXEC fs -rmr match.r1;
	pureBM 128,512,512 0,0,0 8,512,512 1,1,1 0,0,0 8,512,512 hdf-pipeFile match.r1;
	$EXEC fs -rmr mismatch.r1;
	pureBM 128,512,512 0,0,0 8,512,512 1,1,1 0,0,0 128,512,32 hdf-pipeFile mismatch.r1;
} 

eval "$@";
