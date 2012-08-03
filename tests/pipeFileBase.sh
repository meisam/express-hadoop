#!/bin/bash
# MakePipeFile

source `dirname $0`/../conf/express-env.sh;

pureMakePF() {
	echo "pureMakePF $@";
	local pipeNum=$1; 
	local outDir=$2;
	
	$EXEC jar $JAR express.util.MakePipeFile $pipeNum $outDir 2>/dev/null;
}

pureAppendRecord() {
	echo "pureAppend2File $@";
	local FILEPATH=$1;
	
	$EXEC jar $JAR express.util.AppendOneRecord $FILEPATH;
}

testMakePF() {
	echo "testMakePF $@";
	$EXEC fs -rmr hdf-pipe;
	pureMakePF 8 hdf-pipe;
}

touchPipe() {
	echo "touchPipe $@";
	local pipeNum=$1; 
	local outDir=$2;
	
	$EXEC fs -mkdir $outDir;
	for i in $(seq 0 $((${pipeNum} -1))); do 
		$EXEC fs -touchz $outDir/${i};  
	done
}

eval "$@"; 