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
	local OFFSET=$2;
	local LENGTH=$3;
	
	$EXEC jar $JAR express.util.AppendOneRecord $FILEPATH $OFFSET $LENGTH;
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

pipeFileTest01() {
	touchPipe 8 hdf-pipe;
	$TESTDIR/validate.sh pureBM 64,16,8 0,0,0 8,16,8 1,1,1 0,0,0 8,16,8 hdf-pipe match.r1 &
	for fid in $(seq 0 7); do
		pureAppendRecord /user/siyuan/hdf-pipe/${fid} $((8*${fid})),8,8 8,16,8;
	done
}

eval "$@"; 