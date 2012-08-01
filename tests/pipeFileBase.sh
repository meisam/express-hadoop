#!/bin/bash
# MakePipeFile

source `dirname $0`/../conf/express-env.sh;

pureMakePF() {
	echo "pureMakePF $@";
	local pipeNum=$1; 
	local outDir=$2;
	
	$EXEC jar $JAR express.util.MakePipeFile $pipeNum $outDir;
}

testMakePF() {
	echo "testMakePF $@";
	$EXEC fs -rmr hdf-pipeFile;
	pureMakePF 8 hdf-pipeFile;
}

eval "$@"; 