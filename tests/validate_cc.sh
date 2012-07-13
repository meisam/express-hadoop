#!/bin/bash
# Validate Conflict Calculator

TESTDIR=`dirname $0`;
source $TESTDIR/env.sh;
source $TESTDIR/util.sh;

#####################

echo "@Test conducts on 8 nodes";
format $CONF.8;


oneRec(){
	# The first testGen is ineffective, but is must to have
	$TESTDIR/genBase.sh testGen;
	$TESTDIR/genBase.sh testGen;
	$TESTDIR/ccBase.sh testCC;
}

TwoRec(){
	# The first testGen is ineffective, but is must to have
	$TESTDIR/genBase.sh testGen;
	$EXEC fs -rmr hdf-test;
	echo "TwoRec $@";

	$TESTDIR/genBase.sh pureGen 1024,512,2048 0,0,0 8,512,2048 8,1,1 hdf-test/rec01;
	$TESTDIR/genBase.sh pureGen 1024,512,2048 0,0,0 64,512,256 1,1,8 hdf-test/rec02;
	$TESTDIR/ccBase.sh testCC;
}

eval "$@";