#!/bin/bash
# Validate Conflict Calculator

TESTDIR=`dirname $0`;
source $TESTDIR/env.sh;

#####################
format(){
	echo "......Format for test under configuration $CONF.8";
	$TESTDIR/util.sh format $CONF.8;
}

oneRec(){
	# The first testGen is ineffective, but is must to have
	$TESTDIR/genBase.sh testGen;
	$TESTDIR/genBase.sh testGen;
	$TESTDIR/ccBase.sh testCC;
}

twoRec(){
	# The first testGen is ineffective, but is must to have
	$TESTDIR/genBase.sh testGen;
	$EXEC fs -rmr hdf-test;
	echo "twoRec $@";

	$TESTDIR/genBase.sh pureGen 1024,512,2048 0,0,0 8,512,2048 8,1,1 hdf-test/rec01;
	$TESTDIR/genBase.sh pureGen 1024,512,2048 0,0,0 64,512,256 1,1,8 hdf-test/rec02;
	$TESTDIR/ccBase.sh testCC;
}

threeRec(){
	# The first testGen is ineffective, but is must to have
	$TESTDIR/genBase.sh testGen;
	$EXEC fs -rmr hdf-test;
	echo "threeRec $@";

	$TESTDIR/genBase.sh pureGen 1024,512,2048 0,0,0 64,512,2048 1,1,1 hdf-test/rec01;
	$TESTDIR/genBase.sh pureGen 1024,512,2048 0,0,0 64,512,1024 1,1,1 hdf-test/rec02;
	$TESTDIR/genBase.sh pureGen 1024,512,2048 0,0,0 16,4096,128 1,1,1 hdf-test/rec03;
	$TESTDIR/ccBase.sh testCC;
}

eval "$@";