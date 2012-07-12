#!/bin/bash
# Validate Conflict Calculator

TESTDIR=`dirname $0`;
source $TESTDIR/env.sh;
source $TESTDIR/util.sh;

#####################
dataSize=$1; 
partitionOffset=$2; 
recordSize=$3; 
partitionSize=$4;
outDir=$5;

echo "@Test conducts on 8 nodes";
format $CONF.8;

# The first testGen is ineffective, but is must to have
$TESTDIR/genBase.sh testGen;
$TESTDIR/genBase.sh testGen;
$TESTDIR/ccBase.sh testCC;
