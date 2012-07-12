#!/bin/bash
# Validate Conflict Calculator

TESTDIR=`dirname $0`;
TOOLDIR=$TESTDIR/../tools;
source $TESTDIR/env.sh;
source $TESTDIR/util.sh;

#####################
dataSize=$1; 
partitionOffset=$2; 
recordSize=$3; 
partitionSize=$4;
outDir=$5;

$TESTDIR/genBase.sh testGen;
$TESTDIR/ccBase.sh testCC;
