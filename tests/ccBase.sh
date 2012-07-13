#!/bin/bash
# Conflict Calculator

TESTDIR=`dirname $0`;
source $TESTDIR/env.sh;

pureCC() {
	local INDIR=$1;
	local OUTDIR=$2;

	echo "pureCC $@";
	$EXEC jar $JAR express.hdd.ConflictCalculator ${INDIR} ${OUTDIR};
}

testCC() {
	echo "testCC $@";
	$EXEC fs -rmr hdf-cc;
	$EXEC jar $JAR express.hdd.ConflictCalculator hdf-test hdf-cc;
}

eval "$@"; 