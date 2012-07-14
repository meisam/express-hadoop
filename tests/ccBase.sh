#!/bin/bash
# Conflict Calculator

TESTDIR=`dirname $0`;
source $TESTDIR/env.sh;

pureCC() {
	local INDIR=$1;
	local OUTDIR=$2;

	echo "pureCC $@";
	$EXEC jar $JAR express.hdd.ConflictCalculator ${INDIR} ${OUTDIR};
	$EXEC fs -rmr /tmp;
}

testCC() {
	echo "testCC $@";
	$EXEC fs -rmr hdf-cc;
	$EXEC jar $JAR express.hdd.ConflictCalculator hdf-test hdf-cc;
}

report() {
	echo "report $@";
	local DFSDIR=$1;
	local RECORDS=$($EXEC fs -ls $DFSDIR| awk '{print $8}');
	for RECORD in $RECORDS; do
		local ID=$(basename $RECORD);
		local TMP=$(mktemp);
		$EXEC fs -text $RECORD > $TMP;
		local LEVELS=$(cat $TMP|awk -F'\t' '{print $3}'|uniq);
		for LEVEL in $LEVELS; do
			local SUM=$(cat $TMP| awk -F'\t' -v L=$LEVEL '{if ($3 == l) print $0}'| awk -F']|\t|;|,|\\[' '{print $7*$8*$9}');
			echo "${RECORD}\t${LEVEL}\t${SUM}"
		done
		rm $TMP;
	done
}

eval "$@"; 