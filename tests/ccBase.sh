#!/bin/bash
# Conflict Calculator

source `dirname $0`/../conf/.env;

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
	local RECORDS=$($EXEC fs -ls $DFSDIR 2>/dev/null| awk '{print $8}');
	for RECORD in $RECORDS; do
		local ID=$(basename $RECORD);
		local TMP=$(mktemp);
		$EXEC fs -text $RECORD > $TMP 2>/dev/null;
		local LEVELS=$(cat $TMP|awk -F'\t' '{print $3}'|uniq);
		for LEVEL in $LEVELS; do
			local SUM=$(cat $TMP| awk -F'\t' -v L=$LEVEL '{if ($3 == L) print $0}'| awk -F']|\t|;|,|\\[' '{SUM+=$7*$8*$9} END {print SUM/1024/1204}');
			echo -e "${RECORD}\t${LEVEL}\t${SUM}"
		done
		rm $TMP;
	done
}

eval "$@"; 