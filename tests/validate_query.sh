#!/bin/bash
# Validate HDF Set Query

source `dirname $0`/../conf/.env;
#####################

oneQuery(){
	echo "oneQuery $@";
	
	RECINFO="1024,512,512 0,0,0 64,512,512 1,1,1";
	INDIR=hdf-test;
	OUTDIR=hdf-query;
	
	$EXEC fs -rmr ${INDIR};
	$EXEC fs -rmr ${OUTDIR};
	$TESTDIR/genBase.sh pureGen ${RECINFO} ${INDIR};
	$TESTDIR/queryBase.sh pureQuery ${RECINFO} "32,32,32" "128,128,128" ${INDIR} ${OUTDIR};
}

twoQueries(){
	echo "twoQuery $@";
	
	RECINFO="1024,512,512 0,0,0 64,512,512 1,1,1";
	INDIR=hdf-test;
	OUTDIR=hdf-query;
	
	$EXEC fs -rmr ${INDIR};
	$EXEC fs -rmr ${OUTDIR};
	$TESTDIR/genBase.sh pureGen ${RECINFO} ${INDIR};
	$TESTDIR/queryBase.sh pureQuery ${RECINFO} "32,32,32/256,256,256" "128,128,128/192,128,64" ${INDIR} ${OUTDIR};
}

eval "$@";