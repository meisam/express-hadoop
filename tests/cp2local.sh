#!/bin/bash

TESTDIR=$(dirname $0);
source $TESTDIR/env.sh;

INDIR=$1;
OUTDIR=$2;

FLIST="$($EXEC fs -ls $INDIR 2>/dev/null|grep data|awk '{print $8}')";
for file in $FLIST; do
	#echo $file;
	$EXEC dfs -copyToLocal $file $OUTDIR 2>/dev/null & 
done

wait4.sh -c 'jps|grep FsShell|wc -l' -v 0 -s 1;

