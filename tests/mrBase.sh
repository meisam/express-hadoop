#!/bin/bash

source `dirname $0`/../conf/express-env.sh;

DSIZE=$1;
RSIZE=$2;
PSIZE=$3;
USAGE=$4;
REPLICA=$5;
PREFIX=$6;
LOGDIR=$7;
SUMMARY=$LOGDIR/${PREFIX}_s;

ISLOC=$8;
ISBYPASS=$9;
ISREC=${10};

flush() {
	local _NODES=$1;
	echo "clear cache on nodes {${_NODES}}"
	for node in $_NODES; do 
		ssh $node cat /etc/hostname; 
		ssh $node $HOME/exec/sbin/cache-cleanup.sh -f; 
	done
}

NODES=$(cat $CONF);

OUTPUT=${PREFIX}_${USAGE}_hdf.L${ISLOC}.B${ISBYPASS}.R${ISREC}.r${REPLICA};
echo "OUTPUT = $OUTPUT"
$EXEC jar $JAR express.hdd.HDFMicroBenchmark $DSIZE 0,0,0 $RSIZE $PSIZE 0,0,0 $USAGE ${PREFIX}_hdf-data ${OUTPUT} ${ISLOC} ${ISBYPASS} ${ISREC};
$EXEC job -history all $OUTPUT > $LOGDIR/$OUTPUT;
echo $OUTPUT >> $SUMMARY;
cat $LOGDIR/$OUTPUT|egrep -e "JobConf|Average|Finished" >> $SUMMARY;

