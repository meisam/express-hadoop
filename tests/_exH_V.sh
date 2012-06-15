#!/bin/bash
#Horizontal partitioning Versus Vertical partitioning

DSIZE=$1;
RSIZE=$2;
PSIZE=$3;
USAGES=$4;
REPLICA=$5;
PREFIX=$6;
LOGDIR=$7;
LDIR=$LOGDIR/$PREFIX;
SUMMARY=$LOGDIR/${PREFIX}_s;

flush() {
	local _NODES=$1;
	echo "clear cache on nodes {${_NODES}}"
	for node in $_NODES; do 
		ssh $node cat /etc/hostname; 
		ssh $node $HOME/exec/sbin/cache-cleanup.sh -f; 
	done
}

HADOOP=/mnt/common/siyuan/src/hadoop-1.0.1;
BIN=$HADOOP/bin;
CONF=$HADOOP/conf/slaves;
EXEC=$HADOOP/bin/hadoop;
JAR=$HADOOP/hdf_micro.jar;

#echo "reload runtime";
#$BIN/stop-all.sh;
#$BIN/start-all.sh;
#$EXEC dfsadmin -safemode wait;

NODES=$(cat $CONF);
#echo "@generate test data"
#$EXEC jar $JAR hdf.test.HDFGen $DSIZE 0,0,0 $RSIZE $PSIZE ${PREFIX}hdf-data;

echo "#replica = $REPLICA";
for usage in $USAGES; do
	flush "$NODES";
	OUTPUT=${PREFIX}_${usage}_hdf.Loff.r${REPLICA};
	echo "OUTPUT = $OUTPUT"
	$EXEC jar $JAR hdf.test.HDFMicroBenchmark $DSIZE 0,0,0 $RSIZE $PSIZE 0,0,0 $usage ${PREFIX}_hdf-data ${OUTPUT} 'false';
	$EXEC job -history all $OUTPUT > $LDIR/$OUTPUT;
	echo $OUTPUT >> $SUMMARY;
	cat $LDIR/$OUTPUT|egrep -e "JobConf|Average|Finished" >> $SUMMARY;

	flush "$NODES";
	echo "USAGE = $usage"
	OUTPUT=${PREFIX}_${usage}_hdf.Lon.r${REPLICA};
	echo "OUTPUT = $OUTPUT"
	$EXEC jar $JAR hdf.test.HDFMicroBenchmark $DSIZE 0,0,0 $RSIZE $PSIZE 0,0,0 $usage ${PREFIX}_hdf-data ${OUTPUT} 'true';
	$EXEC job -history all $OUTPUT > $LDIR/$OUTPUT;
	echo $OUTPUT >> $SUMMARY;
	cat $LDIR/$OUTPUT|egrep -e "JobConf|Average|Finished" >> $SUMMARY;

#	flush "$NODES";
#	OUTPUT=${PREFIX}_${usage}_hdf.bypass.r${REPLICA};
#	echo "OUTPUT = $OUTPUT"
#	$EXEC jar $JAR hdf.test.HDFMicroBenchmark $DSIZE 0,0,0 $RSIZE $PSIZE 0,0,0 $usage ${PREFIX}_hdf-data ${OUTPUT} 'false' 'true';
#	$EXEC job -history all $OUTPUT > $LDIR/$OUTPUT;
#	echo $OUTPUT >> $SUMMARY;
#	cat $LDIR/$OUTPUT|egrep -e "JobConf|Average|Finished" >> $SUMMARY;
done
flush "$NODES";
