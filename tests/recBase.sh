#!/bin/bash
#Test for block reconstruction

source `dirname $0`/../conf/.env;

NODES=$(cat $CONF);

flush() {
    local _NODES=$1;
    echo "clear cache on nodes {${_NODES}}"
    for node in $_NODES; do
        ssh $node cat /etc/hostname;
        ssh $node $HOME/exec/sbin/cache-cleanup.sh -f;
    done
}

format() {
	local NEWCONF=$1;
	$BIN/stop-all.sh;
	cp $NEWCONF $CONF;
	NODES=$(cat $CONF);
	$TOOLDIR/_quickinit.sh "$NODES" "/home/siyuan_hadoop10" "/mnt/common/siyuan/src/hadoop-1.0.1/logs/";
	$EXEC namenode -format;
	$BIN/start-all.sh;
	$EXEC dfsadmin -safemode wait;
	$BIN/stop-all.sh;
	$BIN/start-all.sh;
}

set4hdfgen() {
	local DIR=$1;
    echo "stop all";
    $BIN/stop-all.sh;

    python $TOOLDIR/orc-xonf.py -f "$CONF_DIR/mapred-site.xml" -k 'mapred.tasktracker.map.tasks.maximum' -v 1;
    $BIN/start-all.sh;

    echo "wait safe mode";
    $EXEC dfsadmin -safemode wait;

    echo "@clear existing data"
    $EXEC fs -rmr $DIR;

    echo "@write test";
    $HADOOP/bin/hadoop jar $JAR express.hdd.HDFGen 1024,128,128 0,0,0 8,128,128 16,1,1 $DIR;
    $EXEC fs -rmr $DIR;
}

hdfgen() {
	local DIR=$1;
	local DSIZE=$2;
	local RSIZE=$3;
	local PSIZE=$4;
	
	echo "";
    echo "DSIZE=$DSIZE, RSIZE=$RSIZE, PSIZE=$PSIZE, DIR=$DIR";
    $HADOOP/bin/hadoop jar $JAR express.hdd.HDFGen $DSIZE 0,0,0 $RSIZE $PSIZE $DIR;
}

reset4test() {
    echo "reset conf for tests";
    $BIN/stop-all.sh;
    python $TOOLDIR/orc-xonf.py -f "$CONF_DIR/mapred-site.xml" -k 'mapred.tasktracker.map.tasks.maximum' -v 8;
    python $TOOLDIR/orc-xonf.py -f "$CONF_DIR/mapred-site.xml" -k 'mapred.tasktracker.reduce.tasks.maximum' -v 8;
    python $TOOLDIR/orc-xonf.py -f "$CONF_DIR/hdfs-site.xml" -k 'dfs.block.size' -v 268435456;
    $BIN/start-all.sh;
    echo "wait safe mode";
    $EXEC dfsadmin -safemode wait;
}

XS=$1;
YS=$2;
REPS=$3;
PREFIX=$4;
CONFSUFFIX=$5;
DOFORMAT=$6;
DOGEN=$7;
DOTEST=$8;

echo "Tests run...";
LDIR=$LOGDIR/$PREFIX;
mkdir $LDIR;
touch $LOGDIR/${PREFIX}_s;

if [ "$DOFORMAT" == "true" ]; then
	format ${CONF}${CONFSUFFIX};
	set4hdfgen ${PREFIX}hdf-data;
fi

if [ "$DOGEN" == "true" ]; then
	for X in $XS; do
		for Y in $YS; do
			if [ $Y -gt $X ]; then
				break;
			fi
			DSIZE="$X,1024,1024";
			RSIZE="1,1024,1024";
			PSIZE="$Y,1,1";
			hdfgen ${PREFIX}_X${X}Y${Y}_hdf-data $DSIZE $RSIZE $PSIZE;
		done
	done
	reset4test;
fi

if [ "$DOTEST" == "true" ]; then
	for rep in $REPS; do
		for X in $XS; do
			if [ $Y -gt $X ]; then
				break;
			fi

			for Y in $YS; do
				$EXEC dfs -setrep -R -w $rep ${PREFIX}_X${X}Y${Y}_hdf-data;
			done
		done
		flush "${NODES}";
		for X in $XS; do
			for Y in $YS; do
				DSIZE="$X,1024,1024";
				RSIZE="1,1024,1024";
				PSIZE="$Y,1,1";
	
				$TESTDIR/mrBase.sh $DSIZE $RSIZE $PSIZE ${DSIZE} $rep ${PREFIX}_X${X}Y${Y} $LOGDIR 'false' 'false' 'true';
			done
		done
	done
fi
