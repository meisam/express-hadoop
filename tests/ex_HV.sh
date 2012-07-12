#!/bin/bash
#Horizontal partitioning Versus Vertical partitioning

TESTDIR=$(dirname $0);
TOOLDIR=$TESTDIR/../tools;
HADOOP=/mnt/common/siyuan/src/hadoop-1.0.1;
BIN=$HADOOP/bin;
CONF=$HADOOP/conf/slaves;
CONF_DIR=$HADOOP/conf;
EXEC=$HADOOP/bin/hadoop;
JAR=$HADOOP/express-hadoop.jar;
LOGDIR=/mnt/common/siyuan/log;

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

hdfgen() {
	local DIR=$1;
	local DSIZE=$2;
	local RSIZE=$3;
	local PSIZE=$4;
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

    echo "@generate test data";
    $HADOOP/bin/hadoop jar $JAR express.hdd.HDFGen $DSIZE 0,0,0 $RSIZE $PSIZE $DIR;
}

reset4test() {
    echo "reset conf for tests";
    $BIN/stop-all.sh;
    python $TOOLDIR/orc-xonf.py -f "$CONF_DIR/mapred-site.xml" -k 'mapred.tasktracker.map.tasks.maximum' -v 4;
    python $TOOLDIR/orc-xonf.py -f "$CONF_DIR/mapred-site.xml" -k 'mapred.tasktracker.reduce.tasks.maximum' -v 12;
    python $TOOLDIR/orc-xonf.py -f "$CONF_DIR/hdfs-site.xml" -k 'dfs.block.size' -v 268435456;
    $BIN/start-all.sh;
    echo "wait safe mode";
    $EXEC dfsadmin -safemode wait;
}

DSIZE=$1;
RSIZE=$2;
PSIZE=$3;
USIZES=$4;
REPS=$5;
PREFIX=$6;
DOFORMAT=$7; #opetional, true or false
CONFSUFFIX=$8;
SUMMARY=$LOGDIR/${PREFIX}_s;

echo "DSIZE=$DSIZE";
echo "RSIZE=$RSIZE";
echo "PSIZE=$PSIZE";
echo "USIZES=$USIZES";
echo "REPS=$REPS";
echo "PREFIX=$PREFIX";
echo "DOFORMAT=$DOFORMAT";
echo "CONFSUFFIX=$CONFSUFFIX";

echo "Tests run...";
LDIR=$LOGDIR/$PREFIX;
mkdir $LDIR;
rm $SUMMARY;
touch $SUMMARY;
if [ "$DOFORMAT" == "true" ]; then
	echo "generate data";
	format ${CONF}${CONFSUFFIX};
	hdfgen ${PREFIX}_hdf-data $DSIZE $RSIZE $PSIZE;
	reset4test;
fi

for rep in $REPS; do
	echo "Start to make replica $rep" >> $SUMMARY
	/usr/bin/time --append -o $SUMMARY $EXEC dfs -setrep -R -w $rep ${PREFIX}_hdf-data;
	$TESTDIR/_exH_V.sh $DSIZE $RSIZE $PSIZE "$USIZES" $rep $PREFIX $LOGDIR;
done
