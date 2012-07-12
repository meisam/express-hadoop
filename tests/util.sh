#!/bin/bash

flush() {
	local _NODES=$1;
	echo "clear cache on nodes {${_NODES}}"
	for node in $_NODES; do 
		ssh $node cat /etc/hostname; 
		ssh $node $TOOLDIR/cache-cleanup.sh -f; 
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

reset() {
	local MM=$1;
	local MR=$2;
	echo "reset: $@";
	$BIN/stop-all.sh;
	python $TOOLDIR/orc-xonf.py -f "$CONF_DIR/mapred-site.xml" -k 'mapred.tasktracker.map.tasks.maximum' -v $MM;
	python $TOOLDIR/orc-xonf.py -f "$CONF_DIR/mapred-site.xml" -k 'mapred.tasktracker.map.tasks.maximum' -v $MR;
	$BIN/start-all.sh;
	echo "wait safe mode";
	$EXEC dfsadmin -safemode wait;
}