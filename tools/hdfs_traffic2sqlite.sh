#!/bin/bash

# extract traffic info of one job from $HADOOP_LOG into a sqlite3 db
# pre: 	hdfs_traffic2table.py
#		txt2sqlite.py

usage()
{
    echo "Usage: `echo $0| awk -F/ '{print $NF}'`  [-option]"
    echo "[option]:"
    echo "  -d  db "
    echo "  -L  log directory "
    echo "  -j  jobid		  "
    echo
    echo "Copyright by Siyuan Ma  2011-12."
    echo
}

if [ $# -lt 3 ]
then
        usage
        exit
fi

while getopts "d:L:j:" OPTION
do
    case $OPTION in
        d)
            DB=$OPTARG;
            ;;
        L)
            LOGDIR=$OPTARG;
            ;;
        j)
            JID=$OPTARG;
            ;;
        ?)
            usage
			exit
            ;;
    esac
done

DN_LOGS=$(ls $LOGDIR|fgrep datanode|fgrep log);
TT_LOGS=$(ls $LOGDIR|fgrep tasktracker|fgrep log);
FDN_LOG=$(echo $DN_LOGS|awk '{print $1}');
FDN_LOG=$(fgrep -R $JID $LOGDIR|grep HDFS|head -1|awk -F ':' '{print $1}');
echo "FDN_LOG=$FDN_LOG"

#src     task    taskid  hour    min     mm      dd      blockid bytes   jobid   yy      sec     ms      offset  duration        dst     op
KEYS=$(cat $FDN_LOG|grep $JID|grep HDFS|hdfs_traffic2table.py -k);
echo "KEYS = $KEYS"
TYPES="text, integer, integer, integer, integer, integer, integer, text, integer, text, integer, integer, integer, integer, integer, text, text";
#PRIMARY="hour, min, mm, dd, yy, sec, ms"
echo "create table $JID in DB $DB"
txt2sqlite.py -o $DB -n $JID -k "$KEYS" -t "$TYPES"

for DN_LOG in $DN_LOGS; do
	echo "parse $DN_LOG and load information into db"
	cat $LOGDIR/$DN_LOG|grep $JID|grep HDFS|hdfs_traffic2table.py -v|txt2sqlite.py -o $DB -n $JID -k "$KEYS";
done	
for TT_LOG in $TT_LOGS; do
	echo "parse $TT_LOG and load information into db"
	cat $LOGDIR/$TT_LOG|grep $JID|grep MAPRED_SHUFFLE|hdfs_taskTraffic2table.py -v|txt2sqlite.py -o $DB -n $JID -k "$KEYS";
done


