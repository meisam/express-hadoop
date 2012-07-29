#!/bin/bash


source `dirname $0`/../conf/express-env.sh;

FLIST="$($EXEC fs -ls 2>/dev/null|grep data|awk '{print $8}')";
REPS="1";
PREFIX=REC_N16;
LOGDIR=/mnt/common/siyuan/log/rec;

mkdir -p $LOGDIR;

$TESTDIR/recTest.sh "$FLIST" "$REPS" $PREFIX $LOGDIR;

