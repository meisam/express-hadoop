#!/bin/bash

source `dirname $0`/../conf/.env;

FLIST="$($EXEC fs -ls 2>/dev/null|grep data|awk '{print $8}')";
REPS="1";
PREFIX=REC_N52_LONGW;
LOGDIR=/mnt/common/siyuan/log/rec;

mkdir -p $LOGDIR;

$TESTDIR/recTest.sh "$FLIST" "$REPS" $PREFIX $LOGDIR;

