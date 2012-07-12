#!/bin/bash

TESTDIR=$(dirname $0);
source $TESTDIR/env.sh;

FLIST="$($EXEC fs -ls 2>/dev/null|grep data|awk '{print $8}')";
REPS="1";
PREFIX=REC_N59_LONGW;
LOGDIR=/mnt/common/siyuan/log/rec;

mkdir -p $LOGDIR;

$TESTDIR/recTest.sh "$FLIST" "$REPS" $PREFIX $LOGDIR;

