#!/bin/bash
# MakePipeFile

source `dirname $0`/../conf/express-env.sh;

r-pipe-w() {
	$EXEC fs -rmr hdf-pipe;
	$TESTDIR/pipeFileBase.sh touchPipe 8 hdf-pipe;
	$EXEC fs -rmr match.r1;
	$TESTDIR/validate.sh pureBM 64,16,8 0,0,0 8,16,8 1,1,1 0,0,0 8,16,8 hdf-pipe match.r1 &
	sleep 16;
	for fid in $(seq 0 7); do
		$TESTDIR/pipeFileBase.sh pureAppendRecord /user/siyuan/hdf-pipe/${fid} $((8*${fid})),0,0 8,16,8;
	done
}

eval "$@"; 