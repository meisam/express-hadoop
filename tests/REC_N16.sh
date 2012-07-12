#!/bin/bash
#Horizontal partitioning Versus Vertical partitioning

TESTDIR=$(dirname $0);

#$TESTDIR/recBase.sh "32 64 128 256 512" "1 2 4 8 16 32" "1" "REC_N16" '.16' 'true' 'true' 'true';
$TESTDIR/recBase.sh "16 32 64 128 256" "1 2 4 8 16 32 64 128 256" "1" "REC_N16" '.16' 'false' 'true' 'false';
#$TESTDIR/recBase.sh "32" "1 2 4" "1" "REC_TEST" '.16' 'false' 'true' 'true';
#$TESTDIR/recBase.sh "32" "1 2 4" "1" "REC_TEST" '.16' 'false' 'true' 'false';
#$TESTDIR/recBase.sh "32" "1 2 4" "1" "REC_TEST" '.16' 'false' 'false' 'true';
