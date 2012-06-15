#!/bin/bash
#Horizontal partitioning Versus Vertical partitioning

TESTDIR=$(dirname $0);

$TESTDIR/recBase.sh "32 128" "1 2 4 16" "1" "REC_N59" '.59' 'true' 'true' 'false';
#$TESTDIR/recBase.sh "9 27 81 243" "3 9 27 81 243" "1" "REC_N16" '.59' 'true' 'true' 'false';
