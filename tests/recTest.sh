#!/bin/bash

source `dirname $0`/../conf/express-env.sh;

FLIST=$1;
REPS=$2;
PREFIX=$3;
LOGDIR=$4;

echo "FLIST=$1";
echo "REPS=$2";
echo "PREFIX=$3";
echo "LOGDIR=$4";

NODES=$(cat $CONF);

flush() {
    local _NODES=$1;
    echo "clear cache on nodes {${_NODES}}"
    for node in $_NODES; do
        ssh $node cat /etc/hostname;
        ssh $node $HOME/exec/sbin/cache-cleanup.sh -f;
    done
}

TMPDIR=/home/tmp;

for rep in $REPS; do
	for file in $FLIST; do
		$EXEC dfs -setrep -R -w $rep $file;
	done

	flush "${NODES}";
	res=$LOGDIR/$PREFIX.r$rep;
	touch $res;
	for file in $FLIST; do 
		echo $file >> $res; 
		echo "/usr/bin/time --append -o res $EXEC fs -copyToLocal $file /mnt/common/siyuan/tmp "; 
#/usr/bin/time --append -o $res $EXEC fs -copyToLocal $file /mnt/common/siyuan/tmp ; 
#/usr/bin/time --append -o $res $TESTDIR/cp2local.sh $file /mnt/common/siyuan/tmp; 
		/usr/bin/time --append -o $res $TESTDIR/cp2local.sh $file $TMPDIR; 
		#eval 'rm -rf /mnt/common/siyuan/tmp/*';
		eval 'rm -rf /home/tmp/*';
		$HOME/exec/sbin/cache-cleanup.sh -f;
		sleep 64;
	done
done
