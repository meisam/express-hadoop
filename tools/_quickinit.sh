NLIST=$1;
STORAGE=$2;
LOGDIR=$3;

echo NLIST=$NLIST;
echo "STORAGE=$STORAGE";
echo "LOGDIR=$LOGDIR";

for node in $NLIST;
	do echo "init node $node";
	ssh $node cat /etc/hostname;
	ssh $node sudo rm -rf ${STORAGE};
	ssh $node sudo mkdir ${STORAGE};
	ssh $node sudo chmod 777 ${STORAGE};
	ssh $node eval 'sudo rm -rf /tmp/*';
done

eval 'sudo rm -rf ${STORAGE}/*';
rm -rf ${LOGDIR};
mkdir ${LOGDIR};
