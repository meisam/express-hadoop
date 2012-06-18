ST=$1;
END=$2;
STORAGE="/home/siyuan_hadoop10";
NODE_PREFIX="hec-";
LOGDIR="/mnt/common/siyuan/src/hadoop-1.0.1/logs/";

for i in $(seq $ST $END); do
	ssh ${NODE_PREFIX}$i cat /etc/hostname;
	ssh ${NODE_PREFIX}$i sudo rm -rf ${STORAGE};
	ssh ${NODE_PREFIX}$i sudo mkdir ${STORAGE};
	ssh ${NODE_PREFIX}$i sudo chmod 777 ${STORAGE};
	ssh ${NODE_PREFIX}$i eval 'sudo rm -rf /tmp/*';
done

eval 'sudo rm -rf ${STORAGE}/*';
rm -rf ${LOGDIR};
mkdir ${LOGDIR};
hadoop namenode -format;
