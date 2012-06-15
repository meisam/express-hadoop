#!/bin/bash

TESTSDIR=$(dirname $0);

sync_hdf_micro() {
echo "copy hdf_micro.jar"; 
scp ../hdf_micro.jar siyuan@mars.cs.iit.edu:./tmp;
echo "copy hdf_micro.jar from mars:./tmp to hec:/mnt/common/siyuan/src/hadoop-1.0.1";
ssh siyuan@mars.cs.iit.edu eval 'scp ./tmp/hdf_micro.jar siyuan@hec.cs.iit.edu:/mnt/common/siyuan/src/hadoop-1.0.1'; 
ssh siyuan@mars.cs.iit.edu eval 'scp ./tmp/hdf_micro.jar siyuan@hec.cs.iit.edu:/mnt/common/siyuan/src/hadoop-1.0.1/lib'; 
}

sync_core(){
echo "copy hadoop-1.0.1 jar";
scp /cygdrive/g/eclipse/hadoop-1.0.1/build/hadoop-core-1.0.2-SNAPSHOT.jar siyuan@mars.cs.iit.edu:./tmp;
echo "copy hadoop-core.jar from mars:./tmp to hec:/mnt/common/siyuan/src/hadoop-1.0.1";
ssh siyuan@mars.cs.iit.edu eval 'scp ./tmp/hadoop-core-1.0.2-SNAPSHOT.jar siyuan@hec.cs.iit.edu:/mnt/common/siyuan/src/hadoop-1.0.1'; 
}

sync_tests() {
echo "copy tests to mars";
scp -r $TESTSDIR/../tests siyuan@mars.cs.iit.edu:./tmp;
ssh siyuan@mars.cs.iit.edu chmod 777 ./tmp/tests;
echo "copy tests from mars to hec:./tmp";
ssh siyuan@mars.cs.iit.edu eval 'scp -r ./tmp/tests siyuan@hec.cs.iit.edu:./tmp ';
}

sync_tools() {
echo "copy tools to mars";
scp -r $TESTSDIR/../tools siyuan@mars.cs.iit.edu:./tmp;
ssh siyuan@mars.cs.iit.edu chmod 777 ./tmp/tools;
echo "copy tools from mars to hec:./tmp";
ssh siyuan@mars.cs.iit.edu eval 'scp -r ./tmp/tools siyuan@hec.cs.iit.edu:./tmp ';
}

while getopts "achs" OPTION
do
    case $OPTION in
        a)
			sync_hdf_micro;
			sync_core;
			sync_tests;
            sync_tools;
			exit;
            ;;
        c)
          	sync_core;  
            ;;
        h)
            sync_hdf_micro;
            ;;
        s)
			sync_tests;
            sync_tools;
            ;;
        ?)
            echo "unknown parameter!"
			exit;
			;;
    esac
done


