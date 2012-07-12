#!/bin/bash

flush() {
	local _NODES=$1;
	echo "clear cache on nodes {${_NODES}}"
	for node in $_NODES; do 
		ssh $node cat /etc/hostname; 
		ssh $node $HOME/exec/sbin/cache-cleanup.sh -f; 
	done
}

