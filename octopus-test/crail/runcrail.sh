#!/bin/bash
for ((i=1; i<=16; i++))
do
	echo round $i
	ssh -t -p 22 10.0.2.82 "rm /mnt/huge/* -rf"
	ssh -t -p 22 10.0.2.83 "rm /mnt/huge/* -rf"
	ssh -t -p 22 10.0.2.87 "rm /mnt/huge/* -rf"
	rm -rf /mnt/huge/*
	sleep 10
	/usr/local/openmpi/bin/mpirun --allow-run-as-root --hostfile mfile -np $i /home/chenyoumin/jdk1.8.0_101/bin/java -classpath .:/home/chenyoumin/crail/jars/crail-client-1.0.jar:/home/chenyoumin/crail/jars/log4j-1.2.17.jar:/home/chenyoumin/crail/jars/slf4j-api-1.7.5.jar:/home/chenyoumin/crail/jars/slf4j-log4j12-1.7.12.jar:/home/chenyoumin/crail/jars/crail-storage-rdma-1.0.jar:/home/chenyoumin/crail/jars/disni-1.0.jar:/home/chenyoumin/crail/jars/darpc-1.0.jar:/home/chenyoumin/crail/jars/crail-rpc-darpc-1.0.jar CrailPerformance 1 1
done
