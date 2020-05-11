#!/usr/bin/env bash

# Set the spark submit
spark=/home/dan/data/software/spark-2.4.5-bin-hadoop2.7/bin/spark-submit
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export PATH=$JAVA_HOME/bin:$PATH

# Run the job
if [[ $# -ne 1 ]]
then
	echo "Usage: run_job.sh <rumble_source>"
	echo "Running Rumble in interactive mode"
	$spark spark-rumble-1.5.jar --shell yes	
else
	$spark spark-rumble-1.5.jar --query-path $1 # --output-path "hdfs:///user/me/results/output" --log-path "hdfs:///user/me/logging/mylog"
fi


