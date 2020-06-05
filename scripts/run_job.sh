#!/usr/bin/env bash

# Set the spark submit
spark=/home/dan/data/software/spark-2.4.5-bin-hadoop2.7/bin/spark-submit
rumble=/home/dan/data/software/rumble/spark-rumble-1.6.3.jar
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export PATH=$JAVA_HOME/bin:$PATH

# Run the job
if [[ $# -ne 1 ]]
then
	echo "Usage: run_job.sh <rumble_source>"
	echo "Running Rumble in interactive mode"
	$spark $rumble --shell yes	
else
	echo "Submitting Ruble query located at $1"
	$spark $rumble --query-path $1 # --output-path "hdfs:///user/me/results/output" --log-path "hdfs:///user/me/logging/mylog"
fi


