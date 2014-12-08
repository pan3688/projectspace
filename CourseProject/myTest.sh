#!/bin/bash

n="4"

while [ $n -lt 65 ]
do
java -cp ./bin/ mpp.benchmarks.Driver -n $n -d 10000 -w 5000 mpp.benchmarks.QueueBenchmark Transactional -o 5 -i 256 -r 1000 -w 5
echo "------------------------"
n=$[$n+4]
done
