#!/bin/bash
echo $1
if [ -z "$2" ]
  then
     dbIndex="workload_stress_index.csv"
  else
     dbIndex=$2
fi 
echo "generate skydiveFlows${1}.csv jmeter${1}.csv from $dbIndex"
python indexToDBs.py -i $dbIndex -s skydiveFlows${1}.csv -j jmeter${1}.csv
echo "copy to cos skydiveFlows${1}.csv"
rclone copy    skydiveFlows${1}.csv nagin:slicenet.slydive
echo "copy to cos jmeter${1}.csv"
rclone copy    jmeter${1}.csv nagin:slicenet.slydive
cp $dbIndex workload_stress_begin_end_$1.csv
echo "copy to cos workload_stress_index_$1.csv"
rclone copy  workload_stress_begin_end_$1.csv nagin:slicenet.slydive

