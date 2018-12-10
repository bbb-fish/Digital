#!/bin/bash

#so the usage on this is ./loop.sh <file_list>
#and I usually generate the file list with something like this:
#aws s3 ls s3://srdata-lab/PTEF/ | grep "201810" | awk '{print $4}' > file_list

#$1 the file name to be loaded from S3
#$2 the internal ip location of the master node. ex.: ip-172-10-23-32
echo "Install Required Dependencies"
sudo yum -y install pigz lzop

echo "Start Convert $(date)"
cd /mnt
mkdir -p s3
cd s3

echo "Start Download $(date)"
aws s3 cp --quiet s3://srdata-lab/Praveen/iptv/$1 - | pigz -dc | lzop > $1.lzo

echo "Copy to HDFS $(date)"
hadoop fs -mkdir -p /tmp/iptv
hadoop fs -copyFromLocal /mnt/s3/$1.lzo /tmp/iptv/
#hadoop fs -copyFromLocal /mnt/s3/$1.lzo hdfs://$2:8020/tmp/PTEF/$1.lzo
hadoop jar /usr/lib/hadoop-lzo/lib/hadoop-lzo.jar com.hadoop.compression.lzo.LzoIndexer  /tmp/iptv

echo "Begin Conversion $(date)"
aws s3 cp s3://srdata-lab/Bobby/digital_parquet_converter.hql .
hive -f digital_parquet_converter.hql

echo "Cleanup $(date)"
hadoop fs -rm -r /tmp/iptv/

echo "Remove Files from Local $(date)"
rm $1.lzo

echo "Complete $(date)"
