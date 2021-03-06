#!/usr/bin/env bash
####################################################################################
#
# usage:
#   ./odsCreateTopic.sh topicName partitionNum
#
####################################################################################
KAFKA_HOME=/opt/kafka3
ZookeeperServer="bigdata-appsvr-130-1:2183,bigdata-appsvr-130-2:2183,bigdata-appsvr-130-3:2183,bigdata-appsvr-130-4:2183,bigdata-appsvr-130-5:2183"

if [ $# -ne 2 ]
then
  echo "usage:
  ./odsCreateTopic.sh topicName partitionNum
example:
  ./odsCreateTopic.sh log-raw-boikgpokn78sb95kjhfrendo 12";
  exit 1;
fi

topic=$1
partitionNum=$2

$KAFKA_HOME/bin/kafka-topics.sh --zookeeper $ZookeeperServer --topic $topic \
 --create --partitions $partitionNum --replication-factor 2