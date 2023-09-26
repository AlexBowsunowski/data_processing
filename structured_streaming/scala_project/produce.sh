#!/bin/bash

display_usage() {
  echo "Usage: $0 <KAFKA_BROKER> <KAFKA_TOPIC>"
}

if [ "$#" -ne 2 ]; then
  display_usage
  exit 1
fi

KAFKA_BROKER=$1
KAFKA_TOPIC=$2

STRINGS=("This is an example string"  "The quick brown fox jumps over the lazy dog" "The unique example of word count")

while sleep 1; do
  str=${STRINGS[$RANDOM % ${#STRINGS[@]}]}
  epochSeconds=$(date '+%s')
  echo "{\"ts\":$epochSeconds,\"str\":\"$str\"}" | kafka-console-producer --broker-list ${KAFKA_BROKER} --topic ${KAFKA_TOPIC}
done