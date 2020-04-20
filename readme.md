# Kafka Beginner Tutorial

This work follows the Apache Kafka Series - Learn Apache Kafka for Beginners v2 guide on Udemy.

This project takes tweets from Twitter and in real time using a Kafka producer insert them into a Kafka Topic. The Topics are then consumer by a Kafka consumer and inserted into ElasticSearch.

So far only the the project only produces the tweets.

## Requirements
- Java 8
- Twitter API Credentials (if you don't have any you can apply here https://developer.twitter.com/en)

## Setting up Kafka (for mac)
- Download one of the Kafka binaries from https://kafka.apache.org/downloads
- Un-tar the download using `tar -xvf filename`
- add the Kafka bin directory to the PATH
- If you don't want to add the bin directory to the bath you can use `brew install kafka` 
- By default zookeeper will save data to a tmp directory, change this to a permanent directory by changing the value of `dataDir` in `config/zookeeper.properties`
- By default Kafka saves it's data to a tmp directory, change this to a permanent directory by changing the value of `log.dirs` in `config/server.properties` 
 
## Starting Kafka
- Navigate to the Kafka directory
- Start Zookeeper using `zookeeper-server-start config/zookeeper.properties`. If this works it should bind to port `0.0.0.0:2181`
- In a new terminal window start Kafka using `kafka-server-start config/server.properties`

NOTE: If you did not install kafka with brew `zookeeper-server-start`, `kafka-server-start` and any other Kafka commands need a `.sh` at the end. 

## Running the Kafka Producer
- Run Kafka producer by running the the Main in the `tutorial2` package.
- If you want to change the search terms for the tweets being polled change the value for the `terms` variable.