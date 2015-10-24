kafka-start / kafka-stop
sudo /usr/local/kafka/bin/kafka-server-start.sh /usr/local/kafka/config/server.properties &

kafka-topics
kafka-topics --create --replication-factor 2 --partitions 4 --topic twitter-json5
/usr/local/kafka/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 2 --partitions 4 --topic my-topic

kafka-topics
/usr/local/kafka/bin/kafka-topics.sh --describe --zookeeper localhost:2181 --topic my-topic

kafka-producer
/usr/local/kafka/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic my-topic

kafka-consumer
/usr/local/kafka/bin/kafka-console-consumer.sh --zookeeper localhost:2181 --from-beginning --topic my-topic
