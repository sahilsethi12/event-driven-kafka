./kafka-console-consumer.sh 
--bootstrap-server=my-cluster-kafka-bootstrap.amq-stream.svc.cluster.local:9092 --topic booking-aggregate-topic 
--from-beginning


./kafka-topics.sh --create --topic booking-aggregate-topic-new 
--bootstrap-server=my-cluster-kafka-bootstrap.amq-stream.svc.cluster.local:9092 