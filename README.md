# save_pending_transactions
subscribes to ethereum ropstein pending transactions and writes to a kafka topic, then saves to cassandra db
run kafka and cassandra 1st

subscribe_pending_transactions.py subscribes to the ethereum ropstein pending transactions via an infura websocket endpoint and writes to a kafka topic
consume_kafka.py subscribes to the kafka topic and writes to cassandra db

To run, run each command below in seperate terminals:
```
bin/zookeeper-server-start.sh config/zookeeper.properties
bin/kafka-server-start.sh config/server.properties 
cassandra
python subscribe_pending_transactions.py
python consume_kafka.py
```

Improvements:
Run script
Dependencies management?
Cassandra connection details
General refactoring
More async
