from kafka import KafkaConsumer
from json import loads
from cassandra.cluster import Cluster
from datetime import datetime
from constants import *


consumer = KafkaConsumer(kafka_topic, value_deserializer=lambda x: loads(x.decode('utf-8')))
print("subscribed to", kafka_topic)


def event_handler(session):
    print("event_handler started")
    session.execute('USE ' + keyspace)
    for msg in consumer:
        print ("")
        print ("")
        print ("")
        print ("msg:")
        print (msg)
        try:
            tx_data = msg.value["result"]
            print ("\ntx_data:")
            print (msg.value["result"])
            tx_hash = tx_data["hash"]
            session.execute_async("insert into "+column_group+"(tx_id, tx_data, last_update_timestamp) values (%s, %s, %s)", (str(tx_hash), str(tx_data), datetime.now()))
        except Exception as e:
            print("error:", e)
        
        
def setup_db(session, keyspace, column_group):
    a = session.execute("CREATE KEYSPACE IF NOT EXISTS "+keyspace+" WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : '1' }")
    b = session.execute("CREATE TABLE IF NOT EXISTS "+keyspace+"."+column_group+" (tx_id text PRIMARY KEY, tx_data text, last_update_timestamp timestamp);")
    print("database set up")
    
def main():
    cluster = Cluster(cassandra_addresses,port=cassandra_port)
    session = cluster.connect('', wait_for_all_pools=True)
    setup_db(session, keyspace, column_group)
    
    session.execute('USE ' + keyspace)
    event_handler(session)
        

if __name__ == "__main__":
    main()