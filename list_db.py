
from json import loads
from cassandra.cluster import Cluster
from constants import *


def subscribe_and_print(session):
    for msg in consumer:
        print ("")
        print (msg)
    
def main():
    prepared_statement_sql="SELECT * FROM " + column_group
    
    cluster = Cluster(cassandra_addresses,port=cassandra_port)
    session = cluster.connect(keyspace, wait_for_all_pools=True)
    lookup_stmt = session.prepare(prepared_statement_sql)
    
    session.execute('USE ' + keyspace)
    rows = session.execute(lookup_stmt)
    number_of_records = 0
    for row in rows:
        number_of_records += 1
        print("")
        print(row)
    print("Retrieved: " + str(number_of_records) + " rows")
        

if __name__ == "__main__":
    main()