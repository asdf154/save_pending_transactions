import json
import asyncio
import websockets
import requests
import logging
from kafka import KafkaProducer

wss_infura_url = "wss://ropsten.infura.io/ws/v3/64b0f0c353554b1381d08b60683a6461"
http_infura_url = "https://ropsten.infura.io/v3/64b0f0c353554b1381d08b60683a6461"
# go here for the test websocket server: https://socketsbay.com/test-websockets
# test = "wss://socketsbay.com/wss/v2/2/demo/"
test_tx_hash = "0x195648e785c18e6aa25fdec4feaad37ad9f199d5d74278031559b297709f1462"
test_kafka_topic = "quickstart-events"
bootstrap_servers = "localhost:9092"


# https://stackoverflow.com/questions/10588644/how-can-i-see-the-entire-http-request-thats-being-sent-by-my-python-application
## These two lines enable debugging at httplib level (requests->urllib3->http.client)
## You will see the REQUEST, including HEADERS and DATA, and RESPONSE with HEADERS but without DATA.
## The only thing missing will be the response.body which is not logged.
#try:
#    import http.client as http_client
#except ImportError:
#    # Python 2
#    import httplib as http_client
#http_client.HTTPConnection.debuglevel = 1
#
## You must initialize logging, otherwise you'll not see debug output.
#logging.basicConfig()
## logging.getLogger().setLevel(logging.DEBUG)
#requests_log = logging.getLogger("requests.packages.urllib3")
## requests_log.setLevel(logging.DEBUG)
#requests_log.propagate = True


async def get_tx_details(websocket, tx_hash):
    request_data = '{"jsonrpc":"2.0","method":"eth_getTransactionByHash","params": ["' + tx_hash + '"], "id":1}'
    await websocket.send(request_data)
    raw_tx_details_received = await websocket.recv()
    tx_details_received = json.loads(raw_tx_details_received)
    print("tx_details_received", tx_details_received)
    return tx_details_received


async def main():
    first_message = True
    pending_transactions = '{"jsonrpc":"2.0", "id": 1, "method": "eth_subscribe", "params": ["newPendingTransactions"]}'
    print("trying to get websocket connection")
    producer = KafkaProducer(bootstrap_servers=bootstrap_servers, value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    async with websockets.connect(wss_infura_url) as websocket:
        async with websockets.connect(wss_infura_url) as websocket2:
            print("websocket connected. sending pending_transaction")
            await websocket.send(pending_transactions)
            print("sent")
            while True:
                print("")
                print("")
                print("")
                print("awaiting new pending transaction")
                received = await websocket.recv()
                print("received: ", received)
                json_data = json.loads(received)
                if first_message:
                    first_message = False
                else:
                    tx_hash = json_data["params"]["result"]
                    tx_details = await get_tx_details(websocket2, tx_hash)
                    print("tx_details", tx_details)
                    producer.send(test_kafka_topic, tx_details) #might need to make the payload into bytes
            


if __name__ == "__main__":
    asyncio.run(main())