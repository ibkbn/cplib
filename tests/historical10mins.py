#! /usr/bin/env python3

import websockets
import requests
import logging
import urllib3
import asyncio
import ssl
import json
import sys

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    handlers=[
                        logging.StreamHandler()
                        ]
                    )

# Uverified context is required in order to ignore certificate check
ssl_context = ssl._create_unverified_context()

local_ip = "127.0.0.1:5000"
base_url = f"https://{local_ip}/v1/api"

#Historical data payload:
def create_SMH_req(conID, period, barSize, dataType, dateFormat):
    msg = f"smh+{conID}+" + json.dumps({
        "period": period,
        "bar": barSize,
        "source": dataType,
        "format": dateFormat 
        }) 
    return msg

def unsubscibeHistoricalData(serverID):
    msg = "umh+" + serverID 
    return msg

async def sendMessages(historicalQuery):

    messages = [historicalQuery] 

    async with websockets.connect("wss://" + local_ip + "/v1/api/ws", ssl=ssl_context) as websocket:
        # Session can be initialized here by using the websocket object 

        rst = await websocket.recv()
        jsonData = json.loads(rst.decode())
        
        while True:
            # *Imitates queue* 
            logging.info(f"Messages queue: {messages}")
            if len(messages) != 0:
                currentMsg = messages.pop(0)
            await asyncio.sleep(1)
            await websocket.send(currentMsg)

            rst = await websocket.recv()
            jsonData = json.loads(rst.decode())
            if 'topic' in jsonData.keys():
                
                if 'error' in jsonData.keys() and jsonData['topic'] == 'smh':
                    # Relogin
                    logging.info(jsonData['error'])
                    sys.exit()
                
                if jsonData['topic'].startswith("smh+"):
                    logging.info('Received historical data')
                    serverID = jsonData['serverId']
                    unsubHistMsg = unsubscibeHistoricalData(serverID)
                    messages.append(unsubHistMsg)
                
                logging.info(jsonData.keys())
                if 'error' in jsonData.keys() and jsonData['topic'] == 'umh':
                    logging.error(jsonData['error'])
                    logging.info(messages)
                    logging.info(jsonData)
                    messages.append(historicalQuery)

                if 'message' in jsonData.keys():

                    logging.info(f"message: {jsonData['message']}")
                    sys.exit()

                    if jsonData['message'].startswith('Unsubscribed'):
                        # Safe to add more historical queries to the queue
                        serverId = jsonData['message'].split(' ')[1]
                        logging.info(f'Unsubscribed from historical chart. ServerID: {serverId}')


def testHdrRequest():
    hdr = hdrMsg('265598', period = '5min', barSize='5min', source='trades',
            dateFormat='%o/%c/%h/%l')
    messages = [hdr]
    asyncio.get_event_loop().run_until_complete(sendMessages(messages))

def testSMHrequest():
    smh_req = create_SMH_req(265598, "1d", "1hour", "trades", "%o/%c/%h/%l") 
    messages = [smh_req]
#    asyncio.get_event_loop().run_until_complete(sendMessages(messages))
    loop = asyncio.get_event_loop()
    task = loop.create_task(sendMessages(messages))
    loop.run_until_complete(task)


def main():
    testSMHrequest()

if __name__ == "__main__":
    urllib3.disable_warnings()
    try:
        main()
    except ConnectionRefusedError:
        print(f"Connection refused at {local_ip}, is the gateway running?")
