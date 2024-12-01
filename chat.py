import asyncio
import requests
import websockets
import json
import datetime
import uvicorn
from fastapi import FastAPI
from kafka import KafkaProducer

########################################################################################
## 채팅 API 관련하여 https://github.com/Buddha7771/ChzzkChat 내 코드를 일부 참고하였습니다. ##
########################################################################################

BROKER_URI = "localhost:9092"
HEADERS = {'User-Agent': ''}
CHZZK_CHAT_CMD = {
    'ping'                : 0,
    'pong'                : 10000,
    'connect'             : 100,
    'send_chat'           : 3101,
    'request_recent_chat' : 5101,
    'chat'                : 93101,
    'donation'            : 93102,
}
TEMP_COOKIES = {
    "NID_AUT" : None,
    "NID_SES" : None
}

app = FastAPI()

def fetch_chatChannelId(streamer: str, cookies: dict) -> str:
    url = f'https://api.chzzk.naver.com/polling/v2/channels/{streamer}/live-status'
    try:
        response = requests.get(url, cookies=cookies, headers=HEADERS)
        response.raise_for_status()
        response = response.json()
        
        chatChannelId = response['content']['chatChannelId']
        assert chatChannelId!=None
        return chatChannelId
    except Exception as e:
        raise e

def fetch_accessToken(chatChannelId, cookies: dict) -> str:
    url = f'https://comm-api.game.naver.com/nng_main/v1/chats/access-token?channelId={chatChannelId}&chatType=STREAMING'
    try:
        response = requests.get(url, cookies=cookies, headers=HEADERS)
        response.raise_for_status()
        response = response.json()
        return response['content']['accessToken'], response['content']['extraToken']
    except Exception as e:
        raise e

def fetch_channelName(streamer: str) -> str:
    url = f'https://api.chzzk.naver.com/service/v1/channels/{streamer}'
    try:
        response = requests.get(url, headers=HEADERS)
        response.raise_for_status()
        response = response.json()
        return response['content']['channelName']
    except Exception as e:
        raise e

class MessageProducer:
    broker = ""
    topic = ""
    producer = None

    def __init__(self, broker, topic):
        self.broker = broker
        self.topic = topic
        self.producer = KafkaProducer(bootstrap_servers=self.broker,
                                      value_serializer=lambda x: json.dumps(x).encode('utf-8'),
                                      acks=0,
                                      api_version=(2,5,0),
                                      retries=3
                                      )

    def send_message(self, msg):
        try:
            future = self.producer.send(self.topic, msg)
            self.producer.flush()   # 비우는 작업
            future.get(timeout=60)
            return {'status_code': 200, 'error': None}
        except Exception as e:
            print("error:::::",e)
            return e

class ChatScrapper:
    def __init__(self, streamer : str, producer : MessageProducer):
        self.is_running = True

        self.streamer = streamer
        self.cookies  = TEMP_COOKIES
        # self.logger   = logger
        self.producer = producer

        self.sid           = None
        self.userIdHash    = None
        self.sock          = None
        self.chatChannelId = fetch_chatChannelId(self.streamer, self.cookies)
        self.channelName   = fetch_channelName(self.streamer)
        self.accessToken, self.extraToken = fetch_accessToken(self.chatChannelId, self.cookies)

    async def initialize(self):
        await self.handshake()

    async def handshake(self):
        # self.chatChannelId = api.fetch_chatChannelId(self.streamer, self.cookies)
        self.accessToken, self.extraToken = fetch_accessToken(self.chatChannelId, self.cookies)

        sock = await websockets.connect("wss://kr-ss1.chat.naver.com/chat")
        print(f'{self.channelName} 채팅창에 연결 시도 .', end="")

        default_dict = {  
            "ver"   : "3",
            "svcid" : "game",
            "cid"   : self.chatChannelId,
        }

        send_dict = {
            "cmd"   : CHZZK_CHAT_CMD['connect'],
            "tid"   : 1,
            "bdy"   : {
                "uid"     : self.userIdHash,
                "devType" : 2001,
                "accTkn"  : self.accessToken,
                "auth"    : "READ"
            }
        }

        await sock.send(json.dumps(dict(send_dict, **default_dict)))
        sock_response = json.loads(await sock.recv())
        self.sid = sock_response['bdy']['sid']
        print(f'\r{self.channelName} 채팅창에 연결 중 ..', end="")

        send_dict = {
            "cmd"   : CHZZK_CHAT_CMD['request_recent_chat'],
            "tid"   : 2,
            
            "sid"   : self.sid,
            "bdy"   : {
                "recentMessageCount" : 50
            }
        }

        await sock.send(json.dumps(dict(send_dict, **default_dict)))
        await sock.recv()
        print(f'\r{self.channelName} 채팅창에 연결 중 ...')

        self.sock = sock
        await self.sock.send(
            json.dumps({
                "ver" : "3",
                "cmd" : CHZZK_CHAT_CMD['ping']
            })
        )
        raw_message = await self.sock.recv()
        raw_message = json.loads(raw_message)
        if raw_message['cmd'] == CHZZK_CHAT_CMD['pong']:
            print('연결 완료')
        else:
            raise ValueError('오류 발생')

    async def close(self):
        self.is_running = False
        if self.sock:
            await self.sock.close()
            print(f"{self.channelName} 소켓 연결 종료")

    async def run(self):
        try:
            while self.is_running:
                try:
                    raw_message = await self.sock.recv()
                except KeyboardInterrupt:
                    break
                except websockets.ConnectionClosed:
                    if self.is_running:
                        await self.handshake()
                    continue
                except Exception as e:
                    print(f"Exception: {e}")
                    break

                raw_message = json.loads(raw_message)
                chat_cmd    = raw_message['cmd']
                
                if chat_cmd == CHZZK_CHAT_CMD['ping']:
                    await self.sock.send(
                        json.dumps({
                            "ver" : "2",
                            "cmd" : CHZZK_CHAT_CMD['pong']
                        })
                    )

                    if self.chatChannelId != fetch_chatChannelId(self.streamer, self.cookies): # 방송 시작시 chatChannelId가 달라지는 문제
                        await self.handshake()

                    continue
                
                if chat_cmd == CHZZK_CHAT_CMD['chat']:
                    chat_type = '채팅'

                elif chat_cmd == CHZZK_CHAT_CMD['donation']:
                    chat_type = '후원'
                    continue

                else:
                    continue

                for chat_data in raw_message['bdy']:
                    
                    if chat_data['uid'] == 'anonymous':
                        nickname = '익명의 후원자'

                    else:
                        try:
                            profile_data = json.loads(chat_data['profile'])
                            nickname = profile_data["nickname"]

                            if 'msg' not in chat_data:
                                continue
                        except:
                            continue

                    now = datetime.datetime.fromtimestamp(chat_data['msgTime']/1000)
                    now = datetime.datetime.strftime(now, '%Y-%m-%d %H:%M:%S')

                    print(f'[{now}][{chat_type}] {nickname} : {chat_data["msg"]}')
                    res = self.producer.send_message(chat_data["msg"])   
        finally:
            await self.close()
            
class ChatScrapperManager:
    def __init__(self):
        self.active_scrappers: dict[str, asyncio.Task] = {}

    async def init(self, channel_id: str, scrapper: ChatScrapper):
        await scrapper.initialize()
        task = asyncio.create_task(scrapper.run())
        self.active_scrappers[channel_id] = task

    async def close(self, channel_id: str):
        if channel_id in self.active_scrappers:
            task = self.active_scrappers[channel_id]
            task.cancel()
            try:
                await task  # Task 종료를 기다림
            except asyncio.CancelledError:
                pass
            del self.active_scrappers[channel_id]

scrapper_manager = ChatScrapperManager()

@app.post("/init/{channel_id}")
async def init_channel(channel_id: str):
    topic = 'chat-logger-' + channel_id
    message_producer = MessageProducer(BROKER_URI, topic)
    scrapper = ChatScrapper(channel_id, message_producer)

    await scrapper_manager.init(channel_id=channel_id, scrapper=scrapper)

    return "200 OK: Successfully Initiated"

@app.post("/close/{channel_id}")
async def close_channel(channel_id: str):
    await scrapper_manager.close(channel_id=channel_id)

    return "200 OK: Successfully Closed"

if __name__ == '__main__':
    uvicorn.run("chat:app", reload=True, host="127.0.0.1", port=4464)