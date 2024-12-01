import io
import json
import wave
import base64
import uvicorn
import requests
import google.cloud.texttospeech as tts
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, BackgroundTasks
from pydantic import BaseModel

SERVER_URI_WS="ws://127.0.0.1:4400"
API_KEY_STRING=""
PROJECT_ID=""
VOICE_NAME="ko-KR-Standard-A"
CHAT_CRAWLER_MODULE_URI = "http://127.0.0.1:4464"
CLUSTERING_MODULE_URI = "http://127.0.0.1:4474"

HEADERS = {'User-Agent': ''}

class TTSRequest(BaseModel):
    channel_id: str
    msg_text: str
    image_data: str
    texts: list[str]

class ConnectionManager:
    def __init__(self):
        self.active_connections: dict[str, list[WebSocket]] = {}

    async def connect(self, websocket: WebSocket, channel_id: str):
        await websocket.accept()
        if channel_id not in self.active_connections:
            self.active_connections[channel_id] = []
        self.active_connections[channel_id].append(websocket)

    def disconnect(self, websocket: WebSocket, channel_id: str):
        try:
            # 연결이 이미 존재하는지 확인
            if channel_id in self.active_connections and websocket in self.active_connections[channel_id]:
                self.active_connections[channel_id].remove(websocket)
                print(f"웹소켓 {websocket.client} 제거 완료. 남은 연결: {len(self.active_connections[channel_id])}")
                
                # 연결 리스트가 비어있으면 삭제
                if not self.active_connections[channel_id]:
                    del self.active_connections[channel_id]
                    print(f"채널 {channel_id}의 모든 연결이 종료됨. 리소스 정리 중...")
                    try:
                        requests.post(CHAT_CRAWLER_MODULE_URI + "/close/" + channel_id)
                        # requests.post(CLUSTERING_MODULE_URI + "/close/" + channel_id)
                        channel_manager.channel_on_going.remove(channel_id)
                    except requests.RequestException as e:
                        print(f"[ERROR] 채널 {channel_id} 종료 중 오류 발생: {e}")
        except ValueError as e:
            print(f"[WARNING] 웹소켓 제거 실패: {websocket}. {e}")
        except KeyError as e:
            print(f"[WARNING] 채널 {channel_id}가 이미 삭제됨: {e}")

    async def send_audio(self, channel_id: str, data: bytes):
        identifier = b'\x01'
        connections = self.active_connections.get(channel_id, [])
        for connection in connections:
            await connection.send_bytes(identifier + data)

    async def send_image(self, channel_id: str, data: bytes):
        identifier = b'\x02'
        connections = self.active_connections.get(channel_id, [])
        for connection in connections:
            await connection.send_bytes(identifier + data)

    async def send_json(self, channel_id: str, json: str):
        connections = self.active_connections.get(channel_id, [])
        for connection in connections:
            await connection.send_json(json)

class ChannelStateManager:
    def __init__(self) -> None:
        self.channel_in_ready: list[str] = []
        self.channel_on_going: list[str] = []

    def is_in_ready(self, channel_id: str):
        if channel_id in self.channel_in_ready:
            return True
        else:
            return False
        
    def is_on_going(self, channel_id: str):
        if channel_id in self.channel_on_going:
            return True
        else:
            return False

app = FastAPI()
connection_manager = ConnectionManager()
channel_manager = ChannelStateManager()

def fetch_channelName(streamer: str) -> str:
    url = f'https://api.chzzk.naver.com/service/v1/channels/{streamer}'
    try:
        response = requests.get(url, headers=HEADERS)
        response.raise_for_status()
        response = response.json()
        return response['content']['channelName']
    except Exception as e:
        raise e

async def tts_with_google(voice_name: str, msg: str):
    try:
    	# Client 생성
        client = tts.TextToSpeechClient(client_options={"api_key": API_KEY_STRING, "quota_project_id": PROJECT_ID})
		
        # Voice 파라미터 적용
        language_code = "-".join(voice_name.split("-")[:2])
        text_input = tts.SynthesisInput(text=msg)
        voice_params = tts.VoiceSelectionParams(
            language_code=language_code, name=voice_name
        )
        audio_config = tts.AudioConfig(audio_encoding=tts.AudioEncoding.LINEAR16)
		
        request = tts.SynthesizeSpeechRequest(
            input=text_input,
            voice=voice_params,
            audio_config=audio_config,
        )

        # TTS 생성
        response = client.synthesize_speech(request=request)
        audio_content = response.audio_content

        # 오디오 재생
        # audio_array = np.frombuffer(audio_content, dtype=np.int16)
        # sd.play(audio_array, samplerate=24000)
        # sd.wait()

        if (audio_content is None):
            print(f"[ERROR] {audio_content}가 None입니다.")
            pass

        with wave.open("output_audio.wav", 'wb') as f:
            # WAV 파일 설정 (1 채널, 샘플링 레이트 24000, 2 바이트 샘플)
            f.setnchannels(1)
            f.setsampwidth(2)
            f.setframerate(24000)
            f.writeframes(audio_content)

        return audio_content
    
    except Exception as e:
        print("Google TTS Error: ", e.args)

def get_audio_length(audio_data: bytes) -> float:
    with io.BytesIO() as audio_file:
        # WAV 파일 헤더를 설정
        with wave.open(audio_file, 'wb') as wav_file:
            wav_file.setnchannels(1)  # 모노
            wav_file.setsampwidth(2)  # 샘플 크기 2바이트 (16비트 PCM)
            wav_file.setframerate(24000)  # 샘플링 레이트
            wav_file.writeframes(audio_data)  # 실제 오디오 데이터를 작성

        # 헤더가 추가된 파일을 읽어서 길이 계산
        audio_file.seek(0)  # 파일 포인터를 처음으로 이동
        with wave.open(audio_file, "rb") as wav_file:
            frames = wav_file.getnframes()
            rate = wav_file.getframerate()
            duration = frames / float(rate)
    return duration

async def run_new_channel(channel_id: str):
    channel_manager.channel_in_ready.append(channel_id)

    print(f"{channel_id} 준비 시작")

    result = requests.post(CHAT_CRAWLER_MODULE_URI + "/init/" + channel_id)
    print(result.text)
    if (result.text != '"200 OK: Successfully Initiated"'):
        return
    
    result = requests.post(CLUSTERING_MODULE_URI + "/init/" + channel_id)
    print(result.text)
    if (result.text != '"200 OK: Successfully Initiated"'):
        return

    print(f"{channel_id} 준비 완료")

    channel_manager.channel_in_ready.remove(channel_id)
    channel_manager.channel_on_going.append(channel_id)

@app.get("/connect/{channel_id}")
async def connect_service(channel_id: str, bg_tasks: BackgroundTasks):
    if channel_id in channel_manager.channel_in_ready:
        return ""
    if channel_id in channel_manager.channel_on_going:
        return SERVER_URI_WS + "/chat/" + channel_id
    
    bg_tasks.add_task(run_new_channel, channel_id)
    return ""

@app.get("/name/{channel_id}")
async def get_channel_name(channel_id:str):
    return fetch_channelName(channel_id)

@app.post("/tts/google")
async def internal_voice(request: TTSRequest):
    try:
        if request.channel_id in connection_manager.active_connections:
            audio_content = await tts_with_google(VOICE_NAME, request.msg_text)
            audio_length = get_audio_length(audio_content)

            await connection_manager.send_audio(request.channel_id, audio_content)
            await connection_manager.send_image(request.channel_id, base64.b64decode(request.image_data))
            await connection_manager.send_json(request.channel_id, json.dumps({'top_chat': request.msg_text, 'texts': request.texts}))

            print(f"TTS 완료 : 최소 {audio_length} 뒤 다음 클러스터링 시작")
            return {"audio_length_seconds": audio_length}
    finally:
        return {"audio_length_seconds": 0}

@app.websocket("/chat/{channel_id}")
async def websocket_endpoint(websocket: WebSocket, channel_id: str):
    print(f"소켓 연결 요청 확인됨 : {channel_id} - {websocket.client}")
    await connection_manager.connect(websocket, channel_id)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        print(f"웹소켓 연결 해제 요청 확인됨: {channel_id} - {websocket.client}")
    finally:
        connection_manager.disconnect(websocket, channel_id)
        try:
            await websocket.close()
        except Exception as e:
            print(f"[WARNING] 웹소켓 종료 중 오류 발생: {e}")

if __name__ == "__main__" :
	uvicorn.run("api:app", reload=True, host="0.0.0.0", port=4400)