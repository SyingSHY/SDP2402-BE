import numpy as np
import json
import re
import time
import requests
import base64
import asyncio
import uvicorn
import matplotlib.pyplot as plt
from fastapi import FastAPI
from kafka import KafkaConsumer
from sklearn.decomposition import PCA
from sklearn.metrics import pairwise_distances_argmin_min
from sklearn.cluster import AgglomerativeClustering, DBSCAN

MAIN_SERVER_URI = "http://127.0.0.1:4400"
MAIN_SERVER_API_TTS = "/tts/google"
BOOTSTRAP_SERVERS_URI = 'localhost:9092'
EMBEDDING_MODULE_URI = "http://localhost:4484/embedding"

app = FastAPI()

# 유효하지 않은 서로게이트 코드 포인트 제거 함수
def remove_invalid_unicode_characters(s):
    # 유니코드 서로게이트 범위에 해당하는 문자를 제거
    return re.sub(r'[\ud800-\udfff]', '', s)

def plot_clusters(X_pca, labels, largest_cluster_indices, center_sentence_index, filename='cluster_plot.png'):
    plt.figure(figsize=(8, 6))
    
    # 클러스터들 시각화
    for i, label in enumerate(np.unique(labels)):
        cluster_points = X_pca[labels == label]
        plt.scatter(cluster_points[:, 0], cluster_points[:, 1], 
                    c=colors[label], label=f'Cluster {label}', alpha=0.6)
    
    # 가장 큰 클러스터와 중심 문장 시각화
    largest_cluster_points = X_pca[largest_cluster_indices]
    plt.scatter(largest_cluster_points[:, 0], largest_cluster_points[:, 1], 
                c='black', label='Largest Cluster', edgecolor='k', s=100)
    
    # 중심 문장 표시
    center_point = X_pca[center_sentence_index]
    plt.scatter(center_point[0], center_point[1], c='yellow', 
                edgecolor='red', s=300, marker='*', label='Cluster Center')
    
    plt.title('Clustering of Sentences with Largest Cluster Center')
    plt.xlabel('PCA Component 1')
    plt.ylabel('PCA Component 2')
    plt.legend()
    plt.grid(True)

    # 4. 이미지 저장 (덮어쓰기 방식)
    plt.savefig(filename)
    plt.close()

def encode_image_to_base64(image_path: str) -> str:
    with open(image_path, "rb") as image_file:
        encoded_string = base64.b64encode(image_file.read()).decode('utf-8')
    return encoded_string

class MessageConsumer:
    topic = ""
    consumer = None

    def __init__(self, topic, bootstrap_servers):
        self.topic = topic
        self.consumer = KafkaConsumer(
            self.topic,
            bootstrap_servers=[bootstrap_servers],
            auto_offset_reset='latest',
            enable_auto_commit=True,
            group_id='my-group'
        )
    
    async def read(self):
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(None, self.consumer.poll, 1000)
        return result

class ClusteringModule:
    def __init__(self, channel_id: str, consumer: MessageConsumer):
        self.is_running = True

        self.channel_id = channel_id
        self.consumer = consumer

    async def run(self):
        # 최초 실행 시 메시지 읽어서 지우기
        while True:
            message = await self.consumer.read()  # 1초 대기하며 메시지 읽기
            if not message:
                break  # 더 이상 메시지가 없으면 종료
            
        try:
            while self.is_running:
                # 메시지들을 저장할 리스트
                messages = []

                # 메시지 읽기
                while self.is_running:
                    message = await self.consumer.read()  # 1초 대기하며 메시지 읽기
                    if not message and len(messages) > 0:
                        break  # 더 이상 메시지가 없으면 종료

                    for _, records in message.items():
                        for record in records:
                            # 메시지를 UTF-8로 디코딩한 후 Unicode escape 형식 처리
                            try:
                                decoded_message = record.value.decode('utf-8', 'replace').encode().decode('unicode_escape')
                            except UnicodeDecodeError as e:
                                print(f"UnicodeDecodeError occurred: {e}")
                                decoded_message = record.value.decode('utf-8', errors='ignore')  # unicode_escape 적용 없이 사용

                            try:
                                print(f"Consumed new message: {decoded_message}")
                            except UnicodeEncodeError as e:
                                print(f"UnicodeEncodeError occurred: {e}")
                                continue

                            messages.append(decoded_message)

                # 현 시점까지 Pub된 모든 메시지를 Sub 후 처리
                if len(messages) > 3:
                    print("-------------------")
                    start_time_all = time.perf_counter()

                    ##### 임베딩 단계 #####
                    start_time_embedding = time.perf_counter()

                    input_json = {
                        "texts" : list(messages)
                    }

                    try:
                        response = requests.post(EMBEDDING_MODULE_URI, json=input_json)
                    except:
                        continue
                    embeddings = response.json()["embeddings"]
                    embeddings = np.array(embeddings)

                    end_time_embedding = time.perf_counter()
                    elapsed_time_embedding = end_time_embedding - start_time_embedding
                    elapsed_time_embedding_ms = elapsed_time_embedding * 1000

                    ##### 클러스터링 단계 #####
                    start_time_clustering = time.perf_counter()

                    # 계층적 클러스터링 (Agglomerative Clustering)
                    agg_clustering = AgglomerativeClustering(n_clusters=3)
                    agg_labels = agg_clustering.fit_predict(embeddings)

                    # DBSCAN 클러스터링
                    # dbscan = DBSCAN(eps=0.5, min_samples=2, metric='euclidean')
                    # dbscan_labels = dbscan.fit_predict(embeddings)

                    # 가장 큰 클러스터 찾기
                    unique, counts = np.unique(agg_labels, return_counts=True)
                    largest_cluster = unique[np.argmax(counts)]  # 가장 큰 클러스터 라벨

                    # 가장 큰 클러스터의 문장 인덱스
                    largest_cluster_indices = np.where(agg_labels == largest_cluster)[0]

                    # 클러스터의 중심과 각 문장의 거리 계산
                    cluster_sentences_vectors = embeddings[largest_cluster_indices]  # 해당 클러스터에 속하는 문장들의 벡터
                    cluster_center = cluster_sentences_vectors.mean(axis=0)  # 클러스터 중심

                    # 중심에 가장 가까운 문장 찾기
                    closest, _ = pairwise_distances_argmin_min([cluster_center], cluster_sentences_vectors)

                    # 가장 큰 클러스터에서 가장 중심에 있는 문장 출력
                    center_sentence_index = largest_cluster_indices[closest[0]]
                    center_sentence = messages[center_sentence_index]

                    end_time_clustering = time.perf_counter()
                    elapsed_time_clustering = end_time_clustering - start_time_clustering
                    elapsed_time_clustering_ms = elapsed_time_clustering * 1000

                    try:
                        # 1. PCA를 사용해 2차원으로 차원 축소
                        pca = PCA(n_components=min(len(embeddings), 2))
                        X_pca = pca.fit_transform(embeddings)

                        # 2. 클러스터별로 색상 정의
                        colors = ['red', 'green', 'blue', 'orange', 'purple', 'cyan', 'yellow']
                        cluster_colors = [colors[label] for label in agg_labels]

                        plot_clusters(X_pca, agg_labels, largest_cluster_indices, center_sentence_index)
                    except:
                        print("[ERROR] 클러스터링 시각화 중 오류 발생")
                        pass

                    end_time_all = time.perf_counter()
                    elapsed_time_all = end_time_all - start_time_all
                    elapsed_time_all_ms = elapsed_time_all * 1000

                    # 결과 출력
                    print(f"{len(messages)}개의 메시지에 대한")
                    print("Agglomerative Clustering 결과:")
                    print(agg_labels)
                    print(f"가장 큰 클러스터: {largest_cluster}")
                    print(f"가장 중심에 있는 문장: {center_sentence}")
                    print(f"처리 소요 시간: {elapsed_time_all_ms:.2f} ms\n")

                    cluster_data = {'channel_id': self.channel_id, 'msg_text': center_sentence, 'image_data': encode_image_to_base64("cluster_plot.png"), 'texts': messages}
                    try:
                        res = requests.post(MAIN_SERVER_URI+MAIN_SERVER_API_TTS, data=json.dumps(cluster_data))
                    except:
                        continue

                    res_data = res.json()
                    await asyncio.sleep(res_data["audio_length_seconds"])

                    messages.clear()
                    continue
                else:
                                        # 3개 이하 메시지로 인해 가장 최근 메시지를 TTS 변환
                    print("-------------------")
                    cluster_data = {'channel_id': self.channel_id, 'msg_text': messages[-1], 'image_data': encode_image_to_base64("cluster_skipped.png"), 'texts': messages}
                    try:
                        res = requests.post(MAIN_SERVER_URI+MAIN_SERVER_API_TTS, data=json.dumps(cluster_data))
                    except:
                        continue

                    print(f"{len(messages)}개의 메시지 만이 프레임에 존재하여 최후 메시지 출력")
                    print(messages[-1])

                    res_data = res.json()
                    await asyncio.sleep(res_data["audio_length_seconds"])

                    messages.clear()
                    continue
        except asyncio.CancelledError:
            print(f"ClusteringModule {self.channel_id} canceled.")
        #     self.consumer.consumer.close()
        # finally:
        #     self.consumer.consumer.close()
        #     await self.close()
    # async def close(self):
    #     self.is_running = False
    #     if self.consumer:
    #         self.consumer.consumer.close()


class ClusteringManager:
    def __init__(self):
        self.active_clusterings: dict[str, asyncio.Task] = {}

    async def init(self, channel_id: str, clustering: ClusteringModule):
        if channel_id not in self.active_clusterings:
            task = asyncio.create_task(clustering.run())
            self.active_clusterings[channel_id] = task

    async def close(self, channel_id: str):
        if channel_id in self.active_clusterings:
            task = self.active_clusterings[channel_id]
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            del self.active_clusterings[channel_id]

clustering_manager = ClusteringManager()

@app.post("/init/{channel_id}")
async def init_channel(channel_id: str):
    if channel_id not in clustering_manager.active_clusterings:
        topic = 'chat-logger-' + channel_id
        message_consumer = MessageConsumer(topic=topic, bootstrap_servers=BOOTSTRAP_SERVERS_URI)
        clustering = ClusteringModule(channel_id, message_consumer)

        await clustering_manager.init(channel_id=channel_id, clustering=clustering)
    print("Reconnection!")
    return "200 OK: Successfully Initiated"

@app.post("/close/{channel_id}")
async def close_channel(channel_id: str):
    # await clustering_manager.close(channel_id=channel_id)

    return "200 OK: Successfully Closed"

if __name__ == '__main__':
    uvicorn.run("cluster:app", reload=True, host="127.0.0.1", port=4474)
