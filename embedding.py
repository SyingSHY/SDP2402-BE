import uvicorn
import onnxruntime
import numpy as np
from typing import List
from pydantic import BaseModel
from fastapi import FastAPI
from transformers import AutoTokenizer

MODEL_REPO_URI = "./../paraphrase-multilingual-MiniLM-L12-v2"
ONNX_MODEL_URI = "./../paraphrase-multilingual-MiniLM-L12-v2/onnx/model.onnx"

app = FastAPI()
session = onnxruntime.InferenceSession(ONNX_MODEL_URI)
tokenizer = AutoTokenizer.from_pretrained(MODEL_REPO_URI)

class EmbeddingRequest(BaseModel):
    texts: List[str]

# Mean Pooling - Take attention mask into account for correct averaging
def mean_pooling(embeddings, attention_mask):
    input_mask_expanded = attention_mask[:, :, np.newaxis]
    pooled_embeddings = np.sum(embeddings * input_mask_expanded, axis=1)
    attention_mask_sum = np.clip(np.sum(input_mask_expanded, axis=1), a_min=1e-9, a_max=None)
    return pooled_embeddings / attention_mask_sum


@app.post("/embedding")
async def run_embedding(request: EmbeddingRequest):
    texts = request.texts

    inputs = tokenizer(
        texts,
        padding=True,
        truncation=True,
        return_tensors="np",
    )

    onnx_inputs = {
        input_key.name: inputs[input_key.name] 
        for input_key in session.get_inputs()
        if input_key.name in inputs
    }
    onnx_outputs = session.run(None, onnx_inputs)

    embeddings = mean_pooling(onnx_outputs[0], inputs['attention_mask'])

    print(np.array(onnx_outputs).shape)
    print(np.array(embeddings).shape)

    # JSON 형식으로 변환
    embeddings_dict = {
        "texts": texts,
        "embeddings": embeddings.tolist(),  # numpy 배열을 리스트로 변환
    }

    return embeddings_dict

if __name__ == "__main__" :
    uvicorn.run("embedding:app", host="127.0.0.1", port=4484)