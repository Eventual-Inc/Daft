from __future__ import annotations

import time

import numpy as np
import ray
import torch
from torchvision import transforms
from torchvision.models import ResNet18_Weights, resnet18

import daft
from daft import col

NUM_GPU_NODES = 8
INPUT_PATH = "s3://daft-public-datasets/imagenet/benchmark"
OUTPUT_PATH = "s3://eventual-dev-benchmarking-results/ai-benchmark-results/image-classification-results"
IMAGE_DIM = (3, 224, 224)

daft.set_runner_ray()


# Wait for Ray cluster to be ready
@ray.remote
def warmup():
    pass


ray.get([warmup.remote() for _ in range(64)])

weights = ResNet18_Weights.DEFAULT
transformer = transforms.Compose([transforms.ToTensor(), weights.transforms()])


@daft.func(return_dtype=daft.DataType.tensor(dtype=daft.DataType.float32(), shape=IMAGE_DIM))
def transform(input):
    return transformer(input)


@daft.cls(
    max_concurrency=NUM_GPU_NODES,
    gpus=1,
)
class ResNetModel:
    def __init__(self):
        self.weights = weights
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        self.model = resnet18(weights=weights).to(self.device)
        self.model.eval()

    @daft.method.batch(
        return_dtype=daft.DataType.string(),
    )
    def __call__(self, images):
        if len(images) == 0:
            return []
        torch_batch = torch.from_numpy(np.array(images.to_pylist())).to(self.device)
        with torch.inference_mode():
            prediction = self.model(torch_batch)
            predicted_classes = prediction.argmax(dim=1).detach().cpu()
            predicted_labels = [self.weights.meta["categories"][i] for i in predicted_classes]
            return predicted_labels


daft.set_planning_config(default_io_config=daft.io.IOConfig(s3=daft.io.S3Config.from_env()))
daft.set_execution_config(enable_dynamic_batching=True)

start_time = time.time()

df = daft.read_parquet(INPUT_PATH)
df = df.with_column(
    "decoded_image",
    df["image_url"].download().decode_image(mode=daft.ImageMode.RGB),
)
df = df.with_column("norm_image", transform(df["decoded_image"]))
df = df.with_column("label", ResNetModel()(col("norm_image")))
df = df.select("image_url", "label")
df.write_parquet(OUTPUT_PATH)

end_time = time.time()
print(f"Time taken: {end_time - start_time} seconds")
