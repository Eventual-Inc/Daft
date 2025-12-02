from __future__ import annotations

import time

import ray
import torch
import torchvision
from PIL import Image
from ultralytics import YOLO

import daft
from daft.expressions import col

NUM_GPU_NODES = 8
YOLO_MODEL = "yolo11n.pt"
INPUT_PATH = "s3://daft-public-data/videos/Hollywood2-actions-videos/Hollywood2/AVIClips/"
OUTPUT_PATH = "s3://eventual-dev-benchmarking-results/ai-benchmark-results/video-object-detection-result"
IMAGE_HEIGHT = 640
IMAGE_WIDTH = 640


# Wait for Ray cluster to be ready
@ray.remote
def warmup():
    pass


ray.get([warmup.remote() for _ in range(64)])


@daft.cls(
    max_concurrency=NUM_GPU_NODES,
    gpus=1,
)
class ExtractImageFeatures:
    def __init__(self):
        self.model = YOLO(YOLO_MODEL)
        if torch.cuda.is_available():
            self.model.to("cuda")

    def to_features(self, res):
        return [
            {
                "label": label,
                "confidence": confidence.item(),
                "bbox": bbox.tolist(),
            }
            for label, confidence, bbox in zip(res.names, res.boxes.conf, res.boxes.xyxy)
        ]

    @daft.method.batch(
        return_dtype=daft.DataType.list(
            daft.DataType.struct(
                {
                    "label": daft.DataType.string(),
                    "confidence": daft.DataType.float32(),
                    "bbox": daft.DataType.list(daft.DataType.int32()),
                }
            )
        )
    )
    def __call__(self, images):
        if len(images) == 0:
            return []
        batch = [torchvision.transforms.functional.to_tensor(Image.fromarray(image)) for image in images]
        stack = torch.stack(batch, dim=0)
        return daft.Series.from_pylist([self.to_features(res) for res in self.model(stack)])


daft.set_runner_ray()

daft.set_planning_config(default_io_config=daft.io.IOConfig(s3=daft.io.S3Config.from_env()))
daft.set_execution_config(enable_dynamic_batching=True)

start_time = time.time()

df = daft.from_glob_path(INPUT_PATH)
df = df.with_column("video", daft.functions.video_file(df["path"]))
df = df.with_column("image", daft.functions.video_keyframes(df["video"]).explode())
df = df.with_column("resized", daft.functions.resize(df["image"], w=IMAGE_WIDTH, h=IMAGE_HEIGHT))
df = df.with_column("features", ExtractImageFeatures()(col("resized")).explode())
df = df.with_column("object", daft.col("resized").crop(daft.col("features")["bbox"]).encode_image("png"))
df = df.select("path", "features", "object")
df.write_parquet(OUTPUT_PATH)

end_time = time.time()
print(f"Time taken: {end_time - start_time} seconds")
