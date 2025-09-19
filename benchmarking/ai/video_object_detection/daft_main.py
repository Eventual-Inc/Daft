from __future__ import annotations

import daft
import torch
import torchvision
from daft.expressions import col
from PIL import Image
from ultralytics import YOLO

NUM_GPU_NODES = 8
YOLO_MODEL = "yolo11n.pt"
INPUT_DATA_PATH = (
    "s3://daft-public-data/videos/Hollywood2-actions-videos/Hollywood2/AVIClips/"
)
OUTPUT_PATH = "s3://desmond-test/colin-test/video-object-detection-result"
IMAGE_HEIGHT = 640
IMAGE_WIDTH = 640
BATCH_SIZE = 100


@daft.udf(
    return_dtype=daft.DataType.list(
        daft.DataType.struct(
            {
                "label": daft.DataType.string(),
                "confidence": daft.DataType.float32(),
                "bbox": daft.DataType.list(daft.DataType.int32()),
            }
        )
    ),
    concurrency=NUM_GPU_NODES,
    num_gpus=1.0,
    batch_size=BATCH_SIZE,
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
            for label, confidence, bbox in zip(
                res.names, res.boxes.conf, res.boxes.xyxy
            )
        ]

    def __call__(self, images):
        if len(images) == 0:
            return []
        batch = [
            torchvision.transforms.functional.to_tensor(Image.fromarray(image))
            for image in images
        ]
        stack = torch.stack(batch, dim=0)
        return daft.Series.from_pylist(
            [self.to_features(res) for res in self.model(stack)]
        )


daft.context.set_runner_ray()

df = daft.read_video_frames(
    INPUT_DATA_PATH,
    image_height=IMAGE_HEIGHT,
    image_width=IMAGE_WIDTH,
)
df = df.with_column("features", ExtractImageFeatures(col("data")))
df = df.explode("features")
df = df.with_column("object", daft.col("data").image.crop(daft.col("features")["bbox"]))
df = df.exclude("data")
df.write_parquet(OUTPUT_PATH)
