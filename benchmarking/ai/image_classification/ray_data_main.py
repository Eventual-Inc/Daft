from __future__ import annotations

import ray.data
import torch
from PIL import Image
from torchvision import transforms
from torchvision.models import ResNet18_Weights, resnet18

NUM_GPU_NODES = 8
INPUT_PATH = "s3://daft-public-datasets/imagenet/benchmark"
OUTPUT_PATH = "s3://eventual-dev-benchmarking-results/ai-benchmark-results/image-classification-results"
BATCH_SIZE = 100

weights = ResNet18_Weights.DEFAULT
transform = transforms.Compose([transforms.ToTensor(), weights.transforms()])


def transform_image(row):
    row["image"] = Image.fromarray(row["image"]).convert("RGB")
    row["norm_image"] = transform(row["image"])
    return row


class ResNetActor:
    def __init__(self):
        self.weights = weights
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        self.model = resnet18(weights=self.weights).to(self.device)
        self.model.eval()

    def __call__(self, batch):
        torch_batch = torch.from_numpy(batch["norm_image"]).to(self.device)
        with torch.inference_mode():
            prediction = self.model(torch_batch)
            predicted_classes = prediction.argmax(dim=1).detach().cpu()
            predicted_labels = [self.weights.meta["categories"][i] for i in predicted_classes]
            batch["label"] = predicted_labels
            return batch


paths = ray.data.read_parquet(INPUT_PATH).take_all()
paths = [row["image_url"] for row in paths]
ds = (
    ray.data.read_images(paths, include_paths=True, ignore_missing_paths=True)
    .map(fn=transform_image)
    .map_batches(fn=ResNetActor, batch_size=BATCH_SIZE, num_gpus=1.0, concurrency=NUM_GPU_NODES)
    .select_columns(["path", "label"])
)
ds.write_parquet(OUTPUT_PATH)
