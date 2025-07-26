from __future__ import annotations

import time
import warnings

import numpy as np
import torch
from torchvision import transforms
from torchvision.models import ResNet50_Weights, resnet50

import daft

warnings.filterwarnings("ignore")


@daft.udf(
    return_dtype=daft.DataType.list(dtype=daft.DataType.float32()),
    batch_size=64,
)
class ResNetModel:
    def __init__(self):
        # print(
        #     "python version, torch version, cuda version, cuda available",
        #     sys.version,
        #     torch.__version__,
        #     torch.version.cuda,
        #     torch.cuda.is_available(),
        # )
        weights = ResNet50_Weights.DEFAULT
        self.model = resnet50(weights=weights)
        # self.model.compile()
        self.model.eval()
        self.counter = 0
        print("Model compiled")

    def __call__(self, images):
        self.counter += len(images)
        # print(f"Processing batch of {len(images)} images, total processed: {self.counter}")
        if len(images) == 0:
            # print("Empty batch received, returning empty list")
            return []
        with torch.inference_mode():
            start_time = time.time()
            # print("Converting images to tensor")
            tensor = torch.as_tensor(np.array(images.to_pylist()))
            end_time = time.time()
            print(f"Time taken to convert images to tensor: {end_time - start_time}")
            # print(f"Tensor shape: {tensor.shape}")
            # print("Running inference")
            start_time = time.time()
            result = self.model(tensor)
            end_time = time.time()
            print(f"Time taken to run inference: {end_time - start_time}")
            # print("Moving result to CPU")
            start_time = time.time()
            cpu_result = result.cpu()
            end_time = time.time()
            print(f"Time taken to move result to CPU: {end_time - start_time}")
            # print("Converting to numpy")
            start_time = time.time()
            numpy_result = cpu_result.numpy()
            end_time = time.time()
            print(f"Time taken to convert to numpy: {end_time - start_time}")
            # print(f"Final result shape: {numpy_result.shape}")
            return numpy_result


start_time = time.time()
df = daft.read_parquet("s3://desmond-test/colin-test/imagenet_100_rows/").limit(10)
df = df.with_column(
    "image",
    df["image"].image.decode(mode=daft.ImageMode.RGB),
)

transform = transforms.Compose(
    [
        transforms.ToTensor(),
        transforms.Resize(256),
        transforms.CenterCrop(224),
        transforms.Normalize(mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225]),
    ]
)

df = df.with_column(
    "norm_image",
    df["image"].apply(
        lambda x: transform(x),
        return_dtype=daft.DataType.tensor(dtype=daft.DataType.float32(), shape=(3, 224, 224)),
    ),
).exclude("image")

# df = df.with_column("result", ResNetModel(col("norm_image"))).exclude("norm_image")
df = df.write_lance(
    "embed.lance",
    mode="overwrite",
)
print(df)
