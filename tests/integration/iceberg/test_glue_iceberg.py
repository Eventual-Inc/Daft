from __future__ import annotations

import random
import uuid

import pandas as pd
import pytest
import torch
import torchvision.models as models
from boto3 import session
from torch.utils.data import DataLoader

import daft
from daft.catalog.__glue import GlueCatalog


@pytest.mark.integration()
def test_dataloading_from_glue_iceberg(pytestconfig):
    if pytestconfig.getoption("--credentials") is not True:
        pytest.skip("Test can only run in a credentialled environment, and when run with the `--credentials` flag")

    sess = session.Session()
    catalog = GlueCatalog.from_session(name="glue_catalog", session=sess)

    images_table = catalog.get_table("glue_iceberg_test.coco_images")
    images_df = images_table.read()  # noqa: F841

    categories_table = catalog.get_table("glue_iceberg_test.coco_categories")
    categories_df = categories_table.read().with_column_renamed("id", "category_id")  # noqa: F841

    annotations_table = catalog.get_table("glue_iceberg_test.coco_annotations")
    annotations_df = annotations_table.read().with_column_renamed("id", "annotation_id")  # noqa: F841

    df = daft.sql("""
        SELECT
            id, annotation_id, bbox, bucket_url
        FROM
            images_df
        JOIN
            annotations_df ON id = image_id
        JOIN
            categories_df USING (category_id)
        WHERE
            supercategory = 'person'
    """)
    df = df.with_column("image", df["bucket_url"].url.download().image.decode())
    df = df.with_column("cropped", df["image"].image.crop(df["bbox"]))
    df = df.with_column("thumbnail", df["cropped"].image.resize(64, 64))
    df = df.with_column("image_tensor", df["thumbnail"]).select("id", "annotation_id", "image_tensor").limit(100)

    df.collect()
    assert df.count_rows() == 100

    model = models.resnet18()
    model = torch.nn.Sequential(*list(model.children())[:-1])
    model.eval()
    device = torch.device("cpu")
    model.to(device)

    torch_dataset = df.to_torch_iter_dataset()
    dataloader = DataLoader(torch_dataset, batch_size=16)

    embeddings = []
    ids = []
    annotation_ids = []

    for batch in iter(dataloader):
        image_tensor = batch["image_tensor"].to(device)
        image_tensor = image_tensor.permute(0, 3, 1, 2).float() / 255.0
        outputs = model(image_tensor)
        outputs = outputs.view(outputs.size(0), -1)
        embeddings.append(outputs.cpu())
        ids.extend(batch["id"])
        annotation_ids.extend(batch["annotation_id"])

    all_embeddings = torch.cat(embeddings, dim=0)

    assert all_embeddings.shape[0] == len(ids), "Number of embeddings should match number of ids"
    assert all_embeddings.shape[1] == 512, "ResNet18 should produce 512-dimensional embeddings"


@pytest.mark.integration()
def test_pandas_to_daft_to_glue_iceberg(pytestconfig):
    if pytestconfig.getoption("--credentials") is not True:
        pytest.skip("Test requires AWS credentials and `--credentials` flag")

    # Create random data in pandas.
    num_rows = 10
    df_pd = pd.DataFrame(
        {
            "id": [str(uuid.uuid4()) for _ in range(num_rows)],
            "double_col": [random.random() for _ in range(num_rows)],
            "int_col": [random.randint(0, 3) for _ in range(num_rows)],
        }
    )

    # Convert to daft DataFrame.
    df = daft.from_pandas(df_pd)

    # Overwrite data in Glue Iceberg table.
    sess = session.Session()
    catalog = GlueCatalog.from_session(name="glue_catalog", session=sess)
    table = catalog.get_table("glue_iceberg_test.pandas_to_glue_table")
    table.write(df, mode="overwrite")

    # Validate that we wrote the data correctly.
    read_back = table.read()
    read_back_pd = read_back.sort("id").to_pandas().reset_index(drop=True)
    df_pd = df_pd.sort_values("id").reset_index(drop=True)
    assert read_back_pd.equals(df_pd), "Did not write data correctly to Glue Iceberg table"
