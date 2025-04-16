import pytest
import torch
import torchvision.models as models
from pyiceberg.catalog import load_catalog
from torch.utils.data import DataLoader

import daft


@pytest.mark.integration()
def test_dataloading_from_glue_iceberg():
    catalog = load_catalog(
        "glue_catalog",
        warehouse="s3://daft-public-data/test_fixtures/glue_iceberg_test/glue_iceberg_test_bucket/",
        type="glue",
    )

    images_table = catalog.load_table("glue_iceberg_test.coco_images")
    images_df = daft.read_iceberg(images_table)  # noqa: F841

    categories_table = catalog.load_table("glue_iceberg_test.coco_categories")
    categories_df = daft.read_iceberg(categories_table).with_column_renamed("id", "category_id")  # noqa: F841

    annotations_table = catalog.load_table("glue_iceberg_test.coco_annotations")
    annotations_df = daft.read_iceberg(annotations_table).with_column_renamed("id", "annotation_id")  # noqa: F841

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
