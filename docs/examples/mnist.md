# MNIST Digit Classification with Daft

[Run this tutorial on Google Colab](https://colab.research.google.com/github/Eventual-Inc/Daft/blob/main/tutorials/mnist.ipynb)

The MNIST Dataset is a "large database of handwritten digits that is commonly used for training various image processing systems". A classic example used in machine learning.

## Loading JSON Data

This is a JSON file containing all the data for the MNIST test set. Let's load it up into a Daft Dataframe!

```python
import daft
from daft import DataType, col, udf

URL = "https://github.com/Eventual-Inc/mnist-json/raw/master/mnist_handwritten_test.json.gz"
images_df = daft.read_json(URL)
```

To peek at the dataset, simply display the `images_df` that was just created.

```python
images_df.show(10)
```

You just loaded your first Daft Dataframe! It consists of two columns:

1. The "image" column is a Python column of type `list` - where it looks like each row contains a list of digits representing the pixels of each image
2. The "label" column is an Integer column, consisting of just the label of that image.

## Processing Columns with [User-Defined Functions (UDF)](../custom-code/udfs.md)

It seems our JSON file has provided us with a one-dimensional array of pixels instead of two-dimensional images. We can easily modify data in this column by instructing Daft to run a method on every row in the column like so:

```python
import numpy as np

images_df = images_df.with_column(
    "image_2d",
    col("image").apply(lambda img: np.array(img).reshape(28, 28), return_dtype=DataType.python()),
)
images_df.show(10)
```

Great, but we can do one better - let's convert these two-dimensional arrays into Images. Computers speak in pixels and arrays, but humans do much better with visual patterns!

To do this, we can leverage the `.apply` expression method. Similar to the `.as_py` method, this allows us to run a single function on all rows of a given column, but provides us with more flexibility as it takes as input any arbitrary function.

```python
from PIL import Image

images_df = images_df.with_column(
    "pil_image",
    col("image_2d").apply(lambda arr: Image.fromarray(arr.astype(np.uint8)), return_dtype=DataType.python()),
)
images_df.show(10)
```

Amazing! This looks great and we can finally get some idea of what the dataset truly looks like.

## Running a model with UDFs

Next, let's try to run a deep learning model to classify each image. Models are expensive to initialize and load, so we want to do this as few times as possible, and share a model across multiple invocations.

For the convenience of this quickstart tutorial, we pre-trained a model using a PyTorch-provided example script and saved the trained weights at https://github.com/Eventual-Inc/mnist-json/raw/master/mnist_cnn.pt.  We need to define the same deep learning model "scaffold" as the trained model that we want to load (this part is all PyTorch and is not specific at all to Daft)

```python
###
# Model was trained using a script provided in PyTorch Examples: https://github.com/pytorch/examples/blob/main/mnist/main.py
###

import torch
import torch.hub
import torch.nn as nn
import torch.nn.functional as F


class Net(nn.Module):
    def __init__(self):
        super().__init__()
        self.conv1 = nn.Conv2d(1, 32, 3, 1)
        self.conv2 = nn.Conv2d(32, 64, 3, 1)
        self.dropout1 = nn.Dropout(0.25)
        self.dropout2 = nn.Dropout(0.5)
        self.fc1 = nn.Linear(9216, 128)
        self.fc2 = nn.Linear(128, 10)

    def forward(self, x):
        x = self.conv1(x)
        x = F.relu(x)
        x = self.conv2(x)
        x = F.relu(x)
        x = F.max_pool2d(x, 2)
        x = self.dropout1(x)
        x = torch.flatten(x, 1)
        x = self.fc1(x)
        x = F.relu(x)
        x = self.dropout2(x)
        x = self.fc2(x)
        output = F.log_softmax(x, dim=1)
        return output
```

Now comes the fun part - we can define a UDF using the `@udf` decorator. Notice that for a batch of data we only initialize our model once!

```python
@udf(return_dtype=DataType.int64())
class ClassifyImages:
    def __init__(self):
        # Perform expensive initializations - create the model, download model weights and load up the model with weights
        self.model = Net()
        state_dict = torch.hub.load_state_dict_from_url(
            "https://github.com/Eventual-Inc/mnist-json/raw/master/mnist_cnn.pt"
        )
        self.model.load_state_dict(state_dict)

    def __call__(self, images_2d_col):
        images_arr = np.array(images_2d_col.to_pylist())
        normalized_image_2d = images_arr / 255
        normalized_image_2d = normalized_image_2d[:, np.newaxis, :, :]
        classifications = self.model(torch.from_numpy(normalized_image_2d).float())
        return classifications.detach().numpy().argmax(axis=1)
```

Using this UDF is really easy, we simply run it on the columns that we want to process:

```python
classified_images_df = images_df.with_column("model_classification", ClassifyImages(col("image_2d")))

classified_images_df.show(10)
```

Our model ran successfully, and produced a new classification column. These look pretty good - let's filter our Dataframe to show only rows that the model predicted wrongly.

```python
classified_images_df.where(col("label") != col("model_classification")).show(10)
```

Some of these look hard indeed, even for a human!

## Analytics

We just managed to run our model, but how well did it actually do? Dataframes expose a powerful set of operations in Groupbys/Aggregations to help us report on aggregates of our data.

Let's group our data by the true labels and calculate how many mistakes our model made per label.

```python
analysis_df = (
    classified_images_df.with_column("correct", (col("model_classification") == col("label")).cast(DataType.int64()))
    .with_column("wrong", (col("model_classification") != col("label")).cast(DataType.int64()))
    .groupby(col("label"))
    .agg(
        col("label").count().alias("num_rows"),
        col("correct").sum(),
        col("wrong").sum(),
    )
    .sort(col("label"))
)

analysis_df.show()
```

Pretty impressive, given that the model only actually trained for one epoch!
