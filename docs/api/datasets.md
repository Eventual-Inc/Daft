# Datasets

Daft provides simple, performant, and responsible ways to access useful datasets like [Common Crawl](https://commoncrawl.org/get-started), [DROID](https://droid-dataset.github.io/), and [ABC-130k](https://huggingface.co/datasets/XDOF/ABC-130k).

## Common Crawl

Check out our [Common Crawl dataset guide](../datasets/common-crawl.md) for more examples!

::: daft.datasets.common_crawl.common_crawl
    options:
        filters: ["!^_"]
        heading_level: 3

## LeRobot v3

See the [LeRobot v3 dataset guide](../datasets/lerobot.md) for episode vs frame workflows and Hub/local paths.

::: daft.datasets.lerobot.read
    options:
        filters: ["!^_"]
        heading_level: 3

::: daft.datasets.lerobot.read_episodes
    options:
        filters: ["!^_"]
        heading_level: 3

::: daft.datasets.lerobot.load_episode_frames
    options:
        filters: ["!^_"]
        heading_level: 3

::: daft.datasets.lerobot.read_tasks

## DROID

Check out our [DROID dataset guide](../datasets/droid.md) for more examples!

::: daft.datasets.droid.raw
    options:
        filters: ["!^_"]
        heading_level: 3

::: daft.datasets.droid.scenes
    options:
        filters: ["!^_"]
        heading_level: 3

::: daft.datasets.droid.trajectory
    options:
        filters: ["!^_"]
        heading_level: 3

::: daft.datasets.droid.camera_frames
    options:
        filters: ["!^_"]
        heading_level: 3

## ABC-130k

Check out our [ABC-130k dataset guide](../datasets/abc.md) for a staged workflow covering episode discovery, MCAP metadata, native message reads, annotations, and video decoding.

::: daft.datasets.abc.raw
    options:
        filters: ["!^_"]
        heading_level: 3

::: daft.datasets.abc.metadata
    options:
        filters: ["!^_"]
        heading_level: 3

::: daft.datasets.abc.messages
    options:
        filters: ["!^_"]
        heading_level: 3

::: daft.datasets.abc.annotations
    options:
        filters: ["!^_"]
        heading_level: 3

::: daft.datasets.abc.camera_frames
    options:
        filters: ["!^_"]
        heading_level: 3
