# Datasets

Daft provides simple, performant, and responsible ways to access useful datasets like [Common Crawl](https://commoncrawl.org/get-started) and [DROID](https://droid-dataset.github.io/).

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
