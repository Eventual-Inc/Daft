# Using Daft with Flyte

Daft works well with the [Flyte](https://flyte.org/) workflow engine and its Ray plugin.

## Using from Flyte task

To run Daft in a Flyte task, use Daft as if you were using Daft in a local machine!

## Using Daft from a Ray Flyte task

To run Daft in a Ray Flyte task and connect to a Ray cluster, use Daft as if `ray.init()` has already been called.

That is to say, run Daft with: `daft.context.set_ray_runner()`.

See: `app.py` for an example.

## Setup

To run this locally, perform Flyte local setup as detailed here: https://github.com/jeevb/flyte-examples/tree/main/ray
