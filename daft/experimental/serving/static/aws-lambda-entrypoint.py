from __future__ import annotations

import os
import pickle

with open(os.environ["ENDPOINT_PKL_FILEPATH"], "rb") as f:
    endpoint = pickle.loads(f.read())


def lambda_handler(event, context):
    # TODO(jay): implement logic to parse incoming request
    return {
        "response": endpoint("foo"),
    }
