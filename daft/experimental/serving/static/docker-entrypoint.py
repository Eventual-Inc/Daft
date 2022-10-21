from __future__ import annotations

import argparse
import pickle

import fastapi
import uvicorn

app = fastapi.FastAPI()


@app.get("/healthz")
def healthcheck():
    return {"status": "ok"}


if __name__ == "__main__":
    argparser = argparse.ArgumentParser()
    argparser.add_argument("--endpoint-pkl-file", required=True)
    args = argparser.parse_args()

    # Load cloudpickled function
    with open(args.endpoint_pkl_file, "rb") as f:
        endpoint = pickle.loads(f.read())
    app.get("/")(endpoint)

    config = uvicorn.Config(app=app, host="0.0.0.0", port=8000)
    server = uvicorn.Server(config)
    server.run()
