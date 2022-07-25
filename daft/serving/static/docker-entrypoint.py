import os
import pickle

import fastapi
import uvicorn

app = fastapi.FastAPI()


if __name__ == "__main__":
    # Load cloudpickled function
    with open(os.environ["ENDPOINT_PKL_FILEPATH"], "rb") as f:
        endpoint = pickle.loads(f.read())
    app.get("/")(endpoint)

    config = uvicorn.Config(app=app, host="0.0.0.0", port=8000)
    server = uvicorn.Server(config)
    server.run()
