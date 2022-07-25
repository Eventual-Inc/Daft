import numpy as np

from daft.serving import App

app = App(pip_dependencies=["numpy"])


@app.endpoint
def endpoint():
    return np.ones((10, 10)).mean().item()


if __name__ == "__main__":
    app.deploy()
