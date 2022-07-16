from daft.serving import App

app = App()


@app.endpoint
def endpoint():
    return "Hello, world!"


if __name__ == "__main__":
    app.deploy()
