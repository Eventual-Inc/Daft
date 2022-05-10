FROM python:3.9-slim-bullseye AS daft

WORKDIR /app

COPY python/requirements.txt /app/python/

RUN --mount=type=cache,target=/root/.cache/pip python -m pip install -r /app/python/requirements.txt

COPY daft /app/daft

CMD ["python", "-m", "daft"]