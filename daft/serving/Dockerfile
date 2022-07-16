FROM python:3.8-slim-buster

WORKDIR /app
RUN pip install fastapi uvicorn cloudpickle
ENV ENDPOINT_PKL_FILEPATH endpoint.pkl
COPY endpoint.pkl endpoint.pkl
COPY entrypoint.py entrypoint.py

CMD ["python", "entrypoint.py"]
