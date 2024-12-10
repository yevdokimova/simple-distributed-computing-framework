FROM python:3.9-slim

WORKDIR /app

COPY common /app/common

COPY worker/worker_server.py /app/

CMD ["python", "worker_server.py"]
