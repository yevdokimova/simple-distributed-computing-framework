FROM python:3.9-slim

WORKDIR /app

COPY common /app/common

COPY master/master_server.py /app/

EXPOSE 5000

CMD ["python", "master_server.py"]
