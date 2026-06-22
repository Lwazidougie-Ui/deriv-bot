FROM python:3.11-slim

WORKDIR /app

COPY . .

RUN pip install --no-cache-dir websocket-client

CMD ["python", "-u", "main.py"]
