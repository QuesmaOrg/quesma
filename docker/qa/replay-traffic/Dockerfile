FROM python:3.11-alpine

RUN pip install requests jsondiff

RUN mkdir -p ./data

COPY data replay.py ./

RUN wget -q -O ./data/small.json https://storage.googleapis.com/elastic-sample-data/version-2024-04-22/traffic/small.json

CMD ["python", "./replay.py"]