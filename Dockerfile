FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONPATH=/app

RUN apt-get update \
    && apt-get install --yes --no-install-recommends openjdk-17-jre-headless make \
    && apt-get clean

WORKDIR /app

COPY requirements.txt requirements-dbt.txt requirements-streaming.txt requirements-dev.txt ./
RUN python -m pip install --upgrade pip \
    && python -m pip install \
        -r requirements.txt \
        -r requirements-dbt.txt \
        -r requirements-streaming.txt \
        -r requirements-dev.txt

COPY . .

CMD ["python", "-m", "streaming.producer.produce_transaction_events", "--help"]
