FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends tini \
  && rm -rf /var/lib/apt/lists/*

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY src ./src
COPY templates ./templates

RUN mkdir -p /app/out && chmod 777 /app/out

ENV PYTHONPATH="/app/src:${PYTHONPATH}"

ENTRYPOINT ["/usr/bin/tini","--","python","-m","calo_logs_analyzer"]
