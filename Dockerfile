FROM python:3.10-slim

ENV API_BASE_URL=https://dlmm-api.meteora.ag
ENV LOG_LEVEL=INFO
ENV DEFAULT_LIMIT=100
ENV DB_PATH="/data"
ENV DB_FILENAME=meteora_dlmm_time_series.duckdb
ENV RATE_LIMIT_CALLS=3
ENV RATE_LIMIT_PERIOD=1

VOLUME ["/data"]

WORKDIR /app

RUN apt-get update && apt-get install -y \
  build-essential \
  curl \
  software-properties-common \
  git \
  procps \
  && rm -rf /var/lib/apt/lists/*

RUN git clone https://github.com/cryptokvothed/meteora-dlmm-project .

RUN pip3 install -r requirements.txt

COPY entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

EXPOSE 8501

HEALTHCHECK CMD curl --fail http://localhost:8501/_stcore/health

ENTRYPOINT ["/app/entrypoint.sh"]