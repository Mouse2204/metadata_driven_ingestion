FROM python:3.10-slim-bullseye

RUN apt-get update && \
    apt-get install -y \
    openjdk-17-jre-headless \
    procps \
    ca-certificates \
    curl \
    wget && \
    update-ca-certificates && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

RUN pip install --upgrade certifi

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ ./src/
COPY configs/ ./configs/
COPY deps/ ./deps/
COPY .env .

ENV REQUESTS_CA_BUNDLE=/usr/local/lib/python3.10/site-packages/certifi/cacert.pem
ENV SSL_CERT_FILE=/usr/local/lib/python3.10/site-packages/certifi/cacert.pem
ENV CURL_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt

CMD ["python", "-m", "src.main"]