# Container for STAC API manager
FROM ubuntu:jammy

RUN apt-get update -qq && \
    apt-get install -y \
    libspatialindex-dev \
    python3-pip \
    openssl \
    curl \
    git \
    gdal-bin \
    python3-gdal \
    && \
    rm -rf /var/lib/apt/lists/*

RUN pip install earthengine-api

SHELL ["/bin/bash", "-c"]

RUN mkdir -p /usr/local/gwsc_app
WORKDIR /usr/local/gwsc_app
COPY requirements.txt /usr/local/gwsc_app/
RUN pip3 install -r requirements.txt
RUN git clone https://github.com/springinnovate/ecoshard.git
WORKDIR /usr/local/gwsc_app/ecoshard
RUN python3 setup.py install
WORKDIR /usr/local/gwsc_app


CMD ["python3"]
