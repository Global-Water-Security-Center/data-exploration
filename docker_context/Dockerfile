# Container for running GWSC scripts
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

RUN mkdir -p /usr/local/gwsc_app_install
WORKDIR /usr/local/gwsc_app_install
COPY requirements.txt /usr/local/gwsc_app_install/
RUN pip3 install -r /usr/local/gwsc_app_install/requirements.txt
RUN git clone https://github.com/springinnovate/ecoshard.git
WORKDIR /usr/local/gwsc_app_install/ecoshard
RUN python3 setup.py install
WORKDIR /usr/local/gwsc_app

CMD ["python3"]
