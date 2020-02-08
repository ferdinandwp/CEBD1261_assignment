FROM ubuntu:latest

ARG DEBIAN_FRONTEND=noninteractive

COPY project /root/.
WORKDIR /root/.

EXPOSE 5000

RUN apt-get update \
    && apt-get install -y python3-pip python3-dev \
    && cd /usr/local/bin \
    && ln -s /usr/bin/python3 python \
    && pip3 install --upgrade pip

RUN pip install -r requirements.txt

CMD python3 ./index.py
