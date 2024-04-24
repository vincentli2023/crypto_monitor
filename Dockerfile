FROM ubuntu:22.04
RUN mkdir /root/crypto_monitor

COPY ./requirements.txt /tmp/requirements.txt
RUN apt-get update && apt-get install -y iputils-ping python3-pip && apt-get clean && pip3 --no-cache-dir install --upgrade pip
RUN pip3 install -r /tmp/requirements.txt && rm /tmp/requirements.txt

COPY ./ /root/crypto_monitor/
ENV PYTHONPATH "${PYTHONPATH}:/root/crypto_monitor/"

WORKDIR /root