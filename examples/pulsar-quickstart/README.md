Pulsar Quickstart
================

This project illustrates how you can interact with Apache Pulsar using MicroProfile Reactive Messaging.

Run Docker
================

    docker run -it \
      -p 6650:6650 \
      -p 8080:8080 \
      --mount source=pulsardata,target=/pulsar/data \
      --mount source=pulsarconf,target=/pulsar/conf \
      apachepulsar/pulsar:2.5.0 \
      bin/pulsar standalone

Run Docker Compose
========================================

    docker-compose up

