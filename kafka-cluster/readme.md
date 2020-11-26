# Docker-based Kafka Cluster

## Install

This kafka cluster is fully containerised.

To run, you will need [Docker](https://docs.docker.com/install/) and [Docker Compose](https://docs.docker.com/compose/) .

## Quickstart

You simply need to create a Docker network called `kafka-network` to enable communication between the Kafka cluster and producers / consumers applications (not yet implemented):

```bash
$ docker network create kafka-network
```

Then, spin up the local single-node Kafka cluster (will run in the background):

```bash
$ cd kafka-cluster
$ docker-compose -f docker-compose.yml up -d
```

## Teardown

To stop the docker-compose services and purge all topic contents:

```bash
$ docker-compose down
```

To remove the Docker network:

```bash
$ docker network rm kafka-network
```
