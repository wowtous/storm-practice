# Storm-practice

## build docker environment
The version of docker is 1.9 or above.

1.`docker-compose.yml`

```yml
version: '2'
services:
    zookeeper:
        image: zookeeper:3.4
        container_name: zookeeper
        restart: always

    nimbus:
        image: storm:1.0.2
        container_name: nimbus
        command: storm nimbus
        depends_on:
            - zookeeper
        restart: always
        ports:
            - 6627:6627

    ui:
        image: storm:1.0.2
        container_name: ui
        command: storm ui
        depends_on:
            - nimbus
            - zookeeper
        restart: always
        ports:
            - 8080:8080

    supervisor:
        image: storm:1.0.2
        container_name: supervisor
        command: storm supervisor
        depends_on:
            - nimbus
            - zookeeper
        restart: always

    redis:
        image: redis:latest
        container_name: redis
        restart: always
        ports:
            - 6379:6379

networks:
  default:
    external:
      name: foo
```

2.Create a network `foo`

```sh
docker network ls
docker network create foo
```

3.Manage storm local cluster

```sh
docker-compose up -d # start
docker-compose stop  # stop
docker-compose rm    # remove
```

4.storm ui monitor,[`127.0.0.1:8080`](127.0.0.1:8080)


## package and publish

```sh
## package
cd .
mvn package

## publish
docker cp ./target/storm-practice-1.0-SNAPSHOT.jar /apache-storm-1.0.2
docker exec -it nimbus bash
storm jar ./storm-practice-1.0-SNAPSHOT.jar org.darebeat.wc.WordCountTopology wordcount

## log
docker exec -it supervisor bash
tail -f /logs/workers-artifacts/wordcount-1-xxx/xxx/worker.log
```
