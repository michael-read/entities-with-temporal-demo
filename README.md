# Entity Demo with Temporal

# 1. start pulsar with Docker Compose
```
docker compose up
```

# 2. create tenent, namespace, and topic partitions

## connect to Pulsar container
```
docker exec -it pulsar /bin/sh
```
## run init script
```
$ /pulsar/init/init-pulsar.sh
```