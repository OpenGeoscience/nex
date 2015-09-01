Launches docker containers with spark using docker-compose.

Spark install is expected to be at ```opt/spark```

Shared data should be in ```data/```


Run ```docker-compose scale worker=N``` where N is the number of workers you want to set up See: https://docs.docker.com/v1.6/compose/cli/#scale

Run ```docker-compse up``` to start the cluster.
