# Toy MapReduce

A toy implementation of the MapReduce model for distributed data processing systems.

## Running the application

To run the master process run:
```
sbt "runMain com.geophy.mapreduce.master.MasterApp"
```

To run a worker process run:
```
sbt "runMain com.geophy.mapreduce.worker.WorkerApp -p=9991 -i=Worker1"
```

