# Toy MapReduce

A toy implementation of the MapReduce model for distributed data processing.

## Running the application

To run the master process run:
```
sbt "runMain com.osocron.mapreduce.master.MasterApp"
```

To run a worker process run:
```
sbt "runMain com.osocron.mapreduce.worker.WorkerApp -p=9991 -i=Worker1"
```

