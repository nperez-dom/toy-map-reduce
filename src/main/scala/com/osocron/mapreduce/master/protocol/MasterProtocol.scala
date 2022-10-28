package com.osocron.mapreduce.master.protocol

import com.osocron.mapreduce.domain.WorkerNode

trait MasterProtocol
case class RegisterWorker(workerId: String, host: String, port: Int) extends MasterProtocol
case class WorkerNodeUnresponsive(workerNode: WorkerNode)            extends MasterProtocol
case class MapTaskFinished(
    workerId: String,
    taskId: String,
    outputPaths: List[String]
)                                                                    extends MasterProtocol
case class ReduceTaskFinished(
    workerId: String,
    partitionKey: String,
    outputPath: String
)                                                                    extends MasterProtocol
