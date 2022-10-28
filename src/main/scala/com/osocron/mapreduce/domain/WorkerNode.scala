package com.osocron.mapreduce.domain

import com.osocron.mapreduce.master.MasterError
import worker.TaskStatus
import worker.ZioWorker.WorkerServiceClient
import zio.Layer

case class WorkerNode(
    workerId: String,
    host: String,
    port: Int,
    taskId: String,
    taskType: TaskType.TaskType,
    taskStatus: TaskStatus,
    clientLayer: Layer[MasterError, WorkerServiceClient]
)

