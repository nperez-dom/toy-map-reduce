package com.osocron.mapreduce.master.data

import worker.TaskStatus

trait Task {
  val status: TaskStatus
}
case class MapTask(
    taskId: String,
    inputLocation: String,
    status: TaskStatus
) extends Task
case class ReduceTask(
    partitionKey: String,
    inputLocations: Set[String],
    status: TaskStatus
) extends Task

object MapTasks {
  val mapTasks: Map[String, MapTask] = Map(
    "1"  -> MapTask(
      "1",
      "worker-fs/map-task-input/file00.txt",
      TaskStatus.IDLE
    ),
    "2"  -> MapTask(
      "2",
      "worker-fs/map-task-input/file01.txt",
      TaskStatus.IDLE
    ),
    "3"  -> MapTask(
      "3",
      "worker-fs/map-task-input/file02.txt",
      TaskStatus.IDLE
    ),
    "4"  -> MapTask(
      "4",
      "worker-fs/map-task-input/file03.txt",
      TaskStatus.IDLE
    ),
    "5"  -> MapTask(
      "5",
      "worker-fs/map-task-input/file04.txt",
      TaskStatus.IDLE
    ),
    "6"  -> MapTask(
      "6",
      "worker-fs/map-task-input/file05.txt",
      TaskStatus.IDLE
    ),
    "7"  -> MapTask(
      "7",
      "worker-fs/map-task-input/file06.txt",
      TaskStatus.IDLE
    ),
    "8"  -> MapTask(
      "8",
      "worker-fs/map-task-input/file07.txt",
      TaskStatus.IDLE
    ),
    "9"  -> MapTask(
      "9",
      "worker-fs/map-task-input/file08.txt",
      TaskStatus.IDLE
    ),
    "10" -> MapTask(
      "10",
      "worker-fs/map-task-input/file09.txt",
      TaskStatus.IDLE
    ),
    "11" -> MapTask(
      "11",
      "worker-fs/map-task-input/file10.txt",
      TaskStatus.IDLE
    ),
    "12" -> MapTask(
      "12",
      "worker-fs/map-task-input/file11.txt",
      TaskStatus.IDLE
    ),
    "13" -> MapTask(
      "13",
      "worker-fs/map-task-input/file12.txt",
      TaskStatus.IDLE
    )
  )
}
