package com.osocron.mapreduce.master

import com.osocron.mapreduce.master.protocol.{MapTaskFinished, MasterProtocol, ReduceTaskFinished, RegisterWorker}
import io.grpc.Status
import master.ZioMaster.MasterService
import master.{MapTaskFinishedRequest, MapTaskFinishedResponse, ReduceTaskFinishedRequest, ReduceTaskFinishedResponse, RegisterWorkerRequest, RegisterWorkerResponse}
import zio.{Has, Queue, ZIO, ZLayer}

package object services {

  val live: ZLayer[Queue[MasterProtocol], Nothing, Has[MasterService]] =
    ZLayer.fromFunction[Queue[MasterProtocol], MasterService](masterQueue =>
      new MasterService {
        override def registerWorker(
            request: RegisterWorkerRequest
        ): ZIO[Any, Status, RegisterWorkerResponse] =
          masterQueue
            .offer(
              RegisterWorker(
                request.workerId,
                request.host,
                request.port
              )
            )
            .as(RegisterWorkerResponse(1000))

        override def mapTaskFinished(
            request: MapTaskFinishedRequest
        ): ZIO[Any, Status, MapTaskFinishedResponse] =
          masterQueue
            .offer(
              MapTaskFinished(
                request.workerId,
                request.taskID,
                request.outputLocation.toList
              )
            )
            .as(MapTaskFinishedResponse("Acknowledged 👍"))

        override def reduceTaskFinished(
            request: ReduceTaskFinishedRequest
        ): ZIO[Any, Status, ReduceTaskFinishedResponse] =
          masterQueue
            .offer(
              ReduceTaskFinished(
                request.workerId,
                request.partitionKey,
                request.outputLocation
              )
            )
            .as(ReduceTaskFinishedResponse("Acknowledged 👍"))
      }
    )
}
