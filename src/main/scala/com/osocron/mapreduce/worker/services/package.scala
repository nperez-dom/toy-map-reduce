package com.osocron.mapreduce.worker

import master.ZioMaster.MasterServiceClient
import worker.ZioWorker.WorkerService
import zio._

package object services {
  type Worker = Has[WorkerService]

  def live(workerId: String): URLayer[MasterServiceClient, Worker] =
    ZLayer.fromServiceM(c =>
      WorkerServiceImpl
        .apply(workerId)
        .provideLayer(ZEnv.live ++ ZLayer.succeed(c))
    )
}
