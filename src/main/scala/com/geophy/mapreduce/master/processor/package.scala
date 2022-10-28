package com.geophy.mapreduce.master

import com.geophy.mapreduce.master.protocol.MasterProtocol
import zio._

package object processor {

  type MasterProcessor = Has[MasterProcessor.Service]
  case class MasterProcessorCtx(
      fib: Fiber.Runtime[MasterError, Nothing],
      messageQueue: Queue[MasterProtocol]
  )

  object MasterProcessor {
    trait Service {
      def build(): UIO[MasterProcessorCtx]
      def runMapPhase: IO[MasterError, Unit]
      def runReducePhase: IO[MasterError, Unit]
    }
    val live: URLayer[Has[zio.ZEnv], MasterProcessor] =
      MasterProcessorLiveImpl.apply.toLayer
  }

  def build: URIO[MasterProcessor, MasterProcessorCtx] =
    ZIO.accessM(_.get.build())

  def runMapPhase: ZIO[MasterProcessor, MasterError, Unit] =
    ZIO.accessM(_.get.runMapPhase)

  def runReducePhase: ZIO[MasterProcessor, MasterError, Unit] =
    ZIO.accessM(_.get.runReducePhase)

}
