package com.geophy.mapreduce.master

import com.geophy.mapreduce.master.processor.MasterProcessor
import io.grpc.ServerBuilder
import io.grpc.protobuf.services.ProtoReflectionService
import scalapb.zio_grpc.CanBind.canBindAny
import scalapb.zio_grpc.ServerLayer
import zio._
import zio.clock.Clock
import zio.console._
import zio.duration._

object MasterApp extends zio.App {
  type AppEnv = MasterProcessor with Console with Clock

  def port: Int = 8888

  private def welcome =
    putStrLn("Server is running. Press Ctrl-C to stop.")

  private def builder =
    ServerBuilder
      .forPort(port)
      .addService(ProtoReflectionService.newInstance())
      .asInstanceOf[ServerBuilder[_]]

  private def serverLive =
    ServerLayer
      .fromServiceLayer(builder)(services.live)
      .mapError(t => ServiceError(t.getMessage, t))

  val myAppLogic: ZIO[AppEnv, MasterError, Unit] =
    for {
      _   <- welcome
      ctx <- processor.build
      f   <- serverLive.build.useForever.fork.provide(ctx.messageQueue)
      _   <- ZIO.sleep(5.seconds)
      _   <- processor.runMapPhase
      _   <- processor.runReducePhase
      _   <- f.interrupt
    } yield ()

  val deps: ULayer[AppEnv] =
    ((ZEnv.live.map(
      Has(_)
    ) >>> MasterProcessor.live) ++ Console.live ++ Clock.live)
      .asInstanceOf[ULayer[AppEnv]]

  def run(args: List[String]): URIO[zio.ZEnv, ExitCode] =
    myAppLogic.provideSomeLayer(deps).exitCode
}
