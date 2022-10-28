package com.osocron.mapreduce.worker

import com.osocron.mapreduce.worker.cli.{CliConfig, CliParser}
import io.grpc.protobuf.services.ProtoReflectionService
import io.grpc.{ManagedChannelBuilder, ServerBuilder}
import master.RegisterWorkerRequest
import master.ZioMaster.MasterServiceClient
import scalapb.zio_grpc.CanBind.canBindAny
import scalapb.zio_grpc.{ServerLayer, ZManagedChannel}
import scopt.OParser
import zio.console.putStrLn
import zio.duration._
import zio.{ExitCode, Schedule, URIO, ZEnv, ZIO, ZLayer}

object WorkerApp extends zio.App {

  def welcome: ZIO[ZEnv, Throwable, Unit] =
    putStrLn("Server is running. Press Ctrl-C to stop.")

  def builder: Int => ServerBuilder[_] = (port: Int) =>
    ServerBuilder
      .forPort(port)
      .addService(ProtoReflectionService.newInstance())
      .asInstanceOf[ServerBuilder[_]]

  private def serverLive = (workerId: String, port: Int) => ServerLayer.fromServiceLayer(builder(port))(services.live(workerId))

  private val channel: ZManagedChannel[Any] = ZManagedChannel(
    ManagedChannelBuilder
      .forAddress("localhost", 8888)
      .usePlaintext()
      .asInstanceOf[ManagedChannelBuilder[_]]
  )

  private val masterLayer: ZLayer[Any, Throwable, MasterServiceClient] =
    MasterServiceClient.live(channel)

  private val myAppLogic = (port: Int, workerId: String) =>
    for {
      _       <- welcome
      serverIO = serverLive(workerId, port).build.useForever
      clientIO =
        MasterServiceClient
          .registerWorker(RegisterWorkerRequest(workerId, "localhost", port))
          .tapBoth(e => putStrLn(s"Failed to connect with error: ${e.getDescription} 😞"), _ => putStrLn(s"Ready to accept tasks"))
          .retry(Schedule.spaced(1.second))
      _       <- serverIO.zipPar(clientIO)
    } yield ()

  def run(args: List[String]): URIO[zio.ZEnv, ExitCode] =
    OParser.parse(CliParser.parser, args, CliConfig()) match {
      case Some(cliConfig) =>
        myAppLogic(cliConfig.port, cliConfig.id)
          .provideCustomLayer(masterLayer)
          .exitCode
      case None            =>
        ZIO.fail(new Exception("No cli arguments provided")).exitCode
    }
}
