package com.osocron.mapreduce.worker.services

import com.osocron.mapreduce.master.data.{MapTask, ReduceTask}
import com.osocron.mapreduce.user.UserDefinedFunctions
import io.grpc.Status
import master.{MapTaskFinishedRequest, ReduceTaskFinishedRequest}
import master.ZioMaster.MasterServiceClient
import worker.ZioWorker.WorkerService
import worker._
import zio._
import zio.clock.Clock
import zio.console._
import zio.duration._

import java.io.{File, FileWriter}
import scala.io.AnsiColor._
import scala.io.BufferedSource

class WorkerServiceImpl(
    val workerId: String,
    val currentMapTask: Ref[MapTask],
    val currentReduceTask: Ref[ReduceTask],
    val clock: Clock.Service,
    val console: Console.Service,
    val masterClient: MasterServiceClient.Service
) extends WorkerService {

  override def healthCheck(request: HealthCheckRequest): ZIO[Any, Status, HealthCheckResponse] =
    currentMapTask.get.map(t => HealthCheckResponse(status = t.status))

  override def startMapTask(request: StartMapTaskRequest): ZIO[Any, Status, StartMapTaskResponse] =
    for {
      time     <- clock.nanoTime
      currTask <- currentMapTask.get
      _        <- putStrLn(s"${YELLOW}Got a request to start a map task with input: ${request.inputLocation} 😬")
                    .provideLayer(ZLayer.succeed(console))
      _        <- if (currTask.status.isInProgress) ZIO.fail(Status.ALREADY_EXISTS)
                  else
                    (for {
                      _      <- currentMapTask.set(MapTask(request.taskId, request.inputLocation, TaskStatus.IN_PROGRESS))
                      _      <- putStrLn(s"${YELLOW}Starting assigned map task! 🔥")
                      output <- executeMapTask(request.inputLocation, request.taskId)
                      _      <- masterClient
                                  .mapTaskFinished(MapTaskFinishedRequest(workerId, request.taskId, output))
                                  .tapBoth(
                                    e => putStrLn(s"${RED}Could not update master about task success with error: ${e.getDescription} 😭"),
                                    _ => putStrLn(s"${GREEN}Successfully notified of task completion 😄")
                                  )
                                  .retry(Schedule.exponential(1.second))
                    } yield ()).forkDaemon
                      .provideLayer(ZLayer.succeed(clock) ++ ZLayer.succeed(console))
    } yield StartMapTaskResponse(
      ack = "Starting map-task",
      startTime = time.toString,
      status = TaskStatus.IN_PROGRESS
    )

  def executeMapTask(inputLocation: String, taskId: String): ZIO[Clock with Console, Throwable, List[String]] =
    for {
      contents  <- fileReader(inputLocation).use(b => ZIO.effect(b.mkString))
      _         <- putStrLn(s"${GREEN}Successfully read file: $inputLocation")
      result    <- ZIO.succeed(UserDefinedFunctions.mapFunction(inputLocation, contents))
      partitions = result.foldLeft(Map.empty[String, List[(String, String)]]) { case (acc, (k, v)) =>
                     val key = UserDefinedFunctions.partitionBy(k)
                     acc + (key -> ((k -> v) :: acc.getOrElse(key, List.empty[(String, String)])))
                   }
      _         <- putStrLn(s"Id of worker is: $workerId")
      output    <- ZIO
                     .collectAllPar {
                       val basePath = s"worker-fs/map-task-output/$workerId/"
                       partitions.foldLeft(List.empty[Task[String]]) { case (effects, (partition, values)) =>
                         val csv  = values.foldLeft("")((acc, next) => acc + s"${next._1},${next._2}\n")
                         val path = s"$basePath$partition.txt"
                         fileWriter(path)
                           .use(fw => ZIO.effect(fw.write(csv)))
                           .as(path) :: effects
                       }
                     }
                     .tapBoth(
                       e => putStrLn(s"${RED}Error while writing output: ${e.getMessage}"),
                       _ => putStrLn(s"${GREEN}Files writen successfully! 👌")
                     )
      _         <- currentMapTask.set(
                     MapTask(
                       taskId,
                       inputLocation,
                       TaskStatus.COMPLETED
                     )
                   )
    } yield output

  def fileWriter(path: String): TaskManaged[FileWriter] =
    Managed.make(ZIO.effect(new FileWriter(path, true)))(r => ZIO.effect(r.close()).orDie)

  def fileReader(path: String): TaskManaged[BufferedSource] =
    Managed.make(ZIO.effect(scala.io.Source.fromFile(new File(path))))(s => ZIO.effect(s.close()).orDie)

  override def startReduceTask(request: StartReduceTaskRequest): ZIO[Any, Status, StartReduceTaskResponse] =
    for {
      time     <- clock.nanoTime
      currTask <- currentReduceTask.get
      _        <- putStrLn(s"${YELLOW}Got a request to start a reduce task 😬").provideLayer(ZLayer.succeed(console))
      _        <- if (currTask.status.isInProgress) ZIO.fail(Status.ALREADY_EXISTS)
                  else
                    (for {
                      _      <- currentReduceTask.set(ReduceTask(request.partitionKey, request.inputLocations.toSet, TaskStatus.IN_PROGRESS))
                      _      <- putStrLn(s"${YELLOW}Starting assigned reduce task! 🔥")
                      output <- executeReduceTask(request.inputLocations, request.partitionKey)
                      _      <- masterClient
                                  .reduceTaskFinished(ReduceTaskFinishedRequest(workerId, request.partitionKey, output))
                                  .tapBoth(
                                    e => putStrLn(s"${RED}Could not update master about task success with error: ${e.getDescription} 😭"),
                                    _ => putStrLn(s"${GREEN}Successfully notified of task completion 😄")
                                  )
                    } yield ()).forkDaemon
                      .provideLayer(ZLayer.succeed(clock) ++ ZLayer.succeed(console))
    } yield StartReduceTaskResponse(
      ack = "Starting reduce task",
      startTime = time.toString,
      status = TaskStatus.IN_PROGRESS
    )

  def executeReduceTask(inputLocations: Seq[String], partitionKey: String): ZIO[Clock with Console, Throwable, String] =
    for {
      _        <- putStrLn(s"These are the input locations: $inputLocations")
      contents <- inputLocations.foldLeft(Task(Map.empty[String, List[String]]))((acc, inputLocation) => {
                    fileReader(inputLocation)
                      .use { source =>
                        source
                          .getLines()
                          .toList
                          .map {
                            case s"$a,$b" => (a, b)
                            case _        => ("", "")
                          }
                          .foldLeft(acc)((acc2, t) => {
                            val (k, v) = t
                            acc2.map(m => m + (k -> (v :: m.getOrElse(k, Nil))))
                          })
                      }
                  })
      _        <- putStrLn(s"${GREEN}Successfully read map-task output 😊")
      results  <- ZIO.effect(contents.map(t => UserDefinedFunctions.reduceFunction(t._1, t._2)))
      basePath  = s"worker-fs/reduce-task-output/$workerId/"
      path      = s"$basePath$partitionKey.txt"
      output   <- fileWriter(path)
                    .use(fw => ZIO.effect(fw.write(results.foldLeft("")((acc, next) => s"$acc\n${next._1},${next._2}"))))
                    .as(path)
                    .tapBoth(
                      e => putStrLn(s"${RED}Error while writing output: ${e.getMessage}"),
                      _ => putStrLn(s"${GREEN}Files writen successfully! 👌")
                    )
      _        <- currentReduceTask.set(ReduceTask(partitionKey, inputLocations.toSet, TaskStatus.COMPLETED))
    } yield output

}

object WorkerServiceImpl {
  def apply(workerId: String): URIO[ZEnv with MasterServiceClient, WorkerService] =
    for {
      currMapTask    <- Ref.make(MapTask("", "", TaskStatus.IDLE))
      currReduceTask <-
        Ref.make(ReduceTask("", Set(), TaskStatus.IDLE))
      masterClient   <- ZIO.service[MasterServiceClient.Service]
      env            <- ZIO.environment
    } yield new WorkerServiceImpl(
      workerId,
      currMapTask,
      currReduceTask,
      env.get[Clock.Service],
      env.get[Console.Service],
      masterClient
    )
}
