package com.osocron.mapreduce.master.processor

import com.osocron.mapreduce.domain.TaskType.TaskType
import com.osocron.mapreduce.domain.{TaskType, WorkerNode}
import com.osocron.mapreduce.master._
import com.osocron.mapreduce.master.data.{MapTask, MapTasks, ReduceTask, Task => MTask}
import com.osocron.mapreduce.master.protocol.{MapTaskFinished, MasterProtocol, ReduceTaskFinished, RegisterWorker, WorkerNodeUnresponsive}
import com.osocron.mapreduce.master.{MasterError, NodeUnresponsiveError}
import io.grpc.{ManagedChannelBuilder, Status}
import scalapb.zio_grpc.ZManagedChannel
import worker.{HealthCheckRequest, StartMapTaskRequest, StartMapTaskResponse, StartReduceTaskRequest, StartReduceTaskResponse, TaskStatus}
import worker.ZioWorker.WorkerServiceClient
import zio._
import zio.console._
import zio.duration._
import zio.stm.{TRef, USTM}

import scala.io.AnsiColor._

class MasterProcessorLiveImpl(
    val messageQueue: Queue[MasterProtocol],
    val registeredWorkers: TRef[Map[String, WorkerNode]],
    val mapTasks: TRef[Map[String, MapTask]],
    val reduceTasks: TRef[Map[String, ReduceTask]],
    val zEnv: ZEnv
) extends MasterProcessor.Service {

  type MIO[A]   = ZIO[ZEnv, MasterError, A]
  type MURIO[A] = URIO[ZEnv, A]

  override def build(): UIO[MasterProcessorCtx] =
    run.map(MasterProcessorCtx(_, messageQueue)).provide(zEnv)

  def run: URIO[zio.ZEnv, Fiber.Runtime[MasterError, Nothing]] = {
    val loop = for {
      message <- messageQueue.take
      _       <- console.putStrLn(s"${WHITE}Taking a message from the queue...")
      _       <- handleMessage(message).forkDaemon
    } yield ()

    loop.forever.fork
  }

  def handleMessage(message: MasterProtocol): MIO[Unit] =
    message match {
      case r: RegisterWorker         => registerWorker(r).commit.flatMap(startSupervisorDaemon)
      case r: WorkerNodeUnresponsive => putStrLn("Having to let one of our workers go 😢") *> handleUnresponsiveNode(r).commit
      case r: MapTaskFinished        => putStrLn(s"$GREEN${r.workerId} finished map task with id: ${r.taskId} 😍") *> markMapTaskCompleted(r).commit
      case r: ReduceTaskFinished     =>
        markReduceTaskCompleted(r).commit <*
          putStrLn(s"$GREEN${r.workerId} finished reduce task with partition key: ${r.partitionKey} 😍")
      case _                         =>
        ZIO.fail(
          ServiceError(
            "Unexpected service Error",
            new Exception("Unexpected service error")
          )
        )
    }

  def registerWorker(req: RegisterWorker): USTM[WorkerNode] =
    for {
      map    <- registeredWorkers.get
      worker <-
        if (map.contains(req.workerId)) registeredWorkers.get.map(m => m(req.workerId))
        else {
          val workerNode =
            WorkerNode(
              req.workerId,
              req.host,
              req.port,
              "",
              TaskType.Map,
              TaskStatus.IDLE,
              generateClientLayer(req)
            )
          registeredWorkers.set(map + (req.workerId -> workerNode)).as(workerNode)
        }
    } yield worker

  def generateClientLayer(req: RegisterWorker): Layer[MasterError, WorkerServiceClient] =
    WorkerServiceClient
      .live[Any, Any](
        ZManagedChannel(
          ManagedChannelBuilder
            .forAddress(req.host, req.port)
            .usePlaintext()
            .asInstanceOf[ManagedChannelBuilder[_]]
        )
      )
      .mapError(t => ServiceError(t.getMessage, t).asInstanceOf[MasterError])

  private def startSupervisorDaemon(workerNode: WorkerNode): MURIO[Unit] =
    for {
      _ <-
        WorkerServiceClient
          .healthCheck(HealthCheckRequest("Please provide status"))
          .tap(r => putStrLn(s"$WHITE" + ("-" * 50) + s">  Status: ${r.status.name} for worker node: ${workerNode.workerId}"))
          .repeat(Schedule.spaced(1.second))
          .provideCustomLayer(workerNode.clientLayer)
          .orElse(
            messageQueue.offer(WorkerNodeUnresponsive(workerNode)) *> ZIO.fail(NodeUnresponsiveError)
          )
          .forkDaemon
    } yield ()

  def handleUnresponsiveNode(r: WorkerNodeUnresponsive): USTM[Unit] =
    for {
      currWorkers       <- registeredWorkers.get
      unresponsiveWorker = currWorkers(r.workerNode.workerId)
      _                 <- if (unresponsiveWorker.taskType == TaskType.Map)
                             mapTasks.update(updateMapTaskStatus(unresponsiveWorker.taskId, TaskStatus.IDLE))
                           else
                             reduceTasks.update(updateReduceTaskStatus(unresponsiveWorker.taskId, TaskStatus.IDLE))
      _                 <- registeredWorkers.update(_ - r.workerNode.workerId)
    } yield ()

  def markMapTaskCompleted(r: MapTaskFinished): USTM[Unit] =
    for {
      _ <- registeredWorkers.update(updateWorkerStatus(r.workerId, TaskStatus.COMPLETED))
      _ <- mapTasks.update(updateMapTaskStatus(r.taskId, TaskStatus.COMPLETED))
      _ <- addReduceTask(r)
    } yield ()

  def markReduceTaskCompleted(r: ReduceTaskFinished): USTM[Unit] =
    for {
      _ <- registeredWorkers.update(updateWorkerStatus(r.workerId, TaskStatus.COMPLETED))
      _ <- reduceTasks.update(updateReduceTaskStatus(r.partitionKey, TaskStatus.COMPLETED))
    } yield ()

  def updateMapTaskStatus(taskId: String, status: TaskStatus)(tasks: Map[String, MapTask]): Map[String, MapTask] =
    tasks + (taskId -> tasks(taskId).copy(status = status))

  def updateReduceTaskStatus(taskId: String, status: TaskStatus)(tasks: Map[String, ReduceTask]): Map[String, ReduceTask] =
    tasks + (taskId -> tasks(taskId).copy(status = status))

  def updateWorkerStatus(workerId: String, status: TaskStatus)(workers: Map[String, WorkerNode]): Map[String, WorkerNode] =
    workers + (workerId -> workers(workerId).copy(taskStatus = status))

  def updateMapTaskWhenNotComplete(taskId: String, status: TaskStatus): USTM[Unit] =
    mapTasks.update(m => updateTaskStatusWhenNotComplete[MapTask](m, taskId, t => t.copy(status = status)))

  def updateReduceTaskWhenNotComplete(taskId: String, status: TaskStatus): USTM[Unit] =
    reduceTasks.update(m => updateTaskStatusWhenNotComplete[ReduceTask](m, taskId, t => t.copy(status = status)))

  def updateTaskStatusWhenNotComplete[T <: MTask](m: Map[String, T], taskId: String, f: T => T): Map[String, T] =
    m + (taskId -> {
      if (m(taskId).status.isCompleted) m(taskId)
      else f(m(taskId))
    })

  def updateWorkerStatusWhenNotComplete(workerId: String, taskId: String, taskType: TaskType, status: TaskStatus): USTM[Unit] =
    registeredWorkers.update(m =>
      m + (workerId -> {
        if (m(workerId).taskStatus.isCompleted) m(workerId)
        else
          m(workerId).copy(
            taskId = taskId,
            taskType = taskType,
            taskStatus = status
          )
      })
    )

  override def runMapPhase: IO[MasterError, Unit] =
    (for {
      _ <- putStrLn(s"${YELLOW}Starting map phase 😎")
      _ <- assignIncompleteMapTasks
      _ <- waitForMapPhaseCompletion
    } yield ()).provide(zEnv)

  def waitForMapPhaseCompletion: MIO[Unit] =
    for {
      tasks <- mapTasks.get.commit
      _     <- putStrLn("Checking if all map tasks have completed 🕵")
      _     <- if (tasks.forall(t => t._2.status.isCompleted)) putStrLn(s"${GREEN}All map tasks completed!!! 🎉🎉🎉🎉🎉")
               else assignIncompleteMapTasks *> waitForMapPhaseCompletion.delay(1.second)
    } yield ()

  def assignIncompleteMapTasks: MIO[Unit] =
    for {
      _     <- putStrLn(s"${YELLOW}Looking for incomplete tasks 🥸")
      tasks <- mapTasks.get.commit
      _     <-
        if (tasks.exists(_._2.status.isIdle))
          for {
            _ <- putStrLn(s"${YELLOW}There are incomplete map tasks 😒")
            _ <- assignMapTask.orElse(putStrLn("Failed to assign a task 😵"))
            _ <- assignIncompleteMapTasks
          } yield ()
        else ZIO.succeed(())
    } yield ()

  def assignMapTask: MIO[Unit] =
    assignTask[MapTask, StartMapTaskResponse, StartMapTaskRequest](
      mapTasks,
      StartMapTaskResponse(),
      _.find(_._2.status.isIdle).get,
      (s, t) => StartMapTaskRequest(s, t.inputLocation),
      WorkerServiceClient.startMapTask,
      updateMapTaskState
    )

  def assignTask[T, R, Q](tasksRef: TRef[Map[String, T]], emptyResponse: R, idleF: Map[String, T] => (String, T), requestF: (String, T) => Q, serviceF: Q => ZIO[WorkerServiceClient with Any, Status, R], updateStateF: (String, String) => USTM[Unit]): MIO[Unit] =
    for {
      workers  <- registeredWorkers.get.commit
      available = workers.filter(w => w._2.taskStatus.isIdle || w._2.taskStatus.isCompleted)
      _        <- putStrLn(s"${GREEN}Found ${available.size} available workers! 😮")
      _        <- putStrLn(s"${YELLOW}Attempting to assign a task 😈")
      tasks    <- tasksRef.get.commit
      shuffled <- random.shuffle(available.keys.toList)
      _        <- shuffled.headOption
                    .map(k => (k, available(k)))
                    .fold[MIO[R]](
                      putStrLn(s"${YELLOW}No available workers when trying to assign a reduce task! 😭").as(emptyResponse)
                    ) { case (workerId, workerNode) =>
                      val flow                 = requestF.tupled andThen serviceF
                      val (partitionKey, task) = idleF(tasks)
                      flow((partitionKey, task))
                        .mapError(WorkerClientError)
                        .tapBoth(
                          e => putStrLn(s"$RED 😵 Error when calling the client with status: ${e.status} ☠️"),
                          v =>
                            putStrLn(s"${GREEN}Client responded with $v 🥰") *>
                              updateStateF(workerId, partitionKey).commit <*
                              putStrLn(s"${GREEN}Successfully updated workers and their assigned tasks 💪")
                        )
                        .provideCustomLayer(workerNode.clientLayer)
                    }
    } yield ()

  def updateMapTaskState(workerId: String, taskId: String): USTM[Unit] =
    for {
      _ <- updateWorkerStatusWhenNotComplete(workerId, taskId, TaskType.Map, TaskStatus.IN_PROGRESS)
      _ <- updateMapTaskWhenNotComplete(taskId, TaskStatus.IN_PROGRESS)
    } yield ()

  override def runReducePhase: IO[MasterError, Unit] =
    (for {
      _ <- putStrLn(s"${YELLOW}Starting reduce phase 😎")
      _ <- assignIncompleteReduceTasks
      _ <- waitForReducePhaseCompletion
    } yield ()).provide(zEnv)

  def waitForReducePhaseCompletion: MIO[Unit] =
    for {
      tasks <- reduceTasks.get.commit
      _     <- putStrLn("Checking if all reduce tasks have completed 🕵")
      _     <- if (tasks.forall(t => t._2.status.isCompleted)) putStrLn(s"${GREEN}All reduce tasks completed!!! 🎉🎉🎉🎉🎉")
               else assignIncompleteReduceTasks *> waitForReducePhaseCompletion.delay(1.second)
    } yield ()

  def assignIncompleteReduceTasks: MIO[Unit] =
    for {
      _     <- putStrLn(s"${YELLOW}Checking if there are incomplete tasks 🥸")
      tasks <- reduceTasks.get.commit
      _     <-
        if (tasks.exists(_._2.status.isIdle))
          for {
            _ <- putStrLn(s"${YELLOW}There are still incomplete reduce tasks 😒")
            _ <- assignReduceTask.orElse(putStrLn("Failed to assign a task 😵"))
            _ <- assignIncompleteMapTasks
          } yield ()
        else
          ZIO.succeed(())
    } yield ()

  def assignReduceTask: MIO[Unit] =
    assignTask[ReduceTask, StartReduceTaskResponse, StartReduceTaskRequest](
      reduceTasks,
      StartReduceTaskResponse(),
      _.find(_._2.status.isIdle).get,
      (s, t) => StartReduceTaskRequest(s, t.inputLocations.toList),
      WorkerServiceClient.startReduceTask,
      updateReduceTaskState
    )

  def addReduceTask(r: MapTaskFinished): USTM[Unit] =
    for {
      _ <- reduceTasks.update { m =>
             r.outputPaths.foldLeft(m)((tasks, path) => {
               val key = path.split("/").last.replace(".txt", "")
               if (tasks.contains(key)) tasks + (key -> tasks(key).copy(inputLocations = tasks(key).inputLocations + path))
               else tasks + (key                     -> ReduceTask(key, Set(path), TaskStatus.IDLE))
             })
           }
    } yield ()

  def updateReduceTaskState(workerId: String, partitionKey: String): USTM[Unit] =
    for {
      _ <- updateWorkerStatusWhenNotComplete(workerId, partitionKey, TaskType.Reduce, TaskStatus.IN_PROGRESS)
      _ <- updateReduceTaskWhenNotComplete(partitionKey, TaskStatus.IN_PROGRESS)
    } yield ()
}

object MasterProcessorLiveImpl {
  def apply: URIO[Has[zio.ZEnv], MasterProcessorLiveImpl] =
    for {
      messageQueue      <- Queue.unbounded[MasterProtocol]
      registeredWorkers <- TRef.make(Map.empty[String, WorkerNode]).commit
      env               <- ZIO.service[ZEnv]
      mapTasks          <- TRef.make(MapTasks.mapTasks).commit
      reduceTasks       <- TRef.make(Map.empty[String, ReduceTask]).commit
    } yield new MasterProcessorLiveImpl(
      messageQueue,
      registeredWorkers,
      mapTasks,
      reduceTasks,
      env
    )
}
