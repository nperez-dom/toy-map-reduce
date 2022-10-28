package com.osocron.mapreduce.master

import io.grpc.Status

trait MasterError
case class ServiceError(msg: String, throwable: Throwable) extends MasterError
case class WorkerRegistrationError(msg: String)            extends MasterError
case object NodeUnresponsiveError                          extends MasterError
case class WorkerClientError(status: Status)               extends MasterError
case object NoAvailableWorkersError                        extends MasterError
