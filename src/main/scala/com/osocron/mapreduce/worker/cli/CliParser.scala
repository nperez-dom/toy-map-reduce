package com.osocron.mapreduce.worker.cli

import scopt.{OParser, OParserBuilder}

object CliParser {

  val builder: OParserBuilder[CliConfig] = OParser.builder[CliConfig]
  val parser: OParser[Unit, CliConfig] = {
    import builder._
    OParser.sequence(
      programName("😛 map-reduce"),
      head("mapreduce", "v0.0.1"),
      opt[Int]('p', "port")
        .required()
        .action((x, c) => c.copy(port = x))
        .text("Port to bind to"),
      opt[String]('i', "id")
        .required()
        .action((x, c) => c.copy(id = x))
        .text("Id of the worker node"),
      help("help").text("Toy mapreduce app")
    )

  }

}
