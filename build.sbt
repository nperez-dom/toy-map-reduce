ThisBuild / scalaVersion := "2.13.5"
ThisBuild / organization := "com.geophy"

lazy val mapReduce = (project in file("."))
  .settings(
    name := "toy-map-reduce",
    scalacOptions += "-Ymacro-annotations",
    libraryDependencies ++= commonDeps,
    mainClass in assembly := Some("com.geophy.mapreduce.master.MasterApp"),
    assemblyJarName in assembly := "master.jar"
  )

lazy val commonDeps = Seq(
  "io.grpc"               % "grpc-netty"           % "1.36.0",
  "com.thesamet.scalapb" %% "scalapb-runtime-grpc" % scalapb.compiler.Version.scalapbVersion,
  "com.github.scopt"     %% "scopt"                % "4.0.0-RC2",
  "io.github.kitlangton" %% "zio-magic"            % "0.1.12"
)

PB.targets in Compile := Seq(
  scalapb.gen(grpc = true)          -> (sourceManaged in Compile).value,
  scalapb.zio_grpc.ZioCodeGenerator -> (sourceManaged in Compile).value
)

assemblyShadeRules in assembly := Seq(ShadeRule.rename("shapeless.**" -> "new_shapeless.@1").inAll)
assemblyMergeStrategy in assembly := {
  case PathList("org", "aopalliance", xs @ _*)      => MergeStrategy.last
  case PathList("javax", "inject", xs @ _*)         => MergeStrategy.last
  case PathList("javax", "servlet", xs @ _*)        => MergeStrategy.last
  case PathList("javax", "activation", xs @ _*)     => MergeStrategy.last
  case PathList("org", "apache", xs @ _*)           => MergeStrategy.last
  case PathList("com", "google", xs @ _*)           => MergeStrategy.last
  case PathList("com", "esotericsoftware", xs @ _*) => MergeStrategy.last
  case PathList("com", "codahale", xs @ _*)         => MergeStrategy.last
  case PathList("com", "yammer", xs @ _*)           => MergeStrategy.last
  case "*.properties"                               => MergeStrategy.last
  case "application.conf"                           => MergeStrategy.concat
  case "reference.conf"                             => MergeStrategy.concat
  case "about.html"                                 => MergeStrategy.rename
  case PathList("META-INF", xs @ _*)                =>
    xs map { _.toLowerCase } match {
      case "manifest.mf" :: Nil | "index.list" :: Nil | "dependencies" :: Nil  =>
        MergeStrategy.discard
      case ps @ x :: xs if ps.last.endsWith(".sf") || ps.last.endsWith(".dsa") =>
        MergeStrategy.discard
      case "plexus" :: xs                                                      =>
        MergeStrategy.discard
      case "services" :: xs                                                    =>
        MergeStrategy.filterDistinctLines
      case "spring.schemas" :: Nil | "spring.handlers" :: Nil                  =>
        MergeStrategy.filterDistinctLines
      case _                                                                   => MergeStrategy.first
    }
  case _                                            => MergeStrategy.first
}
