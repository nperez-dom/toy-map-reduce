addSbtPlugin("com.thesamet"  % "sbt-protoc"   % "1.0.0-RC4")
addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.4.0")
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.10")

libraryDependencies +=
  "com.thesamet.scalapb.zio-grpc" %% "zio-grpc-codegen" % "0.4.4"
