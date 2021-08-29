name := "kafka-streams-playground"

version := "0.1"

scalaVersion := "2.13.6"

idePackagePrefix := Some("in.rcardin.kafka.streams")

libraryDependencies ++= Seq(
  "org.apache.kafka" %  "kafka-clients"        % "2.8.0",
  "org.apache.kafka" %  "kafka-streams"        % "2.8.0",
  "org.apache.kafka" %% "kafka-streams-scala"  % "2.8.0"
)
