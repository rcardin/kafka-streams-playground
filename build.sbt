name := "kafka-streams-playground"

version := "0.1"

scalaVersion := "2.13.6"

idePackagePrefix := Some("in.rcardin.kafka.streams")

val KafkaVersion = "2.8.0"
val CirceVersion = "0.14.1"

libraryDependencies ++= Seq(
  "org.apache.kafka" %  "kafka-clients"       % KafkaVersion,
  "org.apache.kafka" %  "kafka-streams"       % KafkaVersion,
  "org.apache.kafka" %% "kafka-streams-scala" % KafkaVersion,
  "io.circe"         %% "circe-core"          % CirceVersion,
  "io.circe"         %% "circe-generic"       % CirceVersion,
  "io.circe"         %% "circe-parser"        % CirceVersion
)