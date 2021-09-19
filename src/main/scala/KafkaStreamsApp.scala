package in.rcardin.kafka.streams


import io.circe.{Decoder, Encoder}
import io.circe.generic.auto._
import io.circe.parser._
import io.circe.syntax._
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream.{KStream, KTable}
import org.apache.kafka.streams.scala.serialization.Serdes
import org.apache.kafka.streams.scala.serialization.Serdes._
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig, Topology}

import java.util.Properties

// Creation of the topic:
// ----------------------
//
// kafka-topics \
//  --bootstrap-server localhost:9092 \
//  --topic jobs \
//  --create
//
// kafka-topics \
//  --bootstrap-server localhost:9092 \
//  --topic permissions \
//  --create \
//  --config "cleanup.policy=compact"
//
//  Producing some messages
//  -----------------------
//
//  kafka-console-producer \
//   --topic jobs \
//   --broker-list localhost:9092 \
//   --property parse.key=true \
//   --property key.separator=,
//   user1,{"user": "user1", "name": "print", "params": {}}
object KafkaStreamsApp {

  val JobsTopic: String = "jobs"
  val PermissionsTopic: String = "permissions"

  def serde[A >: Null : Decoder : Encoder]: Serde[A] = {
    val serializer = (a: A) => a.asJson.noSpaces.getBytes
    val deserializer = (aAsBytes: Array[Byte]) => {
      val aAsString = new String(aAsBytes)
      val aOrError = decode[A](aAsString)
      aOrError match {
        case Right(a) => Option(a)
        case Left(error) =>
          println(s"There was an error converting the message $aOrError, $error")
          Option.empty
      }
    }
    Serdes.fromFn[A](serializer, deserializer)
  }

  // Can we implement a type class?
  case class Job(user: String, name: String, params: Map[String, String])

  object Job {
    implicit val jobSerde: Serde[Job] = serde[Job]
  }

  case class Permissions(permissions: List[String])

  object Permissions {
    implicit val permissionsSerde: Serde[Permissions] = serde[Permissions]
  }

  case class AuthoredJob(job: Job, permissions: Permissions)

  object AuthoredJob {
    implicit val authoredJobSerde: Serde[AuthoredJob] = serde[AuthoredJob]
  }

  val builder = new StreamsBuilder

  val source: KStream[String, Job] =
    builder.stream[String, Job](JobsTopic)

  val permissionsTable: KTable[String, Permissions] = builder.table[String, Permissions](PermissionsTopic)

  val authoredJobs: KStream[String, AuthoredJob] =
    source.join[Permissions, AuthoredJob](permissionsTable) { (job: Job, permissions: Permissions) =>
      AuthoredJob(job, permissions)
    }

  authoredJobs.foreach { (user, authoredJob) =>
    println(s"The user $user was authored to job $authoredJob")
  }

  val topology: Topology = builder.build()

  def main(args: Array[String]): Unit = {
    val props = new Properties
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "scheduler")
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.stringSerde.getClass)

    println(topology.describe())

    val application: KafkaStreams = new KafkaStreams(topology, props)
    application.start()
  }
}
