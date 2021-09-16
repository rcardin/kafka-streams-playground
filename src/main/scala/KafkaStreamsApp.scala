package in.rcardin.kafka.streams

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

  // Can we implement a type class?
  case class Job(user: String, name: String, params: Map[String, String])
  object Job {
    implicit val jobSerde: Serde[Job] = {
      val serializer = (job: Job) => job.asJson.noSpaces.getBytes
      val deserializer = (jobAsBytes: Array[Byte]) => {
        val jobAsString = new String(jobAsBytes)
        val jobOrError = decode[Job](jobAsString)
        jobOrError match {
          case Right(job) => Option(job)
          case Left(error) =>
            println(s"There was an error converting the message $jobOrError, $error")
            Option.empty
        }
      }
      Serdes.fromFn[Job](serializer, deserializer)
    }
  }

  case class Permissions(permissions: List[String])
  object Permissions {
    implicit val permissionsSerde: Serde[Permissions] = {
      val serializer = (permissions: Permissions) => permissions.asJson.noSpaces.getBytes
      val deserializer = (permissionsAsBytes: Array[Byte]) => {
        val permissionsAsString = new String(permissionsAsBytes)
        val permissionsOrError = decode[Permissions](permissionsAsString)
        permissionsOrError match {
          case Right(permissions) => Option(permissions)
          case Left(error) =>
            println(s"There was an error converting the message $permissionsOrError, $error")
            Option.empty
        }
      }
      Serdes.fromFn[Permissions](serializer, deserializer)
    }
  }

  case class AuthoredJob(job: Job, permissions: Permissions)
  object AuthoredJob {
    implicit val authoredJobSerde: Serde[AuthoredJob] = {
      val serializer = (authoredJob: AuthoredJob) => authoredJob.asJson.noSpaces.getBytes
      val deserializer = (authoredJobAsBytes: Array[Byte]) => {
        val authoredJobAsString = new String(authoredJobAsBytes)
        val authoredJobOrError = decode[AuthoredJob](authoredJobAsString)
        authoredJobOrError match {
          case Right(authoredJob) => Option(authoredJob)
          case Left(error) =>
            println(s"There was an error converting the message $authoredJobAsString, $error")
            Option.empty
        }
      }
      Serdes.fromFn[AuthoredJob](serializer, deserializer)
    }
  }

  val builder = new StreamsBuilder

  val source: KStream[String, Job] =
    builder.stream[String, Job](JobsTopic).peek((k, v) => println(s"($k, $v)"))

  val permissionsTable: KTable[String, Permissions] = builder.table(PermissionsTopic)

  val authoredJobs: KStream[String, AuthoredJob] =
    source.join(permissionsTable) { (job: Job, permissions: Permissions) =>
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
