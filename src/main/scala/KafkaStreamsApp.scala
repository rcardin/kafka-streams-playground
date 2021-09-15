package in.rcardin.kafka.streams

import io.circe.generic.auto._
import io.circe.parser._
import io.circe.syntax._
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.kstream.{KStream, KTable}
import org.apache.kafka.streams.scala.kstream.{Consumed, Materialized}
import org.apache.kafka.streams.scala.serialization.Serdes
import org.apache.kafka.streams.state.KeyValueStore
import org.apache.kafka.streams.{KafkaStreams, StreamsBuilder, StreamsConfig, Topology}

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

  implicit val stringSerde: Serde[String] = Serdes.stringSerde

  // Can we implement a type class?

  case class Job(user: String, name: String, params: Map[String, String])
  object Job {
    implicit val jobSerde: Serde[Job] = {
      val serializer = (job: Job) => job.asJson.noSpaces.getBytes
      val deserializer = (jobAsBytes: Array[Byte]) => {
        val jobAsString = new String(jobAsBytes)
        decode[Job](jobAsString).toOption
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
        val result = decode[Permissions](permissionsAsString)
        result.toOption
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
        decode[AuthoredJob](authoredJobAsString).toOption
      }
      Serdes.fromFn[AuthoredJob](serializer, deserializer)
    }
  }

  val builder = new StreamsBuilder

  val source: KStream[String, Job] =
    builder.stream(JobsTopic, Consumed.`with`[String, Job]).peek((k, v) => println(s"($k, $v)"))

  val permissionsTable: KTable[String, Permissions] = builder.table(
    PermissionsTopic,
    Materialized.`with`[String, Permissions, KeyValueStore[Bytes, Array[Byte]]]
  )

  val authoredJobs: KStream[String, String] = source.join(
    permissionsTable,
    (job: Job, permissions: Permissions) => s"($job, $permissions)"
  ).peek((k, v) => println(s"($k, $v)"))

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
