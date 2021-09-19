package in.rcardin.kafka.streams


import io.circe.{Decoder, Encoder}
import io.circe.generic.auto._
import io.circe.parser._
import io.circe.syntax._
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.kstream.GlobalKTable
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
  val UserClicks: String = "clicks"

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

  // ------------------------

  // Topics
  final val RequestsTopic = "requests"
  final val TokensTopic = "tokens"
  final val RolesTopic = "roles"

  // Value classes
  type Role = String
  type User = String

  case class Request(user: String, path: String, role: Role)
  object Request {
    implicit val requestSerde: Serde[Request] = serde[Request]
  }

  case class AuthoredPaths(role: Role, paths: List[String])
  object AuthoredPaths {
    implicit val authoredPathsSerde: Serde[AuthoredPaths] = serde[AuthoredPaths]
  }

  val builder = new StreamsBuilder

  val requestsStreams: KStream[User, Request] = builder.stream[User, Request](RequestsTopic)

  val rolesTable: KTable[User, Role] = builder.table[User, Role](TokensTopic)

  val rolesToPathsGTable: GlobalKTable[Role, AuthoredPaths] =
    builder.globalTable[Role, AuthoredPaths](RolesTopic)

  val authoredRequests: KStream[User, Request] = requestsStreams.join[Role, (Request, Role)](rolesTable) { (request, role) =>
    (request, role)
  }.filter { case (_, (request, role)) =>
    request.role == role
  }.mapValues(value => value._1)

  // ------------------------

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

  val source: KStream[String, Job] =
    builder.stream[String, Job](JobsTopic)

  val permissionsTable: KTable[String, Permissions] = builder.table[String, Permissions](PermissionsTopic)

  val authoredJobsStream: KStream[String, AuthoredJob] =
    source.join[Permissions, AuthoredJob](permissionsTable) { (job: Job, permissions: Permissions) =>
      AuthoredJob(job, permissions)
    }

  val usersClicksStream: KStream[String, Int] = builder.stream[String, Int](UserClicks)

  // authoredJobsStream.join[Int, Int](usersClicksStream)()

  authoredJobsStream.foreach { (user, authoredJob) =>
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
