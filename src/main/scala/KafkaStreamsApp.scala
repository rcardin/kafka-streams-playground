package in.rcardin.kafka.streams

import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.{KafkaStreams, StreamsBuilder, StreamsConfig, Topology}
import org.apache.kafka.streams.kstream.{KStream, KTable, Materialized}
import org.apache.kafka.streams.scala.serialization.Serdes
import org.apache.kafka.streams.state.KeyValueStore

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
//   user1,{"name": "user1", "name": "print", "params": {}}
object KafkaStreamsApp {

  val Jobs: String = "jobs"
  val Permissions: String = "permissions"

  case class Job(user: String, name: String, params: Map[String, String])

  case class AuthoredJob(job: Job, permissions: String)

  val builder = new StreamsBuilder

  val source: KStream[String, Job] = builder.stream(Jobs).peek { (user, job) => println(job) }

  val permissionsTable: KTable[String, String] = builder.table(
    Permissions,
    Materialized
      .as[String, String, KeyValueStore[Bytes, Array[Byte]]]("permissions-store")
      .withKeySerde(Serdes.stringSerde)
      .withValueSerde(Serdes.stringSerde)
  )

  val authoredJobs: KStream[String, AuthoredJob] = source.join(
    permissionsTable,
    (job, permissions) => AuthoredJob(job, permissions)
  )

  authoredJobs.foreach { (user, authoredJob) =>
    println(s"The user $user, with roles ${authoredJob.permissions}, requested to execute job ${authoredJob.job.name}")
  }

  val topology: Topology = builder.build();

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
