package in.rcardin.kafka.streams

import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.{KStream, KTable, Materialized}
import org.apache.kafka.streams.scala.serialization.Serdes
import org.apache.kafka.streams.state.KeyValueStore

// Creation of the topic:
// kafka-topics \
//  --bootstrap-server localhost:9092 \
//  --topic jobs \
//  --create
//
// kafka-topics \
// --bootstrap-server localhost:9092 \
// --topic permissions \
// --create
// --config "cleanup.policy=compact"
object KafkaStreamsApp {

  val Jobs: String = "jobs"
  val Permissions: String = "permissions"

  case class Job(user: String, params: Map[String, String])

  case class AuthoredJob(job: Job, permissions: String)

  val builder = new StreamsBuilder

  val source: KStream[String, Job] = builder.stream(Jobs)

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

  def main(args: Array[String]): Unit = {

  }
}
