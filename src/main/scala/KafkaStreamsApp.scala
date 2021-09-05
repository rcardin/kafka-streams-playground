package in.rcardin.kafka.streams

import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.KStream

// Creation of the topic:
// kafka-topics \
//  --bootstrap-server localhost:9092 \
//  --topic navigation \
//  --create
object KafkaStreamsApp {

  val NavigationTopic = "navigation"

  case class Navigation(user: String, page: String, millis: Long)

  val builder = new StreamsBuilder

  val source: KStream[String, Navigation] = builder.stream(NavigationTopic)


  def main(args: Array[String]): Unit = {

  }
}
