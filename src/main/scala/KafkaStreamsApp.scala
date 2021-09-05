package in.rcardin.kafka.streams

// Creation of the topic:
// kafka-topics \
//  --bootstrap-server localhost:9092 \
//  --topic navigations \
//  --create
object KafkaStreamsApp {

  case class Navigation(user: String, page: String, millis: Long)

  def main(args: Array[String]): Unit = {

  }
}
