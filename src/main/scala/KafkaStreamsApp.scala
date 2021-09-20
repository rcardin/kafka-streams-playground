package in.rcardin.kafka.streams


import io.circe.generic.auto._
import io.circe.parser._
import io.circe.syntax._
import io.circe.{Decoder, Encoder}
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
//   --bootstrap-server localhost:9092 \
//   --topic orders-by-user \
//   --create
//
// kafka-topics \
//   --bootstrap-server localhost:9092 \
//   --topic discount-profiles-by-user \
//   --create \
//   --config "cleanup.policy=compact"
//
// kafka-topics \
//   --bootstrap-server localhost:9092 \
//   --topic discounts \
//   --create \
//   --config "cleanup.policy=compact"
//
//  Producing some messages
//  -----------------------
//  TODO
object KafkaStreamsApp {

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

  // Topics
  final val OrdersByUserTopic = "orders-by-user"
  final val DiscountProfilesByUserTopic = "discount-profiles-by-user"
  final val Discounts = "discounts"

  type User = String
  type Profile = String
  type Product = String

  case class Order(user: String, products: List[Product], amount: Double)

  object Order {
    implicit val requestSerde: Serde[Order] = serde[Order]
  }

  // Discounts profiles are a (String, String) topic

  case class Discount(profile: Profile, discount: Double)

  object Discount {
    implicit val authoredPathsSerde: Serde[Discount] = serde[Discount]
  }

  val builder = new StreamsBuilder

  val ordersStreams: KStream[User, Order] = builder.stream[User, Order](OrdersByUserTopic)

  val userProfilesTable: KTable[User, Profile] =
    builder.table[User, Profile](DiscountProfilesByUserTopic)

  val discountProfilesGTable: GlobalKTable[Profile, Discount] =
    builder.globalTable[Profile, Discount](Discounts)

  val ordersWithUserProfileStream: KStream[User, (Order, Profile)] =
    ordersStreams.join[Profile, (Order, Profile)](userProfilesTable) { (order, profile) =>
      (order, profile)
    }

  val discountedOrdersStream: KStream[User, Order] =
    ordersWithUserProfileStream.join[Profile, Discount, Order](discountProfilesGTable)(
      { case (_, (_, profile)) => profile },  // Joining key
      { case ((order, _), discount) => order.copy(amount = order.amount * discount) }
    )

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
