package in.rcardin.kafka.streams


import io.circe.generic.auto._
import io.circe.parser._
import io.circe.syntax._
import io.circe.{Decoder, Encoder}
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.kstream.{GlobalKTable, JoinWindows}
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream.{KGroupedStream, KStream, KTable}
import org.apache.kafka.streams.scala.serialization.Serdes
import org.apache.kafka.streams.scala.serialization.Serdes._
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig, Topology}

import java.time.Duration
import java.time.temporal.ChronoUnit
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

  implicit def serde[A >: Null : Decoder : Encoder]: Serde[A] = {
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
  final val DiscountsTopic = "discounts"
  final val OrdersTopic = "orders"
  final val PaymentsTopic = "payments"
  final val PayedOrdersTopic = "payed-orders"

  type UserId = String
  type Profile = String
  type Product = String
  type OrderId = String

  case class Order(orderId: OrderId, user: UserId, products: List[Product], amount: Double)

  // Discounts profiles are a (String, String) topic

  case class Discount(profile: Profile, amount: Double)

  case class Payment(orderId: OrderId, status: String)

  val builder = new StreamsBuilder

  val usersOrdersStreams: KStream[UserId, Order] = builder.stream[UserId, Order](OrdersByUserTopic)

  val expensiveOrders: KStream[UserId, Order] = usersOrdersStreams.filter { (_, order) =>
    order.amount >= 1000
  }

  expensiveOrders.to("suspicious-orders")

  val purchasedListOfProductsStream: KStream[UserId, List[Product]] = usersOrdersStreams.mapValues { order =>
    order.products
  }

  val purchasedProductsStream: KStream[UserId, Product] = usersOrdersStreams.flatMapValues { order =>
    order.products
  }

  val purchasedByFirstLetter: KGroupedStream[String, Product] =
    purchasedProductsStream.groupBy[String] { (userId, products) =>
      userId.charAt(0).toLower.toString
    }

  val productsPurchasedByUsers: KGroupedStream[UserId, Product] = purchasedProductsStream.groupByKey
  val numberOfProductsByUser: KTable[UserId, Long] = productsPurchasedByUsers.count()

  purchasedProductsStream.foreach { (userId, product) =>
    println(s"The user $userId purchased the product $product")
  }

  val userProfilesTable: KTable[UserId, Profile] =
    builder.table[UserId, Profile](DiscountProfilesByUserTopic)

  val discountProfilesGTable: GlobalKTable[Profile, Discount] =
    builder.globalTable[Profile, Discount](DiscountsTopic)

  val ordersWithUserProfileStream: KStream[UserId, (Order, Profile)] =
    usersOrdersStreams.join[Profile, (Order, Profile)](userProfilesTable) { (order, profile) =>
      (order, profile)
    }

  val discountedOrdersStream: KStream[UserId, Order] =
    ordersWithUserProfileStream.join[Profile, Discount, Order](discountProfilesGTable)(
      { case (_, (_, profile)) => profile }, // Joining key
      { case ((order, _), discount) => order.copy(amount = order.amount * discount.amount) }
    )

  discountedOrdersStream.selectKey { (_, order) => order.orderId }.to(OrdersTopic)

  val ordersStream: KStream[OrderId, Order] = builder.stream[OrderId, Order](OrdersTopic)

  val paymentsStream: KStream[OrderId, Payment] = builder.stream[OrderId, Payment](PaymentsTopic)

  val payedOrders: KStream[OrderId, Order] = {

    val joinOrdersAndPayments = (order: Order, payment: Payment) =>
      if (payment.status == "PAYED") Option(order) else Option.empty[Order]

    val joinWindow = JoinWindows.of(Duration.of(5, ChronoUnit.MINUTES))

    ordersStream.join[Payment, Option[Order]](paymentsStream)(joinOrdersAndPayments, joinWindow)
      .flatMapValues(maybeOrder => maybeOrder.toIterable)
  }

  payedOrders.to(PayedOrdersTopic)

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
