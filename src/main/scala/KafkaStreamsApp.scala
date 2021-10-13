package in.rcardin.kafka.streams


import io.circe.generic.auto._
import io.circe.parser._
import io.circe.syntax._
import io.circe.{Decoder, Encoder}
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.kstream.{GlobalKTable, JoinWindows, TimeWindows, Windowed}
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream.{KGroupedStream, KStream, KTable}
import org.apache.kafka.streams.scala.serialization.Serdes
import org.apache.kafka.streams.scala.serialization.Serdes._
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig, Topology}

import java.time.Duration
import java.time.temporal.ChronoUnit
import java.util.Properties
import scala.concurrent.duration.DurationInt
import scala.jdk.DurationConverters._

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
// kafka-topics \
//   --bootstrap-server localhost:9092 \
//   --topic orders \
//   --create
//
// kafka-topics \
//   --bootstrap-server localhost:9092 \
//   --topic payments \
//   --create
//
// kafka-topics \
//   --bootstrap-server localhost:9092 \
//   --topic paid-orders \
//   --create
object KafkaStreamsApp {

  object Implicits {
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
  }

  // Topics
  object Topics {
    final val OrdersByUserTopic = "orders-by-user"
    final val DiscountProfilesByUserTopic = "discount-profiles-by-user"
    final val DiscountsTopic = "discounts"
    final val OrdersTopic = "orders"
    final val PaymentsTopic = "payments"
    final val PaidOrdersTopic = "paid-orders"
  }

  object Domain {
    type UserId = String
    type Profile = String
    type Product = String
    type OrderId = String

    case class Order(orderId: OrderId, user: UserId, products: List[Product], amount: Double)

    case class Discount(profile: Profile, amount: Double)

    case class Payment(orderId: OrderId, status: String)
  }
  val builder = new StreamsBuilder

  import Implicits._
  import Domain._
  import Topics._

  val usersOrdersStreams: KStream[UserId, Order] = builder.stream[UserId, Order](OrdersByUserTopic)

  def expensiveOrdersTopology(): Unit = {
    val expensiveOrders: KStream[UserId, Order] = usersOrdersStreams.filter { (_, order) =>
      order.amount >= 1000
    }
    expensiveOrders.to("suspicious-orders")
  }

  def purchasedListOfProductsTopology(): Unit = {
    val purchasedListOfProductsStream: KStream[UserId, List[Product]] = usersOrdersStreams.mapValues { order =>
      order.products
    }
    purchasedListOfProductsStream.foreach(println)
  }

  def purchasedProductsByFirstLetterTopology(): Unit = {
    val purchasedProductsStream: KStream[UserId, Product] = usersOrdersStreams.flatMapValues { order =>
      order.products
    }

    val purchasedByFirstLetter: KGroupedStream[String, Product] =
      purchasedProductsStream.groupBy[String] { (userId, products) =>
        userId.charAt(0).toLower.toString
      }

    purchasedProductsStream.foreach { (userId, product) =>
      println(s"The user $userId purchased the product $product")
    }
  }

  def numberOfProductsByUserEveryTenSecondsTopology(): Unit = {
    val purchasedProductsStream: KStream[UserId, Product] = usersOrdersStreams.flatMapValues { order =>
      order.products
    }

    val productsPurchasedByUsers: KGroupedStream[UserId, Product] = purchasedProductsStream.groupByKey

    val everyTenSeconds: TimeWindows = TimeWindows.of(10.second.toJava)

    val numberOfProductsByUser: KTable[UserId, Long] = productsPurchasedByUsers.count()

    val numberOfProductsByUserEveryTenSeconds: KTable[Windowed[UserId], Long] =
      productsPurchasedByUsers.windowedBy(everyTenSeconds)
        .aggregate[Long](0L) { (userId, product, counter) => counter + 1 }

    numberOfProductsByUserEveryTenSeconds.toStream.foreach(println)
  }

  def paidOrdersTopology(): Unit = {
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

    val ordersStream: KStream[OrderId, Order] = discountedOrdersStream.selectKey { (_, order) => order.orderId }

    val paymentsStream: KStream[OrderId, Payment] = builder.stream[OrderId, Payment](PaymentsTopic)

    val paidOrders: KStream[OrderId, Order] = {

      val joinOrdersAndPayments = (order: Order, payment: Payment) =>
        if (payment.status == "PAID") Option(order) else Option.empty[Order]

      val joinWindow = JoinWindows.of(Duration.of(5, ChronoUnit.MINUTES))

      ordersStream.join[Payment, Option[Order]](paymentsStream)(joinOrdersAndPayments, joinWindow)
        .flatMapValues(maybeOrder => maybeOrder.toIterable)
    }

    paidOrders.to(PaidOrdersTopic)
  }

  def main(args: Array[String]): Unit = {
    val props = new Properties
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "orders-application")
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.stringSerde.getClass)

    paidOrdersTopology()

    val topology: Topology = builder.build()

    println(topology.describe())

    val application: KafkaStreams = new KafkaStreams(topology, props)
    application.start()
  }
}
