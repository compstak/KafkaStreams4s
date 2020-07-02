package compstak.kafkastreams4s.debezium

import java.{util => ju}

/**
 * A typeclass for turning types into a String representation for the Debezium schema type.
 * For reference check here: https://debezium.io/documentation/reference/1.2/connectors/postgresql.html#postgresql-data-types
 */
trait DebeziumType[A] {
  def debeziumType: String
}

object DebeziumType {
  def fromString[A](s: String): DebeziumType[A] = new DebeziumType[A] {
    def debeziumType: String = s
  }

  def apply[A: DebeziumType]: DebeziumType[A] = implicitly

  implicit val intType: DebeziumType[Int] = fromString("INT32")
  implicit val shortType: DebeziumType[Short] = fromString("INT16")
  implicit val longType: DebeziumType[Long] = fromString("INT64")
  implicit val stringType: DebeziumType[String] = fromString("STRING")
  implicit val uuidType: DebeziumType[ju.UUID] = fromString("STRING")

}
