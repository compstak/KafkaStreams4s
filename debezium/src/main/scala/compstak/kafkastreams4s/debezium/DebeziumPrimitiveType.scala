package compstak.kafkastreams4s.debezium

import java.{util => ju}
import compstak.circe.debezium.DebeziumSchemaPrimitive
import compstak.circe.debezium.DebeziumFieldSchema

/**
 * A typeclass for turning types into a primitive representation for the Debezium schema type.
 * For reference check here: https://debezium.io/documentation/reference/1.2/connectors/postgresql.html#postgresql-data-types
 */
trait DebeziumPrimitiveType[A] {
  def debeziumType: DebeziumSchemaPrimitive
}

object DebeziumPrimitiveType {
  def fromPrimitive[A](s: DebeziumSchemaPrimitive): DebeziumPrimitiveType[A] = new DebeziumPrimitiveType[A] {
    def debeziumType: DebeziumSchemaPrimitive = s
  }

  def apply[A: DebeziumPrimitiveType]: DebeziumPrimitiveType[A] = implicitly

  implicit val booleanType: DebeziumPrimitiveType[Boolean] = fromPrimitive(DebeziumSchemaPrimitive.Boolean)
  implicit val intType: DebeziumPrimitiveType[Int] = fromPrimitive(DebeziumSchemaPrimitive.Int32)
  implicit val shortType: DebeziumPrimitiveType[Short] = fromPrimitive(DebeziumSchemaPrimitive.Int16)
  implicit val longType: DebeziumPrimitiveType[Long] = fromPrimitive(DebeziumSchemaPrimitive.Int64)
  implicit val doubleType: DebeziumPrimitiveType[Double] = fromPrimitive(DebeziumSchemaPrimitive.Float64)
  implicit val stringType: DebeziumPrimitiveType[String] = fromPrimitive(DebeziumSchemaPrimitive.String)
  implicit val uuidType: DebeziumPrimitiveType[ju.UUID] = fromPrimitive(DebeziumSchemaPrimitive.String)

}
