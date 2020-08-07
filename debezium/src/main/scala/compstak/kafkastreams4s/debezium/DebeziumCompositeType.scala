package compstak.kafkastreams4s.debezium

import compstak.circe.debezium.DebeziumFieldSchema

/**
 * An interface for turning product types into a "struct" representation for the Debezium schema type.
 * The schema abstract method should represent all the fields of the product type `A`.
 */
trait DebeziumCompositeType[A] {
  def schema: List[DebeziumFieldSchema]
}

object DebeziumCompositeType {

  /**
   * Returns the DebeziumCompositeType of a value
   */
  def of[A](value: A)(implicit dctA: DebeziumCompositeType[A]): DebeziumCompositeType[A] = dctA

  /**
   * Returns a simple debezium "struct" schema with a single field
   */
  def fromPrimitive[A: DebeziumPrimitiveType](fieldName: String): DebeziumCompositeType[A] =
    new DebeziumCompositeType[A] {
      def schema: List[DebeziumFieldSchema] =
        List(DebeziumFieldSchema(DebeziumPrimitiveType[A].debeziumType, false, fieldName))
    }

  /**
   * Returns a simple debezium "struct" schema with a single optional field
   */
  def fromPrimitiveOption[A: DebeziumPrimitiveType](fieldName: String): DebeziumCompositeType[A] =
    new DebeziumCompositeType[A] {
      def schema: List[DebeziumFieldSchema] =
        List(DebeziumFieldSchema(DebeziumPrimitiveType[A].debeziumType, true, fieldName))
    }
}
