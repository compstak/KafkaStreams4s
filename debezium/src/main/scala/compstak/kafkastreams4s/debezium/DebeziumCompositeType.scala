package compstak.kafkastreams4s.debezium

import compstak.circe.debezium.DebeziumFieldSchema

/**
 * An interfact for turning product types into a "struct" representation for the Debezium schema type.
 * The schema abstract method should represent all the fields of the product type `A`.
 */
trait DebeziumCompositeType[A] {
  def schema: List[DebeziumFieldSchema]
}
