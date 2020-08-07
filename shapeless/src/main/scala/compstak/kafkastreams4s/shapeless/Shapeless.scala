package compstak.kafkastreams4s

import _root_.shapeless._, labelled._

import compstak.circe.debezium.DebeziumFieldSchema
import compstak.kafkastreams4s.debezium.{DebeziumCompositeType, DebeziumPrimitiveType}

package object shapeless extends LowPriorityImplicits {

  /**
   * The DebeziumCompositeType instance for an empty extensible record
   */
  implicit val hnilDCT = new DebeziumCompositeType[HNil] { def schema = Nil }

  /**
   * The DebeziumCompositeType instance for a record element with a symbol key and optional DebeziumPrimitiveType value
   */
  implicit def hconsOptionDCT[K <: Symbol, V, T <: HList](implicit
    witK: Witness.Aux[K],
    dbT: DebeziumPrimitiveType[V],
    dctT: DebeziumCompositeType[T]
  ): DebeziumCompositeType[FieldType[K, Option[V]] :: T] =
    new DebeziumCompositeType[FieldType[K, Option[V]] :: T] {
      def schema: List[DebeziumFieldSchema] =
        DebeziumCompositeType.fromPrimitiveOption(witK.value.name).schema ++ dctT.schema
    }
}

trait LowPriorityImplicits {

  /**
   * The DebeziumCompositeType instance for a record element with a symbol key and DebeziumPrimitiveType value
   */
  implicit def hconsDCT[K <: Symbol, V, T <: HList](implicit
    witK: Witness.Aux[K],
    dbT: DebeziumPrimitiveType[V],
    dctT: DebeziumCompositeType[T]
  ): DebeziumCompositeType[FieldType[K, V] :: T] =
    new DebeziumCompositeType[FieldType[K, V] :: T] {
      def schema: List[DebeziumFieldSchema] =
        DebeziumCompositeType.fromPrimitive(witK.value.name).schema ++ dctT.schema
    }
}
