package compstak.kafkastreams4s.debezium

import org.apache.kafka.streams.{KeyValue, StreamsBuilder}
import org.apache.kafka.streams.kstream.{Consumed, Initializer, KTable, Produced, ValueMapper, ValueMapperWithKey}
import org.apache.kafka.common.serialization.Serde
import compstak.kafkastreams4s.circe.CirceSerdes._
import compstak.circe.debezium.{DebeziumKey, DebeziumValue}
import io.circe.{Decoder, Encoder}
import cats.{Functor, FunctorFilter}
import cats.effect.Sync
import compstak.kafkastreams4s.circe.CirceTable

case class DebeziumTable[K: Encoder: Decoder, V: Encoder: Decoder](
  topicName: String,
  idNameOrKeySchema: Either[String, DebeziumCompositeType[K]],
  toCirceTable: CirceTable[DebeziumKey[K], V]
) {

  def to[F[_]: Sync](outputTopicName: String): F[Unit] =
    toCirceTable.to(outputTopicName)

  def map[V2: Encoder: Decoder](f: V => V2): DebeziumTable[K, V2] =
    copy(toCirceTable = toCirceTable.map(f))

  def mapWithKey[V2: Encoder: Decoder](f: (DebeziumKey[K], V) => V2): DebeziumTable[K, V2] =
    copy(toCirceTable = toCirceTable.mapWithKey(f))

  def mapFilter[V2: Encoder: Decoder](f: V => Option[V2]): DebeziumTable[K, V2] =
    copy(toCirceTable = toCirceTable.mapFilter(f))

  def mapFilterWithKey[V2: Encoder: Decoder](f: (DebeziumKey[K], V) => Option[V2]): DebeziumTable[K, V2] =
    copy(toCirceTable = toCirceTable.mapFilterWithKey(f))

  def join[K2: DebeziumPrimitiveType, V2, Z: Encoder: Decoder](
    other: DebeziumTable[K2, V2]
  )(f: V => K2)(g: (V, V2) => Z): DebeziumTable[K, Z] =
    other.idNameOrKeySchema match {
      case Left(idName) =>
        copy(toCirceTable = JoinTables.join(toCirceTable, other.toCirceTable, idName, other.topicName)(f)(g))
      case Right(schema) =>
        copy(toCirceTable = JoinTables.joinComposite(toCirceTable, other.toCirceTable, schema, other.topicName)(f)(g))
    }

  def joinOption[K2: DebeziumPrimitiveType, V2, Z: Encoder: Decoder](
    other: DebeziumTable[K2, V2]
  )(f: V => Option[K2])(g: (V, V2) => Z): DebeziumTable[K, Z] =
    other.idNameOrKeySchema match {
      case Left(idName) =>
        copy(toCirceTable = JoinTables.joinOption(toCirceTable, other.toCirceTable, idName, other.topicName)(f)(g))
      case Right(schema) =>
        copy(toCirceTable =
          JoinTables.joinOptionComposite(toCirceTable, other.toCirceTable, schema, other.topicName)(f)(g)
        )
    }

  def leftJoin[K2: DebeziumPrimitiveType, V2, Z: Encoder: Decoder](
    other: DebeziumTable[K2, V2]
  )(f: V => K2)(g: (V, V2) => Z): DebeziumTable[K, Z] =
    other.idNameOrKeySchema match {
      case Left(idName) =>
        copy(toCirceTable = JoinTables.leftJoin(toCirceTable, other.toCirceTable, idName, other.topicName)(f)(g))
      case Right(schema) =>
        copy(toCirceTable =
          JoinTables.leftJoinComposite(toCirceTable, other.toCirceTable, schema, other.topicName)(f)(g)
        )
    }

  def leftJoinOption[K2: DebeziumPrimitiveType, V2, Z: Encoder: Decoder](
    other: DebeziumTable[K2, V2]
  )(f: V => Option[K2])(g: (V, V2) => Z): DebeziumTable[K, Z] =
    other.idNameOrKeySchema match {
      case Left(idName) =>
        copy(toCirceTable = JoinTables.leftJoinOption(toCirceTable, other.toCirceTable, idName, other.topicName)(f)(g))
      case Right(schema) =>
        copy(toCirceTable =
          JoinTables.leftJoinOptionComposite(toCirceTable, other.toCirceTable, schema, other.topicName)(f)(g)
        )
    }
}

object DebeziumTable {
  def withCirceDebezium[K: Encoder: Decoder, V: Encoder: Decoder](
    sb: StreamsBuilder,
    topicName: String,
    idName: String
  ): DebeziumTable[K, DebeziumValue[V]] =
    DebeziumTable(
      topicName,
      Left(idName),
      CirceTable.withLogCompaction[DebeziumKey[K], DebeziumValue[V]](sb, topicName)
    )

  def withCompositeKey[K: Encoder: Decoder, V: Encoder: Decoder](
    sb: StreamsBuilder,
    topicName: String,
    schema: DebeziumCompositeType[K]
  ): DebeziumTable[K, DebeziumValue[V]] =
    DebeziumTable(
      topicName,
      Right(schema),
      CirceTable.withLogCompaction[DebeziumKey[K], DebeziumValue[V]](sb, topicName)
    )
}
