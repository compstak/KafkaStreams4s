package compstak.kafkastreams4s.debezium

import org.apache.kafka.streams.{KeyValue, StreamsBuilder}
import org.apache.kafka.streams.kstream.{Consumed, Initializer, KTable, Produced, ValueMapper, ValueMapperWithKey}
import org.apache.kafka.common.serialization.Serde
import compstak.kafkastreams4s.circe.CirceSerdes._
import compstak.circe.debezium.{DebeziumKey, DebeziumValue}
import io.circe.{Decoder, Encoder}
import cats.{Functor, FunctorFilter}
import cats.effect.Sync

case class DebeziumTable[K: Encoder: Decoder, V: Encoder: Decoder](
  topicName: String,
  idNameOrKeySchema: Either[String, DebeziumCompositeType[K]],
  toKTable: KTable[DebeziumKey[K], V]
) {

  def to[F[_]: Sync](outputTopicName: String): F[Unit] =
    Sync[F].delay(
      toKTable.toStream.to(
        outputTopicName,
        producedForCirce[DebeziumKey[K], V]
      )
    )

  def map[V2: Encoder: Decoder](f: V => V2): DebeziumTable[K, V2] =
    copy(toKTable = toKTable.mapValues(((v: V) => f(v)): ValueMapper[V, V2], materializedForCirce[DebeziumKey[K], V2]))

  def mapWithKey[V2: Encoder: Decoder](f: (DebeziumKey[K], V) => V2): DebeziumTable[K, V2] =
    copy(toKTable = toKTable.mapValues(
      ((k: DebeziumKey[K], v: V) => f(k, v)): ValueMapperWithKey[DebeziumKey[K], V, V2],
      materializedForCirce[DebeziumKey[K], V2]
    )
    )

  def mapFilter[V2: Encoder: Decoder](f: V => Option[V2]): DebeziumTable[K, V2] = {
    val mapped: KTable[DebeziumKey[K], Option[V2]] =
      toKTable.mapValues(((v: V) => f(v)): ValueMapper[V, Option[V2]], materializedForCirce[DebeziumKey[K], Option[V2]])
    copy(toKTable = mapped
      .filter((k, o) => o.isDefined)
      .mapValues(((ov: Option[V2]) => ov.get): ValueMapper[Option[V2], V2], materializedForCirce[DebeziumKey[K], V2])
    )
  }

  def mapFilterWithKey[V2: Encoder: Decoder](f: (DebeziumKey[K], V) => Option[V2]): DebeziumTable[K, V2] = {
    val mapped: KTable[DebeziumKey[K], Option[V2]] =
      toKTable.mapValues(
        ((k: DebeziumKey[K], v: V) => f(k, v)): ValueMapperWithKey[DebeziumKey[K], V, Option[V2]],
        materializedForCirce[DebeziumKey[K], Option[V2]]
      )
    copy(toKTable = mapped
      .filter((k, o) => o.isDefined)
      .mapValues(((ov: Option[V2]) => ov.get): ValueMapper[Option[V2], V2], materializedForCirce[DebeziumKey[K], V2])
    )
  }

  def join[K2: DebeziumPrimitiveType, V2, Z: Encoder: Decoder](
    other: DebeziumTable[K2, V2]
  )(f: V => K2)(g: (V, V2) => Z): DebeziumTable[K, Z] =
    other.idNameOrKeySchema match {
      case Left(idName) => copy(toKTable = JoinTables.join(toKTable, other.toKTable, idName, other.topicName)(f)(g))
      case Right(schema) =>
        copy(toKTable = JoinTables.joinComposite(toKTable, other.toKTable, schema, other.topicName)(f)(g))
    }

  def joinOption[K2: DebeziumPrimitiveType, V2, Z: Encoder: Decoder](
    other: DebeziumTable[K2, V2]
  )(f: V => Option[K2])(g: (V, V2) => Z): DebeziumTable[K, Z] =
    other.idNameOrKeySchema match {
      case Left(idName) =>
        copy(toKTable = JoinTables.joinOption(toKTable, other.toKTable, idName, other.topicName)(f)(g))
      case Right(schema) =>
        copy(toKTable = JoinTables.joinOptionComposite(toKTable, other.toKTable, schema, other.topicName)(f)(g))
    }

  def leftJoin[K2: DebeziumPrimitiveType, V2, Z: Encoder: Decoder](
    other: DebeziumTable[K2, V2]
  )(f: V => K2)(g: (V, V2) => Z): DebeziumTable[K, Z] =
    other.idNameOrKeySchema match {
      case Left(idName) => copy(toKTable = JoinTables.leftJoin(toKTable, other.toKTable, idName, other.topicName)(f)(g))
      case Right(schema) =>
        copy(toKTable = JoinTables.leftJoinComposite(toKTable, other.toKTable, schema, other.topicName)(f)(g))
    }

  def leftJoinOption[K2: DebeziumPrimitiveType, V2, Z: Encoder: Decoder](
    other: DebeziumTable[K2, V2]
  )(f: V => Option[K2])(g: (V, V2) => Z): DebeziumTable[K, Z] =
    other.idNameOrKeySchema match {
      case Left(idName) =>
        copy(toKTable = JoinTables.leftJoinOption(toKTable, other.toKTable, idName, other.topicName)(f)(g))
      case Right(schema) =>
        copy(toKTable = JoinTables.leftJoinOptionComposite(toKTable, other.toKTable, schema, other.topicName)(f)(g))
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
      sb.table(topicName, consumedForCirce[DebeziumKey[K], DebeziumValue[V]])
    )

  def withCompositeKey[K: Encoder: Decoder, V: Encoder: Decoder](
    sb: StreamsBuilder,
    topicName: String,
    schema: DebeziumCompositeType[K]
  ): DebeziumTable[K, DebeziumValue[V]] =
    DebeziumTable(
      topicName,
      Right(schema),
      sb.table(topicName, consumedForCirce[DebeziumKey[K], DebeziumValue[V]])
    )
}
