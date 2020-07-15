package compstak.kafkastreams4s.debezium

import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.{Consumed, KTable, Produced}
import org.apache.kafka.common.serialization.Serde
import compstak.kafkastreams4s.circe.CirceSerdes
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
        Produced
          .`with`(CirceSerdes.serdeForCirce[DebeziumKey[K]], CirceSerdes.serdeForCirce[V])
      )
    )

  def map[V2: Encoder: Decoder](f: V => V2): DebeziumTable[K, V2] =
    copy(toKTable = toKTable.mapValues(v => f(v)))

  def mapWithKey[V2: Encoder: Decoder](f: (DebeziumKey[K], V) => V2): DebeziumTable[K, V2] =
    copy(toKTable = toKTable.mapValues((k, v) => f(k, v)))

  def mapFilter[V2: Encoder: Decoder](f: V => Option[V2]): DebeziumTable[K, V2] = {
    val mapped: KTable[DebeziumKey[K], Option[V2]] = toKTable.mapValues(v => f(v))
    copy(toKTable = mapped.filter((k, o) => o.isDefined).mapValues(_.get))
  }

  def mapFilterWithKey[V2: Encoder: Decoder](f: (DebeziumKey[K], V) => Option[V2]): DebeziumTable[K, V2] = {
    val mapped: KTable[DebeziumKey[K], Option[V2]] = toKTable.mapValues((k, v) => f(k, v))
    copy(toKTable = mapped.filter((k, o) => o.isDefined).mapValues(_.get))
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
  ): DebeziumTable[K, DebeziumValue[V]] = {
    val keySerde = CirceSerdes.serdeForCirce[DebeziumKey[K]]
    val valueSerde = CirceSerdes.serdeForCirce[DebeziumValue[V]]
    DebeziumTable(topicName, Left(idName), sb.table(topicName, Consumed.`with`(keySerde, valueSerde)))
  }

  def withCompositeKey[K: Encoder: Decoder, V: Encoder: Decoder](
    sb: StreamsBuilder,
    topicName: String,
    schema: DebeziumCompositeType[K]
  ): DebeziumTable[K, DebeziumValue[V]] = {
    val keySerde = CirceSerdes.serdeForCirce[DebeziumKey[K]]
    val valueSerde = CirceSerdes.serdeForCirce[DebeziumValue[V]]
    DebeziumTable(topicName, Right(schema), sb.table(topicName, Consumed.`with`(keySerde, valueSerde)))
  }
}
