package compstak.kafkastreams4s.debezium

import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.{Consumed, KTable}
import org.apache.kafka.common.serialization.Serde
import compstak.kafkastreams4s.circe.CirceSerdes
import compstak.circe.debezium.{DebeziumKey, DebeziumValue}
import io.circe.{Decoder, Encoder}
import cats.FunctorFilter
import cats.Functor
import org.apache.kafka.streams.kstream.ValueMapper
import org.apache.kafka.streams.kstream.ValueMapperWithKey

case class DebeziumTable[K, V](
  topicName: String,
  idName: String,
  toKTable: KTable[DebeziumKey[K], V]
) {

  def map[V2](f: V => V2): DebeziumTable[K, V2] =
    copy(toKTable = toKTable.mapValues(v => f(v)))

  def mapWithKey[V2](f: (DebeziumKey[K], V) => V2): DebeziumTable[K, V2] =
    copy(toKTable = toKTable.mapValues((k, v) => f(k, v)))

  def mapFilter[V2](f: V => Option[V2]): DebeziumTable[K, V2] = {
    val mapped: KTable[DebeziumKey[K], Option[V2]] = toKTable.mapValues(v => f(v))
    copy(toKTable = mapped.filter((k, o) => o.isDefined).mapValues(_.get))
  }

  def mapFilterWithKey[V2](f: (DebeziumKey[K], V) => Option[V2]): DebeziumTable[K, V2] = {
    val mapped: KTable[DebeziumKey[K], Option[V2]] = toKTable.mapValues((k, v) => f(k, v))
    copy(toKTable = mapped.filter((k, o) => o.isDefined).mapValues(_.get))
  }

  def join[K2: DebeziumType, V2, Z](other: DebeziumTable[K2, V2])(f: V => K2)(g: (V, V2) => Z): DebeziumTable[K, Z] =
    copy(toKTable = JoinTables.join(toKTable, other.toKTable, other.idName, other.topicName)(f)(g))

  def joinOption[K2: DebeziumType, V2, Z](
    other: DebeziumTable[K2, V2]
  )(f: V => Option[K2])(g: (V, V2) => Z): DebeziumTable[K, Z] =
    copy(toKTable = JoinTables.joinOption(toKTable, other.toKTable, other.idName, other.topicName)(f)(g))

  def leftJoin[K2: DebeziumType, V2, Z](
    other: DebeziumTable[K2, V2]
  )(f: V => K2)(g: (V, V2) => Z): DebeziumTable[K, Z] =
    copy(toKTable = JoinTables.leftJoin(toKTable, other.toKTable, other.idName, other.topicName)(f)(g))

  def leftJoinOption[K2: DebeziumType, V2, Z](
    other: DebeziumTable[K2, V2]
  )(f: V => Option[K2])(g: (V, V2) => Z): DebeziumTable[K, Z] =
    copy(toKTable = JoinTables.leftJoinOption(toKTable, other.toKTable, other.idName, other.topicName)(f)(g))
}

object DebeziumTable {
  def withCirceDebezium[K: Encoder: Decoder, V: Encoder: Decoder](
    sb: StreamsBuilder,
    topicName: String,
    idName: String
  ): DebeziumTable[K, DebeziumValue[V]] = {
    val keySerde = CirceSerdes.serdeForCirce[DebeziumKey[K]]
    val valueSerde = CirceSerdes.serdeForCirce[DebeziumValue[V]]
    DebeziumTable(topicName, idName, sb.table(topicName, Consumed.`with`(keySerde, valueSerde)))
  }

  implicit def debeziumTableFunctor[K]: Functor[DebeziumTable[K, *]] = new Functor[DebeziumTable[K, *]] {
    def map[A, B](fa: DebeziumTable[K, A])(f: A => B): DebeziumTable[K, B] = fa.map(f)
  }

  implicit def debeziumTableFunctorFilter[K]: FunctorFilter[DebeziumTable[K, *]] =
    new FunctorFilter[DebeziumTable[K, *]] {
      val functor = debeziumTableFunctor
      def mapFilter[A, B](fa: DebeziumTable[K, A])(f: A => Option[B]): DebeziumTable[K, B] = fa.mapFilter(f)
    }

}
