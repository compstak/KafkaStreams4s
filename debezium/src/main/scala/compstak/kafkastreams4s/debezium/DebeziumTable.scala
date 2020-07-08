package compstak.kafkastreams4s.debezium

import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.{Consumed, KTable}
import org.apache.kafka.common.serialization.Serde
import compstak.kafkastreams4s.circe.CirceSerdes
import compstak.circe.debezium.{DebeziumKey, DebeziumValue}
import io.circe.{Decoder, Encoder}

case class DebeziumTable[K, V](
  topicName: String,
  idName: String,
  toKTable: KTable[DebeziumKey[K], V]
) {

  def join[K2: DebeziumType, V2, Z](other: DebeziumTable[K2, V2])(f: V => K2)(g: (V, V2) => Z): DebeziumTable[K, Z] =
    copy(toKTable = JoinTables.join(toKTable, other.toKTable, other.topicName, other.idName)(f)(g))

  def joinOption[K2: DebeziumType, V2, Z](
    other: DebeziumTable[K2, V2]
  )(f: V => Option[K2])(g: (V, V2) => Z): DebeziumTable[K, Z] =
    copy(toKTable = JoinTables.joinOption(toKTable, other.toKTable, other.topicName, other.idName)(f)(g))

  def leftJoin[K2: DebeziumType, V2, Z](
    other: DebeziumTable[K2, V2]
  )(f: V => K2)(g: (V, V2) => Z): DebeziumTable[K, Z] =
    copy(toKTable = JoinTables.leftJoin(toKTable, other.toKTable, other.topicName, other.idName)(f)(g))

  def leftJoinOption[K2: DebeziumType, V2, Z](
    other: DebeziumTable[K2, V2]
  )(f: V => Option[K2])(g: (V, V2) => Z): DebeziumTable[K, Z] =
    copy(toKTable = JoinTables.leftJoinOption(toKTable, other.toKTable, other.topicName, other.idName)(f)(g))
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

}
