package compstak.kafkastreams4s

import cats.effect.Sync
import org.apache.kafka.streams.{KeyValue, StreamsBuilder}
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.kstream.{KTable, ValueMapper, ValueMapperWithKey}
import SerdeHelpers._
import org.apache.kafka.streams.kstream.KeyValueMapper
import org.apache.kafka.streams.kstream.Initializer
import org.apache.kafka.streams.kstream.Aggregator

class STable[C[x] <: Codec[x]: CodecOption, K: C, V: C](val toKTable: KTable[K, V]) {
  import STable.fromKTable

  def to[F[_]: Sync](outputTopicName: String): F[Unit] =
    Sync[F].delay(
      toKTable.toStream.to(
        outputTopicName,
        producedForCodec[C, K, V]
      )
    )

  def keyBy[K2: C](f: V => K2): STable[C, K2, V] =
    fromKTable(
      toKTable
        .groupBy(
          ((k: K, v: V) => KeyValue.pair(f(v), v)): KeyValueMapper[K, V, KeyValue[K2, V]],
          groupedForCodec[C, K2, V]
        )
        .aggregate[Option[V]](
          (() => Option.empty[V]): Initializer[Option[V]],
          ((k: K2, v: V, agg: Option[V]) => Option(v)): Aggregator[K2, V, Option[V]],
          ((k: K2, v: V, agg: Option[V]) => agg): Aggregator[K2, V, Option[V]],
          materializedForCodec[C, K2, Option[V]]
        )
    ).flattenOption

  def reKey[K2: C](f: K => K2): STable[C, K2, V] =
    fromKTable(
      toKTable
        .groupBy(
          ((k: K, v: V) => KeyValue.pair(f(k), v)): KeyValueMapper[K, V, KeyValue[K2, V]],
          groupedForCodec[C, K2, V]
        )
        .aggregate[Option[V]](
          (() => Option.empty[V]): Initializer[Option[V]],
          ((k: K2, v: V, agg: Option[V]) => Option(v)): Aggregator[K2, V, Option[V]],
          ((k: K2, v: V, agg: Option[V]) => agg): Aggregator[K2, V, Option[V]],
          materializedForCodec[C, K2, Option[V]]
        )
    ).flattenOption

  def transform[K2: C, V2: C](f: (K, V) => (K2, V2)): STable[C, K2, V2] =
    fromKTable(
      toKTable
        .groupBy({ (k: K, v: V) =>
          val (k2, v2) = f(k, v)
          KeyValue.pair(k2, v2)
        }: KeyValueMapper[K, V, KeyValue[K2, V2]], groupedForCodec[C, K2, V2])
        .aggregate[Option[V2]](
          (() => Option.empty[V2]): Initializer[Option[V2]],
          ((k: K2, v: V2, agg: Option[V2]) => Option(v)): Aggregator[K2, V2, Option[V2]],
          ((k: K2, v: V2, agg: Option[V2]) => agg): Aggregator[K2, V2, Option[V2]],
          materializedForCodec[C, K2, Option[V2]]
        )
    ).flattenOption

  def join[K2, V2, Z: C](
    other: STable[C, K2, V2]
  )(f: V => K2)(g: (V, V2) => Z): STable[C, K, Z] =
    fromKTable(toKTable.join(other.toKTable, (v: V) => f(v), (v: V, v2: V2) => g(v, v2), materializedForCodec[C, K, Z]))

  def joinOption[K2, V2, Z: C](
    other: STable[C, K2, V2]
  )(f: V => K2)(g: (V, V2) => Option[Z])(implicit ev: Null <:< Z): STable[C, K, Z] =
    fromKTable(
      toKTable.join(other.toKTable, (v: V) => f(v), (v: V, v2: V2) => g(v, v2).orNull, materializedForCodec[C, K, Z])
    )

  def leftJoin[K2, V2, Z: C](
    other: STable[C, K2, V2]
  )(f: V => K2)(g: (V, V2) => Z): STable[C, K, Z] =
    fromKTable(
      toKTable.leftJoin(other.toKTable, (v: V) => f(v), (v: V, v2: V2) => g(v, v2), materializedForCodec[C, K, Z])
    )

  def leftJoinOption[K2, V2, Z: C](
    other: STable[C, K2, V2]
  )(f: V => K2)(g: (V, V2) => Option[Z])(implicit ev: Null <:< Z): STable[C, K, Z] =
    fromKTable(
      toKTable
        .leftJoin(other.toKTable, (v: V) => f(v), (v: V, v2: V2) => g(v, v2).orNull, materializedForCodec[C, K, Z])
    )

  def scan[V2: C](z: => V2)(f: (V2, V) => V2): STable[C, K, V2] =
    fromKTable(
      toKTable
        .groupBy(((k: K, v: V) => KeyValue.pair(k, v)): KeyValueMapper[K, V, KeyValue[K, V]], groupedForCodec[C, K, V])
        .aggregate[V2](
          (() => z): Initializer[V2],
          ((k: K, v: V, agg: V2) => f(agg, v)): Aggregator[K, V, V2],
          ((k: K, v: V, agg: V2) => agg): Aggregator[K, V, V2],
          materializedForCodec[C, K, V2]
        )
    )

  def scan1(f: (V, V) => V): STable[C, K, V] =
    fromKTable(
      toKTable
        .groupBy(((k: K, v: V) => KeyValue.pair(k, v)): KeyValueMapper[K, V, KeyValue[K, V]], groupedForCodec[C, K, V])
        .aggregate[Option[V]](
          (() => None): Initializer[Option[V]],
          ((k: K, v: V, agg: Option[V]) => Some(agg.fold(v)(acc => f(acc, v)))): Aggregator[K, V, Option[V]],
          ((k: K, v: V, agg: Option[V]) => agg): Aggregator[K, V, Option[V]],
          materializedForCodec[C, K, Option[V]]
        )
    ).flattenOption

  def map[V2: C](f: V => V2): STable[C, K, V2] =
    fromKTable(toKTable.mapValues(((v: V) => f(v)): ValueMapper[V, V2], materializedForCodec[C, K, V2]))

  def mapWithKey[V2: C](f: (K, V) => V2): STable[C, K, V2] =
    fromKTable(
      toKTable.mapValues(
        ((k: K, v: V) => f(k, v)): ValueMapperWithKey[K, V, V2],
        materializedForCodec[C, K, V2]
      )
    )

  def mapFilter[V2: C](f: V => Option[V2]): STable[C, K, V2] = {
    val mapped: KTable[K, Option[V2]] =
      toKTable.mapValues(((v: V) => f(v)): ValueMapper[V, Option[V2]], materializedForCodec[C, K, Option[V2]])
    fromKTable(
      mapped
        .filter((k, o) => o.isDefined)
        .mapValues(((ov: Option[V2]) => ov.get): ValueMapper[Option[V2], V2], materializedForCodec[C, K, V2])
    )
  }

  def mapFilterWithKey[V2: C](f: (K, V) => Option[V2]): STable[C, K, V2] = {
    val mapped: KTable[K, Option[V2]] =
      toKTable.mapValues(
        ((k: K, v: V) => f(k, v)): ValueMapperWithKey[K, V, Option[V2]],
        materializedForCodec[C, K, Option[V2]]
      )
    fromKTable(
      mapped
        .filter((k, o) => o.isDefined)
        .mapValues(((ov: Option[V2]) => ov.get): ValueMapper[Option[V2], V2], materializedForCodec[C, K, V2])
    )
  }

  def flattenOption[V2: C](implicit ev: V =:= Option[V2]): STable[C, K, V2] =
    fromKTable(
      toKTable
        .filter((k, v) => ev(v).isDefined)
        .mapValues(((v: V) => ev(v).get): ValueMapper[V, V2], materializedForCodec[C, K, V2])
    )

  def collect[V2: C](f: PartialFunction[V, V2]): STable[C, K, V2] =
    mapFilter(f.lift)
}

object STable {

  def fromKTable[C[x] <: Codec[x]: CodecOption, K: C, V: C](ktable: KTable[K, V]): STable[C, K, V] =
    new STable[C, K, V](ktable)

  def apply[C[x] <: Codec[x]: CodecOption, K: C, V: C](sb: StreamsBuilder, topicName: String): STable[C, K, V] =
    fromKTable(sb.table(topicName, consumedForCodec[C, K, V]))

  /**
   * Like `STable.apply`, but filters out `null` values.
   */
  def withLogCompaction[C[x] <: Codec[x]: CodecOption, K: C, V: C](
    sb: StreamsBuilder,
    topicName: String
  ): STable[C, K, V] =
    STable[C, K, Option[V]](sb, topicName).flattenOption
}
